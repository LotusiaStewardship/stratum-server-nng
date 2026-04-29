use crate::accounting::AccountingDb;
use crate::config::Config;
use crate::nng::adapter::{BitcoindNngAdapter, NodeEvent};
use crate::stratum::engine::{apply_notify, handle_request, SessionState};
use crate::stratum::job::MiningJob;
use crate::stratum::protocol::{decode_request_line, Method, StratumResponse};
use crate::stratum::validation::{
    prevalidate_submit_shape, validate_submit_meets_difficulty, NativeSubmit,
};
use crate::stratum::vardiff::VarDiff;
use anyhow::Result;
use rand::{thread_rng, Rng};
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc, Mutex,
};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{broadcast, mpsc};
use tokio::time::{timeout, Duration};
use tracing::{debug, info, warn};

#[derive(Clone)]
pub struct StratumRuntime {
    jobs: Arc<Mutex<VecDeque<MiningJob>>>,
    tx: broadcast::Sender<MiningJob>,
    max_jobs: usize,
    epoch_counter: Arc<AtomicU64>,
}

impl StratumRuntime {
    pub fn new(max_jobs: usize) -> Self {
        let (tx, _) = broadcast::channel(1024);
        Self {
            jobs: Arc::new(Mutex::new(VecDeque::new())),
            tx,
            max_jobs,
            epoch_counter: Arc::new(AtomicU64::new(0)),
        }
    }

    pub fn publish_job(&self, job: MiningJob) {
        let mut jobs = self.jobs.lock().expect("jobs lock");
        jobs.push_back(job.clone());
        while jobs.len() > self.max_jobs {
            jobs.pop_front();
        }
        debug!(
            job_id = %job.job_id,
            template_id = job.template_id,
            template_epoch = job.template_epoch,
            clean_jobs = job.clean_jobs,
            cached_jobs = jobs.len(),
            "published mining job"
        );
        let _ = self.tx.send(job);
    }

    pub fn subscribe(&self) -> broadcast::Receiver<MiningJob> {
        self.tx.subscribe()
    }

    pub fn next_template_epoch(&self) -> u64 {
        self.epoch_counter.fetch_add(1, Ordering::SeqCst) + 1
    }

    pub fn find_job(&self, job_id: &str) -> Option<MiningJob> {
        let jobs = self.jobs.lock().expect("jobs lock");
        jobs.iter().rev().find(|j| j.job_id == job_id).cloned()
    }

    pub fn latest_job(&self) -> Option<MiningJob> {
        let jobs = self.jobs.lock().expect("jobs lock");
        jobs.back().cloned()
    }
}

pub async fn run_stratum_server(cfg: Config, db: AccountingDb) -> Result<()> {
    let listener = TcpListener::bind(&cfg.stratum_bind).await?;
    info!(bind = %cfg.stratum_bind, "stratum server listening");

    let runtime = StratumRuntime::new(cfg.max_jobs_cache);
    seed_initial_job(&runtime);

    let runtime_bg = runtime.clone();
    let refresh_secs = cfg.job_refresh_secs;
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(refresh_secs)).await;
            let epoch = runtime_bg.next_template_epoch();
            info!(template_epoch = epoch, "periodic job refresh tick");
            runtime_bg.publish_job(make_job(epoch, true));
        }
    });

    let runtime_nng = runtime.clone();
    let nng_rpc_url = cfg.nng_rpc_url.clone();
    let nng_pub_url = cfg.nng_pub_url.clone();
    tokio::spawn(async move {
        let adapter = match BitcoindNngAdapter::connect(&nng_rpc_url) {
            Ok(a) => a,
            Err(err) => {
                warn!(error = %err, nng_rpc = %nng_rpc_url, "failed to connect NNG adapter, continuing with periodic-only job refresh");
                return;
            }
        };

        let (tx, mut rx) = mpsc::unbounded_channel::<NodeEvent>();
        let runtime_events = runtime_nng.clone();
        tokio::spawn(async move {
            while let Some(event) = rx.recv().await {
                let epoch = runtime_events.next_template_epoch();
                match event {
                    NodeEvent::UpdateBlkTip => {
                        info!(
                            template_epoch = epoch,
                            "NNG event: updateblktip; refreshing job"
                        );
                        runtime_events.publish_job(make_job(epoch, true));
                    }
                    NodeEvent::MempoolRefresh => {
                        info!(
                            template_epoch = epoch,
                            "NNG event: mempool refresh; refreshing job"
                        );
                        runtime_events.publish_job(make_job(epoch, false));
                    }
                    NodeEvent::MiningWorkChanged => {
                        info!(
                            template_epoch = epoch,
                            "NNG event: miningwrkchg; refreshing job"
                        );
                        runtime_events.publish_job(make_job(epoch, true));
                    }
                }
            }
        });

        if let Err(err) = adapter
            .run_pub_loop(&nng_pub_url, move |ev| {
                if tx.send(ev).is_err() {
                    warn!("NNG event queue dropped; stratum event consumer not running");
                }
            })
            .await
        {
            warn!(error = %err, nng_pub = %nng_pub_url, "NNG pub loop exited unexpectedly");
        }
    });

    loop {
        let (socket, peer_addr) = listener.accept().await?;
        info!(peer = %peer_addr, "stratum TCP connection accepted");
        let db = db.clone();
        let runtime = runtime.clone();
        let cfg = cfg.clone();
        tokio::spawn(async move {
            if let Err(err) = handle_conn(socket, db, runtime, cfg).await {
                warn!(error = %err, peer = %peer_addr, "stratum connection closed with error");
            } else {
                info!(peer = %peer_addr, "stratum connection closed");
            }
        });
    }
}

fn seed_initial_job(runtime: &StratumRuntime) {
    let epoch = runtime.next_template_epoch();
    info!(template_epoch = epoch, "seeding initial job");
    runtime.publish_job(make_job(epoch, true));
}

fn make_job(template_epoch: u64, clean_jobs: bool) -> MiningJob {
    MiningJob {
        job_id: format!("job-{template_epoch}"),
        template_id: template_epoch,
        prevhash: "00".repeat(32),
        coinbase1: "01000000".to_string(),
        coinbase2: "ffffffff".to_string(),
        merkle_branches: vec![],
        version: "20000000".to_string(),
        nbits: "1d00ffff".to_string(),
        ntime: "000000000000".to_string(),
        clean_jobs,
        template_epoch,
    }
}

async fn send_json_line(
    writer: &mut tokio::net::tcp::OwnedWriteHalf,
    v: &StratumResponse,
) -> Result<()> {
    let mut data = serde_json::to_vec(v)?;
    data.push(b'\n');
    writer.write_all(&data).await?;
    Ok(())
}

async fn send_notify(writer: &mut tokio::net::tcp::OwnedWriteHalf, job: &MiningJob) -> Result<()> {
    let v = serde_json::json!({
        "id": serde_json::Value::Null,
        "method": "mining.notify",
        "params": job.notify_params(),
    });
    let mut data = serde_json::to_vec(&v)?;
    data.push(b'\n');
    writer.write_all(&data).await?;
    debug!(job_id = %job.job_id, template_epoch = job.template_epoch, "sent mining.notify");
    Ok(())
}

async fn send_set_difficulty(
    writer: &mut tokio::net::tcp::OwnedWriteHalf,
    diff: f64,
) -> Result<()> {
    let v = serde_json::json!({
        "id": serde_json::Value::Null,
        "method": "mining.set_difficulty",
        "params": [diff],
    });
    let mut data = serde_json::to_vec(&v)?;
    data.push(b'\n');
    writer.write_all(&data).await?;
    debug!(difficulty = diff, "sent mining.set_difficulty");
    Ok(())
}

async fn handle_conn(
    socket: TcpStream,
    db: AccountingDb,
    runtime: StratumRuntime,
    cfg: Config,
) -> Result<()> {
    let session_id = format!("s{:016x}", thread_rng().r#gen::<u64>());
    let mut session = SessionState::new(session_id.clone());
    let mut vardiff = VarDiff::new(
        cfg.initial_difficulty,
        cfg.min_difficulty,
        cfg.max_difficulty,
        cfg.vardiff_target_secs,
        cfg.vardiff_retarget_secs,
    );

    info!(session_id = %session_id, "stratum session started");

    let (read_half, mut write_half) = socket.into_split();
    let mut reader = BufReader::new(read_half);
    let mut pub_rx = runtime.subscribe();

    let mut pending_difficulty: Option<f64> = None;
    let mut job_difficulty: HashMap<String, f64> = HashMap::new();

    if let Some(job) = runtime.latest_job() {
        apply_notify(&mut session, &job);
        send_set_difficulty(&mut write_half, vardiff.current).await?;
        send_notify(&mut write_half, &job).await?;
        job_difficulty.insert(job.job_id.clone(), vardiff.current);
        info!(session_id = %session_id, job_id = %job.job_id, difficulty = vardiff.current, "seeded session with latest job");
    }

    let mut recent_ids: VecDeque<String> = VecDeque::new();
    let mut recent_set: HashSet<String> = HashSet::new();
    let mut req_count: u32 = 0;
    let mut req_window_start = std::time::Instant::now();

    loop {
        while let Ok(job) = pub_rx.try_recv() {
            if let Some(next_diff) = pending_difficulty.take() {
                vardiff.current = next_diff;
            }
            apply_notify(&mut session, &job);
            send_notify(&mut write_half, &job).await?;
            job_difficulty.insert(job.job_id.clone(), vardiff.current);
            if job.clean_jobs {
                job_difficulty.retain(|job_id, _| session.active_jobs.contains(job_id));
            }
            info!(
                session_id = %session_id,
                job_id = %job.job_id,
                template_epoch = job.template_epoch,
                clean_jobs = job.clean_jobs,
                difficulty = vardiff.current,
                "forwarded new mining job to miner"
            );
        }

        if req_window_start.elapsed().as_secs() >= 1 {
            req_window_start = std::time::Instant::now();
            req_count = 0;
        }

        let mut line = String::new();
        let n = match timeout(
            Duration::from_secs(cfg.conn_idle_timeout_secs),
            reader.read_line(&mut line),
        )
        .await
        {
            Ok(v) => v?,
            Err(_) => {
                info!(session_id = %session_id, "disconnecting idle miner session");
                break;
            }
        };
        if n == 0 {
            debug!(session_id = %session_id, "peer closed connection");
            break;
        }

        req_count += 1;
        if req_count > cfg.per_conn_req_per_sec {
            warn!(
                session_id = %session_id,
                req_count,
                req_limit = cfg.per_conn_req_per_sec,
                "rate limit exceeded; disconnecting session"
            );
            break;
        }

        let req = match decode_request_line(line.trim_end(), cfg.max_request_line_bytes) {
            Ok(r) => r,
            Err(err) => {
                warn!(session_id = %session_id, error = %err, "invalid stratum request line");
                let err = StratumResponse::err(serde_json::Value::Null, 20, "invalid-request");
                send_json_line(&mut write_half, &err).await?;
                continue;
            }
        };

        debug!(session_id = %session_id, method = ?req.method, id = %req.id, "received stratum request");

        let id_key = req.id.to_string();
        if recent_set.contains(&id_key) {
            warn!(session_id = %session_id, id = %id_key, "duplicate request id rejected");
            let err = StratumResponse::err(req.id.clone(), 22, "duplicate-request-id");
            send_json_line(&mut write_half, &err).await?;
            continue;
        }
        recent_set.insert(id_key.clone());
        recent_ids.push_back(id_key);
        while recent_ids.len() > 2048 {
            if let Some(old) = recent_ids.pop_front() {
                recent_set.remove(&old);
            }
        }

        let method = req.method.clone();
        let params = req.params.clone();
        if let Some(resp) = handle_request(&mut session, req) {
            if matches!(method, Method::Submit) && resp.error.is_null() {
                let worker = params
                    .as_array()
                    .and_then(|v| v.first())
                    .and_then(|v| v.as_str())
                    .unwrap_or_default();
                let job_id = params
                    .as_array()
                    .and_then(|v| v.get(1))
                    .and_then(|v| v.as_str())
                    .unwrap_or_default();
                let extranonce2 = params
                    .as_array()
                    .and_then(|v| v.get(2))
                    .and_then(|v| v.as_str())
                    .unwrap_or_default();
                let ntime = params
                    .as_array()
                    .and_then(|v| v.get(3))
                    .and_then(|v| v.as_str())
                    .unwrap_or_default();
                let nonce = params
                    .as_array()
                    .and_then(|v| v.get(4))
                    .and_then(|v| v.as_str())
                    .unwrap_or_default();

                let submit = NativeSubmit {
                    worker_name: worker.to_string(),
                    job_id: job_id.to_string(),
                    extranonce2: extranonce2.to_string(),
                    ntime_hex_6b: ntime.to_string(),
                    nonce_hex_8b: nonce.to_string(),
                };
                if prevalidate_submit_shape(&submit).is_err() {
                    warn!(session_id = %session_id, worker, job_id, "share prevalidation failed");
                    let err =
                        StratumResponse::err(serde_json::Value::Null, 20, "invalid-submit-shape");
                    send_json_line(&mut write_half, &err).await?;
                    continue;
                }

                let worker = crate::stratum::worker::parse_worker_name(worker)?;
                let worker_row =
                    db.upsert_worker(&worker.payout_address, worker.worker_suffix.as_deref())?;
                let job = runtime
                    .find_job(job_id)
                    .unwrap_or_else(|| make_job(0, false));
                let share_difficulty = job_difficulty
                    .get(job_id)
                    .copied()
                    .unwrap_or(vardiff.current);
                if let Err(err) = validate_submit_meets_difficulty(
                    &job,
                    &session.extranonce1,
                    &submit,
                    share_difficulty,
                ) {
                    warn!(
                        session_id = %session_id,
                        worker_id = worker_row.id,
                        job_id = %job.job_id,
                        difficulty = share_difficulty,
                        error = %err,
                        "low difficulty share rejected"
                    );
                    let err =
                        StratumResponse::err(serde_json::Value::Null, 23, "low-difficulty-share");
                    send_json_line(&mut write_half, &err).await?;
                    continue;
                }

                let dedupe_key = format!(
                    "{}:{}:{}:{}:{}:{}",
                    worker_row.id, job.template_id, job.template_epoch, extranonce2, ntime, nonce
                );
                let inserted = db.insert_share_idempotent(
                    worker_row.id,
                    job.template_id,
                    share_difficulty,
                    true,
                    false,
                    &dedupe_key,
                )?;

                if inserted {
                    info!(
                        session_id = %session_id,
                        worker_id = worker_row.id,
                        payout_address = %worker_row.payout_address,
                        job_id = %job.job_id,
                        template_id = job.template_id,
                        difficulty = share_difficulty,
                        "accepted share persisted"
                    );
                } else {
                    warn!(
                        session_id = %session_id,
                        worker_id = worker_row.id,
                        job_id = %job.job_id,
                        "duplicate share detected"
                    );
                }

                let now = chrono::Utc::now().timestamp();
                vardiff.record_share(now);
                let old_diff = vardiff.current;
                if let Some(new_diff) = vardiff.maybe_retarget(now) {
                    info!(
                        session_id = %session_id,
                        old_diff,
                        new_diff,
                        "vardiff retarget"
                    );
                    send_set_difficulty(&mut write_half, new_diff).await?;
                    pending_difficulty = Some(new_diff);
                }
            }
            send_json_line(&mut write_half, &resp).await?;
        }
    }

    info!(session_id = %session_id, "stratum session ended");
    Ok(())
}
