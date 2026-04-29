use crate::accounting::AccountingDb;
use crate::config::Config;
use crate::nng::adapter::{BitcoindNngAdapter, NodeEvent, NodeMiningAdapter};
use crate::stratum::engine::{apply_notify, handle_request, SessionState};
use crate::stratum::job::MiningJob;
use crate::stratum::protocol::{decode_request_line, Method, StratumResponse};
use crate::stratum::validation::{
    build_candidate_block, prevalidate_submit_shape, validate_submit_meets_difficulty, NativeSubmit,
};
use crate::stratum::vardiff::VarDiff;
use anyhow::{anyhow, Result};
use bitcoinsuite_bitcoind_nng::{MiningSubmitResult, MiningTemplate};
use bitcoinsuite_core::Hashed;
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

#[derive(Default)]
pub struct RuntimeStats {
    pub idle_disconnects: AtomicU64,
    pub rate_limit_disconnects: AtomicU64,
}

impl RuntimeStats {
    pub fn snapshot(&self) -> RuntimeStatsSnapshot {
        RuntimeStatsSnapshot {
            idle_disconnects: self.idle_disconnects.load(Ordering::Relaxed),
            rate_limit_disconnects: self.rate_limit_disconnects.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct RuntimeStatsSnapshot {
    pub idle_disconnects: u64,
    pub rate_limit_disconnects: u64,
}

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

pub async fn run_stratum_server(
    cfg: Config,
    db: AccountingDb,
    stats: Arc<RuntimeStats>,
) -> Result<()> {
    let listener = TcpListener::bind(&cfg.stratum_bind).await?;
    info!(bind = %cfg.stratum_bind, "stratum server listening");

    let adapter: Arc<dyn NodeMiningAdapter> =
        Arc::new(BitcoindNngAdapter::connect(&cfg.nng_rpc_url)?);
    let runtime = StratumRuntime::new(cfg.max_jobs_cache);
    refresh_job_from_node(&runtime, adapter.clone(), true, "startup").await?;

    let runtime_bg = runtime.clone();
    let adapter_bg = adapter.clone();
    let refresh_secs = cfg.job_refresh_secs;
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(refresh_secs)).await;
            if let Err(err) =
                refresh_job_from_node(&runtime_bg, adapter_bg.clone(), true, "periodic").await
            {
                warn!(error = %err, "periodic template refresh failed");
            }
        }
    });

    let runtime_nng = runtime.clone();
    let nng_pub_url = cfg.nng_pub_url.clone();
    let rpc_adapter = BitcoindNngAdapter::connect(&cfg.nng_rpc_url)?;
    let adapter_events = adapter.clone();
    tokio::spawn(async move {
        let (tx, mut rx) = mpsc::unbounded_channel::<NodeEvent>();
        let runtime_events = runtime_nng.clone();
        let adapter_events_inner = adapter_events.clone();
        tokio::spawn(async move {
            while let Some(event) = rx.recv().await {
                let clean = matches!(
                    event,
                    NodeEvent::UpdateBlkTip | NodeEvent::MiningWorkChanged
                );
                let reason = match event {
                    NodeEvent::UpdateBlkTip => "updateblktip",
                    NodeEvent::MempoolRefresh => "mempool",
                    NodeEvent::MiningWorkChanged => "miningwrkchg",
                };
                if let Err(err) = refresh_job_from_node(
                    &runtime_events,
                    adapter_events_inner.clone(),
                    clean,
                    reason,
                )
                .await
                {
                    warn!(error = %err, reason, "template refresh failed after event");
                }
            }
        });

        if let Err(err) = rpc_adapter
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
        let adapter = adapter.clone();
        let stats = stats.clone();
        tokio::spawn(async move {
            if let Err(err) = handle_conn(socket, db, runtime, cfg, adapter, stats).await {
                warn!(error = %err, peer = %peer_addr, "stratum connection closed with error");
            } else {
                info!(peer = %peer_addr, "stratum connection closed");
            }
        });
    }
}

async fn refresh_job_from_node(
    runtime: &StratumRuntime,
    adapter: Arc<dyn NodeMiningAdapter>,
    clean_jobs: bool,
    reason: &str,
) -> Result<()> {
    let template = adapter.get_mining_template(None).await?;
    let epoch = runtime.next_template_epoch();
    let job = make_job_from_template(template, epoch, clean_jobs)?;
    info!(
        template_epoch = epoch,
        template_id = job.template_id,
        reason,
        clean_jobs,
        "refreshed mining template from lotusd"
    );
    runtime.publish_job(job);
    Ok(())
}

fn make_job_from_template(
    template: MiningTemplate,
    template_epoch: u64,
    clean_jobs: bool,
) -> Result<MiningJob> {
    let version = format!("{:08x}", template.version);
    Ok(MiningJob {
        job_id: format!("job-{}-{}", template.template_id, template_epoch),
        template_id: template.template_id,
        prevhash: template.prev_hash_stratum,
        coinbase1: template.coinbase1,
        coinbase2: template.coinbase2,
        merkle_branches: template.merkle_branches,
        version,
        nbits: template.nbits_stratum,
        ntime: template.ntime_stratum,
        clean_jobs,
        template_epoch,
        template_header: template.header,
        template_block: template.block,
    })
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
    adapter: Arc<dyn NodeMiningAdapter>,
    stats: Arc<RuntimeStats>,
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
                stats.idle_disconnects.fetch_add(1, Ordering::Relaxed);
                let snap = stats.snapshot();
                info!(session_id = %session_id, idle_disconnects = snap.idle_disconnects, rate_limit_disconnects = snap.rate_limit_disconnects, "disconnecting idle miner session");
                break;
            }
        };
        if n == 0 {
            break;
        }

        req_count += 1;
        if req_count > cfg.per_conn_req_per_sec {
            stats.rate_limit_disconnects.fetch_add(1, Ordering::Relaxed);
            let snap = stats.snapshot();
            warn!(session_id = %session_id, idle_disconnects = snap.idle_disconnects, rate_limit_disconnects = snap.rate_limit_disconnects, req_count, req_limit = cfg.per_conn_req_per_sec, "rate limit exceeded; disconnecting session");
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

        let id_key = req.id.to_string();
        if recent_set.contains(&id_key) {
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
        let req_id = req.id.clone();
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
                    let err = StratumResponse::err(req_id.clone(), 20, "invalid-submit-shape");
                    send_json_line(&mut write_half, &err).await?;
                    continue;
                }

                let worker = crate::stratum::worker::parse_worker_name(worker)?;
                let worker_row =
                    db.upsert_worker(&worker.payout_address, worker.worker_suffix.as_deref())?;
                let job = runtime
                    .find_job(job_id)
                    .ok_or_else(|| anyhow!("missing job for submit"))?;
                let share_difficulty = job_difficulty
                    .get(job_id)
                    .copied()
                    .unwrap_or(vardiff.current);
                if let Err(_) = validate_submit_meets_difficulty(
                    &job,
                    &session.extranonce1,
                    &submit,
                    share_difficulty,
                ) {
                    let err = StratumResponse::err(req_id.clone(), 23, "low-difficulty-share");
                    send_json_line(&mut write_half, &err).await?;
                    continue;
                }

                let candidate_block =
                    match build_candidate_block(&job, &session.extranonce1, &submit) {
                        Ok(v) => v,
                        Err(_) => {
                            let err = StratumResponse::err(req_id.clone(), 20, "invalid-candidate");
                            send_json_line(&mut write_half, &err).await?;
                            continue;
                        }
                    };

                let proposal = match adapter.validate_proposal(candidate_block.clone()).await {
                    Ok(v) => v,
                    Err(_) => {
                        let err = StratumResponse::err(req_id.clone(), 20, "proposal-rpc-failed");
                        send_json_line(&mut write_half, &err).await?;
                        continue;
                    }
                };
                if !proposal.valid {
                    warn!(session_id = %session_id, worker_id = worker_row.id, job_id = %job.job_id, reject_reason = %proposal.reject_reason, "proposal validation rejected share");
                    let err = StratumResponse::err(req_id.clone(), 20, "proposal-invalid");
                    send_json_line(&mut write_half, &err).await?;
                    continue;
                }

                let submit_result = match adapter.submit_mined_block(candidate_block).await {
                    Ok(v) => v,
                    Err(_) => {
                        let err = StratumResponse::err(req_id.clone(), 20, "submit-rpc-failed");
                        send_json_line(&mut write_half, &err).await?;
                        continue;
                    }
                };

                let share_accepted = matches!(
                    submit_result.result,
                    MiningSubmitResult::Accepted
                        | MiningSubmitResult::Duplicate
                        | MiningSubmitResult::DuplicateInvalid
                        | MiningSubmitResult::DuplicateInconclusive
                );

                let dedupe_key = format!(
                    "{}:{}:{}:{}:{}:{}",
                    worker_row.id, job.template_id, job.template_epoch, extranonce2, ntime, nonce
                );
                let _ = db.insert_share_idempotent(
                    worker_row.id,
                    job.template_id,
                    share_difficulty,
                    share_accepted,
                    false,
                    &dedupe_key,
                )?;

                if !share_accepted {
                    warn!(session_id = %session_id, worker_id = worker_row.id, job_id = %job.job_id, result = ?submit_result.result, reject_reason = %submit_result.reject_reason, "share rejected by lotusd submit path");
                    let err = StratumResponse::err(req_id.clone(), 20, "block-submit-rejected");
                    send_json_line(&mut write_half, &err).await?;
                    continue;
                }

                info!(session_id = %session_id, worker_id = worker_row.id, job_id = %job.job_id, template_id = job.template_id, result = ?submit_result.result, accepted = submit_result.accepted, block_hash = %submit_result.block_hash.to_hex_be(), "share accepted via proposal+submit flow");

                let now = chrono::Utc::now().timestamp();
                vardiff.record_share(now);
                let old_diff = vardiff.current;
                if let Some(new_diff) = vardiff.maybe_retarget(now) {
                    info!(session_id = %session_id, old_diff, new_diff, "vardiff retarget");
                    send_set_difficulty(&mut write_half, new_diff).await?;
                    pending_difficulty = Some(new_diff);
                }
            }
            send_json_line(&mut write_half, &resp).await?;
        }
    }

    Ok(())
}
