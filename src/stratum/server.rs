use crate::accounting::{AccountingDb, ShareOutcomeInsert};
use crate::config::{Config, ResolvedPoolScripts};
use crate::nng::adapter::{BitcoindNngAdapter, NodeEvent, NodeMiningAdapter};
use crate::stratum::engine::{apply_notify, handle_request, SessionState};
use crate::stratum::job::MiningJob;
use crate::stratum::protocol::{decode_request_line, Method, StratumResponse};
use crate::stratum::validation::{
    build_candidate_block, prevalidate_submit_shape, validate_header_meets_target_hex,
    validate_submit_meets_difficulty, NativeSubmit,
};
use crate::stratum::vardiff::VarDiff;
use anyhow::{anyhow, Result};
use bitcoinsuite_bitcoind_nng::{MiningSubmitResult, MiningTemplate};
use bitcoinsuite_core::{BitcoinCode, Bytes, Hashed, LotusBlock};
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
use tracing::{debug, error, info, warn};

#[derive(Default)]
pub struct RuntimeStats {
    pub idle_disconnects: AtomicU64,
    pub rate_limit_disconnects: AtomicU64,
    pub template_payout_mismatch_total: AtomicU64,
    pub candidate_payout_mismatch_total: AtomicU64,
    pub found_block_persist_ok_total: AtomicU64,
    pub found_block_persist_error_total: AtomicU64,
    pub found_block_observed_not_persisted_total: AtomicU64,
}

impl RuntimeStats {
    pub fn snapshot(&self) -> RuntimeStatsSnapshot {
        RuntimeStatsSnapshot {
            idle_disconnects: self.idle_disconnects.load(Ordering::Relaxed),
            rate_limit_disconnects: self.rate_limit_disconnects.load(Ordering::Relaxed),
            template_payout_mismatch_total: self
                .template_payout_mismatch_total
                .load(Ordering::Relaxed),
            candidate_payout_mismatch_total: self
                .candidate_payout_mismatch_total
                .load(Ordering::Relaxed),
            found_block_persist_ok_total: self.found_block_persist_ok_total.load(Ordering::Relaxed),
            found_block_persist_error_total: self
                .found_block_persist_error_total
                .load(Ordering::Relaxed),
            found_block_observed_not_persisted_total: self
                .found_block_observed_not_persisted_total
                .load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct RuntimeStatsSnapshot {
    pub idle_disconnects: u64,
    pub rate_limit_disconnects: u64,
    pub template_payout_mismatch_total: u64,
    pub candidate_payout_mismatch_total: u64,
    pub found_block_persist_ok_total: u64,
    pub found_block_persist_error_total: u64,
    pub found_block_observed_not_persisted_total: u64,
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
    let pool_scripts = cfg.resolve_pool_scripts()?;
    info!(payout_script_fingerprint = %pool_scripts.payout_fingerprint, "pool payout script configured");
    let runtime = StratumRuntime::new(cfg.max_jobs_cache);
    refresh_job_from_node(
        &runtime,
        adapter.clone(),
        &pool_scripts,
        stats.clone(),
        true,
        "startup",
    )
    .await?;

    // NNG event-driven template refresh: subscribes to node events and refreshes
    // the mining template when the chain tip changes, mempool updates, or mining
    // work changes. This replaces the legacy periodic polling approach.
    let runtime_nng = runtime.clone();
    let nng_pub_url = cfg.nng_pub_url.clone();
    let rpc_adapter = BitcoindNngAdapter::connect(&cfg.nng_rpc_url)?;
    let adapter_events = adapter.clone();
    let pool_scripts_events = pool_scripts.clone();
    let stats_events = stats.clone();
    let db_events = db.clone();
    tokio::spawn(async move {
        let (tx, mut rx) = mpsc::unbounded_channel::<NodeEvent>();
        let runtime_events = runtime_nng.clone();
        let adapter_events_inner = adapter_events.clone();
        tokio::spawn(async move {
            while let Some(event) = rx.recv().await {
                let clean = matches!(
                    event,
                    NodeEvent::UpdateBlkTip
                        | NodeEvent::MiningWorkChanged
                        | NodeEvent::BlockDisconnected
                );
                let reason = match event {
                    NodeEvent::UpdateBlkTip => "updateblktip",
                    NodeEvent::MempoolRefresh => "mempool",
                    NodeEvent::MiningWorkChanged => "miningwrkchg",
                    NodeEvent::BlockDisconnected => "blkdisconctd",
                };
                if matches!(event, NodeEvent::BlockDisconnected) {
                    match db_events.mark_pending_blocks_orphaned() {
                        Ok(orphaned) if orphaned > 0 => {
                            let _ = db_events.record_accounting_event(
                                "block_orphaned",
                                Some("orphaned"),
                                None,
                                None,
                                None,
                                None,
                                None,
                                None,
                                None,
                                None,
                                None,
                                Some("{\"reason\":\"blkdisconctd\"}"),
                            );
                            warn!(
                                orphaned,
                                "marked pending found blocks orphaned due to blkdisconctd"
                            );
                        }
                        Ok(_) => {}
                        Err(err) => {
                            error!(error = %err, "failed applying blkdisconctd orphan update")
                        }
                    }
                }
                if let Err(err) = refresh_job_from_node(
                    &runtime_events,
                    adapter_events_inner.clone(),
                    &pool_scripts_events,
                    stats_events.clone(),
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
        let pool_scripts = pool_scripts.clone();
        tokio::spawn(async move {
            if let Err(err) =
                handle_conn(socket, db, runtime, cfg, pool_scripts, adapter, stats).await
            {
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
    pool_scripts: &ResolvedPoolScripts,
    stats: Arc<RuntimeStats>,
    clean_jobs: bool,
    reason: &str,
) -> Result<()> {
    let template = adapter
        .get_mining_template(
            Some(pool_scripts.payout_script.clone()),
            pool_scripts.coinbase_identity_bytes.clone(),
        )
        .await?;
    ensure_block_coinbase_payout_script(&template.block, &pool_scripts.payout_script).map_err(
        |e| {
            stats
                .template_payout_mismatch_total
                .fetch_add(1, Ordering::Relaxed);
            anyhow!("template payout script mismatch: {e}")
        },
    )?;
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
        network_target_hex: template.target.to_hex_be(),
        clean_jobs,
        template_epoch,
        template_block: template.block,
        block_height: template.height as i64,
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

const MAX_ASSIGNED_JOBS_PER_SESSION: usize = 128;

#[derive(Debug, Clone)]
struct AssignedJob {
    share_difficulty: f64,
    ntime_hex_6b: String,
}

#[derive(Debug, Default)]
struct SessionShareStats {
    accepted: u64,
    rejected: u64,
    errored: u64,
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

async fn send_mining_notify(
    writer: &mut tokio::net::tcp::OwnedWriteHalf,
    job: &MiningJob,
) -> Result<()> {
    // Use standard Stratum V1 notify params (9 elements, no template_header)
    let params = job.notify_params();
    let v = serde_json::json!({
        "id": serde_json::Value::Null,
        "method": "mining.notify",
        "params": params,
    });
    let mut data = serde_json::to_vec(&v)?;
    data.push(b'\n');
    writer.write_all(&data).await?;
    debug!(job_id = %job.job_id, "sent mining.notify (standard Stratum V1)");
    Ok(())
}

fn prune_assigned_jobs(
    session: &SessionState,
    assigned_jobs: &mut HashMap<String, AssignedJob>,
    assigned_job_order: &mut VecDeque<String>,
) {
    assigned_jobs.retain(|job_id, _| session.active_jobs.contains(job_id));
    assigned_job_order.retain(|job_id| assigned_jobs.contains_key(job_id));
    while assigned_job_order.len() > MAX_ASSIGNED_JOBS_PER_SESSION {
        if let Some(evicted_job_id) = assigned_job_order.pop_front() {
            assigned_jobs.remove(&evicted_job_id);
        }
    }
}

fn ensure_block_coinbase_payout_script(block: &[u8], expected_script: &[u8]) -> Result<()> {
    let mut block_bytes = Bytes::from_slice(block);
    let parsed_block = LotusBlock::deser(&mut block_bytes)?;
    let coinbase = parsed_block
        .txs
        .first()
        .ok_or_else(|| anyhow!("block has no txs"))?;
    let payout = coinbase
        .outputs()
        .get(1)
        .ok_or_else(|| anyhow!("coinbase missing vout[1]"))?;

    if payout.script.bytecode().as_ref() != expected_script {
        anyhow::bail!("coinbase vout[1] script mismatch")
    }
    Ok(())
}

async fn handle_conn(
    socket: TcpStream,
    db: AccountingDb,
    runtime: StratumRuntime,
    cfg: Config,
    pool_scripts: ResolvedPoolScripts,
    adapter: Arc<dyn NodeMiningAdapter>,
    stats: Arc<RuntimeStats>,
) -> Result<()> {
    let session_id = format!("s{:016x}", thread_rng().r#gen::<u64>());
    let mut session = SessionState::new(session_id.clone());
    session.extranonce1 = format!("{:08x}", thread_rng().r#gen::<u32>());
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
    let mut assigned_jobs: HashMap<String, AssignedJob> = HashMap::new();
    let mut assigned_job_order: VecDeque<String> = VecDeque::new();
    let mut share_stats = SessionShareStats::default();
    let mut inflight_ids: HashSet<String> = HashSet::new();
    let mut req_count: u32 = 0;
    let mut req_window_start = std::time::Instant::now();

    loop {
        while let Ok(job) = pub_rx.try_recv() {
            if let Some(next_diff) = pending_difficulty.take() {
                vardiff.current = next_diff;
            }
            apply_notify(&mut session, &job);
            if session.is_subscribed && session.is_authorized {
                send_mining_notify(&mut write_half, &job).await?;
                info!(session_id = %session_id, job_id = %job.job_id, template_epoch = job.template_epoch, difficulty = vardiff.current, ntime = %job.ntime, clean_jobs = job.clean_jobs, "assigned mining.notify work");
                assigned_jobs.insert(
                    job.job_id.clone(),
                    AssignedJob {
                        share_difficulty: vardiff.current,
                        ntime_hex_6b: job.ntime.clone(),
                    },
                );
                assigned_job_order.push_back(job.job_id.clone());
                prune_assigned_jobs(&session, &mut assigned_jobs, &mut assigned_job_order);
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

        debug!(session_id = %session_id, method = ?req.method, req_id = %req.id, params = %req.params, "received stratum request");

        let id_key = req.id.to_string();
        if !inflight_ids.insert(id_key.clone()) {
            warn!(session_id = %session_id, req_id = %req.id, "duplicate in-flight request id from miner");
            let err = StratumResponse::err(req.id.clone(), 22, "duplicate-request-id");
            send_json_line(&mut write_half, &err).await?;
            continue;
        }

        let method = req.method.clone();
        let req_id = req.id.clone();
        let params = req.params.clone();
        if let Some(resp) = handle_request(&mut session, req) {
            inflight_ids.remove(&id_key);
            debug!(session_id = %session_id, method = ?method, req_id = %req_id, response_error = %resp.error, response_result = %resp.result, "sending stratum response");
            if matches!(method, Method::Subscribe) && resp.error.is_null() {
                send_set_difficulty(&mut write_half, vardiff.current).await?;
            }
            if matches!(method, Method::Authorize) && resp.error.is_null() {
                if let Some(job) = runtime.latest_job() {
                    apply_notify(&mut session, &job);
                    send_mining_notify(&mut write_half, &job).await?;
                    assigned_jobs.insert(
                        job.job_id.clone(),
                        AssignedJob {
                            share_difficulty: vardiff.current,
                            ntime_hex_6b: job.ntime.clone(),
                        },
                    );
                    assigned_job_order.push_back(job.job_id.clone());
                    prune_assigned_jobs(&session, &mut assigned_jobs, &mut assigned_job_order);
                    info!(session_id = %session_id, job_id = %job.job_id, template_epoch = job.template_epoch, difficulty = vardiff.current, clean_jobs = job.clean_jobs, "assigned initial mining.notify work after authorize");
                }
            }
            if matches!(method, Method::Authorize) {
                let worker_name = params
                    .as_array()
                    .and_then(|v| v.first())
                    .and_then(|v| v.as_str())
                    .unwrap_or_default();
                match crate::stratum::worker::parse_worker_name(worker_name) {
                    Ok(parsed) => {
                        let _ = db.record_authorization_event(
                            &session_id,
                            worker_name,
                            Some(&parsed.payout_address),
                            parsed.worker_suffix.as_deref(),
                            resp.error.is_null(),
                            None,
                        );
                    }
                    Err(err) => {
                        let _ = db.record_authorization_event(
                            &session_id,
                            worker_name,
                            None,
                            None,
                            false,
                            Some(&err.to_string()),
                        );
                    }
                }
            }
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
                if prevalidate_submit_shape(&submit, session.extranonce2_size).is_err() {
                    share_stats.errored += 1;
                    warn!(session_id = %session_id, req_id = %req_id, worker = %submit.worker_name, job_id = %submit.job_id, extranonce2 = %submit.extranonce2, ntime = %submit.ntime_hex_6b, nonce = %submit.nonce_hex_8b, accepted = share_stats.accepted, rejected = share_stats.rejected, errored = share_stats.errored, "submit rejected: invalid-submit-shape");
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
                let round_id = db.resolve_round_for_template(job.template_id)?;
                let Some(assigned) = assigned_jobs.get(job_id).cloned() else {
                    share_stats.rejected += 1;
                    let _ = db.record_share_outcome(ShareOutcomeInsert {
                        session_id: &session_id,
                        worker_id: worker_row.id,
                        worker_name: &submit.worker_name,
                        payout_address: &worker_row.payout_address,
                        template_id: job.template_id,
                        template_epoch: job.template_epoch,
                        job_id: &job.job_id,
                        round_id,
                        dedupe_key: &format!(
                            "{}:{}:{}:{}:{}:{}",
                            worker_row.id,
                            job.template_id,
                            job.template_epoch,
                            extranonce2,
                            ntime,
                            nonce
                        ),
                        status: "stale",
                        reject_reason: Some("stale-job"),
                        node_result: None,
                        low_diff_ok: None,
                        network_target_ok: None,
                        block_hash: None,
                        share_id: None,
                    });
                    warn!(session_id = %session_id, req_id = %req_id, worker = %submit.worker_name, job_id = %submit.job_id, accepted = share_stats.accepted, rejected = share_stats.rejected, errored = share_stats.errored, "submit rejected: stale-job (no assigned context)");
                    let err = StratumResponse::err(req_id.clone(), 21, "stale-job");
                    send_json_line(&mut write_half, &err).await?;
                    continue;
                };
                if submit.ntime_hex_6b != assigned.ntime_hex_6b {
                    share_stats.rejected += 1;
                    warn!(session_id = %session_id, req_id = %req_id, worker = %submit.worker_name, job_id = %submit.job_id, submit_ntime = %submit.ntime_hex_6b, assigned_ntime = %assigned.ntime_hex_6b, accepted = share_stats.accepted, rejected = share_stats.rejected, errored = share_stats.errored, "submit rejected: ntime-mismatch");
                    let err = StratumResponse::err(req_id.clone(), 20, "ntime-mismatch");
                    send_json_line(&mut write_half, &err).await?;
                    continue;
                }
                let share_difficulty = assigned.share_difficulty;
                if let Err(_) = validate_submit_meets_difficulty(
                    &job,
                    &session.extranonce1,
                    &submit,
                    share_difficulty,
                ) {
                    share_stats.rejected += 1;
                    warn!(session_id = %session_id, req_id = %req_id, worker = %submit.worker_name, job_id = %submit.job_id, difficulty = share_difficulty, nonce = %submit.nonce_hex_8b, accepted = share_stats.accepted, rejected = share_stats.rejected, errored = share_stats.errored, "submit rejected: low-difficulty-share");
                    let err = StratumResponse::err(req_id.clone(), 23, "low-difficulty-share");
                    send_json_line(&mut write_half, &err).await?;
                    continue;
                }

                // Build the full candidate block first (preserves height, epoch_hash, etc.)
                let candidate_block = match build_candidate_block(
                    &job,
                    &session.extranonce1,
                    &submit,
                ) {
                    Ok(v) => v,
                    Err(_) => {
                        share_stats.errored += 1;
                        warn!(session_id = %session_id, req_id = %req_id, worker = %submit.worker_name, job_id = %submit.job_id, accepted = share_stats.accepted, rejected = share_stats.rejected, errored = share_stats.errored, "submit rejected: invalid-candidate block assembly");
                        let err = StratumResponse::err(req_id.clone(), 20, "invalid-candidate");
                        send_json_line(&mut write_half, &err).await?;
                        continue;
                    }
                };

                // Extract header from candidate block and validate against network target
                let meets_network_target = {
                    let mut block_bytes = Bytes::from_slice(&candidate_block);
                    let block = match LotusBlock::deser(&mut block_bytes) {
                        Ok(b) => b,
                        Err(_) => {
                            share_stats.errored += 1;
                            warn!(session_id = %session_id, req_id = %req_id, "candidate block deserialization failed");
                            continue;
                        }
                    };
                    let header_bytes = block.header.ser();
                    validate_header_meets_target_hex(header_bytes.as_ref(), &job.network_target_hex)
                        .is_ok()
                };
                if !meets_network_target {
                    let dedupe_key = format!(
                        "{}:{}:{}:{}:{}:{}",
                        worker_row.id,
                        job.template_id,
                        job.template_epoch,
                        extranonce2,
                        ntime,
                        nonce
                    );
                    let share_id = db.insert_share_idempotent(
                        worker_row.id,
                        job.template_id,
                        share_difficulty,
                        true,
                        false,
                        &dedupe_key,
                    )?;
                    let _ = db.record_share_outcome(ShareOutcomeInsert {
                        session_id: &session_id,
                        worker_id: worker_row.id,
                        worker_name: &submit.worker_name,
                        payout_address: &worker_row.payout_address,
                        template_id: job.template_id,
                        template_epoch: job.template_epoch,
                        job_id: &job.job_id,
                        round_id,
                        dedupe_key: &dedupe_key,
                        status: "accepted",
                        reject_reason: None,
                        node_result: Some("pool-only"),
                        low_diff_ok: Some(true),
                        network_target_ok: Some(false),
                        block_hash: None,
                        share_id,
                    });

                    share_stats.accepted += 1;
                    info!(session_id = %session_id, req_id = %req_id, worker_id = worker_row.id, job_id = %job.job_id, template_id = job.template_id, total_accepted = share_stats.accepted, total_rejected = share_stats.rejected, total_errored = share_stats.errored, "share accepted (meets pool difficulty; not submitted to lotusd)");

                    let now = chrono::Utc::now().timestamp();
                    vardiff.record_share(now);
                    let old_diff = vardiff.current;
                    if let Some(new_diff) = vardiff.maybe_retarget(now) {
                        info!(session_id = %session_id, old_diff, new_diff, "vardiff retarget");
                        send_set_difficulty(&mut write_half, new_diff).await?;
                        pending_difficulty = Some(new_diff);
                    }
                    send_json_line(&mut write_half, &resp).await?;
                    continue;
                }

                // candidate_block already built above for target validation

                if let Err(err) = ensure_block_coinbase_payout_script(
                    &candidate_block,
                    &pool_scripts.payout_script,
                ) {
                    stats
                        .candidate_payout_mismatch_total
                        .fetch_add(1, Ordering::Relaxed);
                    error!(session_id = %session_id, req_id = %req_id, worker_id = worker_row.id, template_id = job.template_id, error = %err, "candidate payout script mismatch; rejecting submit");
                    let err = StratumResponse::err(req_id.clone(), 20, "candidate-payout-mismatch");
                    send_json_line(&mut write_half, &err).await?;
                    continue;
                }

                let merkle_matches_block = {
                    let mut block_bytes = Bytes::from_slice(&candidate_block);
                    match LotusBlock::deser(&mut block_bytes) {
                        Ok(mut block) => {
                            let header_merkle = block.header.merkle_root.clone();
                            block.update_merkle_root();
                            block.header.merkle_root == header_merkle
                        }
                        Err(_) => false,
                    }
                };
                if !merkle_matches_block {
                    share_stats.rejected += 1;
                    warn!(session_id = %session_id, req_id = %req_id, worker = %submit.worker_name, job_id = %submit.job_id, accepted = share_stats.accepted, rejected = share_stats.rejected, errored = share_stats.errored, "submit rejected: bad-txnmrklroot");
                    let err = StratumResponse::err(req_id.clone(), 20, "bad-txnmrklroot");
                    send_json_line(&mut write_half, &err).await?;
                    continue;
                }

                let proposal = match adapter.validate_proposal(candidate_block.clone()).await {
                    Ok(v) => v,
                    Err(err) => {
                        share_stats.errored += 1;
                        warn!(session_id = %session_id, req_id = %req_id, worker = %submit.worker_name, job_id = %submit.job_id, error = %err, accepted = share_stats.accepted, rejected = share_stats.rejected, errored = share_stats.errored, "submit rejected: proposal-rpc-failed");
                        let err = StratumResponse::err(req_id.clone(), 20, "proposal-rpc-failed");
                        send_json_line(&mut write_half, &err).await?;
                        continue;
                    }
                };
                if !proposal.valid {
                    share_stats.rejected += 1;
                    warn!(session_id = %session_id, req_id = %req_id, worker_id = worker_row.id, job_id = %job.job_id, reject_reason = %proposal.reject_reason, accepted = share_stats.accepted, rejected = share_stats.rejected, errored = share_stats.errored, "proposal validation rejected share");
                    let err = StratumResponse::err(req_id.clone(), 20, "proposal-invalid");
                    send_json_line(&mut write_half, &err).await?;
                    continue;
                }

                let submit_result = match adapter.submit_mined_block(candidate_block).await {
                    Ok(v) => v,
                    Err(err) => {
                        share_stats.errored += 1;
                        warn!(session_id = %session_id, req_id = %req_id, worker = %submit.worker_name, job_id = %submit.job_id, error = %err, accepted = share_stats.accepted, rejected = share_stats.rejected, errored = share_stats.errored, "submit rejected: submit-rpc-failed");
                        let err = StratumResponse::err(req_id.clone(), 20, "submit-rpc-failed");
                        send_json_line(&mut write_half, &err).await?;
                        continue;
                    }
                };

                let node_result_is_share_only_high_hash =
                    matches!(submit_result.result, MiningSubmitResult::Rejected)
                        && submit_result
                            .reject_reason
                            .eq_ignore_ascii_case("high-hash");

                let share_accepted = matches!(
                    submit_result.result,
                    MiningSubmitResult::Accepted
                        | MiningSubmitResult::Duplicate
                        | MiningSubmitResult::DuplicateInvalid
                        | MiningSubmitResult::DuplicateInconclusive
                ) || node_result_is_share_only_high_hash;

                let dedupe_key = format!(
                    "{}:{}:{}:{}:{}:{}",
                    worker_row.id, job.template_id, job.template_epoch, extranonce2, ntime, nonce
                );
                let share_id = db.insert_share_idempotent(
                    worker_row.id,
                    job.template_id,
                    share_difficulty,
                    share_accepted,
                    false,
                    &dedupe_key,
                )?;
                let submit_block_hash = submit_result.block_hash.to_hex_be();
                let _ = db.record_share_outcome(ShareOutcomeInsert {
                    session_id: &session_id,
                    worker_id: worker_row.id,
                    worker_name: &submit.worker_name,
                    payout_address: &worker_row.payout_address,
                    template_id: job.template_id,
                    template_epoch: job.template_epoch,
                    job_id: &job.job_id,
                    round_id,
                    dedupe_key: &dedupe_key,
                    status: if share_accepted {
                        "accepted"
                    } else {
                        "rejected"
                    },
                    reject_reason: if share_accepted {
                        None
                    } else {
                        Some(submit_result.reject_reason.as_str())
                    },
                    node_result: Some(&format!("{:?}", submit_result.result)),
                    low_diff_ok: Some(true),
                    network_target_ok: Some(true),
                    block_hash: Some(&submit_block_hash),
                    share_id,
                });

                if !share_accepted {
                    share_stats.rejected += 1;
                    warn!(session_id = %session_id, req_id = %req_id, worker_id = worker_row.id, job_id = %job.job_id, result = ?submit_result.result, reject_reason = %submit_result.reject_reason, accepted = share_stats.accepted, rejected = share_stats.rejected, errored = share_stats.errored, "share rejected by lotusd submit path");
                    let err = StratumResponse::err(req_id.clone(), 20, "block-submit-rejected");
                    send_json_line(&mut write_half, &err).await?;
                    continue;
                }

                share_stats.accepted += 1;
                let _ = db.record_accounting_event(
                    "submit_result",
                    Some(if share_accepted {
                        "accepted"
                    } else {
                        "rejected"
                    }),
                    Some(&session_id),
                    Some(worker_row.id),
                    Some(&submit.worker_name),
                    Some(&worker_row.payout_address),
                    Some(round_id),
                    Some(job.template_id),
                    Some(job.template_epoch),
                    Some(&job.job_id),
                    Some(&submit_block_hash),
                    Some(&format!(
                        "{{\"reject_reason\":\"{}\",\"result\":\"{:?}\"}}",
                        submit_result.reject_reason, submit_result.result
                    )),
                );
                if submit_result.accepted {
                    let persist = db.record_found_block(
                        &submit_block_hash,
                        job.template_id,
                        job.block_height,
                        worker_row.id,
                        &submit.worker_name,
                        &worker_row.payout_address,
                        "submit_flow",
                    );
                    match persist {
                        Ok(_) => {
                            stats
                                .found_block_persist_ok_total
                                .fetch_add(1, Ordering::Relaxed);
                        }
                        Err(err) => {
                            stats
                                .found_block_persist_error_total
                                .fetch_add(1, Ordering::Relaxed);
                            error!(session_id = %session_id, block_hash = %submit_result.block_hash.to_hex_be(), error = %err, "node accepted solved block but DB persist failed");
                        }
                    }
                }

                if node_result_is_share_only_high_hash {
                    info!(session_id = %session_id, req_id = %req_id, worker_id = worker_row.id, job_id = %job.job_id, template_id = job.template_id, reject_reason = %submit_result.reject_reason, total_accepted = share_stats.accepted, total_rejected = share_stats.rejected, total_errored = share_stats.errored, "share accepted (met pool difficulty; below network target)");
                } else {
                    info!(session_id = %session_id, req_id = %req_id, worker_id = worker_row.id, job_id = %job.job_id, template_id = job.template_id, result = ?submit_result.result, accepted = submit_result.accepted, block_hash = %submit_result.block_hash.to_hex_be(), total_accepted = share_stats.accepted, total_rejected = share_stats.rejected, total_errored = share_stats.errored, "share accepted via proposal+submit flow");
                }

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
        } else {
            inflight_ids.remove(&id_key);
        }
    }

    info!(session_id = %session_id, accepted = share_stats.accepted, rejected = share_stats.rejected, errored = share_stats.errored, authorized_workers = session.authorized_workers.len(), active_jobs = session.active_jobs.len(), assigned_jobs = assigned_jobs.len(), "stratum session ended");
    Ok(())
}
