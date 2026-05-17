use crate::accounting::{AccountingDb, FoundBlock, ShareOutcomeInsert};
use crate::config::{Config, ResolvedPoolScripts};
use crate::nng::adapter::{
    BitcoindMiningAdapter, JsonRpcClient, NngAdapter, NodeEvent, NodeMiningAdapter,
};
use crate::stratum::diff_cache::DifficultyCache;
use crate::stratum::engine::{apply_notify, handle_request, SessionState};
use crate::stratum::job::MiningJob;
use crate::stratum::protocol::{decode_request_line, Method, StratumResponse};
use crate::stratum::validation::{
    build_candidate_block_with_stratum_hash, prevalidate_submit_shape,
    validate_header_meets_target_hex, validate_submit_meets_difficulty, NativeSubmit,
};
use crate::stratum::vardiff::VarDiff;
use anyhow::{anyhow, Result};
use bitcoinsuite_bitcoind_nng::MiningTemplate;
use bitcoinsuite_bitcoind_stratum::build_stratum_header;
use bitcoinsuite_core::{BitcoinCode, Bytes, Hashed, LotusBlock, LotusHeader};
use rand::{thread_rng, Rng};
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{
    atomic::{AtomicI64, AtomicU64, Ordering},
    Arc, Mutex,
};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{broadcast, mpsc};
use tokio::time::Duration;
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

/// Helper: orphan a found_block and close associated round
fn orphan_found_block(db: &AccountingDb, found_block: &FoundBlock, reason: &str) -> Result<()> {
    // 1. Mark as orphaned
    db.mark_found_block_orphaned(&found_block.block_hash, reason)?;

    // 2. Close round
    let _ = db.close_round(
        found_block.round_id,
        found_block.template_id.map(|v| v as u64),
        "round_closed_orphaned",
        Some(&found_block.block_hash),
    );

    // 3. Record accounting event
    let _ = db.record_accounting_event(
        "found_block_orphaned",
        Some("orphaned"),
        None,
        None,
        None,
        None,
        Some(found_block.round_id),
        None,
        None,
        None,
        Some(&found_block.block_hash),
        Some(&format!("{{\"height\":{}}}", found_block.height)),
    );

    Ok(())
}

/// Reconcile found_blocks with node on startup.
/// Validates each found_block against node, marking orphaned if hash mismatch.
async fn reconcile_found_blocks(
    db: &AccountingDb,
    adapter: &Arc<dyn NodeMiningAdapter>,
) -> Result<i64> {
    // 1. Get our latest found_block from DB
    let latest = match db.find_latest_found_block()? {
        Some(block) => block,
        None => {
            info!("no found_blocks to reconcile");
            return Ok(0);
        }
    };

    let mut orphaned_count = 0u64;
    let mut validated_count = 0u64;

    // 2. Start from latest height and walk down
    let mut check_height = latest.height;

    loop {
        // 3. Get found_block at this height
        let found_block = match db.find_found_block_by_height(check_height)? {
            Some(fb) => fb,
            None => {
                check_height -= 1;
                if check_height < 0 {
                    break;
                }
                continue;
            }
        };

        // 4. Skip already orphaned blocks
        if found_block.status == "orphaned" {
            check_height -= 1;
            if check_height < 0 {
                break;
            }
            continue;
        }

        // 5. Get node's block at this height
        match adapter.get_block_by_height(check_height).await {
            Ok(node_block) => {
                let node_hash = node_block.header.hash.to_hex_be();

                // 6. Compare hashes
                if node_hash == found_block.block_hash {
                    // Match - block is valid
                    validated_count += 1;

                    info!(
                        height = check_height,
                        block_hash = %found_block.block_hash,
                        "found_block validated"
                    );

                    // All blocks below are also valid - we're done
                    break;
                }

                // 7. Hash mismatch - orphan
                warn!(
                    height = check_height,
                    our_hash = %found_block.block_hash,
                    node_hash = %node_hash,
                    "found_block orphaned (hash mismatch)"
                );

                orphan_found_block(db, &found_block, "reorg_detected")?;
                orphaned_count += 1;
            }
            Err(err) => {
                // Node doesn't have block at this height
                warn!(
                    height = check_height,
                    error = %err,
                    "node doesn't have block at found_block height"
                );

                orphan_found_block(db, &found_block, "block_not_found")?;
                orphaned_count += 1;
            }
        }

        check_height -= 1;
        if check_height < 0 {
            break;
        }
    }

    info!(
        orphaned_count,
        validated_count, "found_blocks reconciliation complete"
    );

    Ok(check_height)
}

pub async fn run_stratum_server(
    cfg: Config,
    db: AccountingDb,
    stats: Arc<RuntimeStats>,
    diff_cache: DifficultyCache,
    events_tx: crate::http::DashboardEventSender,
) -> Result<()> {
    let listener = TcpListener::bind(&cfg.stratum_bind).await?;
    info!(bind = %cfg.stratum_bind, "stratum server listening");

    // Validate bitcoind_rpc.url is HTTP(S), not IPC
    if !cfg.bitcoind_rpc.url.starts_with("http://") && !cfg.bitcoind_rpc.url.starts_with("https://")
    {
        anyhow::bail!(
            "bitcoind_rpc.url must be HTTP/HTTPS (e.g., http://127.0.0.1:10604/), got: {}. \
             IPC paths (ipc://) work for nng_rpc_url but not for bitcoind_rpc.url",
            cfg.bitcoind_rpc.url
        );
    }

    info!(bitcoind_rpc_url = %cfg.bitcoind_rpc.url, "using HTTP RPC for submitblock");

    // Create separate NNG and JSON-RPC clients, then compose them
    let nng_adapter = NngAdapter::connect(&cfg.nng_rpc_url)?;
    let json_rpc_client = JsonRpcClient::new(
        cfg.bitcoind_rpc.url.clone(),
        cfg.bitcoind_rpc.rpc_user.clone(),
        cfg.bitcoind_rpc.rpc_pass.clone(),
    );
    let adapter: Arc<dyn NodeMiningAdapter> =
        Arc::new(BitcoindMiningAdapter::new(nng_adapter, json_rpc_client));
    let pool_scripts = cfg.resolve_pool_scripts()?;
    info!(payout_script_fingerprint = %pool_scripts.payout_fingerprint, "pool payout script configured");

    // Fetch actual chain tip from node via RPC
    let node_tip = adapter.get_block_count().await?;
    info!(node_tip, "fetched chain tip from node");

    // Reconcile found_blocks with node on startup
    let _reconciled_height = reconcile_found_blocks(&db, &adapter).await?;

    // Track tip height from blkconnected events for confirmation computation
    // Initialize from node's actual tip, not reconciliation result
    let tip_height = Arc::new(AtomicI64::new(node_tip));

    let runtime = StratumRuntime::new(cfg.max_jobs_cache);
    refresh_job_from_node(
        &runtime,
        adapter.clone(),
        &pool_scripts,
        stats.clone(),
        &diff_cache,
        true,
        "startup",
        cfg.debug,
    )
    .await?;

    // NNG event-driven template refresh: subscribes to node events and refreshes
    // the mining template when the chain tip changes, mempool updates, or mining
    // work changes. This replaces the legacy periodic polling approach.
    let runtime_nng = runtime.clone();
    let nng_pub_url = cfg.nng_pub_url.clone();
    let rpc_adapter = NngAdapter::connect(&cfg.nng_rpc_url)?;
    let adapter_events = adapter.clone();
    let pool_scripts_events = pool_scripts.clone();
    let stats_events = stats.clone();
    let db_events = db.clone();
    let diff_cache_events = diff_cache.clone();
    let debug = cfg.debug;
    let tip_height_events = tip_height.clone();
    let min_confirmations = cfg.pool.pplns.min_confirmations as i64;
    tokio::spawn(async move {
        let (tx, mut rx) = mpsc::unbounded_channel::<NodeEvent>();
        let runtime_events = runtime_nng.clone();
        let adapter_events_inner = adapter_events.clone();
        tokio::spawn(async move {
            // Track last seen template_epoch for missed-event detection
            let mut last_template_epoch: Option<u64> = None;

            // Coalescing state: only MiningWorkChanged is debounced. Accounting events
            // (blkconnected/blkdisconctd) are processed immediately since they don't
            // trigger expensive RPC calls.
            let mut pending_mining_work: Option<NodeEvent> = None;
            let debounce_duration = Duration::from_millis(100);
            let mut debounce_timer = tokio::time::interval(debounce_duration);
            debounce_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            loop {
                tokio::select! {
                    // Collect incoming NNG events
                    Some(event) = rx.recv() => {
                        match &event {
                            NodeEvent::MiningWorkChanged { template_epoch, .. } => {
                                // Coalesce: replace pending with newer event. Only the
                                // latest event matters — older ones are superseded.
                                let prev_epoch = pending_mining_work.as_ref().and_then(|e| {
                                    match e {
                                        NodeEvent::MiningWorkChanged { template_epoch: e, .. } => Some(*e),
                                        _ => None,
                                    }
                                });
                                debug!(
                                    previous_epoch = prev_epoch.map(|e| e.to_string()).as_deref().unwrap_or("none"),
                                    new_epoch = template_epoch,
                                    "coalescing miningwrkchg event"
                                );
                                pending_mining_work = Some(event);
                            }
                            NodeEvent::BlockConnected { height, hash: _, prev_hash: _ } => {
                                // Accounting only — process immediately, no debounce
                                tip_height_events.store(*height, Ordering::SeqCst);
                                if let Err(err) = db_events.mark_blocks_matured(
                                    *height,
                                    min_confirmations,
                                ) {
                                    error!(error = %err, "failed marking matured blocks after blkconnected");
                                }
                            }
                            NodeEvent::BlockDisconnected { height, hash, prev_hash: _ } => {
                                // Accounting only — process immediately, no debounce
                                match db_events.find_found_block_by_height_and_hash(*height, hash) {
                                    Ok(Some(found_block)) => {
                                        if let Err(err) = db_events.mark_found_block_orphaned(hash, "blkdisconctd") {
                                            error!(error = %err, block_hash = %hash, "failed marking found_block orphaned");
                                        } else {
                                            warn!(
                                                block_hash = %hash,
                                                height = height,
                                                round_id = found_block.round_id,
                                                worker = %found_block.worker_name.unwrap_or_default(),
                                                "pool-mined block orphaned via blkdisconctd"
                                            );
                                        }
                                        let _ = db_events.close_round(
                                            found_block.round_id,
                                            found_block.template_id.map(|v| v as u64),
                                            "round_closed_orphaned",
                                            Some(hash),
                                        );
                                        let _ = db_events.record_accounting_event(
                                            "found_block_orphaned",
                                            Some("orphaned"),
                                            None, None, None, None,
                                            Some(found_block.round_id),
                                            None, None, None,
                                            Some(hash),
                                            Some(&format!("{{\"height\":{}}}", height)),
                                        );
                                    }
                                    Ok(None) => {
                                        debug!(block_hash = %hash, height = height, "external block disconnected");
                                    }
                                    Err(err) => error!(error = %err, "failed checking orphaned found_block"),
                                }
                                tip_height_events.store(height - 1, Ordering::SeqCst);
                                if let Err(err) = db_events.mark_blocks_matured(
                                    height - 1,
                                    min_confirmations,
                                ) {
                                    error!(error = %err, "failed marking matured blocks after reorg");
                                }
                            }
                        }
                    }

                    // Debounce timer fires — process the latest pending MiningWorkChanged
                    _ = debounce_timer.tick() => {
                        if let Some(event) = pending_mining_work.take() {
                            if let NodeEvent::MiningWorkChanged {
                                ref reason,
                                tip_height,
                                template_epoch,
                                ..
                            } = event
                            {
                                // Detect missed events (gaps in template_epoch sequence)
                                if let Some(last_epoch) = last_template_epoch {
                                    if template_epoch > last_epoch + 1 {
                                        let missed = template_epoch - last_epoch - 1;
                                        warn!(
                                            last_epoch,
                                            current_epoch = template_epoch,
                                            missed,
                                            "missed miningwrkchg events from lotusd"
                                        );
                                    }
                                    if template_epoch <= last_epoch {
                                        debug!(
                                            last_epoch,
                                            current_epoch = template_epoch,
                                            "duplicate or out-of-order miningwrkchg event"
                                        );
                                    }
                                }
                                last_template_epoch = Some(template_epoch);

                                // Update tip height tracker for confirmation computation
                                tip_height_events.store(tip_height, Ordering::SeqCst);

                                // Mark matured blocks based on new tip using configured min_confirmations
                                if let Err(err) = db_events.mark_blocks_matured(
                                    tip_height,
                                    min_confirmations,
                                ) {
                                    error!(error = %err, "failed marking matured blocks after miningwrkchg");
                                }

                                // Refresh the mining template and broadcast to all miners.
                                // clean=true because the Lotus header includes block_size,
                                // so ALL work changes (mempool, new tip, reorg) invalidate
                                // in-flight work.
                                if let Err(err) = refresh_job_from_node(
                                    &runtime_events,
                                    adapter_events_inner.clone(),
                                    &pool_scripts_events,
                                    stats_events.clone(),
                                    &diff_cache_events,
                                    true,  // clean_jobs
                                    reason.as_str(),
                                    debug,
                                ).await {
                                    warn!(error = %err, reason = reason.as_str(),
                                        "template refresh failed after miningwrkchg");
                                }
                            }
                        }
                    }
                }
            }
        });

        if let Err(err) = rpc_adapter
            .run_pub_loop(&nng_pub_url, move |ev| {
                // Log event type for debugging drops
                // Include template_epoch for miningwrkchg events to aid in missed-event detection
                let event_type = match &ev {
                    NodeEvent::MiningWorkChanged {
                        reason,
                        template_epoch,
                        tip_height: _,
                        ..
                    } => {
                        format!("miningwrkchg[{}@{}]", reason.as_str(), template_epoch)
                    }
                    NodeEvent::BlockConnected { height, .. } => format!("blkconnected@{}", height),
                    NodeEvent::BlockDisconnected { height, .. } => {
                        format!("blkdisconctd@{}", height)
                    }
                };
                if tx.send(ev).is_err() {
                    warn!(
                        event_type,
                        "NNG event dropped; stratum event consumer not running"
                    );
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
        let diff_cache = diff_cache.clone();
        let events_tx = events_tx.clone();
        tokio::spawn(async move {
            if let Err(err) = handle_conn(
                socket,
                db,
                runtime,
                cfg,
                pool_scripts,
                adapter,
                stats,
                diff_cache,
                events_tx,
            )
            .await
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
    diff_cache: &DifficultyCache,
    clean_jobs: bool,
    reason: &str,
    debug: bool,
) -> Result<()> {
    let template = adapter
        .get_mining_template(
            Some(pool_scripts.payout_script.clone()),
            pool_scripts.coinbase_identity_bytes.clone(),
        )
        .await?;

    if debug {
        debug!(
            template_id = template.template_id,
            height = template.height,
            prev_hash = %template.prev_hash_stratum,
            nbits = %template.nbits_stratum,
            ntime = %template.ntime_stratum,
            target = %template.target.to_hex_be(),
            coinbase1_len = template.coinbase1.len(),
            coinbase2_len = template.coinbase2.len(),
            merkle_branches_count = template.merkle_branches.len(),
            block_size = template.block.len(),
            version = template.version,
            "fetched mining template from lotusd"
        );
    }

    // Update difficulty cache with new template
    let (old_diff, new_diff, significant) = diff_cache.update_template(&template);
    if significant {
        info!(
            old_diff = %old_diff,
            new_diff = %new_diff,
            "network difficulty updated"
        );
    }

    ensure_block_coinbase_payout_script(&template.block, &pool_scripts.payout_script).map_err(
        |e| {
            stats
                .template_payout_mismatch_total
                .fetch_add(1, Ordering::Relaxed);
            anyhow!("template payout script mismatch: {e}")
        },
    )?;
    let epoch = runtime.next_template_epoch();
    let job = make_job_from_template(template, epoch, clean_jobs, debug)?;
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
    debug: bool,
) -> Result<MiningJob> {
    let version = format!("{:08x}", template.version);

    // Extract epoch_hash and extended_metadata_hash from the template block header
    let block = LotusBlock::deser(&mut Bytes::from_slice(&template.block))?;
    let epoch_hash_hex = block.header.epoch_hash.to_hex_be();
    let extended_metadata_hash_hex = block.header.extended_metadata_hash.to_hex_be();

    // Compute the total block size for the final candidate block.
    // The NNG template splits the coinbase into coinbase1/coinbase2 with the
    // extranonce bytes omitted (lotusd's SplitCoinbase reserves space but does
    // not include the extranonce in the template block).  We must account for
    // the extranonce size delta so the header.size field matches the actual
    // serialized candidate block that lotusd validates.
    let template_coinbase_size = block.txs[0].ser().as_ref().len();
    // Build a sample coinbase with zeroed extranonce to measure the final tx size.
    let sample_coinbase_hex = format!(
        "{}{:016x}{}",
        &template.coinbase1,
        0u64, // dummy extranonce1(4) + extranonce2(4)
        &template.coinbase2,
    );
    let sample_coinbase_bytes =
        hex::decode(&sample_coinbase_hex).map_err(|e| anyhow!("sample coinbase decode: {e}"))?;
    let sample_coinbase_tx =
        bitcoinsuite_core::Tx::deser(&mut Bytes::from_slice(&sample_coinbase_bytes))
            .map_err(|e| anyhow!("sample coinbase deser: {e}"))?;
    let candidate_coinbase_size = sample_coinbase_tx.ser().as_ref().len();
    let block_size =
        template.block.len() as u64 + (candidate_coinbase_size - template_coinbase_size) as u64;

    let job = MiningJob {
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
        block_height: template.height,
        epoch_hash_hex,
        extended_metadata_hash_hex,
        block_size,
    };

    if debug {
        debug!(
            job_id = %job.job_id,
            template_id = job.template_id,
            template_epoch = job.template_epoch,
            prevhash = %job.prevhash,
            nbits = %job.nbits,
            ntime = %job.ntime,
            network_target = %job.network_target_hex,
            clean_jobs = job.clean_jobs,
            block_height = job.block_height,
            epoch_hash = %job.epoch_hash_hex,
            extended_metadata_hash = %job.extended_metadata_hash_hex,
            block_size = job.block_size,
            coinbase1_len = job.coinbase1.len(),
            coinbase2_len = job.coinbase2.len(),
            merkle_branches_count = job.merkle_branches.len(),
            "reconstructed MiningJob from template"
        );
    }

    Ok(job)
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

async fn send_set_extranonce(
    writer: &mut tokio::net::tcp::OwnedWriteHalf,
    extranonce1: &str,
    extranonce2_size: usize,
) -> Result<()> {
    let v = serde_json::json!({
        "id": serde_json::Value::Null,
        "method": "mining.set_extranonce",
        "params": [extranonce1, extranonce2_size],
    });
    let mut data = serde_json::to_vec(&v)?;
    data.push(b'\n');
    writer.write_all(&data).await?;
    info!(extranonce1 = %extranonce1, extranonce2_size = extranonce2_size, "sent mining.set_extranonce");
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
    diff_cache: DifficultyCache,
    events_tx: crate::http::DashboardEventSender,
) -> Result<()> {
    let session_id = format!("s{:016x}", thread_rng().r#gen::<u64>());
    let mut session = SessionState::new(session_id.clone());
    session.extranonce1 = format!("{:08x}", thread_rng().r#gen::<u32>());

    // Get network difficulty (this is now the pool diff baseline)
    let network_diff = diff_cache.network_diff();

    // Start new miners at a low fraction of network difficulty so they can submit
    // initial shares quickly. VarDiff ramps up based on observed share rate.
    let initial_diff = (network_diff * cfg.vardiff.vardiff_initial_pct)
        .max(cfg.vardiff.vardiff_min_floor)
        .min(network_diff);

    // Initialize VarDiff with the low starting difficulty and network diff as ceiling
    let mut vardiff = VarDiff::new(
        initial_diff,
        cfg.vardiff.vardiff_min_floor, // Absolute floor
        network_diff,                  // Dynamic ceiling = network diff
        cfg.vardiff.vardiff_target_secs,
        cfg.vardiff.vardiff_retarget_secs,
    );

    info!(
        session_id = %session_id,
        initial_diff = initial_diff,
        network_diff = network_diff,
        initial_pct = cfg.vardiff.vardiff_initial_pct,
        extranonce1 = %session.extranonce1,
        "stratum session started with dynamic difficulty"
    );

    let (read_half, mut write_half) = socket.into_split();
    let mut reader = BufReader::new(read_half);
    let mut pub_rx = runtime.subscribe();

    // Subscribe to difficulty updates from network
    let mut diff_rx = diff_cache.subscribe();

    // Send extranonce to miner so it can construct the correct coinbase
    // extranonce2_size=4 is standard (allows 2^32 = 4 billion nonces per extranonce1)
    const EXTRANONCE2_SIZE: usize = 4;
    if let Err(e) =
        send_set_extranonce(&mut write_half, &session.extranonce1, EXTRANONCE2_SIZE).await
    {
        warn!(session_id = %session_id, error = %e, "failed to send extranonce");
    }

    let mut pending_difficulty: Option<f64> = None;
    let mut assigned_jobs: HashMap<String, AssignedJob> = HashMap::new();
    let mut assigned_job_order: VecDeque<String> = VecDeque::new();
    let mut share_stats = SessionShareStats::default();
    let mut inflight_ids: HashSet<String> = HashSet::new();
    let mut req_count: u32 = 0;
    let mut req_window_start = std::time::Instant::now();

    let mut line = String::new();

    loop {
        tokio::select! {
            biased;

            // Priority 1: New mining job available — deliver immediately
            job_result = pub_rx.recv() => {
                let job = job_result?;
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

            // Priority 2: Difficulty update from network
            diff_result = diff_rx.recv() => {
                let new_network_diff = diff_result?;

                // Update this miner's VarDiff max to new network diff
                vardiff.update_max(new_network_diff);

                info!(
                    session_id = %session_id,
                    old_diff = vardiff.current,
                    new_network_diff = new_network_diff,
                    "network difficulty updated"
                );

                // Send difficulty update to miner
                if let Err(e) = send_set_difficulty(&mut write_half, new_network_diff).await {
                    warn!(session_id = %session_id, error = %e, "failed to send difficulty update");
                }
            }

            // Priority 3: Idle timeout
            _ = tokio::time::sleep(Duration::from_secs(cfg.conn_idle_timeout_secs)) => {
                stats.idle_disconnects.fetch_add(1, Ordering::Relaxed);
                let snap = stats.snapshot();
                info!(session_id = %session_id, idle_disconnects = snap.idle_disconnects, rate_limit_disconnects = snap.rate_limit_disconnects, "disconnecting idle miner session");
                break;
            }

            // Priority 4: Read request from miner
            result = reader.read_line(&mut line) => {
                let n = result?;
                if n == 0 {
                    break;
                }

                // Rate limit window reset
                if req_window_start.elapsed().as_secs() >= 1 {
                    req_window_start = std::time::Instant::now();
                    req_count = 0;
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
                        line.clear();
                        warn!(session_id = %session_id, error = %err, "invalid stratum request line");
                        let err = StratumResponse::err(serde_json::Value::Null, 20, "invalid-request");
                        send_json_line(&mut write_half, &err).await?;
                        continue;
                    }
                };
                line.clear();

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
                            let err = StratumResponse::rejected(req_id.clone(), 20, "invalid-submit-shape");
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
                            let err = StratumResponse::rejected(req_id.clone(), 21, "stale-job");
                            send_json_line(&mut write_half, &err).await?;
                            continue;
                        };
                        if submit.ntime_hex_6b != assigned.ntime_hex_6b {
                            share_stats.rejected += 1;
                            warn!(session_id = %session_id, req_id = %req_id, worker = %submit.worker_name, job_id = %submit.job_id, submit_ntime = %submit.ntime_hex_6b, assigned_ntime = %assigned.ntime_hex_6b, accepted = share_stats.accepted, rejected = share_stats.rejected, errored = share_stats.errored, "submit rejected: ntime-mismatch");
                            let err = StratumResponse::rejected(req_id.clone(), 20, "ntime-mismatch");
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
                            let err = StratumResponse::rejected(req_id.clone(), 23, "low-difficulty-share");
                            send_json_line(&mut write_half, &err).await?;
                            continue;
                        }

                        // Build the full candidate block and compute the stratum hash
                        // The stratum hash is computed from the header with default values for
                        // height, epoch_hash, extended_metadata_hash, and size - matching what
                        // GPU miners compute.
                        let (candidate_block, stratum_hash_hex, stratum_merkle_root_hex) = match build_candidate_block_with_stratum_hash(
                            &job,
                            &session.extranonce1,
                            &submit,
                        ) {
                            Ok(v) => v,
                            Err(e) => {
                                share_stats.errored += 1;
                                warn!(
                                    session_id = %session_id,
                                    req_id = %req_id,
                                    worker = %submit.worker_name,
                                    job_id = %submit.job_id,
                                    error = %e,
                                    job_template_id = job.template_id,
                                    job_template_epoch = job.template_epoch,
                                    job_coinbase1_len = job.coinbase1.len(),
                                    job_coinbase2_len = job.coinbase2.len(),
                                    job_merkle_branches_count = job.merkle_branches.len(),
                                    job_prevhash = %job.prevhash,
                                    job_template_block_len = job.template_block.len(),
                                    extranonce1 = %session.extranonce1,
                                    extranonce2 = %submit.extranonce2,
                                    ntime = %submit.ntime_hex_6b,
                                    nonce = %submit.nonce_hex_8b,
                                    accepted = share_stats.accepted,
                                    rejected = share_stats.rejected,
                                    errored = share_stats.errored,
                                    "submit rejected: invalid-candidate block assembly"
                                );
                                let err = StratumResponse::err(req_id.clone(), 20, "invalid-candidate");
                                send_json_line(&mut write_half, &err).await?;
                                continue;
                            }
                        };

                        // Validate the stratum hash against network target
                        // This hash matches what the GPU miner computed
                        let (meets_network_target, header_debug, template_debug, computed_hash_hex, header_bytes_hex) = {
                            let header_bytes = build_stratum_header(
                                &job.coinbase1,
                                &session.extranonce1,
                                &submit.extranonce2,
                                &job.coinbase2,
                                &job.merkle_branches,
                                &job.prevhash,
                                &job.version,
                                &job.nbits,
                                &submit.ntime_hex_6b,
                                &submit.nonce_hex_8b,
                                Some(job.block_height),
                                Some(&job.epoch_hash_hex),
                                Some(&job.extended_metadata_hash_hex),
                                Some(job.block_size),
                            ).map_err(|e| anyhow::anyhow!("header build error: {}", e))?;
                            let header = LotusHeader::deser(&mut Bytes::from_slice(&header_bytes))
                                .map_err(|e| anyhow::anyhow!("header deser error: {}", e))?;
                            let hash = header.calc_hash();
                            let mut hash_be = [0u8; 32];
                            hash_be.copy_from_slice(hash.as_ref());
                            hash_be.reverse();
                            let computed_hash_hex = hex::encode(&hash_be);
                            let meets = validate_header_meets_target_hex(&header_bytes, &job.network_target_hex).is_ok();
                            let header_debug = format!(
                                "version={} nbits={:08x} timestamp={} nonce={} merkle={}",
                                header.version,
                                header.bits,
                                header.timestamp,
                                header.nonce,
                                header.merkle_root.to_hex_be()
                            );
                            let template_debug = format!(
                                "template_id={} epoch={} prevhash={} nbits={} ntime={} target={}",
                                job.template_id,
                                job.template_epoch,
                                job.prevhash,
                                job.nbits,
                                job.ntime,
                                job.network_target_hex
                            );
                            (meets, header_debug, template_debug, computed_hash_hex, hex::encode(&header_bytes))
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
                            info!(
                                session_id = %session_id,
                                req_id = %req_id,
                                worker_id = worker_row.id,
                                job_id = %job.job_id,
                                template_id = job.template_id,
                                block_hash = %stratum_hash_hex,
                                merkle_root = %stratum_merkle_root_hex,
                                header = %header_debug,
                                template = %template_debug,
                                share_difficulty = share_difficulty,
                                network_difficulty = diff_cache.network_diff(),
                                total_accepted = share_stats.accepted,
                                total_rejected = share_stats.rejected,
                                total_errored = share_stats.errored,
                                "share accepted (meets pool difficulty; not submitted to lotusd)"
                            );

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
                            let err = StratumResponse::rejected(req_id.clone(), 20, "bad-txnmrklroot");
                            send_json_line(&mut write_half, &err).await?;
                            continue;
                        }

                        // Submit block via HTTP RPC submitblock (performs full validation)
                        let submit_result = match adapter.submit_block(candidate_block.clone()).await {
                            Ok(v) => v,
                            Err(err) => {
                                share_stats.errored += 1;
                                warn!(session_id = %session_id, req_id = %req_id, worker = %submit.worker_name, job_id = %submit.job_id, error = %err, accepted = share_stats.accepted, rejected = share_stats.rejected, errored = share_stats.errored, "submit rejected: submit-rpc-failed");
                                let err = StratumResponse::err(req_id.clone(), 20, "submit-rpc-failed");
                                send_json_line(&mut write_half, &err).await?;
                                continue;
                            }
                        };

                        // DEBUG: Log submitblock request and response
                        debug!(
                            session_id = %session_id,
                            job_id = %submit.job_id,
                            stratum_hash_hex = %stratum_hash_hex,
                            submit_block_hash = %submit_result.block_hash.to_hex_be(),
                            reject_reason = ?submit_result.reject_reason,
                            candidate_block_hex = %hex::encode(&candidate_block),
                            "DEBUG: submitblock response from lotusd"
                        );

                        // BIP22-style result: null means accepted, string means rejected with reason
                        let share_accepted = submit_result.reject_reason.is_none();
                        let reject_reason_opt = submit_result.reject_reason.as_ref().map(|s| s.as_str());

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
                            reject_reason: reject_reason_opt,
                            node_result: Some(if share_accepted { "accepted" } else { "rejected" }),
                            low_diff_ok: Some(true),
                            network_target_ok: Some(true),
                            block_hash: Some(&submit_block_hash),
                            share_id,
                        });

                        if !share_accepted {
                            share_stats.rejected += 1;
                            warn!(
                                session_id = %session_id,
                                req_id = %req_id,
                                worker_id = worker_row.id,
                                job_id = %job.job_id,
                                reject_reason = %submit_result.reject_reason.as_ref().unwrap_or(&String::new()),
                                stratum_hash_hex = %stratum_hash_hex,
                                submit_block_hash = %submit_result.block_hash.to_hex_be(),
                                computed_hash_hex = %computed_hash_hex,
                                network_target_hex = %job.network_target_hex,
                                header_bytes_hex = %header_bytes_hex,
                                candidate_block_hex = %hex::encode(&candidate_block),
                                accepted = share_stats.accepted,
                                rejected = share_stats.rejected,
                                errored = share_stats.errored,
                                "share rejected by lotusd submit path"
                            );
                            let err = StratumResponse::rejected(req_id.clone(), 20, "block-submit-rejected");
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
                                "{{\"reject_reason\":\"{}\"}}",
                                submit_result.reject_reason.as_ref().unwrap_or(&String::new())
                            )),
                        );
                        if share_accepted {
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
                                    
                                    // Emit block found event for real-time dashboard updates
                                    if events_tx.is_enabled() {
                                        use chrono::Utc;
                                        use crate::http::BlockFoundEvent;
                                        use crate::http::DashboardEvent;
                                        
                                        let event = BlockFoundEvent {
                                            height: job.block_height as i64,
                                            hash: submit_block_hash.clone(),
                                            status: "confirmed".to_string(),
                                            confirmations: 1,
                                            found_by: submit.worker_name.clone(),
                                            payout_address: worker_row.payout_address.clone(),
                                            found_at: Utc::now(),
                                        };
                                        events_tx.send(DashboardEvent::BlockFound(event));
                                    }
                                }
                                Err(err) => {
                                    stats
                                        .found_block_persist_error_total
                                        .fetch_add(1, Ordering::Relaxed);
                                    error!(session_id = %session_id, block_hash = %submit_result.block_hash.to_hex_be(), error = %err, "node accepted solved block but DB persist failed");
                                }
                            }
                        }

                        // With RPC submitblock, we don't have the fine-grained result categories
                        // from NNG SubmitMinedBlockResult. The BIP22 response is binary:
                        // - null = accepted
                        // - string = rejected with reason
                        // For shares that meet pool difficulty but not network target, lotusd
                        // will reject with "high-hash" which we treat as an accepted share.
                        let is_high_hash_share = !share_accepted
                            && submit_result.reject_reason.as_ref().map(|s| s.eq_ignore_ascii_case("high-hash")).unwrap_or(false);

                        if is_high_hash_share {
                            info!(
                                session_id = %session_id,
                                req_id = %req_id,
                                worker_id = worker_row.id,
                                job_id = %job.job_id,
                                template_id = job.template_id,
                                block_hash = %stratum_hash_hex,
                                merkle_root = %stratum_merkle_root_hex,
                                header = %header_debug,
                                template = %template_debug,
                                share_difficulty = share_difficulty,
                                network_difficulty = diff_cache.network_diff(),
                                reject_reason = %submit_result.reject_reason.as_ref().unwrap_or(&String::new()),
                                total_accepted = share_stats.accepted,
                                total_rejected = share_stats.rejected,
                                total_errored = share_stats.errored,
                                "share accepted (met pool difficulty; below network target)"
                            );
                        } else {
                            info!(
                                session_id = %session_id,
                                req_id = %req_id,
                                worker_id = worker_row.id,
                                job_id = %job.job_id,
                                template_id = job.template_id,
                                block_hash = %submit_block_hash,
                                header = %header_debug,
                                template = %template_debug,
                                share_difficulty = share_difficulty,
                                network_difficulty = diff_cache.network_diff(),
                                reject_reason = %submit_result.reject_reason.as_ref().unwrap_or(&String::new()),
                                total_accepted = share_stats.accepted,
                                total_rejected = share_stats.rejected,
                                total_errored = share_stats.errored,
                                "share accepted via proposal+submit flow"
                            );
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
        }
    }

    // Check invalid share ratio and ban if threshold exceeded
    let total_validated = share_stats.accepted + share_stats.rejected;
    if cfg.pool.pplns.banning.enabled
        && total_validated >= cfg.pool.pplns.banning.check_threshold as u64
    {
        let rejection_rate = (share_stats.rejected as f64 / total_validated as f64) * 100.0;
        if rejection_rate > cfg.pool.pplns.banning.invalid_percent {
            warn!(
                session_id = %session_id,
                accepted = share_stats.accepted,
                rejected = share_stats.rejected,
                total_validated,
                rejection_rate = format!("{:.1}%", rejection_rate),
                threshold = cfg.pool.pplns.banning.invalid_percent,
                "miner banned: invalid share ratio exceeded threshold"
            );
            return Err(anyhow!(
                "banned: invalid share ratio {:.1}% > {}%",
                rejection_rate,
                cfg.pool.pplns.banning.invalid_percent
            ));
        }
    }

    info!(session_id = %session_id, accepted = share_stats.accepted, rejected = share_stats.rejected, errored = share_stats.errored, authorized_workers = session.authorized_workers.len(), active_jobs = session.active_jobs.len(), assigned_jobs = assigned_jobs.len(), "stratum session ended");
    Ok(())
}
