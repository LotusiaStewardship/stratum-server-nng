use anyhow::Result;
use crate::share_processing::network_target_hex_to_difficulty;
use crate::share_processing::VarDiffConfig;
use crate::node_integration::JsonRpcClient;
use crate::node_integration::block_builder::build_submit_block;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{broadcast, RwLock};
use tracing::{debug, error, info, warn};

use crate::stratum_protocol::session::SessionState;
use crate::stratum_protocol::job::MiningJob;
use crate::stratum_protocol::protocol::{decode_request_line, Method, StratumResponse};
use crate::accounting::{ShareRepository, AuthorizationEvent, AccountingService};
use crate::share_processing::validator;

/// TCP Stratum V1 server that accepts miner connections.
pub struct StratumServer {
    bind_address: SocketAddr,
    session_counter: Arc<RwLock<u64>>,
    connected_miners: Arc<RwLock<u64>>,
    job_cache: Arc<crate::node_integration::JobCache>,
    shutdown_tx: broadcast::Sender<()>,
    /// Broadcast channel for N_diff (network difficulty) changes.
    /// Each session listens on this to update its VarDiff ceiling.
    n_diff_tx: broadcast::Sender<f64>,
    /// Broadcast channel for new mining jobs (Arc-wrapped to share across sessions).
    /// Connection handlers listen on this and send `mining.notify` to miners.
    job_tx: broadcast::Sender<Arc<MiningJob>>,
    accounting_service: Option<AccountingService>,
    json_rpc_client: Option<Arc<JsonRpcClient>>,
    vardiff_config: VarDiffConfig,
    debug: bool,
}

impl StratumServer {
    /// Create a new Stratum server.
    pub fn new(
        bind_address: SocketAddr,
        job_cache: Arc<crate::node_integration::JobCache>,
        shutdown_tx: broadcast::Sender<()>,
        accounting_service: Option<AccountingService>,
        json_rpc_client: Option<Arc<JsonRpcClient>>,
        vardiff_config: VarDiffConfig,
        debug: bool,
    ) -> Self {
        let (n_diff_tx, _) = broadcast::channel::<f64>(128);
        let (job_tx, _) = broadcast::channel::<Arc<MiningJob>>(128);
        Self {
            bind_address,
            session_counter: Arc::new(RwLock::new(0)),
            connected_miners: Arc::new(RwLock::new(0)),
            job_cache,
            shutdown_tx,
            n_diff_tx,
            job_tx,
            accounting_service,
            json_rpc_client,
            vardiff_config,
            debug,
        }
    }

    /// Notify all active sessions of a change in network difficulty (N_diff).
    /// Each session updates its VarDiff ceiling via `update_max()`, ensuring
    /// the per-session P_diff never exceeds the new N_diff.
    /// Broadcast N_diff to all active sessions, updating each session's VarDiff
    /// ceiling from the job's `network_target_hex`. Each session clamps its P_diff
    /// to ensure P_diff ∈ [vardiff_min_floor, N_diff] at all times.
    ///
    /// **This only broadcasts N_diff** — it does NOT send the job through `job_tx`.
    /// Sessions receive new mining jobs via either:
    ///   - `job_cache.get_latest()` on authorize (startup path)
    ///   - The `job_tx` broadcast channel (NNG event consumer / dynamic path)
    ///
    /// The dynamic path (`job_tx`) independently extracts N_diff from the job's
    /// `network_target_hex` in the `job_rx` handler and calls `vardiff.update_max()`,
    /// so N_diff propagation is guaranteed regardless of the broadcast path.
    ///
    /// Called from main.rs on startup. Not used for dynamic template changes
    /// (those flow through the NNG event consumer → `job_tx`).
    pub async fn notify_new_job(&self, job: &MiningJob) {
        let n_diff = network_target_hex_to_difficulty(&job.network_target_hex)
            .unwrap_or(1.0);
        debug!(
            job_id = %job.job_id,
            n_diff = n_diff,
            "broadcasting new job N_diff to all sessions",
        );
        self.notify_new_template(n_diff).await;
    }

    /// Notify all active sessions of a change in network difficulty (N_diff).
    /// Each session updates its VarDiff ceiling via `update_max()`, ensuring
    /// the per-session P_diff never exceeds the new N_diff.
    ///
    /// This is a lower-level function used internally by `notify_new_job()`.
    /// External callers should prefer `notify_new_job()` or ensure the `job_tx`
    /// handler updates VarDiff from the incoming job's `network_target_hex`.
    pub async fn notify_new_template(&self, new_n_diff: f64) {
        debug!(new_n_diff, "broadcasting N_diff change to all sessions");
        let _ = self.n_diff_tx.send(new_n_diff);
    }

    /// Get a clone of the job broadcast sender (used by NNG event consumer).
    pub fn job_tx(&self) -> broadcast::Sender<Arc<MiningJob>> {
        self.job_tx.clone()
    }

    /// Get the number of connected miners.
    pub async fn connected_miners(&self) -> u64 {
        *self.connected_miners.read().await
    }

    /// Run the TCP server, accepting connections until shutdown.
    pub async fn run(&self) -> Result<()> {
        let listener = TcpListener::bind(self.bind_address).await?;
        info!(bind = %self.bind_address, "Stratum TCP server listening");

        let mut shutdown_rx = self.shutdown_tx.subscribe();

        loop {
            tokio::select! {
                result = listener.accept() => {
                    match result {
                        Ok((stream, addr)) => {
                            debug!(addr = %addr, "new miner connection");
                            let session_id = self.generate_session_id().await;
                            // Compute N_diff from the latest job's network target
                            let n_diff = self
                                .job_cache
                                .get_latest()
                                .await
                                .and_then(|job| network_target_hex_to_difficulty(&job.network_target_hex))
                                .unwrap_or(1.0);
                            let session = SessionState::new(
                                session_id.clone(),
                                self.vardiff_config.clone(),
                                n_diff,
                            );
                            
                            // Clone Arcs for the connection handler
                            let connected_miners = self.connected_miners.clone();
                            let job_cache = self.job_cache.clone();
                            let shutdown_rx = shutdown_rx.resubscribe();
                            let n_diff_rx = self.n_diff_tx.subscribe();
                            let job_rx = self.job_tx.subscribe();
                            let accounting_service = self.accounting_service.clone();
                            let json_rpc_client = self.json_rpc_client.clone();
                            let debug = self.debug;
                            
                            tokio::spawn(async move {
                                // Increment connected miners
                                *connected_miners.write().await += 1;
                                
                                // Handle the connection
                                if let Err(e) = handle_connection(
                                    stream,
                                    session,
                                    job_cache,
                                    shutdown_rx,
                                    n_diff_rx,
                                    job_rx,
                                    accounting_service,
                                    json_rpc_client,
                                    debug,
                                ).await {
                                    warn!(addr = %addr, error = %e, "connection error");
                                }
                                
                                // Decrement connected miners
                                *connected_miners.write().await -= 1;
                            });
                        }
                        Err(e) => {
                            error!(error = %e, "failed to accept connection");
                        }
                    }
                }
                _ = shutdown_rx.recv() => {
                    info!("Stratum server shutting down");
                    break;
                }
            }
        }

        Ok(())
    }

    /// Generate a unique session ID.
    async fn generate_session_id(&self) -> String {
        let mut counter = self.session_counter.write().await;
        *counter += 1;
        format!("sess-{}", *counter)
    }
}

/// Handle a single miner connection.
async fn handle_connection(
    stream: TcpStream,
    mut session: SessionState,
    job_cache: Arc<crate::node_integration::JobCache>,
    mut shutdown_signal: broadcast::Receiver<()>,
    mut n_diff_rx: broadcast::Receiver<f64>,
    mut job_rx: broadcast::Receiver<Arc<MiningJob>>,
    accounting_service: Option<AccountingService>,
    json_rpc_client: Option<Arc<JsonRpcClient>>,
    debug: bool,
) -> Result<()> {
    let (reader, mut writer) = stream.into_split();
    let mut reader = BufReader::new(reader);
    let mut line = String::new();

    loop {
        line.clear();
        
        tokio::select! {
            read_result = reader.read_line(&mut line) => {
                match read_result {
                    Ok(0) => {
                        // EOF - client disconnected
                        debug!(session = %session.session_id, "miner disconnected");
                        break;
                    }
                    Ok(_) => {
                        let trimmed = line.trim();
                        if trimmed.is_empty() {
                            continue;
                        }
                        
                        if debug {
                            info!(
                                session = %session.session_id,
                                line = %trimmed,
                                "verbose: stratum request received",
                            );
                        }
                        debug!(session = %session.session_id, line = %trimmed, "received request");
                        
                        // Parse the request
                        let req = match decode_request_line(trimmed, 4096) {
                            Ok(req) => req,
                            Err(e) => {
                                if debug {
                                    info!(
                                        session = %session.session_id,
                                        error = ?e,
                                        raw = %trimmed,
                                        "verbose: invalid request parse failure",
                                    );
                                }
                                warn!(error = ?e, "invalid request");
                                let resp = StratumResponse::err(
                                    serde_json::Value::Null,
                                    1,
                                    "invalid request",
                                );
                                let resp_line = serde_json::to_string(&resp)?;
                                writer.write_all(resp_line.as_bytes()).await?;
                                writer.write_all(b"\n").await?;
                                continue;
                            }
                        };
                        
                        // Handle the request based on method
                        let resp = match req.method {
                            Method::Subscribe => {
                                session.handle_subscribe(&req)
                            }
                            Method::Authorize => {
                                let auth_resp = session.handle_authorize(&req);
                                // Parse worker name once, use for both auth and event recording
                                let worker_name_str = req.params.as_array()
                                    .and_then(|a| a.first())
                                    .and_then(|v| v.as_str())
                                    .unwrap_or_default();
                                let worker_parsed = crate::stratum_protocol::session::parse_worker_name(worker_name_str)
                                    .unwrap_or_else(|_| crate::stratum_protocol::session::WorkerName {
                                        payout_address: worker_name_str.to_string(),
                                        worker_suffix: None,
                                    });
                                // Record authorization event per UBQ §Authorization Event
                                record_authorization_event(
                                    &req,
                                    &session,
                                    &auth_resp,
                                    &worker_parsed,
                                    accounting_service.as_ref().map(|svc| &svc.share_repo),
                                ).await;
                                auth_resp
                            }
                            Method::Submit => {
                                let (resp, new_diff) = handle_submit(
                                    &req,
                                    &mut session,
                                    &job_cache,
                                    accounting_service.as_ref(),
                                    json_rpc_client.as_ref(),
                                    debug,
                                ).await;
                                // If VarDiff retargeted, send mining.set_difficulty to miner
                                if let Some(diff) = new_diff {
                                    let set_diff = serde_json::json!({
                                        "id": null,
                                        "method": "mining.set_difficulty",
                                        "params": [diff]
                                    });
                                    let set_diff_line = serde_json::to_string(&set_diff)?;
                                    if debug {
                                        info!(
                                            session = %session.session_id,
                                            new_diff = diff,
                                            message = %set_diff_line,
                                            "verbose: mining.set_difficulty after VarDiff retarget",
                                        );
                                    }
                                    writer.write_all(set_diff_line.as_bytes()).await?;
                                    writer.write_all(b"\n").await?;
                                    debug!(
                                        session = %session.session_id,
                                        new_diff = diff,
                                        "sent mining.set_difficulty after retarget",
                                    );
                                }
                                resp
                            }
                            Method::Ping => {
                                StratumResponse::ok(req.id.clone(), serde_json::Value::Bool(true))
                            }
                            Method::SetDifficulty | Method::ExtranonceSubscribe | Method::SetExtranonce | Method::SuggestDifficulty => {
                                if debug {
                                    info!(
                                        session = %session.session_id,
                                        method = ?req.method,
                                        "verbose: unsupported method received from miner",
                                    );
                                }
                                debug!(method = ?req.method, "unsupported method");
                                StratumResponse::err(req.id.clone(), 3, "unknown method")
                            }
                            Method::Notify => {
                                // mining.notify is server-to-miner only
                                if debug {
                                    info!(
                                        session = %session.session_id,
                                        "verbose: mining.notify received from miner (should be server-to-miner)",
                                    );
                                }
                                warn!("received mining.notify from miner (should be server-to-miner)");
                                StratumResponse::err(req.id.clone(), 3, "unknown method")
                            }
                            Method::Unknown(ref method_name) => {
                                if debug {
                                    info!(
                                        session = %session.session_id,
                                        method = %method_name,
                                        "verbose: unknown method received from miner",
                                    );
                                }
                                StratumResponse::err(req.id.clone(), 3, "unknown method")
                            }
                        };
                        
                        // Send response
                        let resp_line = serde_json::to_string(&resp)?;
                        if debug {
                            info!(
                                session = %session.session_id,
                                method = ?req.method,
                                response = %resp_line,
                                "verbose: stratum response sent",
                            );
                        }
                        writer.write_all(resp_line.as_bytes()).await?;
                        writer.write_all(b"\n").await?;
                        
                        // If authorized, send mining.set_difficulty with current P_diff
                        // then mining.notify with current job, and record assigned job
                        if session.is_authorized && req.method == Method::Authorize {
                            // Send initial mining.set_difficulty so the miner knows its difficulty
                            let initial_diff = session.current_difficulty();
                            let set_diff_cmd = serde_json::json!({
                                "id": null,
                                "method": "mining.set_difficulty",
                                "params": [initial_diff]
                            });
                            let set_diff_line = serde_json::to_string(&set_diff_cmd)?;
                            if debug {
                                info!(
                                    session = %session.session_id,
                                    initial_diff = initial_diff,
                                    message = %set_diff_line,
                                    "verbose: mining.set_difficulty on session start",
                                );
                            }
                            writer.write_all(set_diff_line.as_bytes()).await?;
                            writer.write_all(b"\n").await?;
                            debug!(
                                session = %session.session_id,
                                initial_diff = initial_diff,
                                "sent mining.set_difficulty on session start",
                            );
                            
                            if let Some(job) = job_cache.get_latest().await {
                                let notify = create_notify(&job, &session.session_id);
                                let notify_line = serde_json::to_string(&notify)?;
                                if debug {
                                    info!(
                                        session = %session.session_id,
                                        job_id = %job.job_id,
                                        message = %notify_line,
                                        "verbose: mining.notify on session start",
                                    );
                                }
                                writer.write_all(notify_line.as_bytes()).await?;
                                writer.write_all(b"\n").await?;
                                
                                // Record assigned job with current P_diff and frozen ntime
                                // Per UBQ: P_diff at assignment time becomes share difficulty
                                let p_diff = session.current_difficulty();
                                session.record_assigned_job(
                                    job.job_id.clone(),
                                    p_diff,
                                    job.ntime.clone(),
                                );
                                debug!(
                                    session = %session.session_id,
                                    job = %job.job_id,
                                    p_diff = p_diff,
                                    ntime = %job.ntime,
                                    "sent mining.notify and recorded assigned job",
                                );
                            }
                        }
                    }
                    Err(e) => {
                        error!(error = %e, "read error");
                        break;
                    }
                }
            }
            _ = shutdown_signal.recv() => {
                info!(session = %session.session_id, "shutting down connection");
                break;
            }
            n_diff = n_diff_rx.recv() => {
                if let Ok(new_n_diff) = n_diff {
                    if debug {
                        info!(
                            session = %session.session_id,
                            new_n_diff = new_n_diff,
                            "verbose: N_diff change received",
                        );
                    }
                    debug!(
                        session = %session.session_id,
                        new_n_diff = new_n_diff,
                        "updating VarDiff ceiling due to N_diff change",
                    );
                    // If clamp lowered P_diff, notify the miner immediately
                    // so it doesn't submit with an outdated difficulty.
                    if let Some(clamped_diff) = session.vardiff.update_max(new_n_diff) {
                        let set_diff = serde_json::json!({
                            "id": null,
                            "method": "mining.set_difficulty",
                            "params": [clamped_diff]
                        });
                        let set_diff_line = serde_json::to_string(&set_diff)?;
                        if debug {
                            info!(
                                session = %session.session_id,
                                clamped_diff = clamped_diff,
                                message = %set_diff_line,
                                "verbose: mining.set_difficulty after N_diff clamp",
                            );
                        }
                        writer.write_all(set_diff_line.as_bytes()).await?;
                        writer.write_all(b"\n").await?;
                        debug!(
                            session = %session.session_id,
                            clamped_diff = clamped_diff,
                            "sent mining.set_difficulty after N_diff clamp",
                        );
                    }
                }
            }
            new_job = job_rx.recv() => {
                if let Ok(job) = new_job {
                    if debug {
                        info!(
                            session = %session.session_id,
                            job_id = %job.job_id,
                            clean_jobs = job.clean_jobs,
                            template_epoch = job.template_epoch,
                            height = job.height,
                            "verbose: new job received via broadcast",
                        );
                    }

                    // Per UBQ §Pool Difficulty: P_diff ∈ [vardiff_min_floor, N_diff] at all times.
                    // When a new template arrives (via NNG event consumer -> job_tx), the N_diff
                    // ceiling must be updated before recording any new assigned jobs.
                    if let Some(n_diff) = network_target_hex_to_difficulty(&job.network_target_hex) {
                        if let Some(clamped_diff) = session.vardiff.update_max(n_diff) {
                            let set_diff = serde_json::json!({
                                "id": null,
                                "method": "mining.set_difficulty",
                                "params": [clamped_diff]
                            });
                            let set_diff_line = serde_json::to_string(&set_diff)?;
                            if debug {
                                info!(
                                    session = %session.session_id,
                                    clamped_diff = clamped_diff,
                                    message = %set_diff_line,
                                    "verbose: mining.set_difficulty after N_diff ceiling update",
                                );
                            }
                            writer.write_all(set_diff_line.as_bytes()).await?;
                            writer.write_all(b"\n").await?;
                            debug!(
                                session = %session.session_id,
                                clamped_diff = clamped_diff,
                                "sent mining.set_difficulty after N_diff ceiling update",
                            );
                        }
                    }

                    // Per UBQ: clean_jobs=true — ALL previous jobs become stale immediately.
                    // Only clear when the job signals clean_jobs on the wire, so server
                    // behavior stays aligned with the wire-level protocol signal.
                    if job.clean_jobs {
                        if debug {
                            info!(
                                session = %session.session_id,
                                job_id = %job.job_id,
                                "verbose: clean_jobs=true — clearing assigned jobs",
                            );
                        }
                        debug!(
                            session = %session.session_id,
                            job_id = %job.job_id,
                            "clean_jobs=true — clearing assigned jobs",
                        );
                        session.clear_assigned_jobs();
                    } else {
                        debug!(
                            session = %session.session_id,
                            job_id = %job.job_id,
                            "clean_jobs=false — preserving assigned jobs",
                        );
                    }

                    // Send mining.notify to the miner
                    let notify = create_notify(&job, &session.session_id);
                    let notify_line = serde_json::to_string(&notify)?;
                    if debug {
                        info!(
                            session = %session.session_id,
                            job_id = %job.job_id,
                            message = %notify_line,
                            "verbose: mining.notify on new job",
                        );
                    }
                    writer.write_all(notify_line.as_bytes()).await?;
                    writer.write_all(b"\n").await?;

                    // Record the new assigned job with current P_diff and frozen ntime
                    let p_diff = session.current_difficulty();
                    session.record_assigned_job(
                        job.job_id.clone(),
                        p_diff,
                        job.ntime.clone(),
                    );
                    debug!(
                        session = %session.session_id,
                        job = %job.job_id,
                        p_diff = p_diff,
                        "sent mining.notify and recorded assigned job",
                    );
                }
            }
        }
    }

    Ok(())
}

/// Create a mining.notify message.
fn create_notify(job: &MiningJob, _session_id: &str) -> serde_json::Value {
    serde_json::json!({
        "id": null,
        "method": "mining.notify",
        "params": job.notify_params()
    })
}

/// Record an authorization event (immutable audit log per UBQ).
/// Every mining.authorize attempt produces one record, regardless of success/failure.
/// Takes a pre-parsed `WorkerName` to avoid redundant parsing with the caller.
async fn record_authorization_event(
    req: &crate::stratum_protocol::protocol::StratumRequest,
    session: &SessionState,
    auth_resp: &StratumResponse,
    worker_parsed: &crate::stratum_protocol::session::WorkerName,
    share_repo: Option<&ShareRepository>,
) {
    let worker_name = req.params.as_array()
        .and_then(|a| a.first())
        .and_then(|v| v.as_str())
        .unwrap_or_default()
        .to_string();

    let authorized = auth_resp.error.is_null();
    let reason = if !authorized {
        auth_resp.error.as_array().and_then(|a| a.get(1)).and_then(|v| v.as_str()).map(|s| s.to_string())
    } else {
        None
    };

    if let Some(repo) = share_repo {
        let event = AuthorizationEvent {
            id: 0,
            session_id: session.session_id.clone(),
            worker_name,
            payout_address: worker_parsed.payout_address.clone(),
            worker_suffix: worker_parsed.worker_suffix.clone(),
            authorized,
            reason,
        };
        if let Err(e) = repo.insert_authorization_event(&event) {
            warn!(error = %e, "failed to record authorization event");
        }
    }
}

/// Handle a mining.submit request: validate, persist share + outcome atomically.
///
/// Per UBQ: inserts into both `shares` (raw submission) and `share_outcomes` (validation result).
/// The validation pipeline checks format, authorization, staleness, ntime-mismatch, and difficulty.
///
/// Returns the protocol response and an optional new P_diff if VarDiff retargeted.
/// When `Some(new_diff)` is returned, the caller should send `mining.set_difficulty` to the miner.
async fn handle_submit(
    req: &crate::stratum_protocol::protocol::StratumRequest,
    session: &mut SessionState,
    job_cache: &crate::node_integration::JobCache,
    accounting_service: Option<&AccountingService>,
    json_rpc_client: Option<&Arc<JsonRpcClient>>,
    debug: bool,
) -> (StratumResponse, Option<f64>) {
    // Fast path: reject unsubscribed miners before any validation or persistence.
    if !session.is_subscribed {
        return (StratumResponse::err(req.id.clone(), 25, "not-subscribed"), None);
    }

    // Extract submit parameters
    let arr = req.params.as_array().cloned().unwrap_or_default();
    let worker_name = arr.first().and_then(|v| v.as_str()).unwrap_or_default();
    let job_id = arr.get(1).and_then(|v| v.as_str()).unwrap_or_default();
    let extranonce2 = arr.get(2).and_then(|v| v.as_str()).unwrap_or_default();
    let ntime_hex = arr.get(3).and_then(|v| v.as_str()).unwrap_or_default();
    let nonce_hex = arr.get(4).and_then(|v| v.as_str()).unwrap_or_default();

    // Get the job from the cache to get template data for header building.
    // Keep the job for potential block submission if network_target_ok=true.
    let cached_job = job_cache.get(job_id).await;
    let (validation, template_id, template_epoch, share_diff) =
        if let Some(ref job) = cached_job {
            // Run the full validation pipeline
            let validation = validator::validate_share(
                worker_name,
                job_id,
                extranonce2,
                ntime_hex,
                nonce_hex,
                session,
                job,
                debug,
            );
            (
                validation,
                job.template_id as i64,
                job.template_epoch as i64,
                // Per UBQ: share difficulty = P_diff at assignment time, not submission time.
                // Look up the specific assigned job's P_diff rather than using the latest.
                session.get_assigned_job(job_id)
                    .map(|a| a.p_diff)
                    .unwrap_or_else(|| session.current_difficulty()),
            )
        } else if let Some(assigned) = session.get_assigned_job(job_id) {
            // Job evicted from JobCache but still in session's assigned_jobs.
            // Can't run full validation without header data, so reject as
            // stale-job. Recover template_id/epoch from job_id format for
            // accurate dedupe-key construction in persistence.
            let (recovered_tid, recovered_epoch) =
                crate::stratum_protocol::job::parse_template_metadata_from_job_id(job_id)
                    .unwrap_or((0i64, 0i64));
            (
                validator::ValidationResult::rejected("stale-job"),
                recovered_tid,
                recovered_epoch,
                assigned.p_diff,
            )
        } else {
            // Job not in cache and not in assigned_jobs — truly stale/unknown.
            (
                validator::ValidationResult::rejected("stale-job"),
                0i64,
                0i64,
                session.current_difficulty(),
            )
        };

    // Verbose share submission details
    if debug {
        info!(
            session = %session.session_id,
            worker = %worker_name,
            job_id = %job_id,
            extranonce2 = %extranonce2,
            ntime = %ntime_hex,
            nonce = %nonce_hex,
            accepted = validation.accepted,
            reject_reason = ?validation.reject_reason,
            low_diff_ok = validation.low_diff_ok,
            network_target_ok = validation.network_target_ok,
            block_hash = ?validation.block_hash,
            p_diff = share_diff,
            template_id = template_id,
            template_epoch = template_epoch,
            "verbose: share submission details",
        );
    }

    // Persist share + outcome via AccountingService (records accounting events too).
    // This runs for ALL submissions (accepted or rejected) per UBQ.
    // AccountingService handles worker upsert, round resolution, dedupe key, and
    // atomic share+outcome insert with accounting event recording.
    // The returned dedupe_key has the real worker_id (not a placeholder).
    let mut actual_dedupe_key = String::new();

    if let Some(acct) = accounting_service {
        if let Ok(worker_parsed) =
            crate::stratum_protocol::session::parse_worker_name(worker_name)
        {
            match acct.record_share(
                &worker_parsed.payout_address,
                worker_parsed.worker_suffix.as_deref(),
                &session.session_id,
                job_id,
                template_id,
                template_epoch,
                extranonce2,
                ntime_hex,
                nonce_hex,
                share_diff,
                if validation.accepted { "accepted" } else { "rejected" },
                validation.reject_reason.as_deref(),
                validation.low_diff_ok,
                validation.network_target_ok,
                validation.block_hash.as_deref(),
            ) {
                Ok((_, _, _, dedupe_key)) => {
                    actual_dedupe_key = dedupe_key;
                }
                Err(e) => {
                    warn!(error = %e, "failed to persist share via AccountingService");
                }
            }
        }
    }

    if debug && validation.network_target_ok {
        info!(
            block_hash = ?validation.block_hash,
            block_bytes_len = cached_job.as_ref().map(|j| j.block_bytes.len()),
            "verbose: block candidate detected, preparing submission",
        );
    }

    // Block submission: if the share meets N_diff, submit to lotusd via JSON-RPC.
    // This is best-effort: the share is already accepted; submission failure does
    // not reject the share. The node_result field captures the submission outcome.
    if validation.network_target_ok {
        if let (Some(job), Some(json_rpc)) = (&cached_job, json_rpc_client) {
            if !job.block_bytes.is_empty() {
                match build_submit_block(
                    job,
                    &session.extranonce1,
                    extranonce2,
                    ntime_hex,
                    nonce_hex,
                    &job.block_bytes,
                ) {
                    Ok(block_hex) => {
                        if debug {
                            info!(
                                block_hash = ?validation.block_hash,
                                block_hex_len = block_hex.len(),
                                "verbose: block built, submitting to lotusd",
                            );
                        }
                        let submit_result = json_rpc.submitblock(&block_hex).await;
                        match submit_result {
                            Ok(result) if result.accepted => {
                                if debug {
                                    info!(
                                        block_hash = ?validation.block_hash,
                                        "verbose: block accepted by lotusd",
                                    );
                                }
                                debug!(
                                    block_hash = ?validation.block_hash,
                                    "block accepted by lotusd"
                                );
                                if let Some(acct) = accounting_service {
                                    if let Some(block_hash) = &validation.block_hash {
                                        // Resolve the actual round for this template (not template_id as round_id)
                                        if let Ok(round) = acct.resolve_round_for_template(template_id) {
                                            let _ = acct.record_found_block(
                                                round.id,
                                                block_hash,
                                                job.height as i64,
                                                None,
                                                Some(job.template_id as i64),
                                                Some("json-rpc"),
                                            );
                                            // Close the round: transition from 'open' to 'found'
                                            let _ = acct.close_round(round.id, template_id, "found");
                                        } else {
                                            warn!(template_id, "failed to resolve round for found block");
                                        }
                                    }
                                    let _ = acct.update_share_outcome_node_result(
                                        &actual_dedupe_key, "accepted"
                                    );
                                }
                            }
                            Ok(result) => {
                                if debug {
                                    info!(
                                        block_hash = ?validation.block_hash,
                                        error = ?result.error,
                                        "verbose: block rejected by lotusd",
                                    );
                                }
                                debug!(
                                    error = ?result.error,
                                    "block rejected by lotusd"
                                );
                                if let Some(acct) = accounting_service {
                                    let reason = result.error.unwrap_or_else(|| "unknown".to_string());
                                    let _ = acct.update_share_outcome_node_result(
                                        &actual_dedupe_key, &format!("rejected: {}", reason)
                                    );
                                }
                            }
                            Err(e) => {
                                if debug {
                                    info!(
                                        block_hash = ?validation.block_hash,
                                        error = %e,
                                        "verbose: block submission to lotusd failed",
                                    );
                                }
                                warn!(error = %e, "failed to submit block to lotusd (best-effort)");
                            }
                        }
                    }
                    Err(e) => {
                        if debug {
                            info!(
                                error = %e,
                                "verbose: failed to build submit block",
                            );
                        }
                        warn!(error = %e, "failed to build submit block");
                    }
                }
            }
        }
    }

    // Record accepted share in VarDiff and check for retarget.
    // Only record timestamps for valid shares that reached validation.
    let new_diff = if validation.accepted {
        let now = Instant::now();
        session.vardiff.record_share(now);
        session.vardiff.maybe_retarget(now)
    } else {
        None
    };

    // Return protocol response based on validation result
    let response = if validation.accepted {
        StratumResponse::ok(req.id.clone(), serde_json::Value::Bool(true))
    } else {
        let (code, reason) = match validation.reject_reason.as_deref() {
            Some("unauthorized-worker") => (24, "unauthorized-worker"),
            Some("stale-job") => (22, "stale-job"),
            Some("ntime-mismatch") => (21, "ntime-mismatch"),
            Some("low-difficulty-share") => (23, "low-difficulty-share"),
            _ => (20, "invalid-submit-shape"),
        };
        StratumResponse::rejected(req.id.clone(), code, reason)
    };

    (response, new_diff)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node_integration::JobCache;
    use std::net::SocketAddr;
    use std::time::Duration;
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    use tokio::net::TcpStream;

    fn create_test_job() -> MiningJob {
        // Real-world lotusd template data at height 1292529
        MiningJob {
            job_id: "job-890-100".to_string(),
            template_id: 890,
            prevhash: "4f7bcee63a20eff92f69a7f0e74af36a9f1e60ee7ecc5b0506e1ae3600000000".to_string(),
            coinbase1: "02000000010000000000000000000000000000000000000000000000000000000000000000ffffffff1900000e2f4c6f747573696120506f6f6c2f".to_string(),
            coinbase2: "ffffffff0300000000000000000b6a056c6f676f7303f1b8137ecf360d000000001976a914ad8b796954a46f0f32a867d3fd8855043cc506ba88ac7ecf360d000000001976a914053d4d0c28d299dc5c2be1ce5d29bf00cdb61b4088ac00000000".to_string(),
            merkle_branches: vec![
                "796f6be745741765f8b19cfa4209ff68447d9e76198fee5d33fbe2c944224f16".to_string(),
                "4b0ce2ddbf0f5352b721b7688109a1e1007722f96fa07f61ea8e655ac804964f".to_string(),
                "c3899f315bc3b284015819a8d77404b4e179528d62559886babf89884966a172".to_string(),
            ],
            version: "00000001".to_string(),
            nbits: "10d0091c".to_string(),
            ntime: "6adc0c6a0000".to_string(),
            network_target_hex: "0000000009d01000000000000000000000000000000000000000000000000000".to_string(),
            clean_jobs: false,
            template_epoch: 100,
            height: 1292529,
            epoch_hash: "00000000061fb84d2a1d30d8767f629a08904b0e70f84587008fd9e91f1583f7".to_string(),
            extended_metadata_hash: "9a538906e6466ebd2617d321f71bc94e56056ce213d366773699e28158e00614".to_string(),
            block_size: 2588,
            block_bytes: vec![],
        }
    }

    #[tokio::test]
    async fn test_notify_new_job_broadcasts() {
        let job_cache = Arc::new(JobCache::new(10));
        let job = create_test_job();
        job_cache.insert(job.clone()).await;
        
        let (shutdown_tx, _) = broadcast::channel::<()>(10);
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let server = StratumServer::new(addr, job_cache, shutdown_tx.clone(), None, None, VarDiffConfig::default(), false);
        
        // Should not panic or error
        server.notify_new_job(&job).await;
    }

    #[tokio::test]
    async fn test_server_starts_and_accepts_connections() {
        let job_cache = Arc::new(JobCache::new(10));
        job_cache.insert(create_test_job()).await;
        
        let (shutdown_tx, _) = broadcast::channel::<()>(10);
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let server = StratumServer::new(addr, job_cache, shutdown_tx.clone(), None, None, VarDiffConfig::default(), false);
        
        // Server should start without error
        assert_eq!(server.connected_miners().await, 0);
    }

    #[tokio::test]
    async fn test_session_id_generation() {
        let job_cache = Arc::new(JobCache::new(10));
        let (shutdown_tx, _) = broadcast::channel::<()>(10);
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let server = StratumServer::new(addr, job_cache, shutdown_tx.clone(), None, None, VarDiffConfig::default(), false);
        
        let id1 = server.generate_session_id().await;
        let id2 = server.generate_session_id().await;
        let id3 = server.generate_session_id().await;
        
        assert_eq!(id1, "sess-1");
        assert_eq!(id2, "sess-2");
        assert_eq!(id3, "sess-3");
    }

    #[tokio::test]
    async fn test_create_notify() {
        let job = create_test_job();
        let notify = create_notify(&job, "sess-1");
        
        assert_eq!(notify["method"], "mining.notify");
        assert_eq!(notify["id"], serde_json::Value::Null);
        
        let params = notify["params"].as_array().unwrap();
        assert_eq!(params[0], "job-890-100"); // job_id with epoch
        assert_eq!(params[1], job.prevhash); // prevhash
        assert_eq!(params.len(), 13); // 9 standard + 4 Lotus extension params
    }

    /// Integration test: full subscribe → authorize → submit flow
    #[tokio::test]
    async fn test_integration_subscribe_authorize_submit() {
        // Start server
        let job_cache = Arc::new(JobCache::new(10));
        job_cache.insert(create_test_job()).await;
        
        let (shutdown_tx, _) = broadcast::channel::<()>(10);
        let addr: SocketAddr = "127.0.0.1:13334".parse().unwrap();
        let server = StratumServer::new(addr, job_cache.clone(), shutdown_tx.clone(), None, None, VarDiffConfig::default(), false);
        
        let server_handle = tokio::spawn(async move {
            server.run().await
        });
        
        tokio::time::sleep(Duration::from_millis(100)).await;
        
        let stream = TcpStream::connect("127.0.0.1:13334").await.unwrap();
        let (read_half, mut write_half) = stream.into_split();
        let mut reader = BufReader::new(read_half);
        
        // Subscribe
        write_half.write_all(b"{\"id\":1,\"method\":\"mining.subscribe\",\"params\":[]}\n").await.unwrap();
        let mut response = String::new();
        reader.read_line(&mut response).await.unwrap();
        let resp: serde_json::Value = serde_json::from_str(&response).unwrap();
        assert!(resp["error"].is_null());
        assert_eq!(resp["result"].as_array().unwrap().len(), 3);
        
        // Authorize
        write_half.write_all(b"{\"id\":2,\"method\":\"mining.authorize\",\"params\":[\"lotus_16PSJNf1EDEfGvaYzaXJCJZrXH4pgiTo7kyW61iGi.rig\",\"x\"]}\n").await.unwrap();
        response.clear();
        reader.read_line(&mut response).await.unwrap();
        let resp: serde_json::Value = serde_json::from_str(&response).unwrap();
        assert!(resp["error"].is_null());
        assert_eq!(resp["result"], serde_json::Value::Bool(true));
        
        // Receive mining.set_difficulty after authorize
        response.clear();
        tokio::time::timeout(Duration::from_millis(100), reader.read_line(&mut response))
            .await
            .expect("should receive mining.set_difficulty")
            .unwrap();
        let set_diff: serde_json::Value = serde_json::from_str(&response.trim()).unwrap();
        assert_eq!(set_diff["method"], "mining.set_difficulty");
        let diff_val = set_diff["params"][0].as_f64().unwrap();
        assert!(diff_val > 0.0, "initial P_diff should be positive, got {}", diff_val);
        
        // Receive mining.notify after set_difficulty
        response.clear();
        tokio::time::timeout(Duration::from_millis(100), reader.read_line(&mut response))
            .await
            .expect("should receive mining.notify")
            .unwrap();
        let notify: serde_json::Value = serde_json::from_str(&response.trim()).unwrap();
        assert_eq!(notify["method"], "mining.notify");
        assert_eq!(notify["params"][0], "job-890-100");
        
        // Submit with correct params. Share may be accepted or rejected as
        // low-difficulty — both are valid pipeline outcomes.
        write_half.write_all(b"{\"id\":3,\"method\":\"mining.submit\",\"params\":[\"lotus_16PSJNf1EDEfGvaYzaXJCJZrXH4pgiTo7kyW61iGi.rig\",\"job-890-100\",\"00000003\",\"6adc0c6a0000\",\"B02B4ABB3DD6E835\"]}\n").await.unwrap();
        
        response.clear();
        reader.read_line(&mut response).await.unwrap();
        let resp: serde_json::Value = serde_json::from_str(&response).unwrap();
        assert!(!resp.get("result").is_none(), "expected a result field in response");
        
        shutdown_tx.send(()).unwrap();
        let _ = tokio::time::timeout(Duration::from_secs(1), server_handle).await;
    }

    // Note: A TCP-level VarDiff retarget test (verifying mining.set_difficulty
    // is sent after fast shares) is impractical because random share submissions
    // have ~10^-9 probability of passing P_diff=0.26 validation. The VarDiff
    // algorithm is thoroughly covered by 10+ unit tests in difficulty.rs.
    // The initial mining.set_difficulty on authorize is verified in
    // test_integration_subscribe_authorize_submit above.

    #[tokio::test]
    async fn test_authorize_requires_subscribe() {
        let job_cache = Arc::new(JobCache::new(10));
        job_cache.insert(create_test_job()).await;
        
        let (shutdown_tx, _) = broadcast::channel::<()>(10);
        let addr: SocketAddr = "127.0.0.1:13335".parse().unwrap();
        let server = StratumServer::new(addr, job_cache.clone(), shutdown_tx.clone(), None, None, VarDiffConfig::default(), false);
        
        let server_handle = tokio::spawn(async move {
            server.run().await
        });
        
        tokio::time::sleep(Duration::from_millis(100)).await;
        
        let stream = TcpStream::connect("127.0.0.1:13335").await.unwrap();
        let (read_half, mut write_half) = stream.into_split();
        let mut reader = BufReader::new(read_half);
        
        write_half.write_all(b"{\"id\":1,\"method\":\"mining.authorize\",\"params\":[\"lotus_16PSJNf1EDEfGvaYzaXJCJZrXH4pgiTo7kyW61iGi.rig\",\"x\"]}\n").await.unwrap();
        
        let mut response = String::new();
        reader.read_line(&mut response).await.unwrap();
        let resp: serde_json::Value = serde_json::from_str(&response).unwrap();
        
        assert!(!resp["error"].is_null());
        assert_eq!(resp["error"].as_array().unwrap()[1], "not-subscribed");
        
        shutdown_tx.send(()).unwrap();
        let _ = tokio::time::timeout(Duration::from_secs(1), server_handle).await;
    }

    #[tokio::test]
    async fn test_invalid_json_request() {
        let job_cache = Arc::new(JobCache::new(10));
        job_cache.insert(create_test_job()).await;
        
        let (shutdown_tx, _) = broadcast::channel::<()>(10);
        let addr: SocketAddr = "127.0.0.1:13336".parse().unwrap();
        let server = StratumServer::new(addr, job_cache.clone(), shutdown_tx.clone(), None, None, VarDiffConfig::default(), false);
        
        let server_handle = tokio::spawn(async move {
            server.run().await
        });
        
        tokio::time::sleep(Duration::from_millis(100)).await;
        
        let stream = TcpStream::connect("127.0.0.1:13336").await.unwrap();
        let (read_half, mut write_half) = stream.into_split();
        let mut reader = BufReader::new(read_half);
        
        write_half.write_all(b"not valid json\n").await.unwrap();
        
        let mut response = String::new();
        reader.read_line(&mut response).await.unwrap();
        let resp: serde_json::Value = serde_json::from_str(&response).unwrap();
        
        assert!(!resp["error"].is_null());
        
        shutdown_tx.send(()).unwrap();
        let _ = tokio::time::timeout(Duration::from_secs(1), server_handle).await;
    }

    /// Integration test: verify shares persisted during graceful shutdown
    #[tokio::test]
    async fn test_graceful_shutdown_persists_in_flight_shares() {
        use tempfile::NamedTempFile;
        use crate::accounting::{init_schema, AccountingService, ShareRepository};
        use parking_lot::Mutex;
        use rusqlite::Connection;

        let temp_file = NamedTempFile::new().unwrap();
        let db_path = temp_file.path().to_str().unwrap().to_string();
        let db_conn = Connection::open(&db_path).unwrap();
        init_schema(&db_conn).unwrap();
        let db_conn_arc = Arc::new(Mutex::new(db_conn));
        let accounting_svc = AccountingService::new(db_conn_arc.clone());

        let job_cache = Arc::new(JobCache::new(10));
        job_cache.insert(create_test_job()).await;
        
        let (shutdown_tx, _) = broadcast::channel::<()>(10);
        let addr: SocketAddr = "127.0.0.1:13337".parse().unwrap();
        let server = StratumServer::new(
            addr,
            job_cache.clone(),
            shutdown_tx.clone(),
            Some(accounting_svc),
            None,
            VarDiffConfig::default(),
            false,
        );
        
        let server_handle = tokio::spawn(async move {
            server.run().await
        });
        
        tokio::time::sleep(Duration::from_millis(100)).await;
        
        let stream = TcpStream::connect("127.0.0.1:13337").await.unwrap();
        let (read_half, mut write_half) = stream.into_split();
        let mut reader = BufReader::new(read_half);
        
        // Subscribe
        write_half.write_all(b"{\"id\":1,\"method\":\"mining.subscribe\",\"params\":[]}\n").await.unwrap();
        let mut response = String::new();
        reader.read_line(&mut response).await.unwrap();
        
        // Authorize
        write_half.write_all(b"{\"id\":2,\"method\":\"mining.authorize\",\"params\":[\"lotus_16PSJNf1EDEfGvaYzaXJCJZrXH4pgiTo7kyW61iGi.rig\",\"x\"]}\n").await.unwrap();
        response.clear();
        reader.read_line(&mut response).await.unwrap();
        
        // Read mining.set_difficulty (sent after authorize)
        response.clear();
        reader.read_line(&mut response).await.unwrap();
        
        // Wait for mining.notify
        response.clear();
        tokio::time::timeout(Duration::from_millis(100), reader.read_line(&mut response))
            .await
            .expect("should receive mining.notify")
            .unwrap();
        
        // Submit share with correct params. Share may be accepted or rejected;
        // the key assertion is that the pipeline runs and persists the outcome.
        write_half.write_all(b"{\"id\":3,\"method\":\"mining.submit\",\"params\":[\"lotus_16PSJNf1EDEfGvaYzaXJCJZrXH4pgiTo7kyW61iGi.rig\",\"job-890-100\",\"00000003\",\"6adc0c6a0000\",\"B02B4ABB3DD6E835\"]}\n").await.unwrap();
        
        response.clear();
        reader.read_line(&mut response).await.unwrap();
        let resp: serde_json::Value = serde_json::from_str(&response).unwrap();
        assert!(!resp.get("result").is_none(), "expected a result field in response");
        
        shutdown_tx.send(()).unwrap();
        let _ = tokio::time::timeout(Duration::from_secs(5), server_handle).await;
        
        // Verify both share and share_outcome were persisted
        let share_repo = ShareRepository::new(db_conn_arc.clone());
        let total_shares = share_repo.total_count().unwrap();
        let total_outcomes = share_repo.total_outcome_count().unwrap();
        
        assert_eq!(total_shares, 1, "raw share should be persisted");
        assert_eq!(total_outcomes, 1, "share outcome should be persisted");
    }

    /// Integration test: verify authorizing records an authorization event
    #[tokio::test]
    async fn test_authorization_event_recorded() {
        use tempfile::NamedTempFile;
        use crate::accounting::{init_schema, AccountingService};
        use parking_lot::Mutex;
        use rusqlite::Connection;

        let temp_file = NamedTempFile::new().unwrap();
        let db_conn = Connection::open(temp_file.path()).unwrap();
        init_schema(&db_conn).unwrap();
        let db_conn_arc = Arc::new(Mutex::new(db_conn));
        let accounting_svc = AccountingService::new(db_conn_arc.clone());

        let job_cache = Arc::new(JobCache::new(10));
        job_cache.insert(create_test_job()).await;
        
        let (shutdown_tx, _) = broadcast::channel::<()>(10);
        let addr: SocketAddr = "127.0.0.1:13338".parse().unwrap();
        let server = StratumServer::new(
            addr,
            job_cache.clone(),
            shutdown_tx.clone(),
            Some(accounting_svc),
            None,
            VarDiffConfig::default(),
            false,
        );
        
        let server_handle = tokio::spawn(async move {
            server.run().await
        });
        
        tokio::time::sleep(Duration::from_millis(100)).await;
        
        let stream = TcpStream::connect("127.0.0.1:13338").await.unwrap();
        let (read_half, mut write_half) = stream.into_split();
        let mut reader = BufReader::new(read_half);
        
        // Subscribe
        write_half.write_all(b"{\"id\":1,\"method\":\"mining.subscribe\",\"params\":[]}\n").await.unwrap();
        let mut response = String::new();
        reader.read_line(&mut response).await.unwrap();
        
        // Authorize with valid worker
        write_half.write_all(b"{\"id\":2,\"method\":\"mining.authorize\",\"params\":[\"lotus_16PSJNf1EDEfGvaYzaXJCJZrXH4pgiTo7kyW61iGi.rig\",\"x\"]}\n").await.unwrap();
        response.clear();
        reader.read_line(&mut response).await.unwrap();
        
        // Read mining.set_difficulty (sent after authorize)
        response.clear();
        reader.read_line(&mut response).await.unwrap();
        
        // Wait for mining.notify
        response.clear();
        tokio::time::timeout(Duration::from_millis(100), reader.read_line(&mut response))
            .await
            .expect("should receive mining.notify")
            .unwrap();
        
        // Shutdown
        shutdown_tx.send(()).unwrap();
        let _ = tokio::time::timeout(Duration::from_secs(5), server_handle).await;
        
        // Verify auth event was recorded
        // Query the authorization_events table directly
        let conn = db_conn_arc.lock();
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM authorization_events WHERE session_id = 'sess-1'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 1, "authorization event should be recorded");
    }

    /// Integration test: rejected share (unauthorized worker) persists share_outcome.
    #[tokio::test]
    async fn test_rejected_share_unauthorized_persists_outcome() {
        use tempfile::NamedTempFile;
        use crate::accounting::{init_schema, AccountingService};
        use parking_lot::Mutex;
        use rusqlite::Connection;

        let temp_file = NamedTempFile::new().unwrap();
        let db_conn = Connection::open(temp_file.path()).unwrap();
        init_schema(&db_conn).unwrap();
        let db_conn_arc = Arc::new(Mutex::new(db_conn));
        let accounting_svc = AccountingService::new(db_conn_arc.clone());

        let job_cache = Arc::new(JobCache::new(10));
        job_cache.insert(create_test_job()).await;

        let (shutdown_tx, _) = broadcast::channel::<()>(10);
        let addr: SocketAddr = "127.0.0.1:13340".parse().unwrap();
        let server = StratumServer::new(
            addr,
            job_cache.clone(),
            shutdown_tx.clone(),
            Some(accounting_svc),
            None,
            VarDiffConfig::default(),
            false,
        );

        let server_handle = tokio::spawn(async move {
            server.run().await
        });

        tokio::time::sleep(Duration::from_millis(100)).await;

        let stream = TcpStream::connect("127.0.0.1:13340").await.unwrap();
        let (read_half, mut write_half) = stream.into_split();
        let mut reader = BufReader::new(read_half);

        // Subscribe
        write_half.write_all(b"{\"id\":1,\"method\":\"mining.subscribe\",\"params\":[]}\n").await.unwrap();
        let mut response = String::new();
        reader.read_line(&mut response).await.unwrap();

        // Do NOT authorize. Submit with a valid Lotus address that was never authorized.
        write_half.write_all(b"{\"id\":2,\"method\":\"mining.submit\",\"params\":[\"lotus_16PSJHGmfZkU8zFrzU8Gw198o4j2XUryNnrMccuvZ.rig\",\"job-890-100\",\"00000003\",\"6adc0c6a0000\",\"B02B4ABB3DD6E835\"]}\n").await.unwrap();

        response.clear();
        reader.read_line(&mut response).await.unwrap();
        let resp: serde_json::Value = serde_json::from_str(&response).unwrap();
        // Must be rejected
        assert!(
            resp["result"] == serde_json::Value::Bool(false),
            "expected rejected (false), got {:?}",
            resp,
        );

        // Shutdown
        shutdown_tx.send(()).unwrap();
        let _ = tokio::time::timeout(Duration::from_secs(5), server_handle).await;

        // Verify outcome was persisted with correct reject_reason
        let share_repo = ShareRepository::new(db_conn_arc.clone());
        let total = share_repo.total_outcome_count().unwrap();
        assert_eq!(total, 1, "share outcome should be persisted for unauthorized worker");
        let reasons = share_repo.count_rejected_by_reason().unwrap();
        assert_eq!(reasons.get("unauthorized-worker"), Some(&1));
    }

    /// Integration test: rejected share (stale job) persists share_outcome.
    #[tokio::test]
    async fn test_rejected_share_stale_job_persists_outcome() {
        use tempfile::NamedTempFile;
        use crate::accounting::{init_schema, AccountingService};
        use parking_lot::Mutex;
        use rusqlite::Connection;

        let temp_file = NamedTempFile::new().unwrap();
        let db_conn = Connection::open(temp_file.path()).unwrap();
        init_schema(&db_conn).unwrap();
        let db_conn_arc = Arc::new(Mutex::new(db_conn));
        let accounting_svc = AccountingService::new(db_conn_arc.clone());

        let job_cache = Arc::new(JobCache::new(10));
        job_cache.insert(create_test_job()).await;

        let (shutdown_tx, _) = broadcast::channel::<()>(10);
        let addr: SocketAddr = "127.0.0.1:13341".parse().unwrap();
        let server = StratumServer::new(
            addr,
            job_cache.clone(),
            shutdown_tx.clone(),
            Some(accounting_svc),
            None,
            VarDiffConfig::default(),
            false,
        );

        let server_handle = tokio::spawn(async move {
            server.run().await
        });

        tokio::time::sleep(Duration::from_millis(100)).await;

        let stream = TcpStream::connect("127.0.0.1:13341").await.unwrap();
        let (read_half, mut write_half) = stream.into_split();
        let mut reader = BufReader::new(read_half);

        // Subscribe and authorize
        write_half.write_all(b"{\"id\":1,\"method\":\"mining.subscribe\",\"params\":[]}\n").await.unwrap();
        let mut response = String::new();
        reader.read_line(&mut response).await.unwrap();

        write_half.write_all(b"{\"id\":2,\"method\":\"mining.authorize\",\"params\":[\"lotus_16PSJNf1EDEfGvaYzaXJCJZrXH4pgiTo7kyW61iGi.rig\",\"x\"]}\n").await.unwrap();
        response.clear();
        reader.read_line(&mut response).await.unwrap();

        // Read mining.set_difficulty (sent after authorize)
        response.clear();
        reader.read_line(&mut response).await.unwrap();

        // Wait for mining.notify
        response.clear();
        tokio::time::timeout(Duration::from_millis(100), reader.read_line(&mut response))
            .await
            .expect("should receive mining.notify")
            .unwrap();

        // Submit against a non-existent job — job not in session's assigned_jobs
        write_half.write_all(b"{\"id\":3,\"method\":\"mining.submit\",\"params\":[\"lotus_16PSJNf1EDEfGvaYzaXJCJZrXH4pgiTo7kyW61iGi.rig\",\"nonexistent-job\",\"00000003\",\"6adc0c6a0000\",\"B02B4ABB3DD6E835\"]}\n").await.unwrap();

        response.clear();
        reader.read_line(&mut response).await.unwrap();
        let resp: serde_json::Value = serde_json::from_str(&response).unwrap();
        assert!(
            resp["result"] == serde_json::Value::Bool(false),
            "expected rejected (false), got {:?}",
            resp,
        );

        // Shutdown
        shutdown_tx.send(()).unwrap();
        let _ = tokio::time::timeout(Duration::from_secs(5), server_handle).await;

        // Verify outcome was persisted
        let share_repo = ShareRepository::new(db_conn_arc.clone());
        let total = share_repo.total_outcome_count().unwrap();
        assert!(total >= 1, "share outcome should be persisted for stale job");
        let reasons = share_repo.count_rejected_by_reason().unwrap();
        assert_eq!(reasons.get("stale-job"), Some(&1));
    }

    /// Integration test: rejected share (ntime-mismatch) persists share_outcome.
    #[tokio::test]
    async fn test_rejected_share_ntime_mismatch_persists_outcome() {
        use tempfile::NamedTempFile;
        use crate::accounting::{init_schema, AccountingService};
        use parking_lot::Mutex;
        use rusqlite::Connection;

        let temp_file = NamedTempFile::new().unwrap();
        let db_conn = Connection::open(temp_file.path()).unwrap();
        init_schema(&db_conn).unwrap();
        let db_conn_arc = Arc::new(Mutex::new(db_conn));
        let accounting_svc = AccountingService::new(db_conn_arc.clone());

        let job_cache = Arc::new(JobCache::new(10));
        job_cache.insert(create_test_job()).await;

        let (shutdown_tx, _) = broadcast::channel::<()>(10);
        let addr: SocketAddr = "127.0.0.1:13342".parse().unwrap();
        let server = StratumServer::new(
            addr,
            job_cache.clone(),
            shutdown_tx.clone(),
            Some(accounting_svc),
            None,
            VarDiffConfig::default(),
            false,
        );

        let server_handle = tokio::spawn(async move {
            server.run().await
        });

        tokio::time::sleep(Duration::from_millis(100)).await;

        let stream = TcpStream::connect("127.0.0.1:13342").await.unwrap();
        let (read_half, mut write_half) = stream.into_split();
        let mut reader = BufReader::new(read_half);

        // Subscribe and authorize
        write_half.write_all(b"{\"id\":1,\"method\":\"mining.subscribe\",\"params\":[]}\n").await.unwrap();
        let mut response = String::new();
        reader.read_line(&mut response).await.unwrap();

        write_half.write_all(b"{\"id\":2,\"method\":\"mining.authorize\",\"params\":[\"lotus_16PSJNf1EDEfGvaYzaXJCJZrXH4pgiTo7kyW61iGi.rig\",\"x\"]}\n").await.unwrap();
        response.clear();
        reader.read_line(&mut response).await.unwrap();

        // Read mining.set_difficulty (sent after authorize)
        response.clear();
        reader.read_line(&mut response).await.unwrap();

        // Wait for mining.notify (assigned_jobs records job with frozen ntime)
        response.clear();
        tokio::time::timeout(Duration::from_millis(100), reader.read_line(&mut response))
            .await
            .expect("should receive mining.notify")
            .unwrap();

        // Submit with a different ntime than what was assigned
        write_half.write_all(b"{\"id\":3,\"method\":\"mining.submit\",\"params\":[\"lotus_16PSJNf1EDEfGvaYzaXJCJZrXH4pgiTo7kyW61iGi.rig\",\"job-890-100\",\"00000003\",\"aaaaaaaaaaaa\",\"B02B4ABB3DD6E835\"]}\n").await.unwrap();

        response.clear();
        reader.read_line(&mut response).await.unwrap();
        let resp: serde_json::Value = serde_json::from_str(&response).unwrap();
        assert!(
            resp["result"] == serde_json::Value::Bool(false),
            "expected rejected (false), got {:?}",
            resp,
        );

        // Shutdown
        shutdown_tx.send(()).unwrap();
        let _ = tokio::time::timeout(Duration::from_secs(5), server_handle).await;

        // Verify outcome was persisted
        let share_repo = ShareRepository::new(db_conn_arc.clone());
        let total = share_repo.total_outcome_count().unwrap();
        assert!(total >= 1, "share outcome should be persisted for ntime mismatch");
        let reasons = share_repo.count_rejected_by_reason().unwrap();
        assert_eq!(reasons.get("ntime-mismatch"), Some(&1));
    }

    /// Integration test: rejected share (invalid-submit-shape) persists share_outcome.
    #[tokio::test]
    async fn test_rejected_share_invalid_shape_persists_outcome() {
        use tempfile::NamedTempFile;
        use crate::accounting::{init_schema, AccountingService};
        use parking_lot::Mutex;
        use rusqlite::Connection;

        let temp_file = NamedTempFile::new().unwrap();
        let db_conn = Connection::open(temp_file.path()).unwrap();
        init_schema(&db_conn).unwrap();
        let db_conn_arc = Arc::new(Mutex::new(db_conn));
        let accounting_svc = AccountingService::new(db_conn_arc.clone());

        let job_cache = Arc::new(JobCache::new(10));
        job_cache.insert(create_test_job()).await;

        let (shutdown_tx, _) = broadcast::channel::<()>(10);
        let addr: SocketAddr = "127.0.0.1:13343".parse().unwrap();
        let server = StratumServer::new(
            addr,
            job_cache.clone(),
            shutdown_tx.clone(),
            Some(accounting_svc),
            None,
            VarDiffConfig::default(),
            false,
        );

        let server_handle = tokio::spawn(async move {
            server.run().await
        });

        tokio::time::sleep(Duration::from_millis(100)).await;

        let stream = TcpStream::connect("127.0.0.1:13343").await.unwrap();
        let (read_half, mut write_half) = stream.into_split();
        let mut reader = BufReader::new(read_half);

        // Subscribe and authorize
        write_half.write_all(b"{\"id\":1,\"method\":\"mining.subscribe\",\"params\":[]}\n").await.unwrap();
        let mut response = String::new();
        reader.read_line(&mut response).await.unwrap();

        write_half.write_all(b"{\"id\":2,\"method\":\"mining.authorize\",\"params\":[\"lotus_16PSJNf1EDEfGvaYzaXJCJZrXH4pgiTo7kyW61iGi.rig\",\"x\"]}\n").await.unwrap();
        response.clear();
        reader.read_line(&mut response).await.unwrap();

        // Read mining.set_difficulty (sent after authorize)
        response.clear();
        reader.read_line(&mut response).await.unwrap();

        // Wait for mining.notify
        response.clear();
        tokio::time::timeout(Duration::from_millis(100), reader.read_line(&mut response))
            .await
            .expect("should receive mining.notify")
            .unwrap();

        // Submit with only 2 params (missing extranonce2, ntime, nonce) → invalid-submit-shape
        write_half.write_all(b"{\"id\":3,\"method\":\"mining.submit\",\"params\":[\"lotus_16PSJNf1EDEfGvaYzaXJCJZrXH4pgiTo7kyW61iGi.rig\",\"job-890-100\"]}\n").await.unwrap();

        response.clear();
        reader.read_line(&mut response).await.unwrap();
        let resp: serde_json::Value = serde_json::from_str(&response).unwrap();
        assert!(
            resp["result"] == serde_json::Value::Bool(false),
            "expected rejected (false), got {:?}",
            resp,
        );

        // Shutdown
        shutdown_tx.send(()).unwrap();
        let _ = tokio::time::timeout(Duration::from_secs(5), server_handle).await;

        // Verify outcome was persisted
        let share_repo = ShareRepository::new(db_conn_arc.clone());
        let total = share_repo.total_outcome_count().unwrap();
        assert!(total >= 1, "share outcome should be persisted for invalid shape");
        let reasons = share_repo.count_rejected_by_reason().unwrap();
        assert_eq!(reasons.get("invalid-submit-shape"), Some(&1));
    }

    /// Integration test: verify accounting events are recorded in the full TCP submission path.
    /// Per UBQ §Accounting Event: the hot path must produce share_outcome events.
    #[tokio::test]
    async fn test_accounting_event_recorded_in_submission_path() {
        use tempfile::NamedTempFile;
        use crate::accounting::{init_schema, AccountingService};
        use parking_lot::Mutex;
        use rusqlite::Connection;

        let temp_file = NamedTempFile::new().unwrap();
        let db_conn = Connection::open(temp_file.path()).unwrap();
        init_schema(&db_conn).unwrap();
        let db_conn_arc = Arc::new(Mutex::new(db_conn));
        let accounting_svc = AccountingService::new(db_conn_arc.clone());

        let job_cache = Arc::new(JobCache::new(10));
        job_cache.insert(create_test_job()).await;

        let (shutdown_tx, _) = broadcast::channel::<()>(10);
        let addr: SocketAddr = "127.0.0.1:13344".parse().unwrap();
        let server = StratumServer::new(
            addr,
            job_cache.clone(),
            shutdown_tx.clone(),
            Some(accounting_svc.clone()),
            None,
            VarDiffConfig::default(),
            false,
        );

        let server_handle = tokio::spawn(async move {
            server.run().await
        });

        tokio::time::sleep(Duration::from_millis(100)).await;

        let stream = TcpStream::connect("127.0.0.1:13344").await.unwrap();
        let (read_half, mut write_half) = stream.into_split();
        let mut reader = BufReader::new(read_half);

        // Subscribe
        write_half.write_all(b"{\"id\":1,\"method\":\"mining.subscribe\",\"params\":[]}\n").await.unwrap();
        let mut response = String::new();
        reader.read_line(&mut response).await.unwrap();

        // Authorize
        write_half.write_all(b"{\"id\":2,\"method\":\"mining.authorize\",\"params\":[\"lotus_16PSJNf1EDEfGvaYzaXJCJZrXH4pgiTo7kyW61iGi.rig\",\"x\"]}\n").await.unwrap();
        response.clear();
        reader.read_line(&mut response).await.unwrap();

        // Read mining.set_difficulty (sent after authorize)
        response.clear();
        reader.read_line(&mut response).await.unwrap();

        // Wait for mining.notify
        response.clear();
        tokio::time::timeout(Duration::from_millis(100), reader.read_line(&mut response))
            .await
            .expect("should receive mining.notify")
            .unwrap();

        // Submit a share
        write_half.write_all(b"{\"id\":3,\"method\":\"mining.submit\",\"params\":[\"lotus_16PSJNf1EDEfGvaYzaXJCJZrXH4pgiTo7kyW61iGi.rig\",\"job-890-100\",\"00000003\",\"6adc0c6a0000\",\"B02B4ABB3DD6E835\"]}\n").await.unwrap();
        response.clear();
        reader.read_line(&mut response).await.unwrap();

        // Shutdown
        shutdown_tx.send(()).unwrap();
        let _ = tokio::time::timeout(Duration::from_secs(5), server_handle).await;

        // Verify accounting events were recorded (regardless of accept/reject status)
        let events = accounting_svc.event_repo.list_by_type("share_outcome", 10, 0).unwrap();
        assert!(
            !events.is_empty(),
            "share_outcome accounting event should be recorded in TCP submission path"
        );
        // Verify the event has the correct shape: event_type, status, session_id, worker_id
        assert!(
            events[0].status == "accepted" || events[0].status == "rejected",
            "share outcome status must be 'accepted' or 'rejected', got '{}'",
            events[0].status,
        );
        assert_eq!(events[0].event_type, "share_outcome");
        assert!(events[0].session_id.is_some(), "session_id should be recorded");
        assert!(events[0].worker_id.is_some(), "worker_id should be recorded");
    }

    /// Integration test: submitting the identical share twice via TCP results
    /// in only one database record (dedupe_key enforcement through the full path).
    #[tokio::test]
    async fn test_deduplicate_identical_share_tcp() {
        use tempfile::NamedTempFile;
        use crate::accounting::{init_schema, AccountingService, ShareRepository};
        use parking_lot::Mutex;
        use rusqlite::Connection;

        let temp_file = NamedTempFile::new().unwrap();
        let db_conn = Connection::open(temp_file.path()).unwrap();
        init_schema(&db_conn).unwrap();
        let db_conn_arc = Arc::new(Mutex::new(db_conn));
        let arc_for_assert = db_conn_arc.clone();
        let accounting_svc = AccountingService::new(db_conn_arc);

        let job_cache = Arc::new(JobCache::new(10));
        job_cache.insert(create_test_job()).await;

        let (shutdown_tx, _) = broadcast::channel::<()>(10);
        let addr: SocketAddr = "127.0.0.1:13345".parse().unwrap();
        let server = StratumServer::new(
            addr,
            job_cache.clone(),
            shutdown_tx.clone(),
            Some(accounting_svc),
            None,
            VarDiffConfig::default(),
            false,
        );

        let server_handle = tokio::spawn(async move { server.run().await });
        tokio::time::sleep(Duration::from_millis(100)).await;

        let stream = TcpStream::connect("127.0.0.1:13345").await.unwrap();
        let (read_half, mut write_half) = stream.into_split();
        let mut reader = BufReader::new(read_half);

        // Subscribe
        write_half.write_all(b"{\"id\":1,\"method\":\"mining.subscribe\",\"params\":[]}\n").await.unwrap();
        let mut response = String::new();
        reader.read_line(&mut response).await.unwrap();

        // Authorize
        write_half.write_all(b"{\"id\":2,\"method\":\"mining.authorize\",\"params\":[\"lotus_16PSJNf1EDEfGvaYzaXJCJZrXH4pgiTo7kyW61iGi.rig\",\"x\"]}\n").await.unwrap();
        response.clear();
        reader.read_line(&mut response).await.unwrap();

        // Read mining.set_difficulty (sent after authorize)
        response.clear();
        reader.read_line(&mut response).await.unwrap();

        // Wait for mining.notify
        response.clear();
        tokio::time::timeout(Duration::from_millis(100), reader.read_line(&mut response))
            .await
            .expect("should receive mining.notify")
            .unwrap();

        // Submit the same share twice
        let share_data = b"{\"id\":3,\"method\":\"mining.submit\",\"params\":[\"lotus_16PSJNf1EDEfGvaYzaXJCJZrXH4pgiTo7kyW61iGi.rig\",\"job-890-100\",\"00000003\",\"6adc0c6a0000\",\"B02B4ABB3DD6E835\"]}\n";

        write_half.write_all(share_data).await.unwrap();
        response.clear();
        reader.read_line(&mut response).await.unwrap();

        write_half.write_all(share_data).await.unwrap();
        response.clear();
        reader.read_line(&mut response).await.unwrap();

        // Shutdown
        shutdown_tx.send(()).unwrap();
        let _ = tokio::time::timeout(Duration::from_secs(5), server_handle).await;

        // Verify only ONE share was persisted (dedupe key prevented duplicate)
        let share_repo = ShareRepository::new(arc_for_assert);
        let total = share_repo.total_count().unwrap();
        assert_eq!(
            total, 1,
            "dedupe key should have prevented duplicate share, got {} records",
            total
        );
        let total_outcomes = share_repo.total_outcome_count().unwrap();
        assert_eq!(
            total_outcomes, 1,
            "only one share outcome should exist, got {}",
            total_outcomes
        );
    }

    /// Integration test: when a new job with lower N_diff arrives through `job_tx`,
    /// the session's VarDiff ceiling is updated, P_diff is clamped, and
    /// `mining.set_difficulty` is sent to the miner with the clamped value.
    ///
    /// Verifies GAP 1 fix: the NNG event consumer path (via `job_tx`) must update
    /// VarDiff ceilings, not just the startup path (via `notify_new_template` / `n_diff_tx`).
    ///
    /// On the unfixed code, this test would fail: the `job_rx` handler sends
    /// `mining.notify` (no `mining.set_difficulty`), so the test reads `mining.notify`
    /// and fails the `set_difficulty` assertion.
    #[tokio::test]
    async fn test_vardiff_ceiling_clamped_on_new_job_broadcast() {
        let job_cache = Arc::new(JobCache::new(10));
        let initial_job = create_test_job();
        job_cache.insert(initial_job.clone()).await;

        let (shutdown_tx, _) = broadcast::channel::<()>(10);
        let addr: SocketAddr = "127.0.0.1:13350".parse().unwrap();
        let server = StratumServer::new(
            addr,
            job_cache.clone(),
            shutdown_tx.clone(),
            None,
            None,
            VarDiffConfig::default(),
            false,
        );

        // Capture job_tx before spawning the server task
        let job_tx = server.job_tx();

        let server_handle = tokio::spawn(async move {
            let _ = server.run().await;
        });

        tokio::time::sleep(Duration::from_millis(100)).await;

        let stream = TcpStream::connect("127.0.0.1:13350").await.unwrap();
        let (read_half, mut write_half) = stream.into_split();
        let mut reader = BufReader::new(read_half);

        // Subscribe
        write_half
            .write_all(
                b"{\"id\":1,\"method\":\"mining.subscribe\",\"params\":[]}\n",
            )
            .await
            .unwrap();
        let mut response = String::new();
        reader.read_line(&mut response).await.unwrap();

        // Authorize
        write_half
            .write_all(
                b"{\"id\":2,\"method\":\"mining.authorize\",\"params\":[\"lotus_16PSJNf1EDEfGvaYzaXJCJZrXH4pgiTo7kyW61iGi.rig\",\"x\"]}\n",
            )
            .await
            .unwrap();
        response.clear();
        reader.read_line(&mut response).await.unwrap();

        // Read mining.set_difficulty (initial P_diff, sent after authorize)
        response.clear();
        reader.read_line(&mut response).await.unwrap();
        let set_diff: serde_json::Value =
            serde_json::from_str(response.trim()).unwrap();
        assert_eq!(set_diff["method"], "mining.set_difficulty");
        let initial_p_diff = set_diff["params"][0].as_f64().unwrap();
        assert!(initial_p_diff > 0.0, "initial P_diff must be positive");

        // Read mining.notify (initial job)
        response.clear();
        tokio::time::timeout(Duration::from_millis(100), reader.read_line(&mut response))
            .await
            .expect("should receive mining.notify")
            .unwrap();

        // Compute the initial N_diff for context
        let _initial_n_diff =
            network_target_hex_to_difficulty(&initial_job.network_target_hex)
                .unwrap_or(f64::INFINITY);

        // Create a new job with a vastly lower N_diff (all-0xFF target ≈ 0 difficulty)
        // This forces P_diff to clamp because P_diff > N_diff after the update.
        // Per UBQ: miningwrkchg-triggered jobs use clean_jobs=true.
        let mut new_job = create_test_job();
        new_job.job_id = "job-891-101".to_string();
        new_job.template_id = 891;
        new_job.template_epoch = 101;
        new_job.clean_jobs = true;
        new_job.network_target_hex =
            "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
                .to_string();

        // Broadcast the new job through job_tx (the NNG event consumer path)
        job_tx.send(Arc::new(new_job)).unwrap();

        // After the fix, the job_rx handler should send mining.set_difficulty
        // BEFORE mining.notify. On the unfixed code, only mining.notify is sent
        // and this assertion will fail.
        response.clear();
        tokio::time::timeout(Duration::from_millis(500), reader.read_line(&mut response))
            .await
            .expect(
                "should receive mining.set_difficulty after VarDiff ceiling update",
            )
            .unwrap();

        let clamped: serde_json::Value =
            serde_json::from_str(response.trim()).unwrap();
        assert_eq!(
            clamped["method"],
            "mining.set_difficulty",
            "expected mining.set_difficulty before mining.notify after N_diff clamp"
        );
        let clamped_p_diff = clamped["params"][0].as_f64().unwrap();
        assert!(
            clamped_p_diff < initial_p_diff,
            "P_diff should be clamped from {initial} down to {clamped} when N_diff drops below P_diff",
            initial = initial_p_diff,
            clamped = clamped_p_diff,
        );

        // Cleanup
        shutdown_tx.send(()).unwrap();
        let _ = tokio::time::timeout(Duration::from_secs(5), server_handle).await;
    }
}
