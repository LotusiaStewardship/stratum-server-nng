use anyhow::Result;
use bitcoinsuite_bitcoind_stratum::target_to_difficulty;
use crate::share_processing::VarDiffConfig;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{broadcast, RwLock};
use parking_lot::Mutex;
use rusqlite::Connection;
use tracing::{debug, error, info, warn};

use crate::stratum_protocol::session::SessionState;
use crate::stratum_protocol::job::MiningJob;
use crate::stratum_protocol::protocol::{decode_request_line, Method, StratumResponse};
use crate::accounting::{ShareRepository, WorkerRepository, Share, ShareOutcome, AuthorizationEvent, AccountingService};
use crate::share_processing::validator;

/// TCP Stratum V1 server that accepts miner connections.
pub struct StratumServer {
    bind_address: SocketAddr,
    session_counter: Arc<RwLock<u64>>,
    connected_miners: Arc<RwLock<u64>>,
    job_cache: Arc<crate::node_integration::JobCache>,
    shutdown_tx: broadcast::Sender<()>,
    share_repo: Option<ShareRepository>,
    worker_repo: Option<WorkerRepository>,
    accounting_service: Option<AccountingService>,
    vardiff_config: VarDiffConfig,
}

impl StratumServer {
    /// Create a new Stratum server.
    pub fn new(
        bind_address: SocketAddr,
        job_cache: Arc<crate::node_integration::JobCache>,
        shutdown_tx: broadcast::Sender<()>,
        db_conn: Option<Arc<Mutex<Connection>>>,
        accounting_service: Option<AccountingService>,
        vardiff_config: VarDiffConfig,
    ) -> Self {
        let (share_repo, worker_repo) = if let Some(conn) = db_conn {
            (Some(ShareRepository::new(conn.clone())), Some(WorkerRepository::new(conn)))
        } else {
            (None, None)
        };
        Self {
            bind_address,
            session_counter: Arc::new(RwLock::new(0)),
            connected_miners: Arc::new(RwLock::new(0)),
            job_cache,
            shutdown_tx,
            share_repo,
            worker_repo,
            accounting_service,
            vardiff_config,
        }
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
                                .and_then(|job| {
                                    let target_bytes = hex::decode(&job.network_target_hex).ok()?;
                                    let target_arr: [u8; 32] = target_bytes.as_slice().try_into().ok()?;
                                    target_to_difficulty(&target_arr).ok()
                                })
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
                            let share_repo = self.share_repo.clone();
                            let worker_repo = self.worker_repo.clone();
                            let accounting_service = self.accounting_service.clone();
                            
                            tokio::spawn(async move {
                                // Increment connected miners
                                *connected_miners.write().await += 1;
                                
                                // Handle the connection
                                if let Err(e) = handle_connection(
                                    stream,
                                    session,
                                    job_cache,
                                    shutdown_rx,
                                    share_repo,
                                    worker_repo,
                                    accounting_service,
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
    share_repo: Option<ShareRepository>,
    worker_repo: Option<WorkerRepository>,
    accounting_service: Option<AccountingService>,
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
                        
                        debug!(session = %session.session_id, line = %trimmed, "received request");
                        
                        // Parse the request
                        let req = match decode_request_line(trimmed, 4096) {
                            Ok(req) => req,
                            Err(e) => {
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
                                // Record authorization event per UBQ §Authorization Event
                                record_authorization_event(
                                    &req,
                                    &session,
                                    &auth_resp,
                                    share_repo.as_ref(),
                                ).await;
                                auth_resp
                            }
                            Method::Submit => {
                                let (resp, new_diff) = handle_submit(
                                    &req,
                                    &mut session,
                                    &job_cache,
                                    share_repo.as_ref(),
                                    worker_repo.as_ref(),
                                    accounting_service.as_ref(),
                                ).await;
                                // If VarDiff retargeted, send mining.set_difficulty to miner
                                if let Some(diff) = new_diff {
                                    let set_diff = serde_json::json!({
                                        "id": null,
                                        "method": "mining.set_difficulty",
                                        "params": [diff]
                                    });
                                    let set_diff_line = serde_json::to_string(&set_diff)?;
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
                                debug!(method = ?req.method, "unsupported method");
                                StratumResponse::err(req.id.clone(), 3, "unknown method")
                            }
                            Method::Notify => {
                                // mining.notify is server-to-miner only
                                warn!("received mining.notify from miner (should be server-to-miner)");
                                StratumResponse::err(req.id.clone(), 3, "unknown method")
                            }
                            Method::Unknown(_) => {
                                StratumResponse::err(req.id.clone(), 3, "unknown method")
                            }
                        };
                        
                        // Send response
                        let resp_line = serde_json::to_string(&resp)?;
                        writer.write_all(resp_line.as_bytes()).await?;
                        writer.write_all(b"\n").await?;
                        
                        // If authorized, send mining.notify with current job
                        // and record the assigned job in session (per UBQ §Assigned Job)
                        if session.is_authorized && req.method == Method::Authorize {
                            if let Some(job) = job_cache.get_latest().await {
                                let notify = create_notify(&job, &session.session_id);
                                let notify_line = serde_json::to_string(&notify)?;
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
async fn record_authorization_event(
    req: &crate::stratum_protocol::protocol::StratumRequest,
    session: &SessionState,
    auth_resp: &StratumResponse,
    share_repo: Option<&ShareRepository>,
) {
    let arr = req.params.as_array().cloned().unwrap_or_default();
    let worker_name = arr.first().and_then(|v| v.as_str()).unwrap_or_default().to_string();

    let (payout_address, worker_suffix) = match crate::stratum_protocol::session::parse_worker_name(&worker_name) {
        Ok(w) => (w.payout_address, w.worker_suffix),
        Err(_) => (worker_name.clone(), None),
    };

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
            payout_address,
            worker_suffix,
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
    share_repo: Option<&ShareRepository>,
    worker_repo: Option<&WorkerRepository>,
    accounting_service: Option<&AccountingService>,
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
    // If not found, create a stale validation result directly.
    let (validation, template_id, template_epoch, share_diff) =
        if let Some(job) = job_cache.get(job_id).await {
            // Run the full validation pipeline
            let validation = validator::validate_share(
                worker_name,
                job_id,
                extranonce2,
                ntime_hex,
                nonce_hex,
                session,
                &job,
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
        } else {
            // Job not in cache — treat as stale.
            // Use defaults for template metadata since the job isn't available.
            (
                validator::ValidationResult::rejected("stale-job"),
                0i64,
                0i64,
                session.current_difficulty(),
            )
        };

    // Persist share + outcome atomically if repositories are available.
    // This runs for ALL submissions (accepted or rejected) per UBQ.
    if let (Some(share_repo), Some(worker_repo)) = (share_repo, worker_repo) {
        // Parse worker name to get payout address and suffix.
        // If unparseable, we cannot persist (need a worker_id for FK).
        if let Ok(worker_parsed) =
            crate::stratum_protocol::session::parse_worker_name(worker_name)
        {
            if let Ok(worker) = worker_repo.upsert(
                &worker_parsed.payout_address,
                worker_parsed.worker_suffix.as_deref(),
            ) {
                // Resolve round_id via AccountingService (if available)
                // Per UBQ: round_id is resolved at insert time, not backfilled.
                let round_id = accounting_service
                    .and_then(|svc| svc.resolve_round_for_template(template_id).ok())
                    .map(|round| round.id);

                // Build dedupe key per UBQ format
                let dedupe_key = ShareRepository::build_dedupe_key(
                    worker.id,
                    template_id,
                    template_epoch,
                    extranonce2,
                    ntime_hex,
                    nonce_hex,
                );

                // Create raw share record
                let share = Share {
                    id: 0,
                    worker_id: worker.id,
                    session_id: session.session_id.clone(),
                    job_id: job_id.to_string(),
                    template_id,
                    template_epoch,
                    extranonce2: extranonce2.to_string(),
                    ntime_hex_6b: ntime_hex.to_string(),
                    nonce_hex_8b: nonce_hex.to_string(),
                    difficulty: share_diff,
                    dedupe_key: dedupe_key.clone(),
                };

                // Create share outcome based on validation result
                let outcome = ShareOutcome {
                    id: 0,
                    share_id: 0,
                    session_id: session.session_id.clone(),
                    worker_id: worker.id,
                    job_id: job_id.to_string(),
                    round_id,
                    dedupe_key: dedupe_key.clone(),
                    status: if validation.accepted {
                        "accepted"
                    } else {
                        "rejected"
                    }
                    .to_string(),
                    reject_reason: validation.reject_reason.clone(),
                    node_result: None,
                    low_diff_ok: Some(validation.low_diff_ok),
                    network_target_ok: Some(validation.network_target_ok),
                    block_hash: validation.block_hash.clone(),
                };

                // Insert atomically
                if let Err(e) =
                    share_repo.insert_share_and_outcome_atomic(&share, &outcome)
                {
                    warn!(error = %e, "failed to atomically persist share and outcome");
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
        }
    }

    #[tokio::test]
    async fn test_server_starts_and_accepts_connections() {
        let job_cache = Arc::new(JobCache::new(10));
        job_cache.insert(create_test_job()).await;
        
        let (shutdown_tx, _) = broadcast::channel::<()>(10);
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let server = StratumServer::new(addr, job_cache, shutdown_tx.clone(), None, None, VarDiffConfig::default());
        
        // Server should start without error
        assert_eq!(server.connected_miners().await, 0);
    }

    #[tokio::test]
    async fn test_session_id_generation() {
        let job_cache = Arc::new(JobCache::new(10));
        let (shutdown_tx, _) = broadcast::channel::<()>(10);
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let server = StratumServer::new(addr, job_cache, shutdown_tx.clone(), None, None, VarDiffConfig::default());
        
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
        assert_eq!(params.len(), 9); // 9 params total
    }

    /// Integration test: full subscribe → authorize → submit flow
    #[tokio::test]
    async fn test_integration_subscribe_authorize_submit() {
        // Start server
        let job_cache = Arc::new(JobCache::new(10));
        job_cache.insert(create_test_job()).await;
        
        let (shutdown_tx, _) = broadcast::channel::<()>(10);
        let addr: SocketAddr = "127.0.0.1:13334".parse().unwrap();
        let server = StratumServer::new(addr, job_cache.clone(), shutdown_tx.clone(), None, None, VarDiffConfig::default());
        
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
        
        // Receive mining.notify after authorize
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

    #[tokio::test]
    async fn test_authorize_requires_subscribe() {
        let job_cache = Arc::new(JobCache::new(10));
        job_cache.insert(create_test_job()).await;
        
        let (shutdown_tx, _) = broadcast::channel::<()>(10);
        let addr: SocketAddr = "127.0.0.1:13335".parse().unwrap();
        let server = StratumServer::new(addr, job_cache.clone(), shutdown_tx.clone(), None, None, VarDiffConfig::default());
        
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
        let server = StratumServer::new(addr, job_cache.clone(), shutdown_tx.clone(), None, None, VarDiffConfig::default());
        
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
        use crate::accounting::{init_schema, ShareRepository};
        use parking_lot::Mutex;

        let temp_file = NamedTempFile::new().unwrap();
        let db_path = temp_file.path().to_str().unwrap().to_string();
        let db_conn = Connection::open(&db_path).unwrap();
        init_schema(&db_conn).unwrap();
        let db_conn_arc = Arc::new(Mutex::new(db_conn));

        let job_cache = Arc::new(JobCache::new(10));
        job_cache.insert(create_test_job()).await;
        
        let (shutdown_tx, _) = broadcast::channel::<()>(10);
        let addr: SocketAddr = "127.0.0.1:13337".parse().unwrap();
        let server = StratumServer::new(
            addr,
            job_cache.clone(),
            shutdown_tx.clone(),
            Some(db_conn_arc.clone()),
            None,
            VarDiffConfig::default(),
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
        use crate::accounting::init_schema;
        use parking_lot::Mutex;

        let temp_file = NamedTempFile::new().unwrap();
        let db_conn = Connection::open(temp_file.path()).unwrap();
        init_schema(&db_conn).unwrap();
        let db_conn_arc = Arc::new(Mutex::new(db_conn));

        let job_cache = Arc::new(JobCache::new(10));
        job_cache.insert(create_test_job()).await;
        
        let (shutdown_tx, _) = broadcast::channel::<()>(10);
        let addr: SocketAddr = "127.0.0.1:13338".parse().unwrap();
        let server = StratumServer::new(
            addr,
            job_cache.clone(),
            shutdown_tx.clone(),
            Some(db_conn_arc.clone()),
            None,
            VarDiffConfig::default(),
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
        use crate::accounting::init_schema;
        use parking_lot::Mutex;

        let temp_file = NamedTempFile::new().unwrap();
        let db_conn = Connection::open(temp_file.path()).unwrap();
        init_schema(&db_conn).unwrap();
        let db_conn_arc = Arc::new(Mutex::new(db_conn));

        let job_cache = Arc::new(JobCache::new(10));
        job_cache.insert(create_test_job()).await;

        let (shutdown_tx, _) = broadcast::channel::<()>(10);
        let addr: SocketAddr = "127.0.0.1:13340".parse().unwrap();
        let server = StratumServer::new(
            addr,
            job_cache.clone(),
            shutdown_tx.clone(),
            Some(db_conn_arc.clone()),
            None,
            VarDiffConfig::default(),
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
        use crate::accounting::init_schema;
        use parking_lot::Mutex;

        let temp_file = NamedTempFile::new().unwrap();
        let db_conn = Connection::open(temp_file.path()).unwrap();
        init_schema(&db_conn).unwrap();
        let db_conn_arc = Arc::new(Mutex::new(db_conn));

        let job_cache = Arc::new(JobCache::new(10));
        job_cache.insert(create_test_job()).await;

        let (shutdown_tx, _) = broadcast::channel::<()>(10);
        let addr: SocketAddr = "127.0.0.1:13341".parse().unwrap();
        let server = StratumServer::new(
            addr,
            job_cache.clone(),
            shutdown_tx.clone(),
            Some(db_conn_arc.clone()),
            None,
            VarDiffConfig::default(),
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
        use crate::accounting::init_schema;
        use parking_lot::Mutex;

        let temp_file = NamedTempFile::new().unwrap();
        let db_conn = Connection::open(temp_file.path()).unwrap();
        init_schema(&db_conn).unwrap();
        let db_conn_arc = Arc::new(Mutex::new(db_conn));

        let job_cache = Arc::new(JobCache::new(10));
        job_cache.insert(create_test_job()).await;

        let (shutdown_tx, _) = broadcast::channel::<()>(10);
        let addr: SocketAddr = "127.0.0.1:13342".parse().unwrap();
        let server = StratumServer::new(
            addr,
            job_cache.clone(),
            shutdown_tx.clone(),
            Some(db_conn_arc.clone()),
            None,
            VarDiffConfig::default(),
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
        use crate::accounting::init_schema;
        use parking_lot::Mutex;

        let temp_file = NamedTempFile::new().unwrap();
        let db_conn = Connection::open(temp_file.path()).unwrap();
        init_schema(&db_conn).unwrap();
        let db_conn_arc = Arc::new(Mutex::new(db_conn));

        let job_cache = Arc::new(JobCache::new(10));
        job_cache.insert(create_test_job()).await;

        let (shutdown_tx, _) = broadcast::channel::<()>(10);
        let addr: SocketAddr = "127.0.0.1:13343".parse().unwrap();
        let server = StratumServer::new(
            addr,
            job_cache.clone(),
            shutdown_tx.clone(),
            Some(db_conn_arc.clone()),
            None,
            VarDiffConfig::default(),
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
}
