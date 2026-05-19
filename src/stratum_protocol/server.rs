use anyhow::Result;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{broadcast, RwLock};
use parking_lot::Mutex;
use rusqlite::Connection;
use tracing::{debug, error, info, warn};

use crate::stratum_protocol::session::SessionState;
use crate::stratum_protocol::job::MiningJob;
use crate::stratum_protocol::protocol::{decode_request_line, Method, StratumResponse};
use crate::accounting::{ShareRepository, WorkerRepository, Share, ShareOutcome, AuthorizationEvent};

/// TCP Stratum V1 server that accepts miner connections.
pub struct StratumServer {
    bind_address: SocketAddr,
    session_counter: Arc<RwLock<u64>>,
    connected_miners: Arc<RwLock<u64>>,
    job_cache: Arc<crate::node_integration::JobCache>,
    shutdown_tx: broadcast::Sender<()>,
    share_repo: Option<ShareRepository>,
    worker_repo: Option<WorkerRepository>,
}

impl StratumServer {
    /// Create a new Stratum server.
    pub fn new(
        bind_address: SocketAddr,
        job_cache: Arc<crate::node_integration::JobCache>,
        shutdown_tx: broadcast::Sender<()>,
        db_conn: Option<Arc<Mutex<Connection>>>,
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
                            let session = SessionState::new(session_id.clone());
                            
                            // Clone Arcs for the connection handler
                            let connected_miners = self.connected_miners.clone();
                            let job_cache = self.job_cache.clone();
                            let shutdown_rx = shutdown_rx.resubscribe();
                            let share_repo = self.share_repo.clone();
                            let worker_repo = self.worker_repo.clone();
                            
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
                                handle_submit(
                                    &req,
                                    &session,
                                    share_repo.as_ref(),
                                    worker_repo.as_ref(),
                                ).await
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

/// Handle a mining.submit request and persist the share + outcome.
/// Per UBQ: inserts into both `shares` (raw submission) and `share_outcomes` (validation result).
async fn handle_submit(
    req: &crate::stratum_protocol::protocol::StratumRequest,
    session: &SessionState,
    share_repo: Option<&ShareRepository>,
    worker_repo: Option<&WorkerRepository>,
) -> StratumResponse {
    // First validate the submit shape and session state via session
    let session_resp = session.handle_submit(req);
    
    // If session validation failed, return the error
    if !session_resp.error.is_null() {
        return session_resp;
    }
    
    // Extract submit parameters
    let arr = req.params.as_array().cloned().unwrap_or_default();
    let worker_name = arr.first().and_then(|v| v.as_str()).unwrap_or_default();
    let job_id = arr.get(1).and_then(|v| v.as_str()).unwrap_or_default();
    let extranonce2 = arr.get(2).and_then(|v| v.as_str()).unwrap_or_default();
    let ntime_hex = arr.get(3).and_then(|v| v.as_str()).unwrap_or_default();
    let nonce_hex = arr.get(4).and_then(|v| v.as_str()).unwrap_or_default();
    
    // Parse worker name to get payout address and suffix
    let worker_parsed = match crate::stratum_protocol::session::parse_worker_name(worker_name) {
        Ok(w) => w,
        Err(_) => {
            return StratumResponse::rejected(req.id.clone(), 24, "unauthorized-worker");
        }
    };
    
    // Look up the assigned job to get template_id, template_epoch, and difficulty
    // Per UBQ: share difficulty = P_diff at assignment time (from assigned_jobs)
    let assigned_job = session.get_assigned_job(job_id);
    let difficulty = assigned_job.map(|a| a.p_diff).unwrap_or(1.0);
    
    // Persist share + outcome if repositories are available
    if let (Some(share_repo), Some(worker_repo)) = (share_repo, worker_repo) {
        // Upsert worker to get worker_id
        match worker_repo.upsert(&worker_parsed.payout_address, worker_parsed.worker_suffix.as_deref()) {
            Ok(worker) => {
                // Build dedupe key per UBQ format
                // For Slice 1 (static jobs), template_id and template_epoch are 0
                let dedupe_key = ShareRepository::build_dedupe_key(
                    worker.id,
                    0,  // template_id — resolved properly in Slice 2+
                    0,  // template_epoch
                    extranonce2,
                    ntime_hex,
                    nonce_hex,
                );
                
                // Create and insert raw share record
                let share = Share {
                    id: 0,
                    worker_id: worker.id,
                    session_id: session.session_id.clone(),
                    job_id: job_id.to_string(),
                    template_id: 0,   // Placeholder for Slice 1 (static job)
                    template_epoch: 0,
                    extranonce2: extranonce2.to_string(),
                    ntime_hex_6b: ntime_hex.to_string(),
                    nonce_hex_8b: nonce_hex.to_string(),
                    difficulty,
                    dedupe_key: dedupe_key.clone(),
                };
                
                match share_repo.insert_share(&share) {
                    Ok(Some(share_id)) => {
                        // Create and insert share outcome (Slice 1: all accepted)
                        let outcome = ShareOutcome {
                            id: 0,
                            share_id,
                            session_id: session.session_id.clone(),
                            worker_id: worker.id,
                            job_id: job_id.to_string(),
                            round_id: None,
                            dedupe_key,
                            status: "accepted".to_string(),
                            reject_reason: None,
                            node_result: None,
                            low_diff_ok: Some(true),
                            network_target_ok: Some(false),
                            block_hash: None,
                        };
                        
                        if let Err(e) = share_repo.insert_share_outcome(&outcome) {
                            warn!(error = %e, "failed to persist share outcome");
                        }
                    }
                    Ok(None) => {
                        debug!(dedupe_key = %dedupe_key, "duplicate share ignored (dedupe_key)");
                    }
                    Err(e) => {
                        warn!(error = %e, "failed to persist raw share");
                    }
                }
            }
            Err(e) => {
                warn!(error = %e, "failed to upsert worker");
            }
        }
    }
    
    // Return success
    StratumResponse::ok(req.id.clone(), serde_json::Value::Bool(true))
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
        MiningJob {
            job_id: "job-1-1234567890".to_string(),
            template_id: 1,
            prevhash: "0000000000000000000000000000000000000000000000000000000000000000".to_string(),
            coinbase1: "0100000001".to_string(),
            coinbase2: "ffffffff02".to_string(),
            merkle_branches: vec![],
            version: "20000000".to_string(),
            nbits: "1d00ffff".to_string(),
            ntime: "5f5f5f5f".to_string(),
            network_target_hex: "ffffffff".to_string(),
            clean_jobs: false,
            template_epoch: 1234567890,
        }
    }

    #[tokio::test]
    async fn test_server_starts_and_accepts_connections() {
        let job_cache = Arc::new(JobCache::new(10));
        job_cache.insert(create_test_job()).await;
        
        let (shutdown_tx, _) = broadcast::channel::<()>(10);
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let server = StratumServer::new(addr, job_cache, shutdown_tx.clone(), None);
        
        // Server should start without error
        assert_eq!(server.connected_miners().await, 0);
    }

    #[tokio::test]
    async fn test_session_id_generation() {
        let job_cache = Arc::new(JobCache::new(10));
        let (shutdown_tx, _) = broadcast::channel::<()>(10);
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let server = StratumServer::new(addr, job_cache, shutdown_tx.clone(), None);
        
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
        assert_eq!(params[0], "job-1-1234567890"); // job_id with epoch
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
        let server = StratumServer::new(addr, job_cache.clone(), shutdown_tx.clone(), None);
        
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
        assert_eq!(notify["params"][0], "job-1-1234567890");
        
        // Submit with the correct ntime that matches the job
        write_half.write_all(b"{\"id\":3,\"method\":\"mining.submit\",\"params\":[\"lotus_16PSJNf1EDEfGvaYzaXJCJZrXH4pgiTo7kyW61iGi.rig\",\"job-1-1234567890\",\"00112233\",\"5f5f5f5f\",\"0011223344556677\"]}\n").await.unwrap();
        
        response.clear();
        reader.read_line(&mut response).await.unwrap();
        let resp: serde_json::Value = serde_json::from_str(&response).unwrap();
        assert!(resp["error"].is_null());
        assert_eq!(resp["result"], serde_json::Value::Bool(true));
        
        shutdown_tx.send(()).unwrap();
        let _ = tokio::time::timeout(Duration::from_secs(1), server_handle).await;
    }

    #[tokio::test]
    async fn test_authorize_requires_subscribe() {
        let job_cache = Arc::new(JobCache::new(10));
        job_cache.insert(create_test_job()).await;
        
        let (shutdown_tx, _) = broadcast::channel::<()>(10);
        let addr: SocketAddr = "127.0.0.1:13335".parse().unwrap();
        let server = StratumServer::new(addr, job_cache.clone(), shutdown_tx.clone(), None);
        
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
        let server = StratumServer::new(addr, job_cache.clone(), shutdown_tx.clone(), None);
        
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
        
        // Submit share with correct ntime from job
        write_half.write_all(b"{\"id\":3,\"method\":\"mining.submit\",\"params\":[\"lotus_16PSJNf1EDEfGvaYzaXJCJZrXH4pgiTo7kyW61iGi.rig\",\"job-1-1234567890\",\"00112233\",\"5f5f5f5f\",\"0011223344556677\"]}\n").await.unwrap();
        
        response.clear();
        reader.read_line(&mut response).await.unwrap();
        let resp: serde_json::Value = serde_json::from_str(&response).unwrap();
        assert!(resp["error"].is_null(), "share should be accepted");
        
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
}
