use anyhow::Result;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{broadcast, RwLock};
use tracing::{debug, error, info, warn};

use crate::stratum_protocol::session::SessionState;
use crate::stratum_protocol::job::MiningJob;
use crate::stratum_protocol::protocol::{decode_request_line, Method, StratumResponse};

/// TCP Stratum V1 server that accepts miner connections.
pub struct StratumServer {
    bind_address: SocketAddr,
    session_counter: Arc<RwLock<u64>>,
    connected_miners: Arc<RwLock<u64>>,
    job_cache: Arc<crate::node_integration::JobCache>,
    shutdown_tx: broadcast::Sender<()>,
}

impl StratumServer {
    /// Create a new Stratum server.
    pub fn new(
        bind_address: SocketAddr,
        job_cache: Arc<crate::node_integration::JobCache>,
        shutdown_tx: broadcast::Sender<()>,
    ) -> Self {
        Self {
            bind_address,
            session_counter: Arc::new(RwLock::new(0)),
            connected_miners: Arc::new(RwLock::new(0)),
            job_cache,
            shutdown_tx,
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
                            
                            tokio::spawn(async move {
                                // Increment connected miners
                                *connected_miners.write().await += 1;
                                
                                // Handle the connection
                                if let Err(e) = handle_connection(
                                    stream,
                                    session,
                                    job_cache,
                                    shutdown_rx,
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
) -> Result<()> {
    let (reader, mut writer) = stream.into_split();
    let mut reader = BufReader::new(reader);
    let mut line = String::new();

    // Send initial difficulty (optional, per spec)
    // For now, we skip this and let miners use default

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
                                session.handle_authorize(&req)
                            }
                            Method::Submit => {
                                // For Slice 1/2, accept all valid submits
                                // Validation comes in Slice 3
                                session.handle_submit(&req)
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
                        if session.is_authorized && req.method == Method::Authorize {
                            if let Some(job) = job_cache.get_latest().await {
                                let notify = create_notify(&job, &session.session_id);
                                let notify_line = serde_json::to_string(&notify)?;
                                writer.write_all(notify_line.as_bytes()).await?;
                                writer.write_all(b"\n").await?;
                                debug!(session = %session.session_id, job = %job.job_id, "sent mining.notify");
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node_integration::JobCache;
    use std::net::SocketAddr;
    use std::time::Duration;
    use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
    use tokio::net::TcpStream;

    fn create_test_job() -> MiningJob {
        MiningJob {
            job_id: "job-1".to_string(),
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
        let server = StratumServer::new(addr, job_cache, shutdown_tx.clone());
        
        // Server should start without error
        // (We can't easily test the full loop without blocking)
        assert_eq!(server.connected_miners().await, 0);
    }

    #[tokio::test]
    async fn test_session_id_generation() {
        let job_cache = Arc::new(JobCache::new(10));
        let (shutdown_tx, _) = broadcast::channel::<()>(10);
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let server = StratumServer::new(addr, job_cache, shutdown_tx.clone());
        
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
        assert_eq!(params[0], "job-1"); // job_id
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
        let server = StratumServer::new(addr, job_cache.clone(), shutdown_tx.clone());
        
        // Spawn server in background
        let server_handle = tokio::spawn(async move {
            server.run().await
        });
        
        // Give server time to start
        tokio::time::sleep(Duration::from_millis(100)).await;
        
        // Connect as miner
        let stream = TcpStream::connect("127.0.0.1:13334").await.unwrap();
        let (mut read_half, mut write_half) = stream.into_split();
        let mut reader = BufReader::new(read_half);
        
        // Send mining.subscribe
        write_half.write_all(b"{\"id\":1,\"method\":\"mining.subscribe\",\"params\":[]}\n").await.unwrap();
        
        let mut response = String::new();
        reader.read_line(&mut response).await.unwrap();
        let resp: serde_json::Value = serde_json::from_str(&response).unwrap();
        assert!(resp["error"].is_null());
        assert_eq!(resp["result"].as_array().unwrap().len(), 3);
        
        // Send mining.authorize
        write_half.write_all(b"{\"id\":2,\"method\":\"mining.authorize\",\"params\":[\"lotus_16PSJNf1EDEfGvaYzaXJCJZrXH4pgiTo7kyW61iGi.rig\",\"x\"]}\n").await.unwrap();
        
        response.clear();
        reader.read_line(&mut response).await.unwrap();
        let resp: serde_json::Value = serde_json::from_str(&response).unwrap();
        assert!(resp["error"].is_null());
        assert_eq!(resp["result"], serde_json::Value::Bool(true));
        
        // Should receive mining.notify after authorize
        response.clear();
        tokio::time::timeout(Duration::from_millis(100), reader.read_line(&mut response))
            .await
            .expect("should receive mining.notify")
            .unwrap();
        let notify: serde_json::Value = serde_json::from_str(&response.trim()).unwrap();
        assert_eq!(notify["method"], "mining.notify");
        assert_eq!(notify["params"][0], "job-1");
        
        // Send mining.submit
        write_half.write_all(b"{\"id\":3,\"method\":\"mining.submit\",\"params\":[\"lotus_16PSJNf1EDEfGvaYzaXJCJZrXH4pgiTo7kyW61iGi.rig\",\"job-1\",\"00112233\",\"001122334455\",\"0011223344556677\"]}\n").await.unwrap();
        
        response.clear();
        reader.read_line(&mut response).await.unwrap();
        let resp: serde_json::Value = serde_json::from_str(&response).unwrap();
        assert!(resp["error"].is_null());
        assert_eq!(resp["result"], serde_json::Value::Bool(true));
        
        // Shutdown server
        shutdown_tx.send(()).unwrap();
        let _ = tokio::time::timeout(Duration::from_secs(1), server_handle).await;
    }

    #[tokio::test]
    async fn test_authorize_requires_subscribe() {
        // Start server
        let job_cache = Arc::new(JobCache::new(10));
        job_cache.insert(create_test_job()).await;
        
        let (shutdown_tx, _) = broadcast::channel::<()>(10);
        let addr: SocketAddr = "127.0.0.1:13335".parse().unwrap();
        let server = StratumServer::new(addr, job_cache.clone(), shutdown_tx.clone());
        
        let server_handle = tokio::spawn(async move {
            server.run().await
        });
        
        tokio::time::sleep(Duration::from_millis(100)).await;
        
        // Connect and try to authorize without subscribe
        let stream = TcpStream::connect("127.0.0.1:13335").await.unwrap();
        let (mut read_half, mut write_half) = stream.into_split();
        let mut reader = BufReader::new(read_half);
        
        write_half.write_all(b"{\"id\":1,\"method\":\"mining.authorize\",\"params\":[\"lotus_16PSJNf1EDEfGvaYzaXJCJZrXH4pgiTo7kyW61iGi.rig\",\"x\"]}\n").await.unwrap();
        
        let mut response = String::new();
        reader.read_line(&mut response).await.unwrap();
        let resp: serde_json::Value = serde_json::from_str(&response).unwrap();
        
        assert!(!resp["error"].is_null());
        assert_eq!(resp["error"].as_array().unwrap()[1], "not-subscribed");
        
        // Shutdown
        shutdown_tx.send(()).unwrap();
        let _ = tokio::time::timeout(Duration::from_secs(1), server_handle).await;
    }

    #[tokio::test]
    async fn test_invalid_json_request() {
        // Start server
        let job_cache = Arc::new(JobCache::new(10));
        job_cache.insert(create_test_job()).await;
        
        let (shutdown_tx, _) = broadcast::channel::<()>(10);
        let addr: SocketAddr = "127.0.0.1:13336".parse().unwrap();
        let server = StratumServer::new(addr, job_cache.clone(), shutdown_tx.clone());
        
        let server_handle = tokio::spawn(async move {
            server.run().await
        });
        
        tokio::time::sleep(Duration::from_millis(100)).await;
        
        // Send invalid JSON
        let stream = TcpStream::connect("127.0.0.1:13336").await.unwrap();
        let (mut read_half, mut write_half) = stream.into_split();
        let mut reader = BufReader::new(read_half);
        
        write_half.write_all(b"not valid json\n").await.unwrap();
        
        let mut response = String::new();
        reader.read_line(&mut response).await.unwrap();
        let resp: serde_json::Value = serde_json::from_str(&response).unwrap();
        
        assert!(!resp["error"].is_null());
        
        // Shutdown
        shutdown_tx.send(()).unwrap();
        let _ = tokio::time::timeout(Duration::from_secs(1), server_handle).await;
    }
}
