use anyhow::Result;
use rusqlite::Connection;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::RwLock;
use parking_lot::Mutex;
use tokio::signal;
use tracing::{info, Level};
use tracing_subscriber::FmtSubscriber;

use stratum_server_nng::http_api::{self, AppState, ServerStats};
use stratum_server_nng::shutdown::ShutdownCoordinator;
use stratum_server_nng::node_integration::{NngRpcClient, JobCache, template_to_job};
use stratum_server_nng::stratum_protocol::server::StratumServer;
use stratum_server_nng::accounting::init_schema;

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    let _subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .with_target(false)
        .init();

    info!("stratum-server-nng starting (Slice 1 & 2: Minimal Server + NNG Integration)");

    // Configuration
    let nng_rpc_url = std::env::var("NNG_RPC_URL")
        .unwrap_or_else(|_| "ipc:///tmp/lotusd.rpc".to_string());
    let stratum_bind: SocketAddr = "0.0.0.0:3334".parse()?;
    let http_bind: SocketAddr = "127.0.0.1:18080".parse()?;
    let db_path = std::env::var("DATABASE_PATH")
        .unwrap_or_else(|_| "stratum.db".to_string());

    // Create shutdown coordinator
    let shutdown = Arc::new(ShutdownCoordinator::new());
    let shutdown_tx = shutdown.broadcast_channel();

    // Initialize database
    info!(path = %db_path, "initializing database");
    let db_conn = Connection::open(&db_path)?;
    init_schema(&db_conn)?;
    info!("database initialized");

    // Create shared state
    let stats = Arc::new(RwLock::new(ServerStats::default()));
    let app_state = AppState {
        stats: stats.clone(),
    };

    // Create NNG RPC client and job cache
    let nng_client = Arc::new(NngRpcClient::new(nng_rpc_url.clone()));
    let job_cache = Arc::new(JobCache::new(512));

    // Connect to lotusd and fetch initial template
    info!(url = %nng_rpc_url, "connecting to lotusd");
    nng_client.connect().await?;
    
    info!("fetching initial mining template");
    let template = nng_client.get_mining_template().await?;
    info!(
        template_id = template.template_id,
        height = template.height,
        "fetched mining template"
    );

    // Convert template to job and cache it
    let job = template_to_job(&template, "00000000");
    job_cache.insert(job.clone()).await;
    info!(job_id = %job.job_id, "cached mining job");

    // Update stats with network difficulty
    {
        let mut s = stats.write().await;
        s.network_difficulty = Some(job.network_target_hex.clone());
    }

    // Wrap database connection for thread-safe access
    let _db_conn = Arc::new(Mutex::new(db_conn)); // Kept for future slices (share recording)

    // Start HTTP API server
    let http_state = app_state.clone();
    let http_shutdown_signal = shutdown.signal();
    let http_handle = tokio::spawn(async move {
        let router = http_api::create_router(http_state);
        let listener = tokio::net::TcpListener::bind(http_bind).await?;
        info!(bind = %http_bind, "HTTP API listening");

        let mut shutdown_signal = http_shutdown_signal;
        
        tokio::select! {
            result = axum::serve(listener, router) => {
                result?;
            }
            _ = shutdown_signal.recv() => {
                info!("HTTP API shutting down");
            }
        }

        Ok::<_, anyhow::Error>(())
    });

    // Start Stratum TCP server
    let stratum_shutdown_signal = shutdown.signal();
    let stratum_server = Arc::new(StratumServer::new(
        stratum_bind,
        job_cache.clone(),
        shutdown_tx.clone(),
    ));
    let stratum_for_stats = stratum_server.clone();
    let stratum_handle = tokio::spawn(async move {
        let mut shutdown_signal = stratum_shutdown_signal;
        
        tokio::select! {
            result = stratum_server.run() => {
                result?;
            }
            _ = shutdown_signal.recv() => {
                info!("Stratum server shutting down");
            }
        }

        Ok::<_, anyhow::Error>(())
    });

    // Stats updater - syncs connected miners count from Stratum server
    let stats_clone = stats.clone();
    let mut stats_shutdown_signal = shutdown.signal();
    let stats_handle = tokio::spawn(async move {
        let mut uptime = 0u64;
        loop {
            tokio::select! {
                _ = tokio::time::sleep(tokio::time::Duration::from_secs(1)) => {
                    uptime += 1;
                    let connected = stratum_for_stats.connected_miners().await;
                    let mut s = stats_clone.write().await;
                    s.uptime_secs = uptime;
                    s.connected_miners = connected;
                }
                _ = stats_shutdown_signal.recv() => {
                    info!("stats updater shutting down");
                    break;
                }
            }
        }
    });

    // Wait for shutdown signal
    info!(stratum = %stratum_bind, http = %http_bind, "server ready");
    info!("press Ctrl+C to shutdown");
    signal_ctrl_c(shutdown.clone()).await?;

    // Initiate graceful shutdown
    shutdown.initiate_shutdown();

    // Wait for tasks to complete
    let _ = tokio::join!(http_handle, stratum_handle, stats_handle);

    info!("server shutdown complete");
    Ok(())
}

async fn signal_ctrl_c(_shutdown: Arc<ShutdownCoordinator>) -> Result<()> {
    signal::ctrl_c().await?;
    info!("received SIGINT");
    Ok(())
}
