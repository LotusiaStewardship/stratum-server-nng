use anyhow::Result;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::signal;
use tracing::{info, Level};
use tracing_subscriber::FmtSubscriber;

use stratum_server_nng::http_api::{self, AppState, ServerStats};
use stratum_server_nng::shutdown::ShutdownCoordinator;

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .with_target(false)
        .init();

    info!("stratum-server-nng starting (Slice 1: Minimal Server)");

    // Create shutdown coordinator
    let shutdown = Arc::new(ShutdownCoordinator::new());

    // Create shared state
    let stats = Arc::new(RwLock::new(ServerStats::default()));
    let app_state = AppState {
        stats: stats.clone(),
    };

    // Start HTTP API server
    let http_bind = "127.0.0.1:18080";
    let http_state = app_state.clone();
    let http_shutdown = shutdown.clone();
    let http_handle = tokio::spawn(async move {
        let router = http_api::create_router(http_state);
        let listener = tokio::net::TcpListener::bind(http_bind).await?;
        info!(bind = %http_bind, "HTTP API listening");

        let mut shutdown_signal = http_shutdown.signal();
        
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

    // Update stats for demo (uptime counter)
    let stats_clone = stats.clone();
    let mut shutdown_signal = shutdown.signal();
    let stats_handle = tokio::spawn(async move {
        let mut uptime = 0u64;
        loop {
            tokio::select! {
                _ = tokio::time::sleep(tokio::time::Duration::from_secs(1)) => {
                    uptime += 1;
                    let mut s = stats_clone.write().await;
                    s.uptime_secs = uptime;
                }
                _ = shutdown_signal.recv() => {
                    info!("stats updater shutting down");
                    break;
                }
            }
        }
    });

    // Wait for shutdown signal
    info!("press Ctrl+C to shutdown");
    signal_ctrl_c(shutdown.clone()).await?;

    // Initiate graceful shutdown
    shutdown.initiate_shutdown();

    // Wait for tasks to complete
    let _ = tokio::join!(http_handle, stats_handle);

    info!("server shutdown complete");
    Ok(())
}

async fn signal_ctrl_c(shutdown: Arc<ShutdownCoordinator>) -> Result<()> {
    signal::ctrl_c().await?;
    info!("received SIGINT");
    Ok(())
}
