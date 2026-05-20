use anyhow::Result;
use rusqlite::Connection;
use std::sync::Arc;
use tokio::sync::RwLock;
use parking_lot::Mutex;
use tokio::signal::unix::{signal, SignalKind};
use tracing::{info, Level};
use tracing_subscriber::FmtSubscriber;

use stratum_server_nng::config::Config;
use stratum_server_nng::http_api::{self, AppState, ServerStats};
use stratum_server_nng::shutdown::ShutdownCoordinator;
use stratum_server_nng::node_integration::{NngRpcClient, JobCache, template_to_job, JsonRpcClient};
use stratum_server_nng::stratum_protocol::server::StratumServer;
use stratum_server_nng::accounting::{init_schema, AccountingService};

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    let _subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .with_target(false)
        .init();

    info!("stratum-server-nng starting (Slice 1 & 2: Minimal Server + NNG Integration)");

    // Load configuration
    let config = Config::load()?;
    info!(
        stratum_bind = %config.stratum_bind,
        api_bind = %config.api_bind,
        nng_rpc_url = %config.nng_rpc_url,
        sqlite_path = %config.sqlite_path,
        "loaded configuration"
    );

    // Initialize database
    info!(path = %config.sqlite_path, "initializing database");
    let db_conn = Connection::open(&config.sqlite_path)?;
    init_schema(&db_conn)?;
    info!("database initialized");

    // Create shutdown coordinator with database connection for WAL checkpoint
    let db_conn_arc = Arc::new(Mutex::new(db_conn));
    let shutdown = Arc::new(ShutdownCoordinator::with_db_conn(db_conn_arc.clone()));
    let shutdown_tx = shutdown.broadcast_channel();

    // Create shared state
    let stats = Arc::new(RwLock::new(ServerStats::default()));
    // Create NNG RPC client and job cache
    let nng_client = Arc::new(NngRpcClient::new(config.nng_rpc_url.clone()));
    let job_cache = Arc::new(JobCache::new(512));

    // Connect to lotusd and fetch initial template
    info!(url = %config.nng_rpc_url, "connecting to lotusd");
    nng_client.connect().await?;
    
    info!("fetching initial mining template");
    let template = nng_client.get_mining_template().await?;
    info!(
        template_id = template.template_id,
        height = template.height,
        "fetched mining template"
    );

    // Convert template to job and cache it
    let job = template_to_job(&template);
    job_cache.insert(job.clone()).await;
    info!(job_id = %job.job_id, "cached mining job");

    // Update stats with network difficulty
    {
        let mut s = stats.write().await;
        s.network_difficulty = Some(job.network_target_hex.clone());
    }

    // Notify the Stratum server about the new job, broadcasting N_diff to all
    // Create AccountingService (wraps all accounting repositories)
    let accounting_service = AccountingService::new(db_conn_arc.clone());
    let app_state = AppState {
        stats: stats.clone(),
        share_repo: Some(accounting_service.share_repo.clone()),
        worker_repo: Some(accounting_service.worker_repo.clone()),
        round_repo: Some(accounting_service.round_repo.clone()),
        found_block_repo: Some(accounting_service.found_block_repo.clone()),
        api_token: config.api_token.clone(),
    };

    // Start HTTP API server
    let http_state = app_state.clone();
    let http_shutdown_signal = shutdown.signal();
    let http_handle = tokio::spawn(async move {
        let router = http_api::create_router(http_state);
        let listener = tokio::net::TcpListener::bind(config.api_bind).await?;
        info!(bind = %config.api_bind, "HTTP API listening");

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
    shutdown.register_task(http_handle);

    // Start Stratum TCP server
    let stratum_shutdown_signal = shutdown.signal();
    // Create JSON-RPC client for lotusd (block submission, chain queries)
    let json_rpc_client = Arc::new(JsonRpcClient::new(
        &config.bitcoind_rpc.url,
        &config.bitcoind_rpc.rpc_user,
        &config.bitcoind_rpc.rpc_pass,
    ));

    // Block reconciliation: validate found_blocks against current chain state
    info!("reconciling found_blocks against chain state");
    {
        let rpc = json_rpc_client.clone();
        match rpc.getblockcount().await {
            Ok(tip_height) => {
                let tip = tip_height as i64;
                if let Err(e) = accounting_service.reconcile_found_blocks(tip, |height| {
                    let rpc = rpc.clone();
                    async move { rpc.getblockhash(height).await }
                }).await {
                    tracing::warn!(error = %e, "found_block reconciliation encountered errors");
                }
            }
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    "block reconciliation skipped: could not get chain tip"
                );
            }
        }
    }

    // Clone accounting service for the NNG event consumer (stratum server also needs it)
    let nng_accounting = accounting_service.clone();

    let stratum_server = Arc::new(StratumServer::new(
        config.stratum_bind,
        job_cache.clone(),
        shutdown_tx.clone(),
        Some(accounting_service),
        Some(json_rpc_client.clone()),
        config.vardiff.into(),
    ));
    // Notify server of the new job, broadcasting N_diff to all sessions.
    // Validates the integration path for future template refreshes (Slice 6).
    stratum_server.notify_new_job(&job).await;

    // Start NNG pub/sub event consumer (template refresh, reorg detection)
    let consumer_shutdown_rx = shutdown_tx.subscribe();
    match stratum_server_nng::node_integration::NngEventConsumer::new(
        &config.nng_pub_url,
        nng_client.clone(),
        job_cache.clone(),
        Some(nng_accounting),
        stratum_server.job_tx(),
    ) {
        Ok(consumer) => {
            let consumer_handle = tokio::spawn(async move {
                if let Err(e) = consumer.run(consumer_shutdown_rx).await {
                    tracing::warn!(error = %e, "NNG event consumer exited with error");
                }
            });
            shutdown.register_task(consumer_handle);
            tracing::info!("NNG pub/sub event consumer started");
        }
        Err(e) => {
            tracing::warn!(
                error = %e,
                "failed to start NNG event consumer (pub/sub may be unavailable)",
            );
        }
    }

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
    shutdown.register_task(stratum_handle);

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
    shutdown.register_task(stats_handle);

    // Wait for shutdown signal
    info!(stratum = %config.stratum_bind, http = %config.api_bind, "server ready");
    info!("press Ctrl+C for graceful shutdown, Ctrl+\\ for emergency shutdown");
    let shutdown_type = wait_for_shutdown_signal().await?;

    // Initiate shutdown (graceful or emergency)
    match shutdown_type {
        ShutdownType::Graceful => {
            info!("initiating graceful shutdown");
            shutdown.initiate_shutdown();
            
            // Wait for all registered tasks to complete
            shutdown.wait_for_completion().await?;
            
            // Disconnect from lotusd
            nng_client.disconnect().await;
            
            info!("server shutdown complete");
        }
        ShutdownType::Emergency => {
            info!("initiating emergency shutdown (no flush)");
            shutdown.initiate_emergency_shutdown();
            // Exit immediately without waiting for tasks
            std::process::exit(1);
        }
    }
    Ok(())
}

#[derive(Debug, Clone, Copy)]
enum ShutdownType {
    Graceful,
    Emergency,
}

async fn wait_for_shutdown_signal() -> Result<ShutdownType> {
    let mut sigint = signal(SignalKind::interrupt())?;
    let mut sigterm = signal(SignalKind::terminate())?;
    let mut sigquit = signal(SignalKind::quit())?;

    tokio::select! {
        _ = sigint.recv() => {
            info!("received SIGINT");
            Ok(ShutdownType::Graceful)
        }
        _ = sigterm.recv() => {
            info!("received SIGTERM");
            Ok(ShutdownType::Graceful)
        }
        _ = sigquit.recv() => {
            info!("received SIGQUIT");
            Ok(ShutdownType::Emergency)
        }
    }
}


