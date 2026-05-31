mod logging;

use anyhow::Result;
use parking_lot::Mutex;
use rusqlite::Connection;
use std::sync::Arc;
use tokio::signal::unix::{signal, SignalKind};
use tokio::sync::RwLock;
use tracing_subscriber::{EnvFilter, FmtSubscriber};

use stratum_server_nng::accounting::{init_schema, AccountingService, ChainTip};
use stratum_server_nng::config::Config;
use stratum_server_nng::http_api::{self, AppState, ServerStats};
use stratum_server_nng::node_integration::{
    template_to_job, JobCache, JsonRpcClient, NngRpcClient,
};
use stratum_server_nng::payout::handler::PayoutHandler;
use stratum_server_nng::payout::signer::{
    external::ExternalSigner, internal::InternalSigner, Signer,
};
use stratum_server_nng::payout::PayoutEvent;
use stratum_server_nng::shutdown::ShutdownCoordinator;
use stratum_server_nng::stratum_protocol::server::StratumServer;

#[tokio::main]
async fn main() -> Result<()> {
    // Load configuration first (needed for logging setup)
    let config = Config::load()?;

    // Initialize logging — EnvFilter reads RUST_LOG first, falls back to config.debug
    let log_filter = if config.debug {
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("debug"))
    } else {
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"))
    };
    let _subscriber = FmtSubscriber::builder()
        .with_env_filter(log_filter)
        .with_target(true)
        .init();

    main_info!("stratum-server-nng starting (Slice 1 & 2: Minimal Server + NNG Integration)");
    main_info!(
        stratum_bind = %config.stratum_bind,
        api_bind = %config.api_bind,
        nng_rpc_url = %config.nng_rpc_url,
        sqlite_path = %config.sqlite_path,
        "loaded configuration"
    );

    // Initialize database
    main_info!(path = %config.sqlite_path, "initializing database");
    let db_conn = Connection::open(&config.sqlite_path)?;
    init_schema(&db_conn)?;
    main_info!("database initialized");

    // Create shutdown coordinator with database connection for WAL checkpoint
    let db_conn_arc = Arc::new(Mutex::new(db_conn));
    let shutdown = Arc::new(ShutdownCoordinator::with_db_conn(db_conn_arc.clone()));
    let shutdown_tx = shutdown.broadcast_channel();

    // Create shared state
    let stats = Arc::new(RwLock::new(ServerStats::default()));

    // Resolve mining identity before connecting (fail fast on misconfiguration)
    // Without payout_address, lotusd creates OP_RETURN outputs and block rewards are BURNED.
    let mining_id = config.pool.mining_identity.as_ref().ok_or_else(|| {
        anyhow::anyhow!(
            "[pool.mining_identity] section not found in config. \
             Without payout_address, lotusd creates OP_RETURN outputs and \
             block rewards are BURNED. See config.example.toml."
        )
    })?;
    let (coinbase_script, coinbase_identity) = mining_id.resolve()?;

    main_info!(
        "mining identity resolved: coinbase_script={} bytes, coinbase_identity={}",
        coinbase_script.len(),
        coinbase_identity
            .as_ref()
            .map(|b| b.len())
            .map_or("none".to_string(), |l| format!("{} bytes", l)),
    );

    // Create NNG RPC client and job cache
    let nng_client = Arc::new(NngRpcClient::new(
        config.nng_rpc_url.clone(),
        Some(coinbase_script),
        coinbase_identity,
    ));
    let job_cache = Arc::new(JobCache::new(512));

    // Connect to lotusd and fetch initial template
    main_info!(url = %config.nng_rpc_url, "connecting to lotusd");
    nng_client.connect().await?;

    main_info!("fetching initial mining template");
    let template = nng_client.get_mining_template().await?;
    main_info!(
        template_id = template.template_id,
        height = template.height,
        "fetched mining template"
    );

    // DIAGNOSTIC: verify the template coinbase has spendable (non-OP_RETURN) outputs.
    // If all outputs are OP_RETURN, block rewards will be burned.
    if let Err(e) = stratum_server_nng::node_integration::verify_coinbase_outputs(&template) {
        main_warn!(
            "coinbase output check: {}. \
             If this pool finds a block, the reward may be BURNED. \
             Check pool.mining_identity configuration.",
            e,
        );
    }

    // Convert template to job and cache it
    let job = template_to_job(&template, false)?;
    job_cache.insert(job.clone()).await;
    main_info!(job_id = %job.job_id, "cached mining job");
    main_debug!(
        job_id = %job.job_id,
        template_id = job.template_id,
        prevhash = %job.prevhash,
        coinbase1 = %job.coinbase1,
        coinbase2 = %job.coinbase2,
        merkle_branches = %serde_json::to_string(&job.merkle_branches).unwrap_or_default(),
        version = %job.version,
        nbits = %job.nbits,
        ntime = %job.ntime,
        network_target_hex = %job.network_target_hex,
        clean_jobs = job.clean_jobs,
        template_epoch = job.template_epoch,
        height = job.height,
        epoch_hash = %job.epoch_hash,
        extended_metadata_hash = %job.extended_metadata_hash,
        block_size = job.block_size,
        "mining job details",
    );

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
        payout_repo: Some(accounting_service.payout_repo.clone()),
        api_token: config.api_token.clone(),
        accounting_service: Some(accounting_service.clone()),
        payout_config: Some(stratum_server_nng::http_api::PayoutConfig {
            fee_bps: config.pool.fee.fee_bps,
            fee_address: config.pool.fee.fee_address.clone(),
            min_payout_sat: config.pool.pplns.min_payout_sat,
            n_multiplier: config.pool.pplns.n_multiplier,
        }),
    };

    // Start HTTP API server
    let http_state = app_state.clone();
    let http_shutdown_signal = shutdown.signal();
    let http_handle = tokio::spawn(async move {
        let router = http_api::create_router(http_state);
        let listener = tokio::net::TcpListener::bind(config.api_bind).await?;
        main_info!(bind = %config.api_bind, "HTTP API listening");

        let mut shutdown_signal = http_shutdown_signal;

        tokio::select! {
            result = axum::serve(listener, router) => {
                result?;
            }
            _ = shutdown_signal.recv() => {
                main_info!("HTTP API shutting down");
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
    main_info!("reconciling found_blocks against chain state");
    {
        let rpc = json_rpc_client.clone();
        match rpc.getblockcount().await {
            Ok(tip_height) => {
                if let Err(e) = accounting_service
                    .reconcile_found_blocks(tip_height, |height| {
                        let rpc = rpc.clone();
                        async move { rpc.getblockhash(height).await }
                    })
                    .await
                {
                    main_warn!(error = %e, "found_block reconciliation encountered errors");
                }
            }
            Err(e) => {
                main_warn!(
                    error = %e,
                    "block reconciliation skipped: could not get chain tip"
                );
            }
        }
    }

    // Payout reconciliation: check submitted payouts against on-chain state
    main_info!("reconciling submitted payouts against chain state");
    {
        let rpc = json_rpc_client.clone();
        match accounting_service
            .reconcile_submitted_payouts(|txid: String| {
                let rpc = rpc.clone();
                async move {
                    match rpc.get_raw_transaction(&txid).await {
                        Ok(tx) => {
                            let confirms = tx["confirmations"].as_i64().unwrap_or(0);
                            Ok(Some(confirms))
                        }
                        Err(e) => {
                            // "No such mempool or blockchain transaction" — not found
                            let err_str = e.to_string();
                            if err_str.contains("No such mempool")
                                || err_str.contains("Invalid txid")
                            {
                                Ok(None)
                            } else {
                                Err(e)
                            }
                        }
                    }
                }
            })
            .await
        {
            Ok(reconciled) => {
                for (batch_id, txid, status) in &reconciled {
                    main_info!(
                        batch_id = batch_id,
                        txid = %txid,
                        status = %status,
                        "payout reconciled at startup",
                    );
                }
            }
            Err(e) => {
                main_warn!(
                    error = %e,
                    "payout reconciliation encountered errors",
                );
            }
        }
    }

    let nng_accounting = accounting_service.clone();
    let payout_accounting = accounting_service.clone();

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

    // Create shared chain tip tracker and maturation event channel
    let chain_tip = ChainTip::new(0);
    let (maturation_tx, maturation_rx) = tokio::sync::mpsc::unbounded_channel::<PayoutEvent>();

    // Start NNG pub/sub event consumer (template refresh, reorg detection, maturation)
    let consumer_shutdown_rx = shutdown_tx.subscribe();
    match stratum_server_nng::node_integration::NngEventConsumer::new(
        &config.nng_pub_url,
        nng_client.clone(),
        job_cache.clone(),
        Some(nng_accounting),
        stratum_server.job_tx(),
        chain_tip.clone(),
        maturation_tx.clone(),
        config.pool.pplns.min_confirmations,
    ) {
        Ok(consumer) => {
            let consumer_handle = tokio::spawn(async move {
                if let Err(e) = consumer.run(consumer_shutdown_rx).await {
                    main_warn!(error = %e, "NNG event consumer exited with error");
                }
            });
            shutdown.register_task(consumer_handle);
            main_info!("NNG pub/sub event consumer started");
        }
        Err(e) => {
            main_warn!(
                error = %e,
                "failed to start NNG event consumer (pub/sub may be unavailable)",
            );
        }
    }

    // Startup maturation reconciliation: check blocks that matured while offline
    main_info!("checking for newly matured blocks at startup");
    {
        let rpc = json_rpc_client.clone();
        match rpc.getblockcount().await {
            Ok(tip_height) => {
                match payout_accounting
                    .check_maturation(tip_height, config.pool.pplns.min_confirmations)
                {
                    Ok(matured) => {
                        for block in &matured {
                            main_info!(
                                hash = %block.block_hash,
                                height = block.height,
                                "block matured during startup reconciliation",
                            );
                            let _ = maturation_tx
                                .send(PayoutEvent::BlockMatured(block.block_hash.clone()));
                        }
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, "startup maturation check failed");
                    }
                }
            }
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    "startup maturation check skipped: could not get chain tip"
                );
            }
        }
    }

    // Construct signer and spawn payout handler (if enabled)
    if config.pool.pplns.payout_enabled {
        let signer: Arc<dyn Signer> = match config.pool.signing.mode.as_str() {
            "internal" => {
                let key = config.pool.signing.private_key.as_deref().ok_or_else(|| {
                    anyhow::anyhow!("pool.signing.private_key required for internal signing mode")
                })?;
                Arc::new(InternalSigner::new(
                    key,
                    json_rpc_client.clone(),
                    config.pool.signing.tx_fee_per_kb,
                )?)
            }
            "external" => {
                let url = config.pool.signing.webhook_url.as_deref().ok_or_else(|| {
                    anyhow::anyhow!("pool.signing.webhook_url required for external signing mode")
                })?;
                Arc::new(ExternalSigner::new(url.to_string()))
            }
            other => anyhow::bail!(
                "unknown signing mode '{}' — expected 'internal' or 'external'",
                other
            ),
        };

        let mut handler = PayoutHandler::new(
            maturation_rx,
            payout_accounting,
            signer,
            json_rpc_client.clone(),
            config.pool.fee.fee_bps,
            config.pool.fee.fee_address.clone(),
            config.pool.pplns.min_payout_sat,
            config.pool.pplns.n_multiplier,
        );
        let mut payout_shutdown_signal = shutdown.signal();
        let payout_handle = tokio::spawn(async move {
            tokio::select! {
                _ = handler.run() => {}
                _ = payout_shutdown_signal.recv() => {
                    main_info!("payout handler shutting down");
                }
            }
        });
        shutdown.register_task(payout_handle);
        main_info!(
            "payout handler started (event-driven, {} mode)",
            config.pool.signing.mode
        );
    } else {
        main_info!("payout handler disabled by config.pool.pplns.payout_enabled");
        // Drop the sender so the channel can close cleanly
        drop(maturation_tx);
    }

    let stratum_for_stats = stratum_server.clone();
    let stratum_handle = tokio::spawn(async move {
        let mut shutdown_signal = stratum_shutdown_signal;

        tokio::select! {
            result = stratum_server.run() => {
                result?;
            }
            _ = shutdown_signal.recv() => {
                main_info!("Stratum server shutting down");
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
                    main_info!("stats updater shutting down");
                    break;
                }
            }
        }
    });
    shutdown.register_task(stats_handle);

    // Wait for shutdown signal
    main_info!(stratum = %config.stratum_bind, http = %config.api_bind, "server ready");
    main_info!("press Ctrl+C for graceful shutdown, Ctrl+\\ for emergency shutdown");
    let shutdown_type = wait_for_shutdown_signal().await?;

    // Initiate shutdown (graceful or emergency)
    match shutdown_type {
        ShutdownType::Graceful => {
            main_info!("initiating graceful shutdown");
            shutdown.initiate_shutdown();

            // Wait for all registered tasks to complete
            shutdown.wait_for_completion().await?;

            // Disconnect from lotusd
            nng_client.disconnect().await;

            main_info!("server shutdown complete");
        }
        ShutdownType::Emergency => {
            main_info!("initiating emergency shutdown (no flush)");
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
            main_info!("received SIGINT");
            Ok(ShutdownType::Graceful)
        }
        _ = sigterm.recv() => {
            main_info!("received SIGTERM");
            Ok(ShutdownType::Graceful)
        }
        _ = sigquit.recv() => {
            main_info!("received SIGQUIT");
            Ok(ShutdownType::Emergency)
        }
    }
}
