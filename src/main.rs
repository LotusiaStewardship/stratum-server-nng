use anyhow::Result;
use clap::Parser;
use stratum_server_nng::accounting::{AccountingDb, PayoutMethod};
use stratum_server_nng::api::start_operator_api;
use stratum_server_nng::config::{CliArgs, Config};
use stratum_server_nng::http::{start_http_dashboard, DashboardEventSender};
use stratum_server_nng::payout::scheduler::run_payout_scheduler;
use stratum_server_nng::stratum::share_aggregator::ShareAggregator;
use stratum_server_nng::stratum::diff_cache::DifficultyCache;
use stratum_server_nng::stratum::network_diff::{DynamicDiffConfig, NetworkDifficultyTracker};
use stratum_server_nng::stratum::server::{run_stratum_server, RuntimeStats};
use tracing::info;

#[tokio::main]
async fn main() -> Result<()> {
    let cli = CliArgs::parse();
    let cfg = Config::load(&cli)?;

    let default_filter = if cfg.debug {
        "stratum_server_nng=debug,bitcoinsuite_bitcoind_nng=info"
    } else {
        "stratum_server_nng=info"
    };

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| default_filter.into()),
        )
        .init();

    let db = AccountingDb::open(&cfg.sqlite_path)?;
    db.init_schema()?;
    db.set_active_payout_method(PayoutMethod::Pplns)?;

    info!("stratum-server-nng initialized");
    info!(
        debug = cfg.debug,
        stratum_bind = %cfg.stratum_bind,
        api_bind = %cfg.api_bind,
        sqlite_path = %cfg.sqlite_path,
        "runtime configuration loaded"
    );
    info!(
        network = %cfg.network,
        nng_rpc = %cfg.nng_rpc_url,
        "network detected (auto-derived from RPC port)"
    );
    info!(nng_rpc = %cfg.nng_rpc_url, nng_pub = %cfg.nng_pub_url, "nng endpoints configured");
    info!(
        http_enabled = cfg.http_enabled,
        http_bind = %cfg.http_bind,
        "HTTP dashboard configured"
    );

    let stats = std::sync::Arc::new(RuntimeStats::default());

    // Initialize dynamic difficulty tracker
    let diff_config = DynamicDiffConfig {
        vardiff_target_secs: cfg.vardiff.vardiff_target_secs,
        vardiff_retarget_secs: cfg.vardiff.vardiff_retarget_secs,
    };
    let tracker = NetworkDifficultyTracker::new(diff_config);
    let diff_cache = DifficultyCache::new(tracker);

    info!(
        vardiff_min_floor = cfg.vardiff.vardiff_min_floor,
        vardiff_initial_pct = cfg.vardiff.vardiff_initial_pct,
        vardiff_target_secs = cfg.vardiff.vardiff_target_secs,
        vardiff_retarget_secs = cfg.vardiff.vardiff_retarget_secs,
        "dynamic pool difficulty initialized (network-aware scaling)"
    );

    let api_db = db.clone();
    let api_bind = cfg.api_bind.clone();
    let api_token = cfg.api_token.clone();
    let api_stats = stats.clone();

    // HTTP dashboard event sender (only if enabled)
    let events_tx = DashboardEventSender::new(cfg.http_enabled);
    
    // Share aggregator for periodic broadcast (only if HTTP dashboard enabled)
    let share_aggregator = ShareAggregator::new(db.clone(), events_tx.clone(), 30);
    
    // HTTP dashboard
    let http_enabled = cfg.http_enabled;
    let http_db = db.clone();
    let http_bind = cfg.http_bind.clone();
    let http_stats = stats.clone();
    let http_pool_config = cfg.pool.clone();
    let http_config = cfg.clone();
    let http_diff_cache = diff_cache.clone();
    let http_events_tx = events_tx.clone();

    let reconcile_db = db.clone();
    let reconcile_stats = stats.clone();
    let reconcile_task = tokio::spawn(async move {
        loop {
            tokio::time::sleep(std::time::Duration::from_secs(60)).await;
            if let Ok(missing_rows) = reconcile_db.list_repairable_missing_found_blocks() {
                let missing = missing_rows.len() as u64;
                reconcile_stats
                    .found_block_observed_not_persisted_total
                    .store(missing, std::sync::atomic::Ordering::Relaxed);
                if missing > 0 {
                    let repaired = reconcile_db
                        .repair_missing_found_blocks_from_submit_events()
                        .unwrap_or(0);
                    tracing::error!(
                        missing,
                        repaired,
                        "accepted submit events missing found_block persistence"
                    );
                }
            }
        }
    });

    let scheduler_cfg = cfg.clone();
    let scheduler_db = db.clone();
    let stratum_cfg = cfg.clone();
    let stratum_db = db.clone();
    let stratum_diff_cache = diff_cache.clone();

    let api_task =
        tokio::spawn(
            async move { start_operator_api(api_bind, api_token, api_db, api_stats).await },
        );
    let http_task = tokio::spawn(async move {
        if http_enabled {
            start_http_dashboard(http_bind, http_db, http_stats, http_pool_config, http_config, http_diff_cache, http_events_tx).await
        } else {
            tracing::info!("HTTP dashboard disabled by configuration");
            Ok(())
        }
    });
    
    // Spawn share aggregator broadcast loop
    let aggregator_clone = share_aggregator.clone();
    tokio::spawn(async move {
        let _ = aggregator_clone.spawn_broadcast_loop().await;
    });
    
    let scheduler_task =
        tokio::spawn(async move { run_payout_scheduler(scheduler_cfg, scheduler_db).await });
    let stratum_task = tokio::spawn(async move {
        run_stratum_server(stratum_cfg, stratum_db, stats.clone(), stratum_diff_cache, events_tx, share_aggregator).await
    });

    let (api_res, stratum_res, _reconcile_res, scheduler_res, http_res) =
        tokio::join!(api_task, stratum_task, reconcile_task, scheduler_task, http_task);
    scheduler_res??;
    api_res??;
    stratum_res??;
    http_res??;
    Ok(())
}
