use anyhow::Result;
use clap::Parser;
use stratum_server_nng::accounting::{AccountingDb, PayoutMethod};
use stratum_server_nng::api::start_operator_api;
use stratum_server_nng::config::{CliArgs, Config};
use stratum_server_nng::payout::scheduler::run_payout_scheduler;
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
    info!(nng_rpc = %cfg.nng_rpc_url, nng_pub = %cfg.nng_pub_url, "nng endpoints configured");

    let stats = std::sync::Arc::new(RuntimeStats::default());

    let api_db = db.clone();
    let api_bind = cfg.api_bind.clone();
    let api_token = cfg.api_token.clone();

    let api_stats = stats.clone();
    let stratum_stats = stats.clone();

    let reconcile_db = db.clone();
    let reconcile_stats = stats.clone();
    let reconcile_task = tokio::spawn(async move {
        loop {
            tokio::time::sleep(std::time::Duration::from_secs(60)).await;
            if let Ok(missing_rows) = reconcile_db.reconcile_missing_found_blocks_detail() {
                let missing = missing_rows.len() as u64;
                reconcile_stats
                    .found_block_observed_not_persisted_total
                    .store(missing, std::sync::atomic::Ordering::Relaxed);
                if let Some(first) = missing_rows.first() {
                    tracing::error!(missing, block_hash = %first.block_hash, "accepted submit events missing found_block persistence");
                }
            }
        }
    });

    let scheduler_cfg = cfg.clone();
    let scheduler_db = db.clone();

    let api_task =
        tokio::spawn(
            async move { start_operator_api(api_bind, api_token, api_db, api_stats).await },
        );
    let scheduler_task =
        tokio::spawn(async move { run_payout_scheduler(scheduler_cfg, scheduler_db).await });
    let stratum_task =
        tokio::spawn(async move { run_stratum_server(cfg, db, stratum_stats).await });

    let (api_res, stratum_res, _reconcile_res, scheduler_res) =
        tokio::join!(api_task, stratum_task, reconcile_task, scheduler_task);
    scheduler_res??;
    api_res??;
    stratum_res??;
    Ok(())
}
