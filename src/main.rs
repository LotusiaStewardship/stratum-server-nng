use anyhow::Result;
use clap::Parser;
use stratum_server_nng::accounting::{AccountingDb, PayoutMethod};
use stratum_server_nng::api::start_operator_api;
use stratum_server_nng::config::Config;
use stratum_server_nng::stratum::server::{run_stratum_server, RuntimeStats};
use tracing::info;

#[tokio::main]
async fn main() -> Result<()> {
    let cfg = Config::parse();

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

    let api_task =
        tokio::spawn(
            async move { start_operator_api(api_bind, api_token, api_db, api_stats).await },
        );
    let stratum_task =
        tokio::spawn(async move { run_stratum_server(cfg, db, stratum_stats).await });

    let (api_res, stratum_res) = tokio::join!(api_task, stratum_task);
    api_res??;
    stratum_res??;
    Ok(())
}
