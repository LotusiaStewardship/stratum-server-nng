use anyhow::Result;
use clap::Parser;
use stratum_server_nng::accounting::{AccountingDb, PayoutMethod};
use stratum_server_nng::api::start_operator_api;
use stratum_server_nng::config::Config;
use tracing::info;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "stratum_server_nng=info".into()),
        )
        .init();

    let cfg = Config::parse();

    let db = AccountingDb::open(&cfg.sqlite_path)?;
    db.init_schema()?;
    db.set_active_payout_method(PayoutMethod::Pplns)?;

    info!("stratum-server-nng initialized");
    info!(stratum_bind = %cfg.stratum_bind, "stratum listener configured");
    info!(nng_rpc = %cfg.nng_rpc_url, nng_pub = %cfg.nng_pub_url, "nng endpoints configured");

    // Phase S3/S4 focus: accounting core and validation primitives are in
    // place, with operator API + auth scaffold active now.
    start_operator_api(cfg.api_bind.clone(), cfg.api_token.clone(), db).await?;

    Ok(())
}
