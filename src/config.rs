use clap::Parser;

/// Runtime configuration for stratum-server-nng.
///
/// Defaults are conservative and local-dev friendly. Production operators
/// should explicitly set token, endpoint addresses, and database location.
#[derive(Debug, Clone, Parser)]
#[command(name = "stratum-server-nng")]
pub struct Config {
    /// Stratum TCP bind address.
    #[arg(long, default_value = "0.0.0.0:3334")]
    pub stratum_bind: String,

    /// Operator API bind address.
    #[arg(long, default_value = "127.0.0.1:18080")]
    pub api_bind: String,

    /// Required bearer token for operator API.
    #[arg(long)]
    pub api_token: String,

    /// SQLite file path for accounting state.
    #[arg(long, default_value = "./stratum-accounting.sqlite3")]
    pub sqlite_path: String,

    /// NNG RPC endpoint (ipc:// or tcp://).
    #[arg(long, default_value = "ipc://datadir/nngrpc.pipe")]
    pub nng_rpc_url: String,

    /// NNG Pub endpoint (ipc:// or tcp://).
    #[arg(long, default_value = "ipc://datadir/nngpub.pipe")]
    pub nng_pub_url: String,

    /// Initial worker share difficulty.
    #[arg(long, default_value_t = 1.0)]
    pub initial_difficulty: f64,

    /// Vardiff target share interval seconds.
    #[arg(long, default_value_t = 15.0)]
    pub vardiff_target_secs: f64,

    /// Vardiff retarget interval seconds.
    #[arg(long, default_value_t = 90.0)]
    pub vardiff_retarget_secs: f64,

    /// Minimum allowed vardiff.
    #[arg(long, default_value_t = 0.0000001)]
    pub min_difficulty: f64,

    /// Maximum allowed vardiff.
    #[arg(long, default_value_t = 1e12)]
    pub max_difficulty: f64,
}
