use anyhow::{anyhow, Result};
use bitcoinsuite_bitcoind_nng::encode_coinbase_identity_utf8;
use bitcoinsuite_core::{ecc::SecKey, Hashed, LotusAddress, Script, Sha256};
use clap::Parser;
use serde::Deserialize;

#[derive(Debug, Clone, Parser)]
#[command(name = "stratum-server-nng")]
pub struct CliArgs {
    #[arg(long, default_value = "./config.toml")]
    pub config: String,
    #[arg(long, default_value_t = false)]
    pub debug: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub debug: bool,
    pub stratum_bind: String,
    pub api_bind: String,
    pub api_token: String,
    #[serde(default)]
    pub sqlite_path: String,
    #[serde(default)]
    pub network: String,
    pub nng_rpc_url: String,
    pub nng_pub_url: String,
    pub bitcoind_rpc: BitcoindRpcConfig,
    pub vardiff: VarDiffConfig,
    pub max_request_line_bytes: usize,
    pub per_conn_req_per_sec: u32,
    pub conn_idle_timeout_secs: u64,
    pub max_jobs_cache: usize,
    //pub job_refresh_secs: u64,
    pub pool: PoolConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PoolConfig {
    pub mining_identity: MiningIdentityConfig,
    pub fee: FeeConfig,
    pub pplns: PplnsConfig,
    pub signing: SigningConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct MiningIdentityConfig {
    pub payout_script_hex: Option<String>,
    pub payout_address: Option<String>,
    pub coinbase_identity: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct FeeConfig {
    pub enabled: bool,
    pub fee_bps: u32,
    pub fee_address: Option<String>,
    pub fee_script_hex: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PplnsConfig {
    /// Difficulty-weighted PPLNS window multiplier.
    /// N = n_multiplier × network_difficulty (in raw difficulty units).
    /// Industry standard: 2.0 (spans ~2 expected rounds for variance smoothing).
    /// Values < 1.0 approach PROP behaviour; values > 5.0 make payouts too sticky.
    #[serde(default = "default_n_multiplier")]
    pub n_multiplier: f64,
    pub min_payout_sat: i64,
    pub payout_interval_secs: u64,
    pub min_confirmations: u32,
    /// Unique identifier for this pool instance (used for scheduler lease)
    #[serde(default = "default_instance_id")]
    pub instance_id: String,
    /// Invalid share banning configuration.
    #[serde(default)]
    pub banning: BanningConfig,
}

/// Configuration for invalid share banning.
///
/// When enabled, miners whose share rejection ratio exceeds the threshold
/// after submitting `check_threshold` shares are disconnected.
#[derive(Debug, Clone, Deserialize)]
pub struct BanningConfig {
    /// Enable or disable invalid share banning.
    #[serde(default = "default_banning_enabled")]
    pub enabled: bool,
    /// Number of shares to collect before checking the rejection ratio.
    #[serde(default = "default_check_threshold")]
    pub check_threshold: u32,
    /// Maximum allowed rejection percentage. Miners exceeding this are banned.
    #[serde(default = "default_invalid_percent")]
    pub invalid_percent: f64,
}

impl Default for BanningConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            check_threshold: 50,
            invalid_percent: 50.0,
        }
    }
}

fn default_n_multiplier() -> f64 {
    2.0
}
fn default_banning_enabled() -> bool {
    true
}
fn default_check_threshold() -> u32 {
    50
}
fn default_invalid_percent() -> f64 {
    50.0
}

fn default_instance_id() -> String {
    "default".to_string()
}

#[derive(Debug, Clone, Deserialize)]
pub struct SigningConfig {
    pub mode: String,
    pub private_key: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct BitcoindRpcConfig {
    pub url: String,
    pub rpc_user: String,
    pub rpc_pass: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct VarDiffConfig {
    /// Absolute minimum difficulty floor for VarDiff (safety only).
    /// This is the absolute lowest difficulty any miner can have.
    /// Default: 0.001 (very low, allows tiny miners to participate)
    #[serde(default = "default_vardiff_min_floor")]
    pub vardiff_min_floor: f64,

    /// Initial difficulty for new miners as a fraction of network difficulty.
    /// Miners start at `network_diff * vardiff_initial_pct` and VarDiff ramps
    /// up based on share rate, allowing the pool to calibrate per-miner hashrate.
    /// Default: 0.01 (1% of network difficulty)
    #[serde(default = "default_vardiff_initial_pct")]
    pub vardiff_initial_pct: f64,

    /// Target time between accepted shares for vardiff tuning.
    #[serde(default = "default_target_secs")]
    pub vardiff_target_secs: f64,

    /// How often vardiff is allowed to retarget per miner.
    #[serde(default = "default_retarget_secs")]
    pub vardiff_retarget_secs: f64,
}

fn default_vardiff_min_floor() -> f64 {
    0.001
}
fn default_vardiff_initial_pct() -> f64 {
    0.01
}
fn default_target_secs() -> f64 {
    15.0
}
fn default_retarget_secs() -> f64 {
    90.0
}

impl VarDiffConfig {
    pub fn validate(&self) -> Result<()> {
        bitcoinsuite_bitcoind_stratum::validate_vardiff_floor(self.vardiff_min_floor)
            .map_err(|e| anyhow::anyhow!("invalid vardiff config: {}", e))?;
        if self.vardiff_initial_pct <= 0.0
            || self.vardiff_initial_pct > 1.0
            || !self.vardiff_initial_pct.is_finite()
        {
            return Err(anyhow::anyhow!("vardiff_initial_pct must be in (0.0, 1.0]"));
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct ResolvedPoolScripts {
    pub payout_script: Vec<u8>,
    pub payout_fingerprint: String,
    pub fee_script: Option<Vec<u8>>,
    pub coinbase_identity_bytes: Option<Vec<u8>>,
}

impl Config {
    pub fn load(cli: &CliArgs) -> Result<Self> {
        let raw = std::fs::read_to_string(&cli.config)?;
        let mut cfg: Config = toml::from_str(&raw)?;
        if cli.debug {
            cfg.debug = true;
        }
        if let Ok(token) = std::env::var("STRATUM_API_TOKEN") {
            cfg.api_token = token;
        }

        // Auto-detect network from RPC port if not explicitly set
        if cfg.network.is_empty() {
            cfg.network = detect_network_from_rpc_port(&cfg.bitcoind_rpc.url)?.to_string();
        }

        // Auto-set database path if using default pattern or not explicitly set
        let should_override_db_path =
            cfg.sqlite_path.contains("stratum-accounting") || cfg.sqlite_path.is_empty();

        if should_override_db_path {
            cfg.sqlite_path = default_db_path_for_network(&cfg.network);
        }

        // Ensure parent directory exists for database path
        if let Some(parent) = std::path::Path::new(&cfg.sqlite_path).parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent)?;
            }
        }

        cfg.validate()?;
        Ok(cfg)
    }

    pub fn validate(&self) -> Result<()> {
        if self.api_token.is_empty() {
            anyhow::bail!("api_token required")
        }
        if self.network.is_empty() {
            anyhow::bail!("network required (auto-detected from RPC port or set explicitly)")
        }
        // Validate network is known value
        if !["mainnet", "testnet", "regtest"].contains(&self.network.as_str()) {
            anyhow::bail!(
                "network must be one of: mainnet, testnet, regtest (got: {})",
                self.network
            );
        }
        let _ = self.resolve_pool_scripts()?;
        self.vardiff.validate()?;
        Ok(())
    }

    pub fn resolve_pool_scripts(&self) -> Result<ResolvedPoolScripts> {
        let payout_script = resolve_script(
            self.pool.mining_identity.payout_script_hex.as_deref(),
            self.pool.mining_identity.payout_address.as_deref(),
        )?;
        reject_nulldata_script(&payout_script, "pool payout")?;

        let fee_script = if self.pool.fee.enabled {
            let s = resolve_script(
                self.pool.fee.fee_script_hex.as_deref(),
                self.pool.fee.fee_address.as_deref(),
            )?;
            reject_nulldata_script(&s, "pool fee")?;
            Some(s)
        } else {
            None
        };

        if self.pool.fee.enabled && self.pool.fee.fee_bps > 10_000 {
            anyhow::bail!("fee_bps must be <= 10000")
        }
        if self.pool.signing.mode == "internal" {
            let key = self.pool.signing.private_key.as_deref().ok_or_else(|| {
                anyhow!("pool.signing.private_key required for internal signer mode")
            })?;
            validate_private_key_format(key)?;
        }

        let coinbase_identity_bytes = self
            .pool
            .mining_identity
            .coinbase_identity
            .as_deref()
            .map(encode_coinbase_identity_utf8)
            .transpose()
            .map_err(|e| anyhow!("invalid pool.mining_identity.coinbase_identity: {e}"))?;

        Ok(ResolvedPoolScripts {
            payout_fingerprint: script_fingerprint(&payout_script),
            payout_script,
            fee_script,
            coinbase_identity_bytes,
        })
    }
}

fn validate_private_key_format(key: &str) -> Result<()> {
    SecKey::from_hex_or_wif(key)
        .map(|_| ())
        .map_err(|e| anyhow!("unsupported private key format: {e}"))
}

fn resolve_script(script_hex: Option<&str>, address: Option<&str>) -> Result<Vec<u8>> {
    if let Some(h) = script_hex {
        let script = Script::from_hex(h).map_err(|e| anyhow!("invalid script hex: {e}"))?;
        if script.bytecode().is_empty() {
            anyhow::bail!("script hex cannot be empty")
        }
        return Ok(script.bytecode().as_ref().to_vec());
    }
    if let Some(addr) = address {
        let lotus: LotusAddress = addr
            .parse()
            .map_err(|e| anyhow!("invalid payout address: {e}"))?;
        return Ok(lotus.script().bytecode().to_vec());
    }
    anyhow::bail!("must configure either script hex or address")
}

fn reject_nulldata_script(script: &[u8], label: &str) -> Result<()> {
    if script.first() == Some(&0x6a) {
        anyhow::bail!("{label} script cannot be OP_RETURN/nulldata")
    }
    Ok(())
}

fn script_fingerprint(script: &[u8]) -> String {
    let digest = Sha256::digest(script.to_vec().into());
    hex::encode(&digest.as_ref()[..6])
}

/// Extracts the network name from an RPC URL by parsing the port.
///
/// Returns:
/// - "mainnet" for port 10604
/// - "testnet" for port 11604
/// - "regtest" for port 12604
/// - Err for unknown ports or parse failures
fn detect_network_from_rpc_port(rpc_url: &str) -> Result<&'static str> {
    // Parse port from URL like "http://127.0.0.1:10604" or "tcp://host:10604"
    let port = url::Url::parse(rpc_url)
        .ok()
        .and_then(|u| u.port())
        .or_else(|| {
            // Fallback: manually extract port if URL parsing fails
            rpc_url
                .rsplit(':')
                .next()
                .and_then(|p| p.parse::<u16>().ok())
        })
        .ok_or_else(|| anyhow!("failed to parse port from rpc_url: {}", rpc_url))?;

    match port {
        10604 => Ok("mainnet"),
        11604 => Ok("testnet"),
        12604 => Ok("regtest"),
        _ => Err(anyhow!(
            "unknown network for RPC port {}: expected 10604 (mainnet), 11604 (testnet), or 12604 (regtest)",
            port
        )),
    }
}

/// Returns the default database path for a given network.
///
/// Convention: ./dbs/{network}/stratum-accounting.sqlite3
fn default_db_path_for_network(network: &str) -> String {
    format!("./dbs/{}/stratum-accounting.sqlite3", network)
}
