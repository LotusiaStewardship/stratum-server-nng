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
    pub sqlite_path: String,
    pub nng_rpc_url: String,
    pub nng_pub_url: String,
    pub bitcoind_rpc: BitcoindRpcConfig,
    pub vardiff: VarDiffConfig,
    pub max_request_line_bytes: usize,
    pub per_conn_req_per_sec: u32,
    pub conn_idle_timeout_secs: u64,
    pub max_jobs_cache: usize,
    pub job_refresh_secs: u64,
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
    pub n_multiplier: f64,
    pub min_payout_sat: i64,
    pub payout_interval_secs: u64,
    pub min_confirmations: u32,
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
    /// Ratio of network difficulty to pool difficulty.
    /// pool_difficulty = network_difficulty / share_target_ratio
    ///
    /// Rationale:
    /// - Higher values = easier shares = more frequent submissions
    /// - Lower values = harder shares = fewer submissions
    /// - 100.0 is a good balance: pool shares are 100x easier than network blocks
    /// - Typical range: 50-200 depending on desired share frequency
    #[serde(default = "default_share_ratio")]
    pub share_target_ratio: f64,

    /// Minimum pool difficulty (absolute floor).
    ///
    /// Rationale:
    /// - Prevents difficulty from crashing to near-zero on low-hashrate testnet
    /// - Old default of 0.0000001 allowed difficulty to become meaningless
    /// - 4.0 is a reasonable floor: high enough to prevent abuse, low enough
    ///   for low-powered miners to contribute
    /// - Should be significantly lower than typical network difficulty
    #[serde(default = "default_min_diff")]
    pub min_difficulty: f64,

    /// Maximum pool difficulty (absolute ceiling).
    ///
    /// Rationale:
    /// - Prevents pool difficulty from exceeding reasonable bounds
    /// - 1_000_000.0 is high enough for any realistic scenario
    /// - Protects against bugs or extreme network difficulty spikes
    #[serde(default = "default_max_diff")]
    pub max_difficulty: f64,

    /// Maximum allowed pool difficulty change per update.
    ///
    /// Rationale:
    /// - Prevents sudden difficulty jumps from destabilizing miners
    /// - 0.5 = 50% max change per update (industry standard)
    /// - Lower values (0.25-0.33) = more stable, slower adaptation
    /// - Higher values (0.67-1.0) = faster adaptation, more volatile
    #[serde(default = "default_max_change_pct")]
    pub max_change_pct: f64,

    /// Target time between accepted shares for vardiff tuning.
    ///
    /// Rationale:
    /// - Lower values (5-10s) = more shares, more precise hashrate estimation,
    ///   more server load, more network traffic
    /// - Higher values (30-60s) = fewer shares, less precision, less load
    /// - 15.0 is a good balance for most deployments
    /// - VarDiff adjusts per-miner from this target
    #[serde(default = "default_target_secs")]
    pub vardiff_target_secs: f64,

    /// How often vardiff is allowed to retarget per miner.
    ///
    /// Rationale:
    /// - Shorter intervals (30s) = faster adaptation to hashrate changes,
    ///   but more volatile difficulty
    /// - Longer intervals (120s+) = more stable difficulty, slower adaptation
    /// - 90.0 provides good stability while allowing reasonable adaptation
    /// - Should be significantly longer than vardiff_target_secs to allow
    ///   enough samples for meaningful statistics
    #[serde(default = "default_retarget_secs")]
    pub vardiff_retarget_secs: f64,
}

fn default_share_ratio() -> f64 {
    100.0
}
fn default_min_diff() -> f64 {
    0.5
}
fn default_max_diff() -> f64 {
    1_000_000.0
}
fn default_max_change_pct() -> f64 {
    0.5
}
fn default_target_secs() -> f64 {
    15.0
}
fn default_retarget_secs() -> f64 {
    90.0
}

impl VarDiffConfig {
    pub fn validate(&self) -> Result<()> {
        if self.max_change_pct <= 0.0 || self.max_change_pct > 1.0 {
            return Err(anyhow::anyhow!(
                "max_change_pct must be between 0.0 and 1.0"
            ));
        }
        bitcoinsuite_bitcoind_stratum::validate_difficulty_config(
            self.min_difficulty,
            self.max_difficulty,
            self.share_target_ratio,
        )
        .map_err(|e| anyhow::anyhow!("invalid vardiff config: {}", e))
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
        cfg.validate()?;
        Ok(cfg)
    }

    pub fn validate(&self) -> Result<()> {
        if self.api_token.is_empty() {
            anyhow::bail!("api_token required")
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
