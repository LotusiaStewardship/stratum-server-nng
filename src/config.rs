use anyhow::{anyhow, Result};
use bitcoinsuite_core::{Hashed, LotusAddress, Sha256, Script};
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
    pub initial_difficulty: f64,
    pub vardiff_target_secs: f64,
    pub vardiff_retarget_secs: f64,
    pub min_difficulty: f64,
    pub max_difficulty: f64,
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
}

#[derive(Debug, Clone, Deserialize)]
pub struct MiningIdentityConfig {
    pub payout_script_hex: Option<String>,
    pub payout_address: Option<String>,
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

#[derive(Debug, Clone)]
pub struct ResolvedPoolScripts {
    pub payout_script: Vec<u8>,
    pub payout_fingerprint: String,
    pub fee_script: Option<Vec<u8>>,
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

        Ok(ResolvedPoolScripts {
            payout_fingerprint: script_fingerprint(&payout_script),
            payout_script,
            fee_script,
        })
    }
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
        let lotus: LotusAddress = addr.parse().map_err(|e| anyhow!("invalid payout address: {e}"))?;
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
