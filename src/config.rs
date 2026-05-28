use anyhow::{anyhow, bail, Result};
use bitcoinsuite_core::LotusAddress;
use serde::Deserialize;
use std::net::SocketAddr;

/// Server configuration loaded from config.toml.
#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    /// Stratum TCP bind address (e.g., "0.0.0.0:3334")
    pub stratum_bind: SocketAddr,
    /// HTTP API bind address (e.g., "127.0.0.1:18080")
    pub api_bind: SocketAddr,
    /// lotusd NNG RPC URL (e.g., "ipc:///tmp/lotusd.rpc")
    pub nng_rpc_url: String,
    /// lotusd NNG Pub/Sub URL (e.g., "ipc:///tmp/lotusd.pub")
    #[serde(default = "default_nng_pub_url")]
    pub nng_pub_url: String,
    /// SQLite database path
    pub sqlite_path: String,
    /// API bearer token for authentication
    #[serde(default = "default_api_token")]
    pub api_token: String,
    /// Variable difficulty settings (maps to VarDiff runtime config)
    #[serde(default)]
    pub vardiff: VarDiffSettings,
    /// Pool settings including mining identity and payout configuration
    #[serde(default)]
    pub pool: PoolSettings,
    /// lotusd JSON-RPC HTTP settings for block submission
    #[serde(default)]
    pub bitcoind_rpc: BitcoindRpcSettings,
    /// Enable verbose debug logging for template data, share submissions,
    /// validation decisions, block submissions, and NNG event payloads.
    /// Default: false (production). Set to true for debugging pool issues.
    #[serde(default)]
    pub debug: bool,
}

/// Variable difficulty settings loaded from config.toml.
/// Converted to `VarDiffConfig` at runtime.
#[derive(Debug, Clone, Deserialize)]
pub struct VarDiffSettings {
    /// Absolute minimum P_diff floor
    #[serde(default = "default_vardiff_min_floor")]
    pub min_floor: f64,
    /// Initial P_diff as fraction of N_diff
    #[serde(default = "default_vardiff_initial_pct")]
    pub initial_pct: f64,
    /// Target seconds between shares
    #[serde(default = "default_vardiff_target_secs")]
    pub target_secs: f64,
    /// Retarget interval in seconds
    #[serde(default = "default_vardiff_retarget_secs")]
    pub retarget_secs: f64,
}

impl Default for VarDiffSettings {
    fn default() -> Self {
        Self {
            min_floor: default_vardiff_min_floor(),
            initial_pct: default_vardiff_initial_pct(),
            target_secs: default_vardiff_target_secs(),
            retarget_secs: default_vardiff_retarget_secs(),
        }
    }
}

/// Pool settings loaded from config.toml `[pool]` section.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct PoolSettings {
    /// Optional pool name for display/identification.
    pub name: Option<String>,
    /// Mining identity configuration (payout destination + optional scriptSig tag).
    /// If None or missing, lotusd defaults to OP_RETURN — block rewards are burned.
    pub mining_identity: Option<MiningIdentity>,
    /// Pool fee configuration.
    #[serde(default)]
    pub fee: FeeSettings,
    /// PPLNS payout scheme configuration.
    #[serde(default)]
    pub pplns: PplnsSettings,
    /// Payout signing configuration.
    #[serde(default)]
    pub signing: SigningSettings,
}

/// Pool fee configuration from `[pool.fee]`.
/// Controls fee deduction from block rewards before miner payouts.
#[derive(Debug, Clone, Deserialize)]
pub struct FeeSettings {
    /// Enable or disable pool fee collection.
    #[serde(default = "default_fee_enabled")]
    pub enabled: bool,
    /// Fee rate in basis points (100 = 1.00%, 50 = 0.50%).
    #[serde(default = "default_fee_bps")]
    pub fee_bps: u32,
    /// Lotus address for fee collection (alternative to fee_script_hex).
    pub fee_address: Option<String>,
    /// Raw hex-encoded output script for fee collection.
    pub fee_script_hex: Option<String>,
}

impl Default for FeeSettings {
    fn default() -> Self {
        Self {
            enabled: default_fee_enabled(),
            fee_bps: default_fee_bps(),
            fee_address: None,
            fee_script_hex: None,
        }
    }
}

/// PPLNS payout scheme configuration from `[pool.pplns]`.
#[derive(Debug, Clone, Deserialize)]
pub struct PplnsSettings {
    /// Difficulty-weighted PPLNS window multiplier.
    #[serde(default = "default_pplns_n_multiplier")]
    pub n_multiplier: f64,
    /// Minimum miner payout threshold in satoshis.
    #[serde(default = "default_pplns_min_payout_sat")]
    pub min_payout_sat: i64,
    /// Automatically create and sign payout batches when blocks mature.
    /// Set to false for manual processing or future payout schemes.
    #[serde(default = "default_pplns_payout_enabled")]
    pub payout_enabled: bool,
    /// Minimum confirmations before payout-eligible.
    #[serde(default = "default_pplns_min_confirmations")]
    pub min_confirmations: u64,
    /// Invalid share banning configuration.
    #[serde(default)]
    pub banning: BanningSettings,
}

impl Default for PplnsSettings {
    fn default() -> Self {
        Self {
            n_multiplier: default_pplns_n_multiplier(),
            min_payout_sat: default_pplns_min_payout_sat(),
            payout_enabled: default_pplns_payout_enabled(),
            min_confirmations: default_pplns_min_confirmations(),
            banning: BanningSettings::default(),
        }
    }
}

/// Invalid share banning configuration from `[pool.pplns.banning]`.
#[derive(Debug, Clone, Deserialize)]
pub struct BanningSettings {
    /// Enable banning.
    #[serde(default = "default_banning_enabled")]
    pub enabled: bool,
    /// Number of shares to collect before checking rejection ratio.
    #[serde(default = "default_banning_check_threshold")]
    pub check_threshold: u64,
    /// Maximum allowed rejection percentage.
    #[serde(default = "default_banning_invalid_percent")]
    pub invalid_percent: f64,
}

impl Default for BanningSettings {
    fn default() -> Self {
        Self {
            enabled: default_banning_enabled(),
            check_threshold: default_banning_check_threshold(),
            invalid_percent: default_banning_invalid_percent(),
        }
    }
}

/// Payout signing configuration from `[pool.signing]`.
#[derive(Debug, Clone, Deserialize)]
pub struct SigningSettings {
    /// Signing mode: "internal" or "external".
    #[serde(default = "default_signing_mode")]
    pub mode: String,
    /// Private key material for internal signing (32-byte hex or WIF).
    pub private_key: Option<String>,
    /// Webhook URL for external signing mode.
    /// Required when mode = "external".
    pub webhook_url: Option<String>,
    /// Transaction fee rate in satoshis per kilobyte for payout txs.
    /// Default: 1000 (1 sat/vByte, standard relay minimum).
    #[serde(default = "default_tx_fee_per_kb")]
    pub tx_fee_per_kb: i64,
}

impl Default for SigningSettings {
    fn default() -> Self {
        Self {
            mode: default_signing_mode(),
            private_key: None,
            webhook_url: None,
            tx_fee_per_kb: default_tx_fee_per_kb(),
        }
    }
}

/// Mining identity configuration from `[pool.mining_identity]`.
///
/// Controls how the pool identifies itself in the coinbase and where the
/// block reward goes. At least `payout_address` or `payout_script_hex` must
/// be set to avoid burning block rewards via OP_RETURN fallback.
#[derive(Debug, Clone, Deserialize)]
pub struct MiningIdentity {
    /// Lotus address for the pool payout destination.
    /// e.g. "lotus_16PSJNRge55cpi1srcnK6A3YXuTZKpzrUir3ZBwTD"
    pub payout_address: Option<String>,
    /// Raw hex-encoded output script (alternative to payout_address).
    /// e.g. "76a914ad8b796954a46f0f32a867d3fd8855043cc506ba88ac"
    pub payout_script_hex: Option<String>,
    /// Optional UTF-8 pool/operator identity tag embedded in coinbase scriptSig.
    /// Visible in block explorers (coinbaseaux-style semantics).
    pub coinbase_identity: Option<String>,
}

impl MiningIdentity {
    /// Resolve the mining identity config into (coinbase_script_bytes, coinbase_identity_bytes).
    ///
    /// - **coinbase_script**: raw output script derived from `payout_address` or
    ///   `payout_script_hex`. Passed as `coinbase_script` to lotusd's
    ///   `GetMiningTemplateRequest`. If this is OP_RETURN, block rewards burn.
    /// - **coinbase_identity**: optional pool tag bytes appended to the coinbase
    ///   input scriptSig. Passed as `coinbase_identity` in the NNG flatbuffer.
    pub fn resolve(&self) -> Result<(Vec<u8>, Option<Vec<u8>>)> {
        let coinbase_script = match (&self.payout_address, &self.payout_script_hex) {
            (Some(addr), None) => {
                let addr: LotusAddress = addr
                    .parse()
                    .map_err(|e| anyhow!("invalid payout_address: {e}"))?;
                let script = addr.script();
                anyhow::ensure!(
                    !script.is_opreturn(),
                    "payout_address resolves to OP_RETURN — block rewards would be BURNED"
                );
                script.bytecode().to_vec()
            }
            (None, Some(hex)) => {
                let bytes =
                    hex::decode(hex).map_err(|e| anyhow!("invalid payout_script_hex: {e}"))?;
                anyhow::ensure!(
                    bytes.first() != Some(&0x6a),
                    "payout_script_hex starts with OP_RETURN — block rewards would be BURNED"
                );
                bytes
            }
            (Some(_), Some(_)) => {
                bail!("set only one of payout_address or payout_script_hex, not both");
            }
            (None, None) => {
                bail!(
                    "pool.mining_identity.payout_address must be set (or payout_script_hex). \
                     Without it lotusd creates OP_RETURN outputs and block rewards are BURNED. \
                     See config.example.toml for configuration."
                );
            }
        };

        let coinbase_identity = self
            .coinbase_identity
            .as_ref()
            .filter(|s| !s.is_empty())
            .map(|s| s.as_bytes().to_vec());

        Ok((coinbase_script, coinbase_identity))
    }
}

impl From<VarDiffSettings> for crate::share_processing::VarDiffConfig {
    fn from(s: VarDiffSettings) -> Self {
        Self {
            min_floor: s.min_floor,
            initial_pct: s.initial_pct,
            target_secs: s.target_secs,
            retarget_secs: s.retarget_secs,
        }
    }
}

/// Lotusd JSON-RPC HTTP settings for block submission and chain queries.
#[derive(Debug, Clone, Deserialize)]
pub struct BitcoindRpcSettings {
    /// JSON-RPC URL (e.g., "http://127.0.0.1:10604")
    #[serde(default = "default_bitcoind_rpc_url")]
    pub url: String,
    /// RPC username for Basic Auth
    #[serde(default)]
    pub rpc_user: String,
    /// RPC password for Basic Auth
    #[serde(default)]
    pub rpc_pass: String,
}

impl Default for BitcoindRpcSettings {
    fn default() -> Self {
        Self {
            url: default_bitcoind_rpc_url(),
            rpc_user: String::new(),
            rpc_pass: String::new(),
        }
    }
}

fn default_bitcoind_rpc_url() -> String {
    "http://127.0.0.1:10604".to_string()
}

fn default_vardiff_min_floor() -> f64 {
    0.001
}
fn default_vardiff_initial_pct() -> f64 {
    0.01
}
fn default_vardiff_target_secs() -> f64 {
    20.0
}
fn default_vardiff_retarget_secs() -> f64 {
    60.0
}

fn default_nng_pub_url() -> String {
    "ipc:///tmp/lotusd.pub".to_string()
}

fn default_api_token() -> String {
    "devtoken".to_string()
}

fn default_fee_enabled() -> bool {
    true
}
fn default_fee_bps() -> u32 {
    100
}

fn default_pplns_n_multiplier() -> f64 {
    2.0
}
fn default_pplns_min_payout_sat() -> i64 {
    crate::constants::DUST_LIMIT
}
fn default_pplns_payout_enabled() -> bool {
    true
}
fn default_pplns_min_confirmations() -> u64 {
    100
}

fn default_banning_enabled() -> bool {
    true
}
fn default_banning_check_threshold() -> u64 {
    50
}
fn default_banning_invalid_percent() -> f64 {
    50.0
}

fn default_signing_mode() -> String {
    "internal".to_string()
}

fn default_tx_fee_per_kb() -> i64 {
    crate::constants::DEFAULT_TX_FEE_PER_KB
}

impl Config {
    /// Load configuration from config.toml with environment variable overrides.
    pub fn load() -> Result<Self> {
        let config_path =
            std::env::var("CONFIG_PATH").unwrap_or_else(|_| "config.toml".to_string());

        let raw = std::fs::read_to_string(&config_path)
            .map_err(|e| anyhow!("failed to read config file '{}': {}", config_path, e))?;

        let mut cfg: Config =
            toml::from_str(&raw).map_err(|e| anyhow!("failed to parse config: {}", e))?;

        // Environment variable overrides
        if let Ok(url) = std::env::var("NNG_RPC_URL") {
            cfg.nng_rpc_url = url;
        }

        if let Ok(url) = std::env::var("NNG_PUB_URL") {
            cfg.nng_pub_url = url;
        }

        if let Ok(token) = std::env::var("STRATUM_API_TOKEN") {
            cfg.api_token = token;
        }

        if let Ok(path) = std::env::var("DATABASE_PATH") {
            cfg.sqlite_path = path;
        }

        if let Ok(url) = std::env::var("BITCOIND_RPC_URL") {
            cfg.bitcoind_rpc.url = url;
        }

        if let Ok(user) = std::env::var("BITCOIND_RPC_USER") {
            cfg.bitcoind_rpc.rpc_user = user;
        }

        if let Ok(pass) = std::env::var("BITCOIND_RPC_PASS") {
            cfg.bitcoind_rpc.rpc_pass = pass;
        }

        // Pool settings environment variable overrides
        if let Ok(addr) = std::env::var("POOL_PAYOUT_ADDRESS") {
            cfg.pool
                .mining_identity
                .get_or_insert_with(|| MiningIdentity {
                    payout_address: None,
                    payout_script_hex: None,
                    coinbase_identity: None,
                })
                .payout_address = Some(addr);
        }

        if let Ok(identity) = std::env::var("POOL_COINBASE_IDENTITY") {
            cfg.pool
                .mining_identity
                .get_or_insert_with(|| MiningIdentity {
                    payout_address: None,
                    payout_script_hex: None,
                    coinbase_identity: None,
                })
                .coinbase_identity = Some(identity);
        }

        // Pool fee env var overrides
        if let Ok(val) = std::env::var("POOL_FEE_ENABLED") {
            cfg.pool.fee.enabled = val == "true" || val == "1";
        }
        if let Ok(val) = std::env::var("POOL_FEE_BPS") {
            if let Ok(bps) = val.parse::<u32>() {
                cfg.pool.fee.fee_bps = bps;
            }
        }
        if let Ok(addr) = std::env::var("POOL_FEE_ADDRESS") {
            cfg.pool.fee.fee_address = Some(addr);
        }

        // PPLNS env var overrides
        if let Ok(val) = std::env::var("POOL_PPLNS_N_MULTIPLIER") {
            if let Ok(m) = val.parse::<f64>() {
                cfg.pool.pplns.n_multiplier = m;
            }
        }
        if let Ok(val) = std::env::var("POOL_PPLNS_MIN_PAYOUT_SAT") {
            if let Ok(sat) = val.parse::<i64>() {
                cfg.pool.pplns.min_payout_sat = sat;
            }
        }

        // Signing env var overrides
        if let Ok(mode) = std::env::var("POOL_SIGNING_MODE") {
            cfg.pool.signing.mode = mode;
        }
        if let Ok(key) = std::env::var("POOL_SIGNING_PRIVATE_KEY") {
            cfg.pool.signing.private_key = Some(key);
        }

        if let Ok(val) = std::env::var("DEBUG") {
            if val == "true" || val == "1" || val == "yes" {
                cfg.debug = true;
            }
        }

        Ok(cfg)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    fn create_test_config(dir: &TempDir, content: &str) -> String {
        let path = dir.path().join("config.toml");
        fs::write(&path, content).unwrap();
        path.to_string_lossy().to_string()
    }

    #[test]
    fn test_parse_toml() {
        let dir = TempDir::new().unwrap();
        let config_path = create_test_config(
            &dir,
            r#"
            stratum_bind = "0.0.0.0:3334"
            api_bind = "127.0.0.1:18080"
            nng_rpc_url = "ipc:///tmp/lotusd.rpc"
            sqlite_path = "./test.db"
            api_token = "mytoken"
        "#,
        );

        let raw = fs::read_to_string(&config_path).unwrap();
        let cfg: Config = toml::from_str(&raw).unwrap();

        assert_eq!(cfg.stratum_bind.to_string(), "0.0.0.0:3334");
        assert_eq!(cfg.api_bind.to_string(), "127.0.0.1:18080");
        assert_eq!(cfg.nng_rpc_url, "ipc:///tmp/lotusd.rpc");
        assert_eq!(cfg.sqlite_path, "./test.db");
        assert_eq!(cfg.api_token, "mytoken");
    }

    #[test]
    fn test_default_api_token() {
        let dir = TempDir::new().unwrap();
        let config_path = create_test_config(
            &dir,
            r#"
            stratum_bind = "0.0.0.0:3334"
            api_bind = "127.0.0.1:18080"
            nng_rpc_url = "ipc:///tmp/lotusd.rpc"
            sqlite_path = "./test.db"
        "#,
        );

        let raw = fs::read_to_string(&config_path).unwrap();
        let cfg: Config = toml::from_str(&raw).unwrap();

        assert_eq!(cfg.api_token, "devtoken"); // default
    }

    #[test]
    fn test_parse_invalid_toml() {
        let result: Result<Config, _> = toml::from_str("not valid toml");
        assert!(result.is_err());
    }

    #[test]
    fn test_vardiff_config_defaults() {
        // Without [vardiff] section, should use defaults
        let toml_str = r#"
            stratum_bind = "0.0.0.0:3334"
            api_bind = "127.0.0.1:18080"
            nng_rpc_url = "ipc:///tmp/lotusd.rpc"
            sqlite_path = "./test.db"
        "#;
        let cfg: Config = toml::from_str(toml_str).unwrap();
        assert!((cfg.vardiff.min_floor - 0.001).abs() < f64::EPSILON);
        assert!((cfg.vardiff.initial_pct - 0.01).abs() < f64::EPSILON);
        assert!((cfg.vardiff.target_secs - 20.0).abs() < f64::EPSILON);
        assert!((cfg.vardiff.retarget_secs - 60.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_vardiff_config_custom() {
        let toml_str = r#"
            stratum_bind = "0.0.0.0:3334"
            api_bind = "127.0.0.1:18080"
            nng_rpc_url = "ipc:///tmp/lotusd.rpc"
            sqlite_path = "./test.db"

            [vardiff]
            min_floor = 0.01
            initial_pct = 0.05
            target_secs = 15.0
            retarget_secs = 30.0
        "#;
        let cfg: Config = toml::from_str(toml_str).unwrap();
        assert!((cfg.vardiff.min_floor - 0.01).abs() < f64::EPSILON);
        assert!((cfg.vardiff.initial_pct - 0.05).abs() < f64::EPSILON);
        assert!((cfg.vardiff.target_secs - 15.0).abs() < f64::EPSILON);
        assert!((cfg.vardiff.retarget_secs - 30.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_vardiff_config_partial() {
        // Only set some fields, rest should use defaults
        let toml_str = r#"
            stratum_bind = "0.0.0.0:3334"
            api_bind = "127.0.0.1:18080"
            nng_rpc_url = "ipc:///tmp/lotusd.rpc"
            sqlite_path = "./test.db"

            [vardiff]
            min_floor = 0.5
        "#;
        let cfg: Config = toml::from_str(toml_str).unwrap();
        assert!((cfg.vardiff.min_floor - 0.5).abs() < f64::EPSILON);
        assert!((cfg.vardiff.initial_pct - 0.01).abs() < f64::EPSILON); // default
        assert!((cfg.vardiff.target_secs - 20.0).abs() < f64::EPSILON); // default
        assert!((cfg.vardiff.retarget_secs - 60.0).abs() < f64::EPSILON); // default
    }

    #[test]
    fn test_debug_default_false() {
        // Without debug field, should default to false
        let toml_str = r#"
            stratum_bind = "0.0.0.0:3334"
            api_bind = "127.0.0.1:18080"
            nng_rpc_url = "ipc:///tmp/lotusd.rpc"
            sqlite_path = "./test.db"
        "#;
        let cfg: Config = toml::from_str(toml_str).unwrap();
        assert!(!cfg.debug, "debug should default to false");
    }

    #[test]
    fn test_debug_true_from_toml() {
        let toml_str = r#"
            debug = true
            stratum_bind = "0.0.0.0:3334"
            api_bind = "127.0.0.1:18080"
            nng_rpc_url = "ipc:///tmp/lotusd.rpc"
            sqlite_path = "./test.db"
        "#;
        let cfg: Config = toml::from_str(toml_str).unwrap();
        assert!(cfg.debug, "debug should be true when set in config");
    }

    // -------------------------------------------------------------------------
    // Pool / MiningIdentity tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_pool_mining_identity_deserializes() {
        let toml_str = r#"
            stratum_bind = "0.0.0.0:3334"
            api_bind = "127.0.0.1:18080"
            nng_rpc_url = "ipc:///tmp/lotusd.rpc"
            sqlite_path = "./test.db"

            [pool]
            name = "Lotusia Pool"

            [pool.mining_identity]
            payout_address = "lotus_16PSJNRge55cpi1srcnK6A3YXuTZKpzrUir3ZBwTD"
            coinbase_identity = "/Lotusia Pool/"
        "#;
        let cfg: Config = toml::from_str(toml_str).unwrap();
        let id = cfg
            .pool
            .mining_identity
            .expect("mining_identity should be present");
        assert_eq!(
            id.payout_address.as_deref(),
            Some("lotus_16PSJNRge55cpi1srcnK6A3YXuTZKpzrUir3ZBwTD")
        );
        assert_eq!(id.coinbase_identity.as_deref(), Some("/Lotusia Pool/"));
        assert!(id.payout_script_hex.is_none());
    }

    #[test]
    fn test_pool_mining_identity_defaults_to_none_when_absent() {
        let toml_str = r#"
            stratum_bind = "0.0.0.0:3334"
            api_bind = "127.0.0.1:18080"
            nng_rpc_url = "ipc:///tmp/lotusd.rpc"
            sqlite_path = "./test.db"
        "#;
        let cfg: Config = toml::from_str(toml_str).unwrap();
        assert!(
            cfg.pool.mining_identity.is_none(),
            "mining_identity should be None when [pool.mining_identity] section is absent"
        );
    }

    #[test]
    fn test_mining_identity_resolve_payout_address() {
        let id = MiningIdentity {
            payout_address: Some("lotus_16PSJNRge55cpi1srcnK6A3YXuTZKpzrUir3ZBwTD".to_string()),
            payout_script_hex: None,
            coinbase_identity: Some("/Lotusia Pool/".to_string()),
        };
        let (script_bytes, identity_bytes) = id.resolve().unwrap();

        // Should be a valid P2PKH script (25 bytes):
        //   OP_DUP (0x76) | OP_HASH160 (0xa9) | PUSH20 (0x14) | <20B hash> | OP_EQUALVERIFY (0x88) | OP_CHECKSIG (0xac)
        assert_eq!(script_bytes.len(), 25, "P2PKH script should be 25 bytes");
        assert_eq!(script_bytes[0], 0x76, "first byte should be OP_DUP");
        assert_eq!(script_bytes[1], 0xa9, "second byte should be OP_HASH160");
        assert_eq!(script_bytes[2], 0x14, "third byte should be PUSH20");
        assert_eq!(script_bytes[23], 0x88, "24th byte should be OP_EQUALVERIFY");
        assert_eq!(script_bytes[24], 0xac, "25th byte should be OP_CHECKSIG");

        // Identity should be the ASCII bytes of "/Lotusia Pool/"
        assert_eq!(
            identity_bytes,
            Some(b"/Lotusia Pool/".to_vec()),
            "coinbase_identity should be UTF-8 bytes of the pool tag"
        );
    }

    #[test]
    fn test_mining_identity_resolve_payout_script_hex() {
        let id = MiningIdentity {
            payout_address: None,
            payout_script_hex: Some(
                "76a914ad8b796954a46f0f32a867d3fd8855043cc506ba88ac".to_string(),
            ),
            coinbase_identity: None,
        };
        let (script_bytes, identity_bytes) = id.resolve().unwrap();

        assert_eq!(
            script_bytes.len(),
            25,
            "decoded hex P2PKH should be 25 bytes"
        );
        assert_eq!(script_bytes[0], 0x76);
        assert_eq!(script_bytes[1], 0xa9);
        assert_eq!(identity_bytes, None, "no identity set should return None");
    }

    #[test]
    fn test_mining_identity_resolve_op_return_hex_fails() {
        let id = MiningIdentity {
            payout_address: None,
            payout_script_hex: Some("6a056c6f676f73".to_string()), // OP_RETURN "logos"
            coinbase_identity: None,
        };
        let err = id.resolve().unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("BURNED"),
            "error should mention BURNED, got: {}",
            msg
        );
    }

    #[test]
    fn test_mining_identity_resolve_neither_fails() {
        let id = MiningIdentity {
            payout_address: None,
            payout_script_hex: None,
            coinbase_identity: None,
        };
        let err = id.resolve().unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("payout_address"),
            "error should mention payout_address, got: {}",
            msg
        );
    }

    #[test]
    fn test_mining_identity_resolve_both_fails() {
        let id = MiningIdentity {
            payout_address: Some("lotus_16PSJNRge55cpi1srcnK6A3YXuTZKpzrUir3ZBwTD".to_string()),
            payout_script_hex: Some(
                "76a914ad8b796954a46f0f32a867d3fd8855043cc506ba88ac".to_string(),
            ),
            coinbase_identity: None,
        };
        let err = id.resolve().unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("only one"),
            "error should mention 'only one', got: {}",
            msg
        );
    }

    #[test]
    fn test_mining_identity_resolve_identity_empty_string() {
        let id = MiningIdentity {
            payout_address: Some("lotus_16PSJNRge55cpi1srcnK6A3YXuTZKpzrUir3ZBwTD".to_string()),
            payout_script_hex: None,
            coinbase_identity: Some(String::new()),
        };
        let (_, identity_bytes) = id.resolve().unwrap();
        assert_eq!(
            identity_bytes, None,
            "empty string identity should be treated as None"
        );
    }

    #[test]
    fn test_mining_identity_resolve_address_invalid_fails() {
        let id = MiningIdentity {
            payout_address: Some("not-a-valid-address".to_string()),
            payout_script_hex: None,
            coinbase_identity: None,
        };
        let err = id.resolve().unwrap_err();
        assert!(
            err.to_string().contains("invalid payout_address"),
            "error should mention invalid payout_address"
        );
    }
}
