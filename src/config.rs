use anyhow::{anyhow, Result};
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
    /// SQLite database path
    pub sqlite_path: String,
    /// API bearer token for authentication
    #[serde(default = "default_api_token")]
    pub api_token: String,
    /// Variable difficulty settings (maps to VarDiff runtime config)
    #[serde(default)]
    pub vardiff: VarDiffSettings,
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

fn default_vardiff_min_floor() -> f64 { 0.001 }
fn default_vardiff_initial_pct() -> f64 { 0.01 }
fn default_vardiff_target_secs() -> f64 { 20.0 }
fn default_vardiff_retarget_secs() -> f64 { 60.0 }

fn default_api_token() -> String {
    "devtoken".to_string()
}

impl Config {
    /// Load configuration from config.toml with environment variable overrides.
    pub fn load() -> Result<Self> {
        let config_path = std::env::var("CONFIG_PATH")
            .unwrap_or_else(|_| "config.toml".to_string());
        
        let raw = std::fs::read_to_string(&config_path)
            .map_err(|e| anyhow!("failed to read config file '{}': {}", config_path, e))?;
        
        let mut cfg: Config = toml::from_str(&raw)
            .map_err(|e| anyhow!("failed to parse config: {}", e))?;
        
        // Environment variable overrides
        if let Ok(url) = std::env::var("NNG_RPC_URL") {
            cfg.nng_rpc_url = url;
        }
        
        if let Ok(token) = std::env::var("STRATUM_API_TOKEN") {
            cfg.api_token = token;
        }
        
        if let Ok(path) = std::env::var("DATABASE_PATH") {
            cfg.sqlite_path = path;
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
        let config_path = create_test_config(&dir, r#"
            stratum_bind = "0.0.0.0:3334"
            api_bind = "127.0.0.1:18080"
            nng_rpc_url = "ipc:///tmp/lotusd.rpc"
            sqlite_path = "./test.db"
            api_token = "mytoken"
        "#);
        
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
        let config_path = create_test_config(&dir, r#"
            stratum_bind = "0.0.0.0:3334"
            api_bind = "127.0.0.1:18080"
            nng_rpc_url = "ipc:///tmp/lotusd.rpc"
            sqlite_path = "./test.db"
        "#);
        
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
}
