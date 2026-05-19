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
}

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
}
