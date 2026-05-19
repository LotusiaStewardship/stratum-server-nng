use anyhow::Result;
use bitcoinsuite_bitcoind_nng::RpcInterface;
use bitcoinsuite_bitcoind_nng::MiningTemplate;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::info;

use crate::shutdown::ShutdownSignal;

/// NNG RPC client for communicating with lotusd.
pub struct NngRpcClient {
    interface: Arc<RwLock<Option<RpcInterface>>>,
    rpc_url: String,
}

impl NngRpcClient {
    /// Create a new NNG RPC client (not yet connected).
    pub fn new(rpc_url: String) -> Self {
        Self {
            interface: Arc::new(RwLock::new(None)),
            rpc_url,
        }
    }

    /// Connect to lotusd via NNG RPC.
    pub async fn connect(&self) -> Result<()> {
        info!(url = %self.rpc_url, "connecting to lotusd via NNG RPC");
        let interface = RpcInterface::open(&self.rpc_url)
            .map_err(|e| anyhow::anyhow!("failed to open NNG RPC connection: {}", e))?;
        let mut guard = self.interface.write().await;
        *guard = Some(interface);
        info!("connected to lotusd");
        Ok(())
    }

    /// Fetch the current mining template from lotusd.
    pub async fn get_mining_template(&self) -> Result<MiningTemplate> {
        let guard = self.interface.read().await;
        let interface = guard.as_ref().ok_or_else(|| {
            anyhow::anyhow!("NNG RPC client not connected. Call connect() first.")
        })?;

        // Fetch template with default parameters
        // coinbase_script: None (lotusd will handle)
        // coinbase_identity: None (lotusd will handle)
        // extranonce1_size: 4 bytes (standard)
        // extranonce2_size: 4 bytes (standard)
        // include_transactions: true (we want full template)
        let template = interface.get_mining_template(None, None, 4, 4, true)
            .map_err(|e| anyhow::anyhow!("failed to fetch mining template: {}", e))?;
        Ok(template)
    }

    /// Check if the client is connected.
    pub async fn is_connected(&self) -> bool {
        let guard = self.interface.read().await;
        guard.is_some()
    }

    /// Run a background task that periodically fetches templates.
    /// This is a placeholder for Slice 6 (event-driven refresh).
    pub async fn run_template_fetcher(
        self: Arc<Self>,
        shutdown_signal: ShutdownSignal,
    ) -> Result<()> {
        info!("starting template fetcher (placeholder for Slice 6)");
        
        // For Slice 2, we just fetch once on startup
        // Slice 6 will add event-driven refresh via pub/sub
        let mut signal = shutdown_signal;
        signal.recv().await;
        
        info!("template fetcher shutting down");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_client_not_connected_initially() {
        let client = NngRpcClient::new("ipc://test".to_string());
        assert!(!client.is_connected().await);
    }

    #[tokio::test]
    async fn test_get_template_fails_when_not_connected() {
        let client = NngRpcClient::new("ipc://test".to_string());
        let result = client.get_mining_template().await;
        
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("not connected"));
    }
}
