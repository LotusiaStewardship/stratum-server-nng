use crate::shutdown::ShutdownSignal;
use crate::stratum_protocol::params;
use anyhow::Result;
use bitcoinsuite_bitcoind_nng::MiningTemplate;
use bitcoinsuite_bitcoind_nng::RpcInterface;
use std::sync::Arc;
use tokio::sync::RwLock;

/// NNG RPC client for communicating with lotusd.
pub struct NngRpcClient {
    interface: Arc<RwLock<Option<RpcInterface>>>,
    rpc_url: String,
    /// Raw output script bytes passed as `coinbase_script` in GetMiningTemplateRequest.
    /// When None, lotusd falls back to OP_RETURN — block rewards would be burned.
    coinbase_script: Option<Vec<u8>>,
    /// Optional pool identity tag bytes passed as `coinbase_identity` in
    /// GetMiningTemplateRequest. Appended to the coinbase input scriptSig.
    coinbase_identity: Option<Vec<u8>>,
}

impl NngRpcClient {
    /// Create a new NNG RPC client (not yet connected).
    ///
    /// * `coinbase_script` — raw output script bytes for the coinbase payout.
    ///   Derived from `pool.mining_identity.payout_address` or `payout_script_hex`.
    /// * `coinbase_identity` — optional pool tag bytes (UTF-8) embedded in the
    ///   coinbase input scriptSig for block explorer attribution.
    pub fn new(
        rpc_url: String,
        coinbase_script: Option<Vec<u8>>,
        coinbase_identity: Option<Vec<u8>>,
    ) -> Self {
        Self {
            interface: Arc::new(RwLock::new(None)),
            rpc_url,
            coinbase_script,
            coinbase_identity,
        }
    }

    /// Connect to lotusd via NNG RPC.
    pub async fn connect(&self) -> Result<()> {
        crate::node_int_info!(url = %self.rpc_url, "connecting to lotusd via NNG RPC");
        let interface = RpcInterface::open(&self.rpc_url)
            .map_err(|e| anyhow::anyhow!("failed to open NNG RPC connection: {}", e))?;
        let mut guard = self.interface.write().await;
        *guard = Some(interface);
        crate::node_int_info!("connected to lotusd");
        Ok(())
    }

    /// Fetch the current mining template from lotusd.
    pub async fn get_mining_template(&self) -> Result<MiningTemplate> {
        let guard = self.interface.read().await;
        let interface = guard.as_ref().ok_or_else(|| {
            anyhow::anyhow!("NNG RPC client not connected. Call connect() first.")
        })?;

        // Fetch template with configured coinbase parameters.
        // coinbase_script: from pool.mining_identity.payout_address / payout_script_hex.
        //   Without this, lotusd creates OP_RETURN outputs and block rewards are burned.
        // coinbase_identity: optional pool tag for scriptSig attribution.
        // extranonce1_size: 4 bytes (standard)
        // extranonce2_size: 4 bytes (standard)
        // include_transactions: true (we want full template)
        let template = interface
            .get_mining_template(
                self.coinbase_script.as_deref(),
                self.coinbase_identity.as_deref(),
                params::EXTRANONCE_1_SIZE.into(),
                params::EXTRANONCE_2_SIZE.into(),
                true,
            )
            .map_err(|e| anyhow::anyhow!("failed to fetch mining template: {}", e))?;
        Ok(template)
    }

    /// Check if the client is connected.
    pub async fn is_connected(&self) -> bool {
        let guard = self.interface.read().await;
        guard.is_some()
    }

    /// Disconnect from lotusd, closing the NNG RPC connection.
    pub async fn disconnect(&self) {
        let mut guard = self.interface.write().await;
        if let Some(interface) = guard.take() {
            crate::node_int_info!(url = %self.rpc_url, "disconnecting from lotusd");
            // RpcInterface is dropped here, which closes the NNG connection
            drop(interface);
            crate::node_int_info!("disconnected from lotusd");
        }
    }

    /// Run a background task that periodically fetches templates.
    /// This is a placeholder for Slice 6 (event-driven refresh).
    pub async fn run_template_fetcher(
        self: Arc<Self>,
        shutdown_signal: ShutdownSignal,
    ) -> Result<()> {
        crate::node_int_info!("starting template fetcher (placeholder for Slice 6)");

        // For Slice 2, we just fetch once on startup
        // Slice 6 will add event-driven refresh via pub/sub
        let mut signal = shutdown_signal;
        signal.recv().await;

        crate::node_int_info!("template fetcher shutting down");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper to create an NngRpcClient for tests that don't need a connection.
    fn test_client() -> NngRpcClient {
        NngRpcClient::new("ipc://test".to_string(), None, None)
    }

    #[tokio::test]
    async fn test_client_not_connected_initially() {
        let client = test_client();
        assert!(!client.is_connected().await);
    }

    #[tokio::test]
    async fn test_get_template_fails_when_not_connected() {
        let client = NngRpcClient::new(
            "ipc://test".to_string(),
            Some(vec![0x76, 0xa9, 0x14, 0x00, 0x88, 0xac]),
            Some(b"/Test/".to_vec()),
        );
        let result = client.get_mining_template().await;

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("not connected"));
    }

    #[tokio::test]
    async fn test_accepts_script_and_identity_params() {
        // Verifies the constructor accepts the new parameters without error.
        let script = Some(vec![0x76, 0xa9, 0x14, 0x00, 0x88, 0xac]);
        let identity = Some(b"/Test/".to_vec());
        let client = NngRpcClient::new("ipc://test".to_string(), script, identity);
        assert!(!client.is_connected().await); // still not connected
    }

    #[tokio::test]
    async fn test_accepts_script_only() {
        let script = Some(vec![0x76, 0xa9, 0x14, 0x00, 0x88, 0xac]);
        let client = NngRpcClient::new("ipc://test".to_string(), script, None);
        assert!(!client.is_connected().await);
    }

    #[tokio::test]
    async fn test_accepts_identity_only() {
        let identity = Some(b"/Test/".to_vec());
        let client = NngRpcClient::new("ipc://test".to_string(), None, identity);
        assert!(!client.is_connected().await);
    }

    // -------------------------------------------------------------------------
    // Integration test against a live lotusd
    // -------------------------------------------------------------------------

    #[tokio::test]
    async fn test_get_mining_template_with_live_lotusd() {
        // Load NNG URL from config (reads config.toml + NNG_RPC_URL env var).
        // If config loading fails, use the default IPC path.
        let config = crate::config::Config::load().ok();
        let rpc_url = config
            .as_ref()
            .map(|c| c.nng_rpc_url.clone())
            .unwrap_or_else(|| "ipc:///tmp/lotusd.rpc".to_string());

        // Resolve coinbase_script from config, or use a hardcoded P2PKH script.
        let coinbase_script: Option<Vec<u8>> = config
            .as_ref()
            .and_then(|c| c.pool.mining_identity.as_ref())
            .and_then(|id| match id.resolve() {
                Ok((script, _)) => Some(script),
                Err(_) => None,
            })
            .or_else(|| {
                // Fallback: a known P2PKH script for testing
                hex::decode("76a914ad8b796954a46f0f32a867d3fd8855043cc506ba88ac").ok()
            });

        let client = NngRpcClient::new(rpc_url.clone(), coinbase_script, None);

        // Try connecting to lotusd. If it's not running, skip gracefully.
        if let Err(e) = client.connect().await {
            eprintln!(
                "WARNING: lotusd not reachable at {} ({}). \
                 Skipping integration test. Start lotusd with nngrpc enabled.",
                rpc_url, e,
            );
            return;
        }

        let template = client
            .get_mining_template()
            .await
            .expect("get_mining_template should succeed with a live lotusd");

        // Verify the template has spendable (non-OP_RETURN) coinbase outputs.
        assert!(
            crate::node_integration::template::verify_coinbase_outputs(&template).is_ok(),
            "Template coinbase outputs are OP_RETURN or zero-valued — block rewards \
             would be BURNED. Check that pool.mining_identity.payout_address is set \
             correctly in config and that coinbase_script bytes are being forwarded."
        );

        // Basic sanity checks on the returned template
        assert!(
            !template.coinbase1.is_empty(),
            "coinbase1 must not be empty"
        );
        assert!(
            !template.coinbase2.is_empty(),
            "coinbase2 must not be empty"
        );
        assert!(template.height > 0, "template height must be > 0");
        assert!(template.coinbase_value > 0, "coinbase_value must be > 0");
    }
}
