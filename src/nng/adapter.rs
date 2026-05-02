use anyhow::Result;
use async_trait::async_trait;
use bitcoinsuite_bitcoind_nng::{Block, BlockIdentifier, MiningTemplate, PubInterface, RpcInterface};
use bitcoinsuite_core::{BitcoinCode, Bytes, Hashed, LotusBlock, Sha256d};
use tracing::{debug, info, warn};

/// Result of submitting a block via JSON-RPC submitblock
#[derive(Debug, Clone)]
pub struct SubmitBlockRpcResult {
    /// BIP22 rejection reason, None if accepted
    pub reject_reason: Option<String>,
    /// Block hash
    pub block_hash: Sha256d,
}

// ============================================================================
// JSON-RPC HTTP Client
// ============================================================================
// Dedicated client for Bitcoin JSON-RPC 2.0 over HTTP.
// Used for: submitblock, sendrawtransaction, and other standard RPC methods.
// This is completely separate from NNG.

pub struct JsonRpcClient {
    client: reqwest::Client,
    url: String,
    rpc_user: String,
    rpc_pass: String,
}

impl JsonRpcClient {
    pub fn new(url: String, rpc_user: String, rpc_pass: String) -> Self {
        Self {
            client: reqwest::Client::new(),
            url,
            rpc_user,
            rpc_pass,
        }
    }

    /// Submit block via JSON-RPC submitblock method (BIP22-style)
    pub async fn submit_block(&self, block: Vec<u8>) -> Result<SubmitBlockRpcResult> {
        let block_hex = hex::encode(&block);

        // Build JSON-RPC 2.0 request
        let rpc_request = serde_json::json!({
            "jsonrpc": "2.0",
            "id": "stratum-submitblock",
            "method": "submitblock",
            "params": [block_hex]
        });

        let response = self
            .client
            .post(&self.url)
            .basic_auth(&self.rpc_user, Some(&self.rpc_pass))
            .json(&rpc_request)
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("HTTP RPC request failed: {}", e))?;

        let rpc_response: serde_json::Value = response
            .json()
            .await
            .map_err(|e| anyhow::anyhow!("HTTP RPC response parse failed: {}", e))?;

        // Parse JSON-RPC error field
        if let Some(error) = rpc_response.get("error") {
            if !error.is_null() {
                anyhow::bail!("RPC error: {}", error);
            }
        }

        // Compute block hash locally
        let block_hash = {
            let mut block_bytes = Bytes::from_slice(&block);
            let parsed_block = LotusBlock::deser(&mut block_bytes)?;
            parsed_block.header.calc_hash()
        };

        // Parse BIP22 result: null = accepted, string = rejected with reason
        let reject_reason = rpc_response
            .get("result")
            .and_then(|r| r.as_str())
            .map(|s| s.to_string());

        Ok(SubmitBlockRpcResult {
            reject_reason,
            block_hash,
        })
    }
}

// ============================================================================
// Node Events (NNG pub/sub)
// ============================================================================

#[derive(Debug, Clone)]
pub enum NodeEvent {
    UpdateBlkTip,
    MempoolRefresh,
    MiningWorkChanged,
    BlockDisconnected,
}

// ============================================================================
// NNG Adapter
// ============================================================================
// Handles NNG-specific operations:
// - NNG RPC calls (get_mining_template, get_block)
// - NNG pub/sub subscriptions
// This does NOT handle block submission (that's JSON-RPC's job).

pub struct NngAdapter {
    rpc: RpcInterface,
}

impl NngAdapter {
    pub fn connect(url: &str) -> Result<Self> {
        info!(nng_rpc = %url, "connecting NNG RPC adapter");
        Ok(Self {
            rpc: RpcInterface::open(url).map_err(|e| anyhow::anyhow!(e.to_string()))?,
        })
    }

    /// Subscribe to NNG pub/sub topics and emit normalized events.
    pub async fn run_pub_loop<F>(&self, pub_url: &str, mut on_event: F) -> Result<()>
    where
        F: FnMut(NodeEvent) + Send + 'static,
    {
        info!(nng_pub = %pub_url, "connecting NNG pub adapter");
        let pubif = PubInterface::open(pub_url).map_err(|e| anyhow::anyhow!(e.to_string()))?;

        // Subscribe to template-affecting topics
        pubif
            .subscribe("updateblktip")
            .map_err(|e| anyhow::anyhow!(e.to_string()))?;
        pubif
            .subscribe("mempooltxadd")
            .map_err(|e| anyhow::anyhow!(e.to_string()))?;
        pubif
            .subscribe("mempooltxrem")
            .map_err(|e| anyhow::anyhow!(e.to_string()))?;
        pubif
            .subscribe("miningwrkchg")
            .map_err(|e| anyhow::anyhow!(e.to_string()))?;
        pubif
            .subscribe("blkdisconctd")
            .map_err(|e| anyhow::anyhow!(e.to_string()))?;

        info!("NNG pub subscriptions active: updateblktip,mempooltxadd,mempooltxrem,miningwrkchg,blkdisconctd");

        tokio::task::spawn_blocking(move || -> Result<()> {
            loop {
                let (topic, payload) = pubif
                    .recv_raw()
                    .map_err(|e| anyhow::anyhow!(e.to_string()))?;
                let topic = topic.trim_end_matches('\0');
                debug!(
                    topic,
                    payload_len = payload.len(),
                    "NNG pub message received"
                );
                match topic {
                    "updateblktip" => on_event(NodeEvent::UpdateBlkTip),
                    "mempooltxadd" | "mempooltxrem" => on_event(NodeEvent::MempoolRefresh),
                    "miningwrkchg" => on_event(NodeEvent::MiningWorkChanged),
                    "blkdisconctd" => on_event(NodeEvent::BlockDisconnected),
                    _ => warn!(topic, "unknown NNG topic ignored"),
                }
            }
        })
        .await
        .map_err(|e| anyhow::anyhow!(e.to_string()))??;

        Ok(())
    }
}

// ============================================================================
// Composite Mining Adapter
// ============================================================================
// Combines NNG adapter (for templates/blocks/pubsub) with JSON-RPC client
// (for submitblock). This is the high-level interface used by the stratum
// server.

pub struct BitcoindMiningAdapter {
    nng: NngAdapter,
    json_rpc: JsonRpcClient,
}

impl BitcoindMiningAdapter {
    pub fn new(nng: NngAdapter, json_rpc: JsonRpcClient) -> Self {
        Self { nng, json_rpc }
    }

    pub fn nng(&self) -> &NngAdapter {
        &self.nng
    }
}

/// High-level mining adapter trait used by stratum server.
/// Combines NNG operations (templates, blocks, pubsub) with JSON-RPC operations (submitblock).
#[async_trait]
pub trait NodeMiningAdapter: Send + Sync {
    async fn get_mining_template(
        &self,
        coinbase_script: Option<Vec<u8>>,
        coinbase_identity: Option<Vec<u8>>,
    ) -> Result<MiningTemplate>;
    async fn get_block_by_hash(&self, block_hash_hex_be: &str) -> Result<Block>;
    async fn submit_block(&self, block: Vec<u8>) -> Result<SubmitBlockRpcResult>;
}

#[async_trait]
impl NodeMiningAdapter for BitcoindMiningAdapter {
    async fn get_mining_template(
        &self,
        coinbase_script: Option<Vec<u8>>,
        coinbase_identity: Option<Vec<u8>>,
    ) -> Result<MiningTemplate> {
        self.nng
            .rpc
            .get_mining_template(
                coinbase_script.as_deref(),
                coinbase_identity.as_deref(),
                4,
                4,
                true,
            )
            .map_err(|e| anyhow::anyhow!(e.to_string()))
    }

    async fn get_block_by_hash(&self, block_hash_hex_be: &str) -> Result<Block> {
        let hash = Sha256d::from_hex_be(block_hash_hex_be)?;
        self.nng
            .rpc
            .get_block(BlockIdentifier::Hash(hash))
            .map_err(|e| anyhow::anyhow!(e.to_string()))
    }

    async fn submit_block(&self, block: Vec<u8>) -> Result<SubmitBlockRpcResult> {
        self.json_rpc.submit_block(block).await
    }
}
