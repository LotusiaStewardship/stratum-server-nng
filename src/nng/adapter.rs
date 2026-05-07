use anyhow::Result;
use async_trait::async_trait;
use bitcoinsuite_bitcoind_nng::{
    OptionExt, Block, BlockIdentifier, MiningTemplate, PubInterface, RpcInterface,
};
use bitcoinsuite_core::{BitcoinCode, Bytes, Hashed, LotusBlock, Sha256d};
use flatbuffers::VerifierOptions;
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

    /// Get current blockchain tip height via JSON-RPC getblockcount
    pub async fn get_block_count(&self) -> Result<i64> {
        let rpc_request = serde_json::json!({
            "jsonrpc": "2.0",
            "id": "stratum-getblockcount",
            "method": "getblockcount",
            "params": []
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

        rpc_response["result"]
            .as_i64()
            .ok_or_else(|| anyhow::anyhow!("getblockcount returned non-integer result"))
    }
}

// ============================================================================
// Node Events (NNG pub/sub)
// ============================================================================

#[derive(Debug, Clone)]
pub enum NodeEvent {
    MempoolRefresh,
    BlockConnected {
        height: i64,
        hash: String,
        prev_hash: String,
    },
    BlockDisconnected {
        height: i64,
        hash: String,
        prev_hash: String,
    },
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
            .subscribe("mempooltxadd")
            .map_err(|e| anyhow::anyhow!(e.to_string()))?;
        pubif
            .subscribe("mempooltxrem")
            .map_err(|e| anyhow::anyhow!(e.to_string()))?;
        pubif
            .subscribe("blkconnected")
            .map_err(|e| anyhow::anyhow!(e.to_string()))?;
        pubif
            .subscribe("blkdisconctd")
            .map_err(|e| anyhow::anyhow!(e.to_string()))?;

        info!("NNG pub subscriptions active: mempooltxadd,mempooltxrem,blkconnected,blkdisconctd");

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
                    "mempooltxadd" | "mempooltxrem" => on_event(NodeEvent::MempoolRefresh),
                    "blkconnected" => {
                        // Parse the BlockConnected flatbuffer to extract height, hash, prev_hash
                        use bitcoinsuite_bitcoind_nng::nng_interface_generated::nng_interface::BlockConnected;
                        
                        let fbb_opts = VerifierOptions {
                            max_tables: 0xffff_ffff,
                            ..Default::default()
                        };
                        match flatbuffers::root_with_opts::<BlockConnected>(&fbb_opts, &payload) {
                            Ok(msg) => {
                                let block = msg.block().field("BlockConnected.block").ok();
                                if let Some(block) = block {
                                    let header = block.header().field("Block.header").ok();
                                    if let Some(header) = header {
                                        let height = get_raw_block_height(header.raw().field("BlockHeader.raw").map(|r| r.bytes()).unwrap_or(&[])).unwrap_or(0);
                                        let hash = header.block_hash().field("BlockHeader.block_hash").ok()
                                            .and_then(|bh| bh.hash().field("BlockHash.hash").ok())
                                            .map(|h| hex::encode(h.0.iter().rev().copied().collect::<Vec<_>>()))
                                            .unwrap_or_default();
                                        let prev_hash = header.prev_block_hash().field("BlockHeader.prev_block_hash").ok()
                                            .and_then(|bh| bh.hash().field("BlockHash.hash").ok())
                                            .map(|h| hex::encode(h.0.iter().rev().copied().collect::<Vec<_>>()))
                                            .unwrap_or_default();
                                        on_event(NodeEvent::BlockConnected { height, hash, prev_hash });
                                        continue;
                                    }
                                }
                            }
                            Err(e) => warn!(error = %e, "failed parsing blkconnected flatbuffer"),
                        }
                        // Fallback: emit event without data if parsing fails
                        on_event(NodeEvent::BlockConnected { height: 0, hash: String::new(), prev_hash: String::new() });
                    }
                    "blkdisconctd" => {
                        // Parse the BlockDisconnected flatbuffer to extract height, hash, prev_hash
                        use bitcoinsuite_bitcoind_nng::nng_interface_generated::nng_interface::BlockDisconnected;
                        
                        let fbb_opts = VerifierOptions {
                            max_tables: 0xffff_ffff,
                            ..Default::default()
                        };
                        match flatbuffers::root_with_opts::<BlockDisconnected>(&fbb_opts, &payload) {
                            Ok(msg) => {
                                let block = msg.block().field("BlockDisconnected.block").ok();
                                if let Some(block) = block {
                                    let header = block.header().field("Block.header").ok();
                                    if let Some(header) = header {
                                        let height = get_raw_block_height(header.raw().field("BlockHeader.raw").map(|r| r.bytes()).unwrap_or(&[])).unwrap_or(0);
                                        let hash = header.block_hash().field("BlockHeader.block_hash").ok()
                                            .and_then(|bh| bh.hash().field("BlockHash.hash").ok())
                                            .map(|h| hex::encode(h.0.iter().rev().copied().collect::<Vec<_>>()))
                                            .unwrap_or_default();
                                        let prev_hash = header.prev_block_hash().field("BlockHeader.prev_block_hash").ok()
                                            .and_then(|bh| bh.hash().field("BlockHash.hash").ok())
                                            .map(|h| hex::encode(h.0.iter().rev().copied().collect::<Vec<_>>()))
                                            .unwrap_or_default();
                                        on_event(NodeEvent::BlockDisconnected { height, hash, prev_hash });
                                        continue;
                                    }
                                }
                            }
                            Err(e) => warn!(error = %e, "failed parsing blkdisconctd flatbuffer"),
                        }
                        // Fallback: emit event without data if parsing fails
                        on_event(NodeEvent::BlockDisconnected { height: 0, hash: String::new(), prev_hash: String::new() });
                    }
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

/// Extract block height from raw header bytes (little-endian u32 at offset 60-64)
/// Per Lotus block header format: https://lotusia.org/docs/specs/blockheader
pub fn get_raw_block_height(header_raw: &[u8]) -> Option<i64> {
    if header_raw.len() < 64 {
        return None;
    }
    Some(i64::from(i32::from_le_bytes([
        header_raw[60],
        header_raw[61],
        header_raw[62],
        header_raw[63],
    ])))
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
    async fn get_block_by_height(&self, height: i64) -> Result<Block>;
    async fn submit_block(&self, block: Vec<u8>) -> Result<SubmitBlockRpcResult>;
    async fn get_block_count(&self) -> Result<i64>;
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

    async fn get_block_by_height(&self, height: i64) -> Result<Block> {
        // Validate height fits in i32 range to prevent silent truncation
        if height < i32::MIN as i64 || height > i32::MAX as i64 {
            anyhow::bail!("block height {} out of valid range [{}, {}]", height, i32::MIN, i32::MAX);
        }
        self.nng
            .rpc
            .get_block(BlockIdentifier::Height(height as i32))
            .map_err(|e| anyhow::anyhow!(e.to_string()))
    }

    async fn submit_block(&self, block: Vec<u8>) -> Result<SubmitBlockRpcResult> {
        self.json_rpc.submit_block(block).await
    }

    async fn get_block_count(&self) -> Result<i64> {
        self.json_rpc.get_block_count().await
    }
}
