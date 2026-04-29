use anyhow::Result;
use async_trait::async_trait;
use bitcoinsuite_bitcoind_nng::{
    MiningTemplate, PubInterface, RpcInterface, SubmitMinedBlockResult,
    ValidateMinedBlockProposalResult,
};
use tracing::{debug, info, warn};

#[derive(Debug, Clone)]
pub enum NodeEvent {
    UpdateBlkTip,
    MempoolRefresh,
    MiningWorkChanged,
}

/// Thin abstraction to keep pool core decoupled from concrete NNG client
/// implementation and ease testing/fault injection.
#[async_trait]
pub trait NodeMiningAdapter: Send + Sync {
    async fn get_mining_template(&self, coinbase_script: Option<Vec<u8>>)
        -> Result<MiningTemplate>;
    async fn submit_mined_block(&self, block: Vec<u8>) -> Result<SubmitMinedBlockResult>;
    async fn validate_proposal(&self, block: Vec<u8>) -> Result<ValidateMinedBlockProposalResult>;
}

pub struct BitcoindNngAdapter {
    rpc: RpcInterface,
}

impl BitcoindNngAdapter {
    pub fn connect(url: &str) -> Result<Self> {
        info!(nng_rpc = %url, "connecting NNG RPC adapter");
        Ok(Self {
            rpc: RpcInterface::open(url).map_err(|e| anyhow::anyhow!(e.to_string()))?,
        })
    }

    /// Subscribe to known template-affecting topics and emit normalized events.
    pub async fn run_pub_loop<F>(&self, pub_url: &str, mut on_event: F) -> Result<()>
    where
        F: FnMut(NodeEvent) + Send + 'static,
    {
        info!(nng_pub = %pub_url, "connecting NNG pub adapter");
        let pubif = PubInterface::open(pub_url).map_err(|e| anyhow::anyhow!(e.to_string()))?;
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
        info!("NNG pub subscriptions active: updateblktip,mempooltxadd,mempooltxrem,miningwrkchg");

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
                    _ => warn!(topic, "unknown NNG topic ignored"),
                }
            }
        })
        .await
        .map_err(|e| anyhow::anyhow!(e.to_string()))??;
        Ok(())
    }
}

#[async_trait]
impl NodeMiningAdapter for BitcoindNngAdapter {
    async fn get_mining_template(
        &self,
        coinbase_script: Option<Vec<u8>>,
    ) -> Result<MiningTemplate> {
        let template = self
            .rpc
            .get_mining_template(coinbase_script.as_deref(), 4, 4, true)
            .map_err(|e| anyhow::anyhow!(e.to_string()))?;
        Ok(template)
    }

    async fn submit_mined_block(&self, block: Vec<u8>) -> Result<SubmitMinedBlockResult> {
        self.rpc
            .submit_mined_block(&block)
            .map_err(|e| anyhow::anyhow!(e.to_string()))
    }

    async fn validate_proposal(&self, block: Vec<u8>) -> Result<ValidateMinedBlockProposalResult> {
        self.rpc
            .validate_mined_block_proposal(&block)
            .map_err(|e| anyhow::anyhow!(e.to_string()))
    }
}
