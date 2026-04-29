use anyhow::Result;
use async_trait::async_trait;
use bitcoinsuite_bitcoind_nng::RpcInterface;

/// Thin abstraction to keep pool core decoupled from concrete NNG client
/// implementation and ease testing/fault injection.
#[async_trait]
pub trait NodeMiningAdapter: Send + Sync {
    async fn get_mining_template_raw(&self) -> Result<Vec<u8>>;
    async fn submit_mined_block(&self, block: Vec<u8>) -> Result<String>;
    async fn validate_proposal(&self, block: Vec<u8>) -> Result<String>;
}

pub struct BitcoindNngAdapter {
    #[allow(dead_code)]
    rpc: RpcInterface,
}

impl BitcoindNngAdapter {
    pub fn connect(url: &str) -> Result<Self> {
        Ok(Self {
            rpc: RpcInterface::open(url).map_err(|e| anyhow::anyhow!(e.to_string()))?,
        })
    }
}

#[async_trait]
impl NodeMiningAdapter for BitcoindNngAdapter {
    async fn get_mining_template_raw(&self) -> Result<Vec<u8>> {
        // TODO(phase-compat): switch to typed GetMiningTemplate once
        // bitcoinsuite-bitcoind-nng is updated to lotusd mining RPC schema.
        Ok(vec![])
    }

    async fn submit_mined_block(&self, _block: Vec<u8>) -> Result<String> {
        // TODO(phase-compat): use SubmitMinedBlock NNG RPC after crate update.
        Ok("unwired".to_string())
    }

    async fn validate_proposal(&self, _block: Vec<u8>) -> Result<String> {
        // TODO(phase-compat): use ValidateMinedBlockProposal RPC after crate update.
        Ok("unwired".to_string())
    }
}
