pub mod external;
pub mod internal;

use crate::payout::plan::PayoutPlan;
use async_trait::async_trait;

/// Data needed by a Signer to build and submit a payout transaction.
///
/// Combines the payout plan (who gets paid what) with the on-chain coinbase
/// UTXO data needed to construct the spending transaction.
#[derive(Debug, Clone)]
pub struct SignedBatchData {
    /// The payout plan describing miner distributions.
    pub plan: PayoutPlan,
    /// Transaction ID of the coinbase transaction being spent.
    pub coinbase_txid: String,
    /// Output index of the coinbase output (typically 1 in Lotus coinbase tx).
    pub coinbase_vout: u32,
    /// Amount of the coinbase output in satoshis.
    pub coinbase_amount: i64,
    /// Hex-encoded scriptPubKey of the coinbase output.
    pub coinbase_script_pubkey_hex: String,
    /// Chain fork id the payout signature must commit to, resolved from the
    /// node's chain state (see `JsonRpcClient::get_chain_fork_id`): 0 before
    /// the replay-protection activation time, `CHAIN_FORK_ID` (0x4c4f54)
    /// at or past it. The signer passes this to
    /// `SigHashType::with_fork_value` — signatures committing to the wrong
    /// fork value are rejected once replay protection is active.
    pub chain_fork_id: u32,
}

/// Abstraction for signing and submitting payout transactions.
///
/// Implementations handle the mechanics of building a payout transaction
/// from a [`SignedBatchData`], signing it (either in-process or via an
/// external service), and broadcasting it to the Lotus network.
#[async_trait]
pub trait Signer: Send + Sync {
    /// Sign and submit a payout transaction.
    ///
    /// Returns the transaction ID on success, or an error describing what
    /// went wrong (e.g., RPC failure, invalid key, webhook error).
    async fn sign_and_submit(&self, data: &SignedBatchData) -> anyhow::Result<String>;
}
