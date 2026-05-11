use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Payout method abstraction. PPLNS is active now; others are scaffolded.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PayoutMethod {
    Pplns,
    /// Scaffold only (future).
    Pps,
    /// Scaffold only (future).
    Prop,
}

impl PayoutMethod {
    pub fn as_str(self) -> &'static str {
        match self {
            PayoutMethod::Pplns => "pplns",
            PayoutMethod::Pps => "pps",
            PayoutMethod::Prop => "prop",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Worker {
    pub id: i64,
    pub payout_address: String,
    pub worker_suffix: Option<String>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Share {
    pub id: i64,
    pub worker_id: i64,
    pub template_id: u64,
    pub difficulty: f64,
    pub accepted: bool,
    pub stale: bool,
    pub dedupe_key: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Round {
    pub id: i64,
    pub start_template_id: u64,
    pub end_template_id: Option<u64>,
    pub found_block_hash: Option<String>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PayoutBatch {
    pub id: i64,
    pub method: String,
    pub status: String,
    pub submitted_txid: Option<String>,
    pub created_at: DateTime<Utc>,
}

/// Represents a block found by the pool.
/// Tracks lifecycle from confirmed → matured → paid, or orphaned.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FoundBlock {
    pub id: i64,
    pub round_id: i64,
    pub block_hash: String,
    pub height: i64,
    pub status: String,
    pub template_id: Option<i64>,
    pub worker_id: Option<i64>,
    pub worker_name: Option<String>,
    pub payout_address: Option<String>,
    pub persist_source: Option<String>,
    pub disconnected_at: Option<DateTime<Utc>>,
    pub orphan_reason: Option<String>,
    pub matured_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
}

impl FoundBlock {
    /// Compute confirmations from authoritative tip height.
    /// Returns -1 for orphaned blocks (per lotusd parlance).
    /// Returns 0 for non-orphaned blocks when tip is behind block height.
    pub fn confirmations(&self, tip_height: i64) -> i64 {
        if self.status == "orphaned" {
            return -1;
        }
        (tip_height - self.height + 1).max(0)
    }

    /// Check if block is matured based on confirmations.
    pub fn is_matured(&self, tip_height: i64, coinbase_maturity: i64) -> bool {
        self.status == "confirmed" && self.confirmations(tip_height) >= coinbase_maturity
    }
}
