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
