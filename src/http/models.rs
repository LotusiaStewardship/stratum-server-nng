//! HTTP Dashboard Response Models
//!
//! These models are used for JSON API responses and template rendering.

use chrono::{DateTime, Utc};
use serde::Serialize;

/// Aggregate pool statistics
#[derive(Debug, Clone, Serialize, Default)]
pub struct PoolStats {
    pub pool_hashrate: f64,
    pub network_difficulty: f64,
    pub active_miners: u64,
    pub blocks_found_total: u64,
    pub blocks_found_24h: u64,
    pub blocks_found_7d: u64,
    pub last_block_found: Option<LastBlockInfo>,
}

/// Last block found information
#[derive(Debug, Clone, Serialize)]
pub struct LastBlockInfo {
    pub height: i64,
    pub hash: String,
    pub found_at: DateTime<Utc>,
}

/// Worker statistics for public display
#[derive(Debug, Clone, Serialize, Default)]
pub struct WorkerStats {
    pub id: i64,
    pub payout_address: String,
    pub worker_suffix: Option<String>,
    pub shares_accepted: u64,
    pub shares_rejected: u64,
    pub shares_stale: u64,
    #[serde(default)]
    pub hashrate: f64,
    #[serde(default)]
    pub blocks_found: u64,
}

/// Detailed miner information (lookup by payout address)
#[derive(Debug, Clone, Serialize)]
pub struct MinerDetail {
    pub payout_address: String,
    pub total_hashrate: f64,
    pub workers: Vec<WorkerStats>,
    pub total_shares_accepted: u64,
    pub total_blocks_found: u64,
}

/// Round statistics
#[derive(Debug, Clone, Serialize)]
pub struct RoundStats {
    pub id: i64,
    pub start_template_id: u64,
    pub end_template_id: Option<u64>,
    pub found_block_hash: Option<String>,
    pub duration_seconds: Option<u64>,
    pub started_at: DateTime<Utc>,
    pub ended_at: Option<DateTime<Utc>>,
}

/// Block finder information
#[derive(Debug, Clone, Serialize)]
pub struct BlockInfo {
    pub height: i64,
    pub hash: String,
    pub status: String,
    pub confirmations: i64,
    pub found_by: Option<String>,
    pub payout_address: Option<String>,
    pub found_at: DateTime<Utc>,
    pub matured_at: Option<DateTime<Utc>>,
}

/// Payout batch information
#[derive(Debug, Clone, Serialize)]
pub struct PayoutInfo {
    pub id: i64,
    pub method: String,
    pub status: String,
    pub total_amount: f64,
    pub miner_count: u64,
    pub submitted_txid: Option<String>,
    pub created_at: DateTime<Utc>,
    pub confirmed_at: Option<DateTime<Utc>>,
}

/// Health check response
#[derive(Debug, Clone, Serialize)]
pub struct HealthResponse {
    pub status: String,
    pub database: String,
    pub last_share: Option<DateTime<Utc>>,
}
