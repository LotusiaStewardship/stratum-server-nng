//! Dashboard Event System
//!
//! Real-time event broadcasting for HTTP dashboard updates.
//! Events are broadcast to all connected WebSocket clients.
//!
//! # Event Flow
//!
//! 1. Stratum server emits `BlockFoundEvent` on block submission
//! 2. `ShareAggregator` emits `ShareUpdateEvent` every 30 seconds (aggregated)
//! 3. Background task emits `StatsUpdateEvent` every 5 seconds
//! 4. WebSocket clients receive all events via broadcast channel
//! 5. Cache invalidation triggers on `BlockFoundEvent` and `ShareUpdateEvent`
//!
//! # Why Share Aggregation?
//!
//! Shares arrive at high frequency (100s-1000s per minute). Broadcasting
//! each share would:
//! - Flood WebSocket clients with excessive updates
//! - Increase bandwidth costs significantly
//! - Provide minimal UX value (users care about aggregates)
//!
//! Instead, shares are aggregated per-worker and broadcast every 30 seconds.

use chrono::{DateTime, Utc};
use tokio::sync::broadcast;

/// Block found event - updates blocks tables on all pages
#[derive(Clone, serde::Serialize)]
pub struct BlockFoundEvent {
    pub height: i64,
    pub hash: String,
    pub status: String,
    pub confirmations: i64,
    pub found_by: String,
    pub payout_address: String,
    pub found_at: DateTime<Utc>,
}

/// Share update event - updates worker stats on miners/home pages
#[derive(Clone, serde::Serialize)]
pub struct ShareUpdateEvent {
    pub worker_id: i64,
    pub payout_address: String,
    pub worker_suffix: Option<String>,
    pub shares_accepted: u64,
    pub shares_rejected: u64,
    pub shares_stale: u64,
    pub blocks_found: u64,
    pub hashrate: f64,
}

/// Aggregate stats update - updates stats cards on home page
#[derive(Clone, serde::Serialize)]
pub struct StatsUpdateEvent {
    pub pool_hashrate: f64,
    pub network_difficulty: f64,
    pub active_miners: u64,
    pub blocks_found_total: u64,
    pub blocks_found_24h: u64,
    pub blocks_found_7d: u64,
}

/// Unified event type for broadcast channel
#[derive(Clone, serde::Serialize)]
#[serde(tag = "type")]
pub enum DashboardEvent {
    #[serde(rename = "stats_update")]
    StatsUpdate(StatsUpdateEvent),

    #[serde(rename = "block_found")]
    BlockFound(BlockFoundEvent),

    #[serde(rename = "share_update")]
    ShareUpdate(ShareUpdateEvent),
}

/// Event sender wrapper - no-op when HTTP dashboard is disabled
///
/// This wrapper ensures that the stratum server can safely call send()
/// without checking if the HTTP dashboard is enabled. If disabled,
/// events are silently dropped with zero overhead.
#[derive(Clone)]
pub struct DashboardEventSender {
    inner: Option<broadcast::Sender<DashboardEvent>>,
}

impl DashboardEventSender {
    /// Create a new sender. If `enabled` is false, events will be silently dropped.
    pub fn new(enabled: bool) -> Self {
        let inner = if enabled {
            let (tx, _rx) = broadcast::channel(1024);
            Some(tx)
        } else {
            None
        };
        Self { inner }
    }

    /// Get a receiver for the channel. Returns None if disabled.
    pub fn subscribe(&self) -> Option<broadcast::Receiver<DashboardEvent>> {
        self.inner.as_ref().map(|tx| tx.subscribe())
    }

    /// Send an event. No-op if disabled.
    pub fn send(&self, event: DashboardEvent) {
        if let Some(ref tx) = self.inner {
            // Ignore errors - channel may have no subscribers or be closed
            let _ = tx.send(event);
        }
    }

    /// Check if event broadcasting is enabled
    pub fn is_enabled(&self) -> bool {
        self.inner.is_some()
    }
}
