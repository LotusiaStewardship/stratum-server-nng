//! Share Aggregation Pipeline
//!
//! Aggregates share submissions per-worker and broadcasts updates periodically.
//! This avoids flooding WebSocket clients with high-frequency per-share events.
//!
//! # Event Flow
//!
//! 1. Stratum server records share to database
//! 2. ShareAggregator increments in-memory counter for that worker
//! 3. Background task runs every 30 seconds:
//!    - Snapshots all active worker counters
//!    - Resets share counters (blocks accumulate)
//!    - Queries hashrate from DB (5-min rolling window)
//!    - Broadcasts ShareUpdateEvent via DashboardEventSender
//!
//! # Why Aggregation?
//!
//! Shares arrive at high frequency (100s-1000s per minute). Broadcasting
//! each share would:
//! - Flood WebSocket clients with excessive updates
//! - Increase bandwidth costs significantly
//! - Provide minimal UX value (users care about aggregates)
//!
//! Instead, shares are aggregated per-worker and broadcast every 30 seconds.

use crate::accounting::AccountingDb;
use crate::http::{DashboardEvent, DashboardEventSender, ShareUpdateEvent};
use dashmap::DashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::time::{interval, Duration};
use tracing::{debug, error, info};

/// Per-worker share counters using lock-free atomics
/// Note: Not Clone - entries are created on-demand in DashMap
struct WorkerShareCounts {
    shares_accepted: AtomicU64,
    shares_rejected: AtomicU64,
    shares_stale: AtomicU64,
    blocks_found: AtomicU64,
    payout_address: String,
    worker_suffix: Option<String>,
}

impl Clone for WorkerShareCounts {
    fn clone(&self) -> Self {
        Self {
            shares_accepted: AtomicU64::new(self.shares_accepted.load(Ordering::Relaxed)),
            shares_rejected: AtomicU64::new(self.shares_rejected.load(Ordering::Relaxed)),
            shares_stale: AtomicU64::new(self.shares_stale.load(Ordering::Relaxed)),
            blocks_found: AtomicU64::new(self.blocks_found.load(Ordering::Relaxed)),
            payout_address: self.payout_address.clone(),
            worker_suffix: self.worker_suffix.clone(),
        }
    }
}

impl WorkerShareCounts {
    fn new(payout_address: String, worker_suffix: Option<String>) -> Self {
        Self {
            shares_accepted: AtomicU64::new(0),
            shares_rejected: AtomicU64::new(0),
            shares_stale: AtomicU64::new(0),
            blocks_found: AtomicU64::new(0),
            payout_address,
            worker_suffix,
        }
    }
}

/// Share aggregator for broadcasting periodic updates
pub struct ShareAggregator {
    /// Per-worker share counters (lock-free concurrent access)
    workers: DashMap<i64, WorkerShareCounts>,
    /// Event sender for broadcasting updates
    events_tx: DashboardEventSender,
    /// Database for hashrate calculation
    db: AccountingDb,
    /// Broadcast interval in seconds
    interval_secs: u64,
}

impl Clone for ShareAggregator {
    fn clone(&self) -> Self {
        Self {
            workers: self.workers.clone(),
            events_tx: self.events_tx.clone(),
            db: self.db.clone(),
            interval_secs: self.interval_secs,
        }
    }
}

impl ShareAggregator {
    /// Create new aggregator (does not spawn broadcast loop)
    pub fn new(db: AccountingDb, events_tx: DashboardEventSender, interval_secs: u64) -> Self {
        Self {
            workers: DashMap::new(),
            events_tx,
            db,
            interval_secs,
        }
    }

    /// Record a share submission (called after DB write)
    pub fn record_share(&self, worker_id: i64, payout_address: String, worker_suffix: Option<String>, accepted: bool, stale: bool) {
        // Get or create worker entry
        let entry = self.workers.entry(worker_id).or_insert_with(|| {
            WorkerShareCounts::new(payout_address, worker_suffix)
        });

        // Increment appropriate counter
        if stale {
            entry.value().shares_stale.fetch_add(1, Ordering::Relaxed);
        } else if accepted {
            entry.value().shares_accepted.fetch_add(1, Ordering::Relaxed);
        } else {
            entry.value().shares_rejected.fetch_add(1, Ordering::Relaxed);
        }

        debug!(
            worker_id,
            accepted,
            stale,
            shares_accepted = entry.value().shares_accepted.load(Ordering::Relaxed),
            shares_rejected = entry.value().shares_rejected.load(Ordering::Relaxed),
            shares_stale = entry.value().shares_stale.load(Ordering::Relaxed),
            "share recorded"
        );
    }

    /// Record a block found (called after DB write)
    pub fn record_block(&self, worker_id: i64, payout_address: String, worker_suffix: Option<String>) {
        let entry = self.workers.entry(worker_id).or_insert_with(|| {
            WorkerShareCounts::new(payout_address, worker_suffix)
        });

        entry.value().blocks_found.fetch_add(1, Ordering::Relaxed);

        debug!(
            worker_id,
            blocks_found = entry.value().blocks_found.load(Ordering::Relaxed),
            "block recorded"
        );
    }

    /// Spawn the broadcast loop (call once from main.rs)
    pub fn spawn_broadcast_loop(self) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            self.broadcast_loop().await;
        })
    }

    /// Main broadcast loop
    async fn broadcast_loop(&self) {
        let mut interval = interval(Duration::from_secs(self.interval_secs));
        info!(interval_secs = self.interval_secs, "share aggregator broadcast loop started");

        loop {
            interval.tick().await;

            // Collect active workers (those with activity this interval)
            let mut active_workers = Vec::new();

            for entry in self.workers.iter() {
                let counts = entry.value();
                let accepted = counts.shares_accepted.swap(0, Ordering::Relaxed);
                let rejected = counts.shares_rejected.swap(0, Ordering::Relaxed);
                let stale = counts.shares_stale.swap(0, Ordering::Relaxed);
                let blocks = counts.blocks_found.load(Ordering::Relaxed); // Don't reset blocks

                // Only broadcast if there was activity
                if accepted > 0 || rejected > 0 || stale > 0 || blocks > 0 {
                    active_workers.push((
                        *entry.key(),
                        accepted,
                        rejected,
                        stale,
                        blocks,
                        counts.payout_address.clone(),
                        counts.worker_suffix.clone(),
                    ));
                }
            }

            if active_workers.is_empty() {
                debug!("no active workers this interval");
                continue;
            }

            debug!(count = active_workers.len(), "broadcasting share updates");

            // For each active worker, calculate hashrate and broadcast
            for (worker_id, accepted, rejected, stale, blocks, payout_address, worker_suffix) in active_workers {
                // Query hashrate for this worker (5-min window)
                let hashrate = match self.db.calculate_worker_hashrate(worker_id, 300) {
                    Ok(h) => h,
                    Err(e) => {
                        error!(worker_id, error = %e, "failed to calculate worker hashrate");
                        0.0
                    }
                };

                let event = DashboardEvent::ShareUpdate(ShareUpdateEvent {
                    worker_id,
                    payout_address,
                    worker_suffix,
                    shares_accepted: accepted,
                    shares_rejected: rejected,
                    shares_stale: stale,
                    blocks_found: blocks,
                    hashrate,
                });

                self.events_tx.send(event);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    #[test]
    fn test_record_share() {
        let f = NamedTempFile::new().unwrap();
        let db = AccountingDb::open(f.path().to_str().unwrap()).unwrap();
        db.init_schema().unwrap();

        let events_tx = DashboardEventSender::new(true);
        let aggregator = ShareAggregator::new(db, events_tx, 30);

        aggregator.record_share(1, "addr1".to_string(), Some("rig1".to_string()), true, false);
        aggregator.record_share(1, "addr1".to_string(), Some("rig1".to_string()), true, false);
        aggregator.record_share(1, "addr1".to_string(), Some("rig1".to_string()), false, false);

        let entry = aggregator.workers.get(&1).unwrap();
        assert_eq!(entry.shares_accepted.load(Ordering::Relaxed), 2);
        assert_eq!(entry.shares_rejected.load(Ordering::Relaxed), 1);
        assert_eq!(entry.shares_stale.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn test_record_block() {
        let f = NamedTempFile::new().unwrap();
        let db = AccountingDb::open(f.path().to_str().unwrap()).unwrap();
        db.init_schema().unwrap();

        let events_tx = DashboardEventSender::new(true);
        let aggregator = ShareAggregator::new(db, events_tx, 30);

        aggregator.record_block(1, "addr1".to_string(), Some("rig1".to_string()));
        aggregator.record_block(1, "addr1".to_string(), Some("rig1".to_string()));

        let entry = aggregator.workers.get(&1).unwrap();
        assert_eq!(entry.blocks_found.load(Ordering::Relaxed), 2);
    }

    // Note: Concurrent access is tested implicitly through integration tests.
    // The DashMap + AtomicU64 pattern is a well-established concurrency primitive.
}
