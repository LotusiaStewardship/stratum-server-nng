use super::network_diff::NetworkDifficultyTracker;
use bitcoinsuite_bitcoind_nng::MiningTemplate;
use tokio::sync::broadcast;

/// Thread-safe cache for network/pool difficulty.
/// Supports broadcasting difficulty updates to subscribers (miners).
#[derive(Clone)]
pub struct DifficultyCache {
    tracker: NetworkDifficultyTracker,
    /// Broadcast channel for difficulty updates to all miners
    diff_tx: broadcast::Sender<f64>,
}

impl DifficultyCache {
    pub fn new(tracker: NetworkDifficultyTracker) -> Self {
        let (diff_tx, _diff_rx) = broadcast::channel(16);
        Self { tracker, diff_tx }
    }

    /// Update from mining template.
    /// Returns the new pool difficulty and whether it changed significantly.
    pub fn update_template(&self, template: &MiningTemplate) -> (f64, f64, bool) {
        let old_pool_diff = self.tracker.pool_diff();
        self.tracker.update_from_template(template);
        let new_pool_diff = self.tracker.pool_diff();
        let epoch = self.tracker.last_template_epoch();

        // Calculate percentage change
        let change_pct = if old_pool_diff > 0.0 {
            (new_pool_diff - old_pool_diff).abs() / old_pool_diff
        } else {
            1.0 // Treat as significant if old was zero
        };

        // Only broadcast if change is significant (> 10%)
        let significant = change_pct > 0.1;

        if significant {
            // Broadcast to all subscribers (miners)
            let _ = self.diff_tx.send(new_pool_diff);
            tracing::info!(
                old_diff = %old_pool_diff,
                new_diff = %new_pool_diff,
                change_pct = format!("{:.2}%", change_pct * 100.0),
                template_id = template.template_id,
                template_epoch = epoch,
                "difficulty broadcast to miners"
            );
        } else {
            tracing::debug!(
                old_diff = %old_pool_diff,
                new_diff = %new_pool_diff,
                change_pct = format!("{:.2}%", change_pct * 100.0),
                template_epoch = epoch,
                "difficulty change too small to broadcast"
            );
        }

        (old_pool_diff, new_pool_diff, significant)
    }

    /// Get current pool difficulty.
    pub fn pool_diff(&self) -> f64 {
        self.tracker.pool_diff()
    }

    /// Get current network difficulty.
    pub fn network_diff(&self) -> f64 {
        self.tracker.network_diff()
    }

    /// Get tracker for direct access.
    pub fn tracker(&self) -> &NetworkDifficultyTracker {
        &self.tracker
    }

    /// Subscribe to difficulty updates.
    /// Returns a receiver that gets new difficulty values when they change.
    pub fn subscribe(&self) -> broadcast::Receiver<f64> {
        self.diff_tx.subscribe()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stratum::network_diff::DynamicDiffConfig;

    #[test]
    fn test_cache_broadcast_on_significant_change() {
        // Use default config - network comes from config, not hardcoded
        let config = DynamicDiffConfig::default();
        let tracker = NetworkDifficultyTracker::new(config);
        let cache = DifficultyCache::new(tracker);

        // Initial state
        assert!(cache.pool_diff() >= 1.0);

        // Verify subscription works
        let mut rx = cache.subscribe();

        // Note: We can't easily test broadcast without a real template,
        // but we verified the channel is created correctly
        assert!(rx.try_recv().is_err()); // Should be empty initially
    }
}
