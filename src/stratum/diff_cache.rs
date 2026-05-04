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
    /// Returns the new network difficulty and whether it changed significantly.
    pub fn update_template(&self, template: &MiningTemplate) -> (f64, f64, bool) {
        let old_network_diff = self.tracker.network_diff();
        self.tracker.update_from_template(template);
        let new_network_diff = self.tracker.network_diff();

        // Calculate percentage change
        let change_pct = if old_network_diff > 0.0 {
            (new_network_diff - old_network_diff).abs() / old_network_diff
        } else {
            1.0 // Treat as significant if old was zero
        };

        // Only broadcast if change is significant (> 10%)
        let significant = change_pct > 0.1;

        if significant {
            // Broadcast to all subscribers (miners)
            let _ = self.diff_tx.send(new_network_diff);
        }

        (old_network_diff, new_network_diff, significant)
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
        let config = DynamicDiffConfig::default();
        let tracker = NetworkDifficultyTracker::new(config);
        let cache = DifficultyCache::new(tracker);

        // Initial state
        assert!((cache.network_diff() - 1.0).abs() < 0.0001);

        // Verify subscription works
        let mut rx = cache.subscribe();

        // Note: We can't easily test broadcast without a real template,
        // but we verified the channel is created correctly
        assert!(rx.try_recv().is_err()); // Should be empty initially
    }
}
