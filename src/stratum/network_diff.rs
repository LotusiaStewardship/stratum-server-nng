use bitcoinsuite_bitcoind_nng::MiningTemplate;
use bitcoinsuite_bitcoind_stratum::{calculate_pool_difficulty, network_target_to_difficulty};
use bitcoinsuite_core::Hashed;
use parking_lot::RwLock;
use std::sync::Arc;

/// Configuration for dynamic pool difficulty.
#[derive(Debug, Clone)]
pub struct DynamicDiffConfig {
    /// Ratio of network difficulty to pool difficulty.
    /// pool_diff = network_diff / share_target_ratio
    /// Default: 100.0 (pool shares are 100x easier than network blocks)
    pub share_target_ratio: f64,

    /// Minimum pool difficulty (absolute floor).
    /// Prevents difficulty from crashing to near-zero on low-hashrate testnet.
    /// Default: 4.0 (higher than old 0.0000001 to prevent crashes)
    pub min_difficulty: f64,

    /// Maximum pool difficulty (absolute ceiling).
    /// Default: 1_000_000.0
    pub max_difficulty: f64,

    /// Maximum allowed pool difficulty change per update.
    /// Prevents sudden jumps that could destabilize miners.
    /// Default: 0.5 (50% max change per update)
    pub max_change_pct: f64,

    /// Target time between accepted shares for vardiff tuning.
    /// Default: 15.0 seconds
    pub vardiff_target_secs: f64,

    /// How often vardiff is allowed to retarget per miner.
    /// Default: 90.0 seconds
    pub vardiff_retarget_secs: f64,
}

impl Default for DynamicDiffConfig {
    fn default() -> Self {
        Self {
            share_target_ratio: 100.0,
            min_difficulty: 1.0,
            max_difficulty: 1_000_000.0,
            max_change_pct: 0.5,
            vardiff_target_secs: 15.0,
            vardiff_retarget_secs: 90.0,
        }
    }
}

/// Tracks network difficulty from lotusd mining templates.
/// Thread-safe, cloneable, and designed for concurrent access.
#[derive(Debug, Clone)]
pub struct NetworkDifficultyTracker {
    inner: Arc<RwLock<TrackerInner>>,
}

#[derive(Debug)]
struct TrackerInner {
    /// Current network difficulty (from latest template)
    current_network_diff: f64,
    /// Current pool difficulty (network_diff / ratio, clamped)
    current_pool_diff: f64,
    /// Template ID when difficulty was last updated
    last_template_id: u64,
    /// Template epoch from NNG pub/sub
    last_template_epoch: u64,
    /// Configuration
    config: DynamicDiffConfig,
}

impl NetworkDifficultyTracker {
    pub fn new(config: DynamicDiffConfig) -> Self {
        Self {
            inner: Arc::new(RwLock::new(TrackerInner {
                current_network_diff: 1.0, // Initial placeholder
                current_pool_diff: config.min_difficulty,
                last_template_id: 0,
                last_template_epoch: 0,
                config,
            })),
        }
    }

    /// Update from a new mining template.
    /// Extracts target, converts to difficulty, recalculates pool diff.
    pub fn update_from_template(&self, template: &MiningTemplate) {
        // Convert target to big-endian bytes for difficulty calculation
        let target_bytes: [u8; 32] = template
            .target
            .to_vec_be()
            .try_into()
            .expect("template target must be 32 bytes");

        let network_diff = network_target_to_difficulty(&target_bytes).unwrap_or(1.0); // Fallback on error

        let inner = self.inner.read();
        let previous_pool_diff = inner.current_pool_diff;
        let pool_diff = calculate_pool_difficulty(
            network_diff,
            previous_pool_diff,
            inner.config.share_target_ratio,
            inner.config.min_difficulty,
            inner.config.max_difficulty,
            inner.config.max_change_pct,
        );
        drop(inner);

        let mut inner = self.inner.write();
        inner.current_network_diff = network_diff;
        inner.current_pool_diff = pool_diff;
        inner.last_template_id = template.template_id;
        // Note: template_epoch comes from NNG pub/sub, not the template itself
    }

    /// Set template epoch from NNG pub/sub event.
    pub fn set_template_epoch(&self, epoch: u64) {
        self.inner.write().last_template_epoch = epoch;
    }

    /// Get current network difficulty.
    pub fn network_diff(&self) -> f64 {
        self.inner.read().current_network_diff
    }

    /// Get current pool difficulty (for new miners).
    pub fn pool_diff(&self) -> f64 {
        self.inner.read().current_pool_diff
    }

    /// Get configuration.
    pub fn config(&self) -> DynamicDiffConfig {
        self.inner.read().config.clone()
    }

    /// Get last template ID.
    pub fn last_template_id(&self) -> u64 {
        self.inner.read().last_template_id
    }

    /// Get last template epoch.
    pub fn last_template_epoch(&self) -> u64 {
        self.inner.read().last_template_epoch
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tracker_default_config() {
        let tracker = NetworkDifficultyTracker::new(DynamicDiffConfig::default());
        let config = tracker.config();
        assert!((config.share_target_ratio - 100.0).abs() < 0.0001);
        assert!((config.min_difficulty - 4.0).abs() < 0.0001);
        assert!((config.max_difficulty - 1_000_000.0).abs() < 0.0001);
    }

    #[test]
    fn test_pool_diff_clamping() {
        let config = DynamicDiffConfig {
            share_target_ratio: 100.0,
            min_difficulty: 4.0,
            max_difficulty: 1_000_000.0,
            max_change_pct: 0.5,
            vardiff_target_secs: 15.0,
            vardiff_retarget_secs: 90.0,
        };
        let tracker = NetworkDifficultyTracker::new(config);

        // Initial pool diff should be min_difficulty (placeholder network diff = 1.0)
        let pool_diff = tracker.pool_diff();
        assert!(pool_diff >= 4.0);
    }
}
