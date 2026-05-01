use bitcoinsuite_bitcoind_stratum::{network_target_to_difficulty, calculate_pool_difficulty};
use bitcoinsuite_bitcoind_nng::MiningTemplate;
use std::sync::Arc;
use parking_lot::RwLock;

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
}

impl Default for DynamicDiffConfig {
    fn default() -> Self {
        Self {
            share_target_ratio: 100.0,
            min_difficulty: 4.0,
            max_difficulty: 1_000_000.0,
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
    /// Configuration
    config: DynamicDiffConfig,
}

impl NetworkDifficultyTracker {
    pub fn new(config: DynamicDiffConfig) -> Self {
        Self {
            inner: Arc::new(RwLock::new(TrackerInner {
                current_network_diff: 1.0,  // Initial placeholder
                current_pool_diff: config.min_difficulty,
                last_template_id: 0,
                config,
            })),
        }
    }
    
    /// Update from a new mining template.
    /// Extracts target, converts to difficulty, recalculates pool diff.
    pub fn update_from_template(&self, template: &MiningTemplate) {
        let target_bytes: [u8; 32] = template.target.as_ref().try_into()
            .expect("template target must be 32 bytes");
        
        let network_diff = network_target_to_difficulty(&target_bytes)
            .unwrap_or(1.0);  // Fallback on error
        
        let pool_diff = calculate_pool_difficulty(
            network_diff,
            self.inner.read().config.share_target_ratio,
            self.inner.read().config.min_difficulty,
            self.inner.read().config.max_difficulty,
        );
        
        let mut inner = self.inner.write();
        inner.current_network_diff = network_diff;
        inner.current_pool_diff = pool_diff;
        inner.last_template_id = template.template_id;
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
        };
        let tracker = NetworkDifficultyTracker::new(config);
        
        // Initial pool diff should be min_difficulty (placeholder network diff = 1.0)
        let pool_diff = tracker.pool_diff();
        assert!(pool_diff >= 4.0);
    }
}
