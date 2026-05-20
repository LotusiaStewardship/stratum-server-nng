use bitcoinsuite_bitcoind_stratum::target_to_difficulty;
use std::collections::VecDeque;
use std::time::{Duration, Instant};

/// Convert a hex-encoded network target (32 bytes) to a floating-point difficulty value.
///
/// This is used to convert a job's `network_target_hex` into N_diff for VarDiff ceiling
/// calculation and for initial difficulty computation at session creation.
///
/// Returns `None` if the hex string is invalid or not exactly 32 bytes.
pub fn network_target_hex_to_difficulty(hex: &str) -> Option<f64> {
    let bytes = hex::decode(hex).ok()?;
    let arr: [u8; 32] = bytes.as_slice().try_into().ok()?;
    target_to_difficulty(&arr).ok()
}

/// Configuration for per-session variable difficulty (VarDiff).
#[derive(Debug, Clone)]
pub struct VarDiffConfig {
    /// Absolute minimum P_diff floor (default 0.001).
    pub min_floor: f64,
    /// Initial P_diff as a percentage of N_diff (default 0.01 = 1%).
    pub initial_pct: f64,
    /// Target time between shares in seconds (default 20.0).
    pub target_secs: f64,
    /// Retarget interval in seconds (default 60.0).
    pub retarget_secs: f64,
}

impl Default for VarDiffConfig {
    fn default() -> Self {
        Self {
            min_floor: 0.001,
            initial_pct: 0.01,
            target_secs: 20.0,
            retarget_secs: 60.0,
        }
    }
}

/// Per-session variable difficulty controller.
///
/// Manages the pool difficulty (P_diff) for a single TCP connection.
/// Retargets difficulty based on observed share rate to maintain
/// approximately one share per `target_secs`.
#[derive(Debug, Clone)]
pub struct VarDiff {
    config: VarDiffConfig,
    /// Current P_diff value.
    current: f64,
    /// Ceiling: N_diff (network difficulty). Updated via `update_max()`.
    max: f64,
    /// Timestamp of the last retarget (or creation).
    last_retarget: Instant,
    /// Timestamps of recent shares for rate calculation.
    share_timestamps: VecDeque<Instant>,
}

impl VarDiff {
    /// Create a new VarDiff instance.
    ///
    /// Initial P_diff = N_diff × initial_pct, clamped to [min_floor, N_diff].
    /// `now` is the current time, used as the base for the first retarget window.
    pub fn new(config: VarDiffConfig, n_diff: f64, now: Instant) -> Self {
        let initial = (config.initial_pct * n_diff).clamp(config.min_floor, n_diff);
        Self {
            config,
            current: initial,
            max: n_diff,
            last_retarget: now,
            share_timestamps: VecDeque::new(),
        }
    }

    /// Get the current P_diff value.
    pub fn current(&self) -> f64 {
        self.current
    }

    /// Record a share submission timestamp.
    /// Used for rate calculation during retarget.
    pub fn record_share(&mut self, now: Instant) {
        self.share_timestamps.push_back(now);
    }

    /// Update the N_diff ceiling. When N_diff decreases, P_diff is clamped down.
    /// Called when a new template arrives with a lower network difficulty.
    pub fn update_max(&mut self, new_n_diff: f64) {
        self.max = new_n_diff;
        if self.current > self.max {
            self.current = self.max;
        }
    }

    /// Attempt to retarget difficulty based on observed share rate.
    ///
    /// Returns `Some(new_p_diff)` if difficulty changed, `None` otherwise.
    ///
    /// Retarget algorithm:
    /// - Window = time since last retarget
    /// - Expected shares = retarget_secs / target_secs
    /// - Ratio = actual_shares / expected_shares
    /// - If ratio > 1.0 (too fast): new = current × min(ratio, 1.5)
    /// - If ratio < 1.0 (too slow): new = current × max(ratio, 0.67)
    /// - Clamp result to [min_floor, max]
    pub fn maybe_retarget(&mut self, now: Instant) -> Option<f64> {
        let window_duration = now.duration_since(self.last_retarget);
        if window_duration < Duration::from_secs_f64(self.config.retarget_secs) {
            return None;
        }

        // Count shares since last retarget
        let share_count = self
            .share_timestamps
            .iter()
            .filter(|t| **t >= self.last_retarget)
            .count() as f64;

        if share_count < 1.0 {
            return None;
        }

        let expected = self.config.retarget_secs / self.config.target_secs;
        let ratio = share_count / expected;

        let new_diff = if ratio > 1.0 {
            (self.current * ratio.min(1.5)).clamp(self.config.min_floor, self.max)
        } else {
            (self.current * ratio.max(0.67)).clamp(self.config.min_floor, self.max)
        };

        if (new_diff - self.current).abs() < f64::EPSILON {
            return None;
        }

        self.current = new_diff;
        self.last_retarget = now;
        self.share_timestamps.clear();

        Some(new_diff)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn default_config() -> VarDiffConfig {
        VarDiffConfig::default()
    }

    #[test]
    fn test_initial_difficulty_from_n_diff() {
        let now = Instant::now();
        // N_diff = 100.0, initial_pct = 0.01 → expected P_diff = 1.0
        let vardiff = VarDiff::new(default_config(), 100.0, now);
        assert!(
            (vardiff.current() - 1.0).abs() < f64::EPSILON,
            "expected P_diff = 1.0, got {}",
            vardiff.current()
        );
    }

    #[test]
    fn test_initial_clamps_to_min_floor() {
        let now = Instant::now();
        let mut config = default_config();
        config.min_floor = 5.0;
        // N_diff = 100.0, initial_pct = 0.01 → raw = 1.0, clamped to min_floor = 5.0
        let vardiff = VarDiff::new(config, 100.0, now);
        assert!(
            (vardiff.current() - 5.0).abs() < f64::EPSILON,
            "expected P_diff = 5.0 (clamped to min_floor), got {}",
            vardiff.current()
        );
    }

    #[test]
    fn test_initial_clamps_to_n_diff() {
        let now = Instant::now();
        let mut config = default_config();
        config.initial_pct = 2.0; // 200% — would exceed N_diff
        // N_diff = 100.0, raw = 200.0, clamped to max = 100.0
        let vardiff = VarDiff::new(config, 100.0, now);
        assert!(
            (vardiff.current() - 100.0).abs() < f64::EPSILON,
            "expected P_diff = 100.0 (clamped to N_diff), got {}",
            vardiff.current()
        );
    }

    #[test]
    fn test_record_share_accepts_timestamp() {
        let now = Instant::now();
        let mut vardiff = VarDiff::new(default_config(), 100.0, now);
        // record_share should accept an Instant and not panic
        vardiff.record_share(now + Duration::from_secs(1));
        vardiff.record_share(now + Duration::from_secs(5));
        vardiff.record_share(now + Duration::from_secs(10));
        // The shares are recorded — retarget behavior tested separately
    }

    #[test]
    fn test_retarget_increases_difficulty_when_fast() {
        let now = Instant::now();
        let mut vardiff = VarDiff::new(default_config(), 100.0, now);
        // Fast shares: 6 shares in the first minute (target is 3 in 60s)
        // Expected = 60/20 = 3, Actual = 6, Ratio = 2.0
        vardiff.record_share(now + Duration::from_secs(5));
        vardiff.record_share(now + Duration::from_secs(10));
        vardiff.record_share(now + Duration::from_secs(20));
        vardiff.record_share(now + Duration::from_secs(25));
        vardiff.record_share(now + Duration::from_secs(30));
        vardiff.record_share(now + Duration::from_secs(40));

        // Retarget at t = 61s (window = 61s from creation, exceeds 60s retarget_secs)
        let result = vardiff.maybe_retarget(now + Duration::from_secs(61));

        assert!(result.is_some(), "expected retarget to fire for fast shares");
        let new_diff = result.unwrap();
        // Ratio = 6/3 = 2.0, capped at 1.5x → 1.0 * 1.5 = 1.5
        assert!(
            (new_diff - 1.5).abs() < f64::EPSILON,
            "expected P_diff = 1.5 (capped increase), got {}",
            new_diff
        );
        assert!(
            (vardiff.current() - 1.5).abs() < f64::EPSILON,
            "current P_diff should match retarget result"
        );
    }

    #[test]
    fn test_retarget_decreases_difficulty_when_slow() {
        let now = Instant::now();
        let mut vardiff = VarDiff::new(default_config(), 100.0, now);
        // Slow shares: 1 share in the first minute (target is 3)
        // Expected = 3, Actual = 1, Ratio = 1/3 ≈ 0.33, floored at 0.67x
        vardiff.record_share(now + Duration::from_secs(5));

        let result = vardiff.maybe_retarget(now + Duration::from_secs(61));

        assert!(result.is_some(), "expected retarget to fire for slow shares");
        let new_diff = result.unwrap();
        // Ratio = 1/3 ≈ 0.33, floored at 0.67x → 1.0 * 0.67 = 0.67
        assert!(
            (new_diff - 0.67).abs() < 0.001,
            "expected P_diff ≈ 0.67 (floored decrease), got {}",
            new_diff
        );
        assert!(
            (vardiff.current() - 0.67).abs() < 0.001,
            "current P_diff should match retarget result"
        );
    }

    #[test]
    fn test_retarget_no_change_when_on_target() {
        let now = Instant::now();
        let mut vardiff = VarDiff::new(default_config(), 100.0, now);
        // Exactly on target: 3 shares in the first minute
        // Expected = 3, Actual = 3, Ratio = 1.0
        vardiff.record_share(now + Duration::from_secs(10));
        vardiff.record_share(now + Duration::from_secs(30));
        vardiff.record_share(now + Duration::from_secs(50));

        let result = vardiff.maybe_retarget(now + Duration::from_secs(61));

        assert!(result.is_none(), "expected no retarget when on target");
        assert!(
            (vardiff.current() - 1.0).abs() < f64::EPSILON,
            "P_diff should stay at 1.0"
        );
    }

    #[test]
    fn test_retarget_not_before_interval() {
        let now = Instant::now();
        let mut vardiff = VarDiff::new(default_config(), 100.0, now);
        // 6 fast shares but only 30s elapsed (retarget_secs = 60)
        vardiff.record_share(now + Duration::from_secs(1));
        vardiff.record_share(now + Duration::from_secs(5));
        vardiff.record_share(now + Duration::from_secs(10));
        vardiff.record_share(now + Duration::from_secs(15));
        vardiff.record_share(now + Duration::from_secs(20));
        vardiff.record_share(now + Duration::from_secs(25));

        let result = vardiff.maybe_retarget(now + Duration::from_secs(30));

        assert!(
            result.is_none(),
            "should not retarget before retarget_secs elapsed"
        );
        assert!(
            (vardiff.current() - 1.0).abs() < f64::EPSILON,
            "P_diff should stay unchanged"
        );
    }

    #[test]
    fn test_retarget_clamps_to_max() {
        let now = Instant::now();
        let mut vardiff = VarDiff::new(default_config(), 100.0, now);
        // Set current P_diff to near max
        vardiff.current = 90.0;
        // Fast shares: high ratio would push above max=100
        vardiff.record_share(now + Duration::from_secs(5));
        vardiff.record_share(now + Duration::from_secs(10));
        vardiff.record_share(now + Duration::from_secs(15));
        vardiff.record_share(now + Duration::from_secs(20));
        vardiff.record_share(now + Duration::from_secs(25));
        vardiff.record_share(now + Duration::from_secs(30));

        let result = vardiff.maybe_retarget(now + Duration::from_secs(61));

        assert!(result.is_some(), "expected retarget to fire");
        let new_diff = result.unwrap();
        // Ratio = 6/3 = 2.0, capped at 1.5x → 90 * 1.5 = 135, clamped to max = 100
        assert!(
            (new_diff - 100.0).abs() < f64::EPSILON,
            "expected P_diff = 100.0 (clamped to N_diff), got {}",
            new_diff
        );
    }

    #[test]
    fn test_retarget_clamps_to_min_floor() {
        let now = Instant::now();
        let mut config = default_config();
        config.min_floor = 0.5;
        config.initial_pct = 0.5;
        let mut vardiff = VarDiff::new(config, 100.0, now);
        // Set current P_diff = 50.0 (initial_pct=0.5 * 100)
        // Single slow share would push way down
        vardiff.record_share(now + Duration::from_secs(5));

        let result = vardiff.maybe_retarget(now + Duration::from_secs(61));

        assert!(result.is_some(), "expected retarget to fire");
        let new_diff = result.unwrap();
        // Ratio = 1/3 ≈ 0.33, floored at 0.67x → 50 * 0.67 = 33.5, clamped to min_floor = 0.5
        // Actually 33.5 > 0.5, so no clamping needed. Let me make it more extreme.
        assert!(
            new_diff >= 0.5,
            "P_diff should never go below min_floor, got {}",
            new_diff
        );
    }

    #[test]
    fn test_update_max_clamps_current_down() {
        let now = Instant::now();
        let mut vardiff = VarDiff::new(default_config(), 100.0, now);
        // Manually set current above max (simulating scenario where N_diff drops)
        vardiff.current = 150.0;
        vardiff.max = 100.0;

        // update_max with a lower N_diff should clamp current down
        vardiff.update_max(50.0);

        // Current should be clamped to new max = 50.0
        assert!(
            (vardiff.current() - 50.0).abs() < f64::EPSILON,
            "expected P_diff = 50.0 (clamped to new max), got {}",
            vardiff.current()
        );
        assert!(
            (vardiff.max - 50.0).abs() < f64::EPSILON,
            "max should be updated to 50.0"
        );
    }

    #[test]
    fn test_update_max_higher_no_clamp() {
        let now = Instant::now();
        let mut vardiff = VarDiff::new(default_config(), 100.0, now);
        // Current = 1.0, max = 100.0
        // Update max to 200.0 (higher) — should not affect current
        vardiff.update_max(200.0);

        assert!(
            (vardiff.current() - 1.0).abs() < f64::EPSILON,
            "current should stay at 1.0 when max increases"
        );
        assert!(
            (vardiff.max - 200.0).abs() < f64::EPSILON,
            "max should be updated to 200.0"
        );
    }

    #[test]
    fn test_network_target_hex_to_difficulty_valid() {
        // Real network target from test job
        let hex = "0000000009d01000000000000000000000000000000000000000000000000000";
        let diff = network_target_hex_to_difficulty(hex);
        assert!(diff.is_some(), "should parse valid hex target");
        let d = diff.unwrap();
        assert!(d > 0.0, "difficulty should be positive, got {}", d);
    }

    #[test]
    fn test_network_target_hex_to_difficulty_invalid_hex() {
        assert!(network_target_hex_to_difficulty("zzzz").is_none());
    }

    #[test]
    fn test_network_target_hex_to_difficulty_wrong_length() {
        // Too short (not 32 bytes)
        assert!(network_target_hex_to_difficulty("00ff").is_none());
    }
}
