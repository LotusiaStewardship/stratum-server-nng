/// Independent vardiff controller for a worker stream.
///
/// Designed for future hashrate growth: clamps and retarget intervals are
/// explicit and stable under sparse share arrivals.
#[derive(Debug, Clone)]
pub struct VarDiff {
    pub current: f64,
    pub min: f64,
    pub max: f64,
    pub target_secs: f64,
    pub retarget_secs: f64,
    last_retarget_ts: i64,
    share_ts: Vec<i64>,
}

impl VarDiff {
    pub fn new(current: f64, min: f64, max: f64, target_secs: f64, retarget_secs: f64) -> Self {
        Self {
            current,
            min,
            max,
            target_secs,
            retarget_secs,
            last_retarget_ts: 0,
            share_ts: Vec::new(),
        }
    }

    pub fn record_share(&mut self, ts: i64) {
        self.share_ts.push(ts);
        // Bound memory; enough for robust moving interval estimate.
        if self.share_ts.len() > 256 {
            self.share_ts.drain(0..self.share_ts.len() - 256);
        }
    }

    pub fn should_retarget(&self, now: i64) -> bool {
        self.last_retarget_ts == 0 || (now - self.last_retarget_ts) as f64 >= self.retarget_secs
    }

    pub fn maybe_retarget(&mut self, now: i64) -> Option<f64> {
        if !self.should_retarget(now) || self.share_ts.len() < 2 {
            return None;
        }
        let first = *self.share_ts.first().unwrap();
        let last = *self.share_ts.last().unwrap();
        let dt = (last - first) as f64;
        let avg = dt / (self.share_ts.len() as f64 - 1.0);
        if avg <= 0.0 {
            return None;
        }
        // new_diff = old_diff * (target/actual)
        let mut ratio = self.target_secs / avg;
        if !ratio.is_finite() {
            return None;
        }
        // Prevent short bursty share windows from causing runaway difficulty jumps.
        // Industry standard: ±50% max change (0.67-1.5 range)
        ratio = ratio.clamp(0.67, 1.5);
        let mut new_diff = self.current * ratio;
        if !new_diff.is_finite() {
            return None;
        }
        new_diff = new_diff.clamp(self.min, self.max);
        self.current = new_diff;
        self.last_retarget_ts = now;

        // Reset retarget sample window so the next decision reflects post-retarget
        // share rate only, preventing compounding from stale low-difficulty history.
        self.share_ts.clear();
        self.share_ts.push(last);

        Some(new_diff)
    }

    /// Update the maximum difficulty (network difficulty ceiling).
    /// Also clamps current difficulty if it exceeds the new max.
    pub fn update_max(&mut self, network_diff: f64) {
        self.max = network_diff;
        // Clamp current to new max if needed
        if self.current > self.max {
            self.current = self.max;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::VarDiff;

    #[test]
    fn vardiff_resets_window_after_retarget() {
        let mut vd = VarDiff::new(1.0, 0.1, 100.0, 15.0, 90.0);

        // Fast shares at diff=1 (every 3s, target is 15s)
        // Would jump to 5.0 without clamping, but clamped to 1.5x max
        for t in [0, 3, 6, 9, 12] {
            vd.record_share(t);
        }
        let d1 = vd.maybe_retarget(90).unwrap();
        // With 0.67-1.5 clamping: ratio=5.0 clamped to 1.5, so new_diff = 1.0 * 1.5 = 1.5
        assert!((d1 - 1.5).abs() < 1e-9);

        // Post-retarget shares come in around target interval; diff should stay bounded,
        // not compound from old low-diff history.
        for t in [27, 42, 57, 72] {
            vd.record_share(t);
        }
        let d2 = vd.maybe_retarget(180).unwrap();
        assert!((d2 - d1).abs() < 1e-9);
    }

    #[test]
    fn vardiff_with_network_diff_ceiling() {
        let network_diff = 72.34;
        let mut vd = VarDiff::new(
            network_diff, // Start at network diff
            0.001,        // Absolute floor
            network_diff, // Ceiling = network diff
            15.0,
            90.0,
        );

        // Verify initial state
        assert!((vd.current - network_diff).abs() < 0.0001);
        assert!((vd.max - network_diff).abs() < 0.0001);
        assert!((vd.min - 0.001).abs() < 0.0001);

        // Test update_max
        vd.update_max(100.0);
        assert!((vd.max - 100.0).abs() < 0.0001);
        assert!((vd.current - network_diff).abs() < 0.0001); // current unchanged

        // Test update_max with lower value (should clamp current)
        vd.update_max(50.0);
        assert!((vd.max - 50.0).abs() < 0.0001);
        assert!((vd.current - 50.0).abs() < 0.0001); // current clamped to new max
    }

    #[test]
    fn vardiff_ramps_up_from_low_start() {
        // Simulates a new miner starting at 1% of network difficulty.
        // VarDiff should ramp up as fast shares arrive.
        let network_diff = 72.34;
        let initial_diff = network_diff * 0.01;

        let mut vd = VarDiff::new(
            initial_diff, // Start at 1% of network diff
            0.001,        // Absolute floor
            network_diff, // Ceiling = network diff
            15.0,
            90.0,
        );

        // Verify initial state: starts low, not at network diff
        assert!((vd.current - initial_diff).abs() < 1e-9);
        assert!(vd.current < network_diff);
        assert!((vd.max - network_diff).abs() < 1e-9);

        // Fast shares arrive (every 3s, target is 15s) — should ramp up
        for t in [0, 3, 6, 9, 12] {
            vd.record_share(t);
        }
        let d1 = vd.maybe_retarget(90).unwrap();
        assert!(
            d1 > initial_diff,
            "difficulty should ramp up from low start"
        );
        assert!(
            d1 < network_diff,
            "difficulty should not exceed network diff"
        );

        // Continue fast shares — should keep ramping up toward network diff
        for round in 0..20 {
            let base = 90 + round * 30;
            for t in [base, base + 3, base + 6, base + 9, base + 12] {
                vd.record_share(t);
            }
            if let Some(d) = vd.maybe_retarget(base + 120) {
                if d >= network_diff * 0.99 {
                    break; // reached near ceiling
                }
            }
        }

        // After sustained fast shares, difficulty should approach the ceiling
        assert!(
            vd.current > initial_diff * 2.0,
            "difficulty should have ramped up significantly from low start"
        );
    }
}
