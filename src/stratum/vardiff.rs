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
        let dt = (self.share_ts.last().unwrap() - self.share_ts.first().unwrap()) as f64;
        let avg = dt / (self.share_ts.len() as f64 - 1.0);
        if avg <= 0.0 {
            return None;
        }
        // new_diff = old_diff * (target/actual)
        let mut new_diff = self.current * (self.target_secs / avg);
        if !new_diff.is_finite() {
            return None;
        }
        new_diff = new_diff.clamp(self.min, self.max);
        self.current = new_diff;
        self.last_retarget_ts = now;
        Some(new_diff)
    }
}
