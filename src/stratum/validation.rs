use sha2::{Digest, Sha256};

/// Native submit shape used by share pre-validator.
#[derive(Debug, Clone)]
pub struct NativeSubmit {
    pub worker_name: String,
    pub job_id: String,
    pub extranonce2: String,
    pub ntime_hex_6b: String,
    pub nonce_hex_8b: String,
}

/// Minimal template projection used for share pre-validation.
#[derive(Debug, Clone)]
pub struct NativeTemplate {
    pub template_id: u64,
    pub prev_hash_stratum: String,
    pub nbits_stratum: String,
    pub coinbase1: String,
    pub coinbase2: String,
    pub merkle_branches: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShareResult {
    Accepted,
    Duplicate,
    Stale,
    Invalid,
    LowDiff,
}

/// Difficulty-1 target from Bitcoin/Lotus SHA-family convention.
const DIFF1_TARGET_HEX: &str =
    "00000000ffff0000000000000000000000000000000000000000000000000000";

/// Parse compact submit fields and perform structural checks only.
///
/// Full network-target validation still belongs to lotusd submit path; this
/// method is for low-latency pool-side filtering.
pub fn prevalidate_submit_shape(sub: &NativeSubmit) -> anyhow::Result<()> {
    if sub.extranonce2.len() != 8 || !is_hex(&sub.extranonce2) {
        anyhow::bail!("invalid extranonce2")
    }
    if sub.ntime_hex_6b.len() != 12 || !is_hex(&sub.ntime_hex_6b) {
        anyhow::bail!("invalid ntime")
    }
    if sub.nonce_hex_8b.len() != 16 || !is_hex(&sub.nonce_hex_8b) {
        anyhow::bail!("invalid nonce")
    }
    Ok(())
}

fn is_hex(s: &str) -> bool {
    s.as_bytes().iter().all(|b| b.is_ascii_hexdigit())
}

/// Compute SHA256d quickly for helper checks / diagnostics.
pub fn sha256d(bytes: &[u8]) -> [u8; 32] {
    let h1 = Sha256::digest(bytes);
    let h2 = Sha256::digest(h1);
    h2.into()
}

/// Convert difficulty to target ratio against DIFF1.
///
/// Used for pool-side low-diff filtering. This implementation returns a
/// floating ratio suitable for fast comparisons in this phase and can be
/// replaced with full 256-bit arithmetic if needed.
pub fn difficulty_ratio_target(difficulty: f64) -> anyhow::Result<f64> {
    if difficulty <= 0.0 || !difficulty.is_finite() {
        anyhow::bail!("invalid difficulty")
    }
    let _ = DIFF1_TARGET_HEX; // pinned constant for doc/implementation parity.
    Ok(1.0 / difficulty)
}
