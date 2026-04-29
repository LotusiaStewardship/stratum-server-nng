use primitive_types::U256;
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
const DIFF1_TARGET_HEX: &str = "00000000ffff0000000000000000000000000000000000000000000000000000";
const DIFF_SCALE: u128 = 100_000_000;

/// Parse compact submit fields and perform structural checks only.
///
/// Full network-target validation still belongs to lotusd submit path; this
/// method is for low-latency pool-side filtering.
pub fn prevalidate_submit_shape(sub: &NativeSubmit) -> anyhow::Result<()> {
    if sub.worker_name.is_empty() || sub.job_id.is_empty() {
        anyhow::bail!("missing worker/job")
    }
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
    Ok(1.0 / difficulty)
}

pub fn validate_submit_meets_difficulty(
    job: &crate::stratum::job::MiningJob,
    extranonce1: &str,
    sub: &NativeSubmit,
    difficulty: f64,
) -> anyhow::Result<()> {
    let header = build_header_bytes(job, extranonce1, sub)?;
    let hash = sha256d(&header);
    let mut hash_be = hash;
    hash_be.reverse();
    let hash_u256 = U256::from_big_endian(&hash_be);
    let share_target = target_for_share_difficulty(difficulty)?;
    if hash_u256 > share_target {
        anyhow::bail!("low difficulty share")
    }
    Ok(())
}

pub fn build_candidate_block(
    job: &crate::stratum::job::MiningJob,
    extranonce1: &str,
    sub: &NativeSubmit,
) -> anyhow::Result<Vec<u8>> {
    let coinbase = hex::decode(format!(
        "{}{}{}{}",
        job.coinbase1, extranonce1, sub.extranonce2, job.coinbase2
    ))?;
    let header = build_header_bytes(job, extranonce1, sub)?;

    let mut offset = job.template_header.len();
    if job.template_block.len() <= offset {
        anyhow::bail!("template block too small")
    }

    let (tx_count_len, _) = read_varint(&job.template_block[offset..])?;
    offset += tx_count_len;
    let (cb_len_len, cb_len) = read_varint(&job.template_block[offset..])?;
    let cb_start = offset + cb_len_len;
    let cb_end = cb_start + cb_len as usize;
    if cb_end > job.template_block.len() {
        anyhow::bail!("template coinbase out of range")
    }

    let mut out = Vec::with_capacity(job.template_block.len() + coinbase.len());
    out.extend_from_slice(&header);
    out.extend_from_slice(&job.template_block[job.template_header.len()..offset]);
    write_varint(&mut out, coinbase.len() as u64);
    out.extend_from_slice(&coinbase);
    out.extend_from_slice(&job.template_block[cb_end..]);
    Ok(out)
}

fn build_header_bytes(
    job: &crate::stratum::job::MiningJob,
    extranonce1: &str,
    sub: &NativeSubmit,
) -> anyhow::Result<Vec<u8>> {
    let coinbase = hex::decode(format!(
        "{}{}{}{}",
        job.coinbase1, extranonce1, sub.extranonce2, job.coinbase2
    ))?;
    let mut merkle = sha256d(&coinbase).to_vec();
    for branch_hex in &job.merkle_branches {
        let branch = hex::decode(branch_hex)?;
        let mut concat = Vec::with_capacity(64);
        concat.extend_from_slice(&merkle);
        concat.extend_from_slice(&branch);
        merkle = sha256d(&concat).to_vec();
    }

    let mut header = Vec::with_capacity(4 + 32 + 32 + 6 + 4 + 8);
    header.extend_from_slice(&hex::decode(&job.version)?);
    header.extend_from_slice(&hex::decode(&job.prevhash)?);
    header.extend_from_slice(&merkle);
    header.extend_from_slice(&hex::decode(&sub.ntime_hex_6b)?);
    header.extend_from_slice(&hex::decode(&job.nbits)?);
    header.extend_from_slice(&hex::decode(&sub.nonce_hex_8b)?);
    Ok(header)
}

fn read_varint(bytes: &[u8]) -> anyhow::Result<(usize, u64)> {
    if bytes.is_empty() {
        anyhow::bail!("missing varint")
    }
    match bytes[0] {
        n @ 0x00..=0xfc => Ok((1, n as u64)),
        0xfd => {
            if bytes.len() < 3 {
                anyhow::bail!("short varint")
            }
            Ok((3, u16::from_le_bytes([bytes[1], bytes[2]]) as u64))
        }
        0xfe => {
            if bytes.len() < 5 {
                anyhow::bail!("short varint")
            }
            Ok((
                5,
                u32::from_le_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]) as u64,
            ))
        }
        0xff => {
            if bytes.len() < 9 {
                anyhow::bail!("short varint")
            }
            Ok((
                9,
                u64::from_le_bytes([
                    bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7], bytes[8],
                ]),
            ))
        }
    }
}

fn write_varint(out: &mut Vec<u8>, n: u64) {
    if n <= 0xfc {
        out.push(n as u8);
    } else if u16::try_from(n).is_ok() {
        out.push(0xfd);
        out.extend_from_slice(&(n as u16).to_le_bytes());
    } else if u32::try_from(n).is_ok() {
        out.push(0xfe);
        out.extend_from_slice(&(n as u32).to_le_bytes());
    } else {
        out.push(0xff);
        out.extend_from_slice(&n.to_le_bytes());
    }
}

fn target_for_share_difficulty(difficulty: f64) -> anyhow::Result<U256> {
    if difficulty <= 0.0 || !difficulty.is_finite() {
        anyhow::bail!("invalid difficulty")
    }
    let scaled = (difficulty * DIFF_SCALE as f64).round();
    if scaled <= 0.0 || !scaled.is_finite() {
        anyhow::bail!("invalid difficulty scale")
    }
    let scaled_u = U256::from(scaled as u128);
    let diff1 = u256_from_hex(DIFF1_TARGET_HEX)?;
    let scaled_diff1 = diff1 * U256::from(DIFF_SCALE);
    let mut target = scaled_diff1 / scaled_u;
    if target.is_zero() {
        target = U256::one();
    }
    Ok(target)
}

fn u256_from_hex(s: &str) -> anyhow::Result<U256> {
    let raw = hex::decode(s)?;
    if raw.len() != 32 {
        anyhow::bail!("expected 32-byte hex")
    }
    Ok(U256::from_big_endian(&raw))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_submit_shape() {
        let ok = NativeSubmit {
            worker_name: "lotus_abc.r1".to_string(),
            job_id: "j1".to_string(),
            extranonce2: "00112233".to_string(),
            ntime_hex_6b: "001122334455".to_string(),
            nonce_hex_8b: "0011223344556677".to_string(),
        };
        assert!(prevalidate_submit_shape(&ok).is_ok());
    }
}
