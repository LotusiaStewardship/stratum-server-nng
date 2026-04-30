use bitcoinsuite_core::lotus_txid;
use bitcoinsuite_core::{BitcoinCode, Bytes, Hashed, LotusBlock, LotusHeader, Sha256d, Tx};
use primitive_types::U256;

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
pub fn prevalidate_submit_shape(sub: &NativeSubmit, extranonce2_size: u8) -> anyhow::Result<()> {
    if sub.worker_name.is_empty() || sub.job_id.is_empty() {
        anyhow::bail!("missing worker/job")
    }
    let expected_extranonce2_hex_len = usize::from(extranonce2_size) * 2;
    if expected_extranonce2_hex_len == 0
        || sub.extranonce2.len() != expected_extranonce2_hex_len
        || !is_hex(&sub.extranonce2)
    {
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
    let hash = Sha256d::digest(bytes.into());
    let mut out = [0u8; 32];
    out.copy_from_slice(hash.as_ref());
    out
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
    validate_header_meets_difficulty(&header, difficulty)
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

    let mut block_bytes = Bytes::from_slice(&job.template_block);
    let mut block = LotusBlock::deser(&mut block_bytes)?;

    let mut header_bytes = Bytes::from_slice(&header);
    block.header = LotusHeader::deser(&mut header_bytes)?;

    let mut coinbase_bytes = Bytes::from_slice(&coinbase);
    let coinbase_tx = Tx::deser(&mut coinbase_bytes)?;
    if block.txs.is_empty() {
        anyhow::bail!("template block has no txs")
    }
    block.txs[0] = coinbase_tx;
    block.update_merkle_root();

    Ok(block.ser().as_ref().to_vec())
}

pub fn build_precomputed_header(
    job: &crate::stratum::job::MiningJob,
    extranonce1: &str,
    extranonce2: &str,
    ntime_hex_6b: &str,
    nonce_hex_8b: &str,
) -> anyhow::Result<Vec<u8>> {
    let sub = NativeSubmit {
        worker_name: String::new(),
        job_id: job.job_id.clone(),
        extranonce2: extranonce2.to_string(),
        ntime_hex_6b: ntime_hex_6b.to_string(),
        nonce_hex_8b: nonce_hex_8b.to_string(),
    };
    build_header_bytes(job, extranonce1, &sub)
}

pub fn share_target_hex_for_difficulty(difficulty: f64) -> anyhow::Result<String> {
    let target = target_for_share_difficulty(difficulty)?;
    let mut out = [0u8; 32];
    target.to_big_endian(&mut out);
    Ok(hex::encode(out))
}

fn header_lotus_hash_u256_be(header: &[u8]) -> anyhow::Result<U256> {
    if header.len() != 160 {
        anyhow::bail!("invalid header length")
    }
    let mut header_bytes = Bytes::from_slice(header);
    let lotus_header = LotusHeader::deser(&mut header_bytes)?;
    let hash = lotus_header.calc_hash();
    let mut hash_be = [0u8; 32];
    hash_be.copy_from_slice(hash.as_ref());
    hash_be.reverse();
    Ok(U256::from_big_endian(&hash_be))
}

pub fn validate_header_meets_difficulty(header: &[u8], difficulty: f64) -> anyhow::Result<()> {
    let hash_u256 = header_lotus_hash_u256_be(header)?;
    let share_target = target_for_share_difficulty(difficulty)?;
    if hash_u256 > share_target {
        anyhow::bail!("low difficulty share")
    }
    Ok(())
}

pub fn validate_header_meets_target_hex(header: &[u8], target_hex_be: &str) -> anyhow::Result<()> {
    let hash_u256 = header_lotus_hash_u256_be(header)?;
    let target = u256_from_hex(target_hex_be)?;
    if hash_u256 > target {
        anyhow::bail!("high hash")
    }
    Ok(())
}

fn build_header_bytes(
    job: &crate::stratum::job::MiningJob,
    extranonce1: &str,
    sub: &NativeSubmit,
) -> anyhow::Result<Vec<u8>> {
    // Decode and deserialize coinbase to Tx
    let coinbase_hex = format!(
        "{}{}{}{}",
        job.coinbase1, extranonce1, sub.extranonce2, job.coinbase2
    );
    let coinbase_bytes = hex::decode(&coinbase_hex)?;
    let mut coinbase_buf = Bytes::from_slice(&coinbase_bytes);
    let coinbase_tx = Tx::deser(&mut coinbase_buf)?;

    // Lotus merkle leaf: SHA256d(txid || lotus_txid) using bitcoinsuite primitives
    let txid = coinbase_tx.hash();
    let lotus_txid_val = lotus_txid(coinbase_tx.unhashed_tx());
    let mut merkle_leaf_raw = Vec::with_capacity(64);
    merkle_leaf_raw.extend_from_slice(txid.as_ref());
    merkle_leaf_raw.extend_from_slice(lotus_txid_val.as_ref());
    let merkle_leaf = sha256d(&merkle_leaf_raw);
    let mut merkle = merkle_leaf.to_vec();
    for branch_hex in &job.merkle_branches {
        let branch = hex::decode(branch_hex)?;
        let mut concat = Vec::with_capacity(64);
        concat.extend_from_slice(&merkle);
        concat.extend_from_slice(&branch);
        merkle = sha256d(&concat).to_vec();
    }

    let mut header_bytes = Bytes::from_slice(&job.template_header);
    let mut header = LotusHeader::deser(&mut header_bytes)?;

    let nbits_bytes: [u8; 4] = hex::decode(&job.nbits)?
        .as_slice()
        .try_into()
        .map_err(|_| anyhow::anyhow!("invalid nbits length"))?;
    let ntime_bytes: [u8; 6] = hex::decode(&sub.ntime_hex_6b)?
        .as_slice()
        .try_into()
        .map_err(|_| anyhow::anyhow!("invalid ntime length"))?;
    let nonce_bytes: [u8; 8] = hex::decode(&sub.nonce_hex_8b)?
        .as_slice()
        .try_into()
        .map_err(|_| anyhow::anyhow!("invalid nonce length"))?;
    let merkle_bytes: [u8; 32] = merkle
        .as_slice()
        .try_into()
        .map_err(|_| anyhow::anyhow!("invalid merkle length"))?;

    let mut ntime_le8 = [0u8; 8];
    ntime_le8[..6].copy_from_slice(&ntime_bytes);

    header.bits = u32::from_le_bytes(nbits_bytes);
    header.timestamp = i64::from_le_bytes(ntime_le8);
    header.nonce = u64::from_le_bytes(nonce_bytes);
    header.merkle_root = Sha256d::new(merkle_bytes);

    Ok(header.ser().as_ref().to_vec())
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
        assert!(prevalidate_submit_shape(&ok, 4).is_ok());
    }

    #[test]
    fn test_lotus_hash_160_vector() {
        let header = hex::decode("0000000000000000000000000000000000000000000000000000000000000000ffff001d00c273600000000041c6ddd303000000010e010000000000000000000000000000000000000000000000000000000000000000000000000000000000934755d60e905ec8778f554164bd9b7f21ab6c15cfed2956123a722a6f6fa62e1406e05881e299367766d313e26c05564ec91bf721d31726bd6e46e60689539a").unwrap();
        let mut header_bytes = Bytes::from_slice(&header);
        let lotus_header = LotusHeader::deser(&mut header_bytes).unwrap();
        let mut hash = lotus_header.calc_hash().as_ref().to_vec();
        hash.reverse();
        assert_eq!(
            hex::encode(hash),
            "000000006275dc5039da85620773f3223d629759495f80b49a381d79cae77c11"
        );
        assert!(validate_header_meets_difficulty(&header, 1.0).is_ok());
    }
}
