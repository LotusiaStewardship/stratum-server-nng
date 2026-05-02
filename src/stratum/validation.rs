use bitcoinsuite_bitcoind_stratum::{
    build_stratum_header, difficulty_to_target, header_meets_difficulty,
};
use bitcoinsuite_core::{BitcoinCode, Bytes, Hashed, LotusBlock, LotusHeader, Tx};
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

/// Validate that a submitted share meets the required difficulty.
///
/// This is for low-latency pool-side filtering before submitting to lotusd.
/// Uses the bitcoinsuite `header_meets_difficulty` primitive for validation.
pub fn validate_submit_meets_difficulty(
    job: &crate::stratum::job::MiningJob,
    extranonce1: &str,
    sub: &NativeSubmit,
    difficulty: f64,
) -> anyhow::Result<()> {
    let header_bytes = build_stratum_header(
        &job.coinbase1,
        extranonce1,
        &sub.extranonce2,
        &job.coinbase2,
        &job.merkle_branches,
        &job.prevhash,
        &job.version,
        &job.nbits,
        &sub.ntime_hex_6b,
        &sub.nonce_hex_8b,
        Some(job.block_height),
        Some(&job.epoch_hash_hex),
        Some(&job.extended_metadata_hash_hex),
        Some(job.block_size),
    )
    .map_err(|e| anyhow::anyhow!("header build error: {}", e))?;

    // Compute hash and convert to big-endian for difficulty check
    let hash = LotusHeader::deser(&mut Bytes::from_slice(&header_bytes))
        .map_err(|e| anyhow::anyhow!("header deser error: {}", e))?
        .calc_hash();
    let mut hash_be = [0u8; 32];
    hash_be.copy_from_slice(hash.as_ref());
    hash_be.reverse();

    // Check if hash meets difficulty using bitcoinsuite primitive
    if !header_meets_difficulty(&hash_be, difficulty)? {
        anyhow::bail!("low-difficulty-share");
    }
    Ok(())
}

/// @deprecated unused; use build_candidate_block_with_stratum_hash instead
pub fn build_candidate_block(
    job: &crate::stratum::job::MiningJob,
    extranonce1: &str,
    sub: &NativeSubmit,
) -> anyhow::Result<Vec<u8>> {
    // Deserialize the template block to get the template-specific header fields
    let mut block = LotusBlock::deser(&mut Bytes::from_slice(&job.template_block))
        .map_err(|e| anyhow::anyhow!("block deser error: {}", e))?;

    // Build the header with the precomputed block size
    let header_bytes = build_stratum_header(
        &job.coinbase1,
        extranonce1,
        &sub.extranonce2,
        &job.coinbase2,
        &job.merkle_branches,
        &job.prevhash,
        &job.version,
        &job.nbits,
        &sub.ntime_hex_6b,
        &sub.nonce_hex_8b,
        Some(job.block_height),
        Some(&job.epoch_hash_hex),
        Some(&job.extended_metadata_hash_hex),
        Some(job.block_size),
    )
    .map_err(|e| anyhow::anyhow!("header build error: {}", e))?;

    let mut header_bytes_for_deser = Bytes::from_slice(&header_bytes);
    block.header = LotusHeader::deser(&mut header_bytes_for_deser)
        .map_err(|e| anyhow::anyhow!("header deser error: {}", e))?;

    // Rebuild coinbase with extranonce - must match exactly what build_stratum_header used
    let coinbase_hex = format!(
        "{}{}{}{}",
        job.coinbase1, extranonce1, sub.extranonce2, job.coinbase2
    );
    let coinbase_bytes = hex::decode(&coinbase_hex)?;
    let mut coinbase_buf = Bytes::from_slice(&coinbase_bytes);
    let coinbase_tx = Tx::deser(&mut coinbase_buf)?;

    if block.txs.is_empty() {
        anyhow::bail!("template block has no txs")
    }
    block.txs[0] = coinbase_tx;

    // Verify merkle root matches
    let expected_merkle_root = block.header.merkle_root.clone();
    block.update_merkle_root();
    if block.header.merkle_root != expected_merkle_root {
        anyhow::bail!(
            "merkle root mismatch: build_stratum_header computed {} but update_merkle_root computed {}",
            expected_merkle_root.to_hex_be(),
            block.header.merkle_root.to_hex_be()
        );
    }

    let final_serialization = block.ser();
    Ok(final_serialization.as_ref().to_vec())
}

/// Build a candidate block with the stratum header hash for submission.
///
/// This rebuilds the header with template values (height, epoch_hash,
/// extended_metadata_hash, size) to ensure the hash matches what lotusd
/// will compute during validation.
///
/// Returns (candidate_block_bytes, block_hash_hex, merkle_root_hex).
pub fn build_candidate_block_with_stratum_hash(
    job: &crate::stratum::job::MiningJob,
    extranonce1: &str,
    sub: &NativeSubmit,
) -> anyhow::Result<(Vec<u8>, String, String)> {
    // Deserialize template block
    let mut block = LotusBlock::deser(&mut Bytes::from_slice(&job.template_block))
        .map_err(|e| anyhow::anyhow!("block deser error: {}", e))?;

    // Build header with all template fields including the precomputed block size.
    // The block size is deterministic because the coinbase size is constant
    // regardless of extranonce values (extranonce1 + extranonce2 = fixed 8 bytes).
    let header_bytes = build_stratum_header(
        &job.coinbase1,
        extranonce1,
        &sub.extranonce2,
        &job.coinbase2,
        &job.merkle_branches,
        &job.prevhash,
        &job.version,
        &job.nbits,
        &sub.ntime_hex_6b,
        &sub.nonce_hex_8b,
        Some(job.block_height),
        Some(&job.epoch_hash_hex),
        Some(&job.extended_metadata_hash_hex),
        Some(job.block_size),
    )
    .map_err(|e| anyhow::anyhow!("header build error: {}", e))?;

    // Apply header to block
    block.header = LotusHeader::deser(&mut Bytes::from_slice(&header_bytes))
        .map_err(|e| anyhow::anyhow!("header deser error: {}", e))?;

    // Rebuild coinbase with extranonce (must match build_stratum_header)
    let coinbase_hex = format!(
        "{}{}{}{}",
        job.coinbase1, extranonce1, sub.extranonce2, job.coinbase2
    );
    let coinbase_bytes = hex::decode(&coinbase_hex)?;
    let coinbase_tx = Tx::deser(&mut Bytes::from_slice(&coinbase_bytes))?;

    if block.txs.is_empty() {
        anyhow::bail!("template block has no txs");
    }
    block.txs[0] = coinbase_tx;

    // Verify merkle root matches
    let expected_merkle_root = block.header.merkle_root.clone();
    block.update_merkle_root();
    if block.header.merkle_root != expected_merkle_root {
        anyhow::bail!(
            "merkle root mismatch: build_stratum_header computed {} but update_merkle_root computed {}",
            expected_merkle_root.to_hex_be(),
            block.header.merkle_root.to_hex_be()
        );
    }

    // Compute hash with the correct size already in the header
    let final_hash_hex = block.header.calc_hash().to_hex_be();
    let final_merkle_root_hex = block.header.merkle_root.to_hex_be();

    let serialized_block = block.ser();

    Ok((
        serialized_block.as_ref().to_vec(),
        final_hash_hex,
        final_merkle_root_hex,
    ))
}

/// Convert a difficulty to its target representation in hex (big-endian).
pub fn share_target_hex_for_difficulty(difficulty: f64) -> anyhow::Result<String> {
    let target = difficulty_to_target(difficulty)?;
    Ok(hex::encode(target))
}

/// Validate that a header hash meets the target.
///
/// Uses big-endian comparison matching lotusd conventions:
/// hash <= target for valid proof-of-work.
pub fn validate_header_meets_target_hex(header: &[u8], target_hex_be: &str) -> anyhow::Result<()> {
    let target = hex::decode(target_hex_be)?;
    if target.len() != 32 {
        anyhow::bail!("invalid target length");
    }
    let hash = LotusHeader::deser(&mut Bytes::from_slice(header))?.calc_hash();
    let mut hash_be = [0u8; 32];
    hash_be.copy_from_slice(hash.as_ref());
    hash_be.reverse();

    let hash_u256 = U256::from_big_endian(&hash_be);
    let target_u256 = U256::from_big_endian(&target);

    if hash_u256 > target_u256 {
        anyhow::bail!("high-hash");
    }
    Ok(())
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
        // mainnet block header for height 1282701
        let header = hex::decode("e0a0b8fe04e0721adcefab2286db7ee111f68702d1912bc469494f0000000000da62021cf07ef56900000000358ce6a9dea9fc97012f0100000000008d9213002b2d937594e5c99d9460a356e7677af15557e798eb7d4e04488f9c00000000004307eed513097d0c37ced90aa8a74230fb0adb62b3cda8e741c1dae0d5c64acf1406e05881e299367766d313e26c05564ec91bf721d31726bd6e46e60689539a").unwrap();
        let mut header_bytes = Bytes::from_slice(&header);
        let lotus_header = LotusHeader::deser(&mut header_bytes).unwrap();
        let mut hash = lotus_header.calc_hash().as_ref().to_vec();
        hash.reverse();
        assert_eq!(
            hex::encode(&hash),
            "0000000000ef357c7b205680b353d62f0edb442cdbb693dc0161629a269b5a6d"
        );
        // Target derived from nBits 0x1c0262da
        let target_hex = "000000000262da00000000000000000000000000000000000000000000000000";
        assert!(validate_header_meets_target_hex(&header, target_hex).is_ok());
    }
}
