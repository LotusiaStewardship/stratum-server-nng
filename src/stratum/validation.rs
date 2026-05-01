use bitcoinsuite_bitcoind_stratum::{
    build_stratum_header, difficulty_to_target, header_meets_difficulty,
};
use bitcoinsuite_core::{BitcoinCode, Bytes, LotusBlock, LotusHeader, Tx};
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

pub fn validate_submit_meets_difficulty(
    job: &crate::stratum::job::MiningJob,
    extranonce1: &str,
    sub: &NativeSubmit,
    difficulty: f64,
) -> anyhow::Result<()> {
    let header = build_stratum_header(
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
    )
    .map_err(|e| anyhow::anyhow!("header build error: {}", e))?;

    // Compute hash of the header
    let hash = LotusHeader::deser(&mut Bytes::from_slice(&header))
        .map_err(|e| anyhow::anyhow!("header deser error: {}", e))?
        .calc_hash();
    let mut hash_be = [0u8; 32];
    hash_be.copy_from_slice(hash.as_ref());
    hash_be.reverse();

    // Check if hash meets difficulty using the primitive
    header_meets_difficulty(&hash_be, difficulty)
        .map_err(|e| anyhow::anyhow!("difficulty check error: {}", e))?;
    Ok(())
}

pub fn build_candidate_block(
    job: &crate::stratum::job::MiningJob,
    extranonce1: &str,
    sub: &NativeSubmit,
) -> anyhow::Result<Vec<u8>> {
    // Deserialize the template block first to get the height, epoch_hash, and extended_metadata_hash
    let mut block = LotusBlock::deser(&mut Bytes::from_slice(&job.template_block))
        .map_err(|e| anyhow::anyhow!("block deser error: {}", e))?;
    
    // Preserve header fields that aren't provided by stratum
    let preserved_height = block.header.height;
    let preserved_epoch_hash = block.header.epoch_hash.clone();
    let preserved_extended_metadata_hash = block.header.extended_metadata_hash.clone();
    
    // Build the header using the shared primitive
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
    )
    .map_err(|e| anyhow::anyhow!("header build error: {}", e))?;

    let mut header_bytes_for_deser = Bytes::from_slice(&header_bytes);
    block.header = LotusHeader::deser(&mut header_bytes_for_deser)
        .map_err(|e| anyhow::anyhow!("header deser error: {}", e))?;
    
    // Restore preserved header fields
    block.header.height = preserved_height;
    block.header.epoch_hash = preserved_epoch_hash;
    block.header.extended_metadata_hash = preserved_extended_metadata_hash;

    // Rebuild coinbase with extranonce
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
    block.update_merkle_root();
    
    // Update the block size field in the header to match the actual serialized size.
    // The size is encoded as 7 bytes little-endian in the header, so the serialized
    // size will be constant regardless of the size value. We serialize once to get
    // the size, set it, then serialize again.
    let first_serialization = block.ser();
    block.header.size = first_serialization.len() as u64;
    let final_serialization = block.ser();
    
    Ok(final_serialization.as_ref().to_vec())
}

pub fn share_target_hex_for_difficulty(difficulty: f64) -> anyhow::Result<String> {
    let target = difficulty_to_target(difficulty)
        .map_err(|e| anyhow::anyhow!("difficulty conversion error: {}", e))?;
    Ok(hex::encode(target))
}

pub fn validate_header_meets_target_hex(header: &[u8], target_hex_be: &str) -> anyhow::Result<()> {
    let target = hex::decode(target_hex_be)?;
    if target.len() != 32 {
        anyhow::bail!("invalid target length")
    }
    let hash = LotusHeader::deser(&mut Bytes::from_slice(header))
        .map_err(|e| anyhow::anyhow!("header deser error: {}", e))?
        .calc_hash();
    let mut hash_be = [0u8; 32];
    hash_be.copy_from_slice(hash.as_ref());
    hash_be.reverse();
    let hash_u256 = U256::from_big_endian(&hash_be);
    let target_u256 = U256::from_big_endian(&target);
    if hash_u256 > target_u256 {
        anyhow::bail!("high hash")
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
        let header = hex::decode("0000000000000000000000000000000000000000000000000000000000000000ffff001d00c273600000000041c6ddd303000000010e010000000000000000000000000000000000000000000000000000000000000000000000000000000000934755d60e905ec8778f554164bd9b7f21ab6c15cfed2956123a722a6f6fa62e1406e05881e299367766d313e26c05564ec91bf721d31726bd6e46e60689539a").unwrap();
        let mut header_bytes = Bytes::from_slice(&header);
        let lotus_header = LotusHeader::deser(&mut header_bytes).unwrap();
        let mut hash = lotus_header.calc_hash().as_ref().to_vec();
        hash.reverse();
        assert_eq!(
            hex::encode(hash),
            "000000006275dc5039da85620773f3223d629759495f80b49a381d79cae77c11"
        );
        // Note: This test would need a proper target hex to test validate_header_meets_target_hex
        // assert!(validate_header_meets_target_hex(&header, "...").is_ok());
    }
}
