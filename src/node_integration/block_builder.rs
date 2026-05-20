use anyhow::Result;
use bitcoinsuite_core::{BitcoinCode, Bytes, BytesMut, LotusBlock};
use crate::stratum_protocol::job::MiningJob;

/// Build the full block hex for submitblock from a MiningJob and miner submit params.
///
/// Reconstructs the coinbase using `coinbase1 + extranonce1 + extranonce2 + coinbase2`,
/// patches the block header with the miner's ntime and nonce, recalculates the
/// Lotus-specific merkle root, extended metadata hash, and block size.
///
/// Returns the hex-encoded serialized block ready for JSON-RPC `submitblock`.
pub fn build_submit_block(
    job: &MiningJob,
    extranonce1: &str,
    extranonce2: &str,
    ntime_hex_6b: &str,
    nonce_hex_8b: &str,
    template_block: &[u8],
) -> Result<String> {
    // Deserialize the template block
    let mut block_data = Bytes::from_slice(template_block);
    let mut block: LotusBlock = BitcoinCode::deser(&mut block_data)?;

    // Build the reconstructed coinbase transaction
    let coinbase_hex = format!(
        "{}{}{}{}",
        job.coinbase1, extranonce1, extranonce2, job.coinbase2
    );
    let coinbase_bytes = hex::decode(coinbase_hex)?;
    let mut coinbase_buf = Bytes::from_slice(&coinbase_bytes);
    let coinbase_tx: bitcoinsuite_core::Tx = BitcoinCode::deser(&mut coinbase_buf)?;

    // Replace the coinbase in the block
    block.txs[0] = coinbase_tx;

    // Decode and set the timestamp (ntime) from 6-byte little-endian hex
    let ntime_raw = hex::decode(ntime_hex_6b)?;
    if ntime_raw.len() != 6 {
        anyhow::bail!("ntime must be 6 bytes, got {}", ntime_raw.len());
    }
    let mut ntime_arr = [0u8; 8];
    ntime_arr[..6].copy_from_slice(&ntime_raw);
    block.header.timestamp = i64::from_le_bytes(ntime_arr);

    // Decode and set the nonce from 8-byte little-endian hex
    let nonce_raw = hex::decode(nonce_hex_8b)?;
    if nonce_raw.len() != 8 {
        anyhow::bail!("nonce must be 8 bytes, got {}", nonce_raw.len());
    }
    let nonce_arr: [u8; 8] = nonce_raw.as_slice().try_into()?;
    block.header.nonce = u64::from_le_bytes(nonce_arr);

    // Recalculate Lotus-specific fields
    block.update_merkle_root();
    block.update_extended_metadata_hash();
    block.update_size();

    // Serialize the block back to hex for submitblock
    let mut out = BytesMut::new();
    block.ser_to(&mut out);
    Ok(hex::encode(out.freeze()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node_integration::template::template_to_job;
    use bitcoinsuite_bitcoind_nng::MiningTemplate;
    use bitcoinsuite_core::Sha256d;

    fn test_template() -> MiningTemplate {
        // Template data from lotusd at height 1292529-ish, trimmed for testing
        MiningTemplate {
            template_id: 890,
            block: vec![],  // Will be set per test
            header: vec![],
            previous_block_hash: Sha256d::new([0u8; 32]),
            height: 1000,
            version: 1,
            bits: 486604799,
            target: Sha256d::new([0u8; 32]),
            curtime: 100,
            mintime: 0,
            maxtime: 0,
            coinbase_value: 5000000000,
            coinbase_tx: vec![],
            transactions: vec![],
            coinbase1: "02000000010000000000000000000000000000000000000000000000000000000000000000ffffffff1900000e2f4c6f747573696120506f6f6c2f".to_string(),
            coinbase2: "ffffffff0300000000000000000b6a056c6f676f7303f1b8137ecf360d000000001976a914ad8b796954a46f0f32a867d3fd8855043cc506ba88ac7ecf360d000000001976a914053d4d0c28d299dc5c2be1ce5d29bf00cdb61b4088ac00000000".to_string(),
            merkle_branches: vec![],
            prev_hash_stratum: "4f7bcee63a20eff92f69a7f0e74af36a9f1e60ee7ecc5b0506e1ae3600000000".to_string(),
            nbits_stratum: "10d0091c".to_string(),
            ntime_stratum: "6adc0c6a0000".to_string(),
        }
    }

    #[test]
    fn test_build_submit_block_rejects_empty_template() {
        let template = test_template();
        let job = template_to_job(&template, false);

        let result = build_submit_block(
            &job,
            "00000001",
            "00000002",
            "6adc0c6a0000",
            "0000000000000001",
            &template.block,
        );

        assert!(
            result.is_err(),
            "empty template block should fail deserialization"
        );
    }

    #[test]
    fn test_build_submit_block_rejects_invalid_ntime() {
        let template = test_template();
        let job = template_to_job(&template, false);

        let result = build_submit_block(
            &job,
            "00000001",
            "00000002",
            "ff", // Too short
            "0000000000000001",
            &template.block,
        );

        assert!(result.is_err(), "short ntime should fail");
    }

    #[test]
    fn test_build_submit_block_rejects_invalid_nonce() {
        let template = test_template();
        let job = template_to_job(&template, false);

        let result = build_submit_block(
            &job,
            "00000001",
            "00000002",
            "6adc0c6a0000",
            "ff", // Too short
            &template.block,
        );

        assert!(result.is_err(), "short nonce should fail");
    }
}
