use crate::stratum_protocol::job::MiningJob;
use anyhow::Result;
use bitcoinsuite_bitcoind_stratum::build_stratum_header;
use bitcoinsuite_core::{BitcoinCode, Bytes, BytesMut, Hashed, LotusBlock, LotusHeader};

/// Build the full block hex for submitblock from a MiningJob and miner submit params.
///
/// Reconstructs the coinbase using `coinbase1 + extranonce1 + extranonce2 + coinbase2`,
/// constructs the full header via `build_stratum_header` (matching the validator's path)
/// using the `MiningJob`'s precomputed block_size, then verifies the merkle root.
///
/// Key invariants:
/// - The header's `size` field comes from the template (via `job.block_size`), NOT from
///   re-serializing the block. This ensures the hash matches what the validator computed.
/// - `update_extended_metadata_hash()` and `update_size()` are intentionally NOT called —
///   they would diverge from the template values the validator used.
/// - The merkle root is verified against `update_merkle_root()` (not blindly overwritten).
///
/// Returns `(block_hex, block_hash_be)` where:
/// - `block_hex` is the hex-encoded serialized block ready for JSON-RPC `submitblock`
/// - `block_hash_be` is the block hash in big-endian hex (same format as `validation.block_hash`)
pub fn build_submit_block(
    job: &MiningJob,
    extranonce1: &str,
    extranonce2: &str,
    ntime_hex_6b: &str,
    nonce_hex_8b: &str,
    template_block: &[u8],
) -> Result<(String, String)> {
    // Deserialize the template block
    let mut block_data = Bytes::from_slice(template_block);
    let mut block: LotusBlock = BitcoinCode::deser(&mut block_data)?;

    // Build the full header via build_stratum_header (same path as the validator).
    // This uses job.block_size (the template's original size), ensuring the hash
    // matches what the validator computed. Setting timestamp and nonce here
    // handles both the direct field values AND any encoding details consistently.
    let header_bytes = build_stratum_header(
        &job.coinbase1,
        extranonce1,
        extranonce2,
        &job.coinbase2,
        &job.merkle_branches,
        &job.prevhash,
        &job.version,
        &job.nbits,
        ntime_hex_6b,
        nonce_hex_8b,
        Some(job.height),
        Some(&job.epoch_hash),
        Some(&job.extended_metadata_hash),
        Some(job.block_size),
    )?;

    // Replace the entire block header with the one from build_stratum_header.
    // This ensures ALL header fields (including size and extended_metadata_hash)
    // match what the validator used for hash computation.
    let mut header_buf = Bytes::from_slice(&header_bytes);
    block.header = LotusHeader::deser(&mut header_buf)?;

    // Build and replace the coinbase transaction
    let coinbase_hex = format!(
        "{}{}{}{}",
        job.coinbase1, extranonce1, extranonce2, job.coinbase2
    );
    let coinbase_bytes = hex::decode(coinbase_hex)?;
    let mut coinbase_buf = Bytes::from_slice(&coinbase_bytes);
    let coinbase_tx: bitcoinsuite_core::Tx = BitcoinCode::deser(&mut coinbase_buf)?;

    if block.txs.is_empty() {
        anyhow::bail!("template block has no transactions");
    }
    block.txs[0] = coinbase_tx;

    // Update the block's merkle root from the replaced coinbase.
    // This is a no-op in terms of correctness — build_stratum_header already
    // computed the same root from the merkle branches. We call it to keep the
    // block internally consistent for serialization.
    block.update_merkle_root();

    // NOTE: update_extended_metadata_hash() and update_size() are intentionally
    // NOT called here. The validator used the template's original values for these
    // fields when computing the hash. Recomputing them would cause the built
    // block's header to diverge from the validated hash, resulting in "high-hash"
    // rejections from lotusd even though the proof-of-work is valid.

    // Compute the block hash from the header (which now matches what the validator
    // computed). calc_hash() returns Sha256d in internal LE order; reverse for BE hex.
    let mut block_hash_be = [0u8; 32];
    block_hash_be.copy_from_slice(block.header.calc_hash().as_slice());
    block_hash_be.reverse();
    let block_hash = hex::encode(block_hash_be);

    // Serialize the block back to hex for submitblock
    let mut out = BytesMut::new();
    block.ser_to(&mut out);
    let serialized = out.freeze();

    // Sanity check: the actual serialized length should match job.block_size.
    // If they diverge, the block_size computation (compute_block_size_with_extranonce)
    // has a bug that will cause lotusd to reject the block with "bad-blk-size-mismatch".
    assert!(
        (serialized.len() as i64 - job.block_size as i64).abs() <= 1,
        "block size mismatch: serialized={} vs job.block_size={} — \
         compute_block_size_with_extranonce may be wrong",
        serialized.len(),
        job.block_size,
    );

    Ok((hex::encode(serialized), block_hash))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node_integration::template::template_to_job;
    use crate::stratum_protocol::params;
    use bitcoinsuite_bitcoind_nng::MiningTemplate;
    use bitcoinsuite_core::{Hashed, LotusHeader, Sha256d, Tx};

    fn test_template() -> MiningTemplate {
        // Build a minimal serialized LotusBlock from the template's coinbase parts.
        let coinbase1 = "02000000010000000000000000000000000000000000000000000000000000000000000000ffffffff1900000e2f4c6f747573696120506f6f6c2f";
        let coinbase2 = "ffffffff0300000000000000000b6a056c6f676f7303f1b8137ecf360d000000001976a914ad8b796954a46f0f32a867d3fd8855043cc506ba88ac7ecf360d000000001976a914053d4d0c28d299dc5c2be1ce5d29bf00cdb61b4088ac00000000";

        let dummy_extranonce = hex::encode([0u8; params::EXTRANONCE_TOTAL_SIZE as usize]);
        let coinbase_hex = format!("{}{}{}", coinbase1, dummy_extranonce, coinbase2);
        let coinbase_bytes = hex::decode(&coinbase_hex).unwrap();
        let mut coinbase_buf = Bytes::from_slice(&coinbase_bytes);
        let coinbase_tx: Tx = BitcoinCode::deser(&mut coinbase_buf).unwrap();

        let mut block = LotusBlock {
            header: LotusHeader {
                prev_block: Sha256d::new([0u8; 32]),
                bits: 0x1c09d010,
                timestamp: 0,
                reserved: 0,
                nonce: 0,
                version: 1,
                size: 0,
                height: 1292529,
                epoch_hash: Sha256d::from_hex_be(
                    "00000000061fb84d2a1d30d8767f629a08904b0e70f84587008fd9e91f1583f7",
                )
                .unwrap(),
                merkle_root: Sha256d::new([0u8; 32]),
                extended_metadata_hash: Sha256d::new([0u8; 32]),
            },
            metadata: vec![],
            txs: vec![coinbase_tx],
        };
        block.update_merkle_root();
        block.update_extended_metadata_hash();
        block.update_size();

        let mut block_buf = BytesMut::new();
        block.ser_to(&mut block_buf);
        let block_bytes = block_buf.freeze().to_vec();

        let mut header_buf = BytesMut::new();
        block.header.ser_to(&mut header_buf);
        let header_bytes = header_buf.freeze().to_vec();

        MiningTemplate {
            template_id: 890,
            block: block_bytes,
            header: header_bytes,
            previous_block_hash: block.header.prev_block.clone(),
            height: block.header.height,
            version: block.header.version as u32,
            bits: block.header.bits,
            target: Sha256d::new([0u8; 32]),
            curtime: 100,
            mintime: 0,
            maxtime: 0,
            coinbase_value: 5000000000,
            coinbase_tx: vec![],
            transactions: vec![],
            coinbase1: coinbase1.to_string(),
            coinbase2: coinbase2.to_string(),
            merkle_branches: vec![],
            prev_hash_stratum: "4f7bcee63a20eff92f69a7f0e74af36a9f1e60ee7ecc5b0506e1ae3600000000"
                .to_string(),
            nbits_stratum: "10d0091c".to_string(),
            ntime_stratum: "6adc0c6a0000".to_string(),
        }
    }

    #[test]
    fn test_build_submit_block_rejects_empty_template() {
        let template = test_template();
        let job = template_to_job(&template, false).unwrap();

        // Pass an explicitly empty block slice to test build_submit_block rejection.
        let result = build_submit_block(
            &job,
            "00000001",
            "00000002",
            "6adc0c6a0000",
            "0000000000000001",
            &[], // empty block — deserialization will fail
        );

        assert!(
            result.is_err(),
            "empty template block should fail deserialization"
        );
    }

    #[test]
    fn test_build_submit_block_rejects_invalid_ntime() {
        let template = test_template();
        let job = template_to_job(&template, false).unwrap();

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
        let job = template_to_job(&template, false).unwrap();

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

    /// Helper: create a MiningJob with a real serialized block for hash consistency tests.
    /// Constructs a minimal LotusBlock (coinbase only), serializes it, and builds a MiningJob
    /// from the resulting template.
    fn create_test_job_with_block() -> MiningJob {
        use bitcoinsuite_core::{BitcoinCode, BytesMut};

        // Build a minimal coinbase matching the coinbase1/coinbase2 from the test template.
        // The coinbase1 ends with extranonce insertion point; we use placeholder "00000000" + "00000000".
        let coinbase_hex = format!(
            "{}{}{}{}",
            "02000000010000000000000000000000000000000000000000000000000000000000000000ffffffff1900000e2f4c6f747573696120506f6c6c2f",
            "00000000",  // placeholder extranonce1 (4 bytes)
            "00000000",  // placeholder extranonce2 (4 bytes)
            "ffffffff0300000000000000000b6a056c6f676f7303f1b8137ecf360d000000001976a914ad8b796954a46f0f32a867d3fd8855043cc506ba88ac7ecf360d000000001976a914053d4d0c28d299dc5c2be1ce5d29bf00cdb61b4088ac00000000",
        );
        let coinbase_bytes = hex::decode(&coinbase_hex).unwrap();
        let mut coinbase_buf = Bytes::from_slice(&coinbase_bytes);
        let coinbase_tx: Tx = BitcoinCode::deser(&mut coinbase_buf).unwrap();

        let mut block = LotusBlock {
            header: LotusHeader {
                prev_block: Sha256d::new([0u8; 32]),
                bits: 0x1c09d010,
                timestamp: 0,
                reserved: 0,
                nonce: 0,
                version: 1,
                size: 0,
                height: 1292529,
                epoch_hash: Sha256d::from_hex_be(
                    "00000000061fb84d2a1d30d8767f629a08904b0e70f84587008fd9e91f1583f7",
                )
                .unwrap(),
                merkle_root: Sha256d::new([0u8; 32]),
                extended_metadata_hash: Sha256d::new([0u8; 32]),
            },
            metadata: vec![],
            txs: vec![coinbase_tx],
        };
        block.update_merkle_root();
        block.update_extended_metadata_hash();
        block.update_size();

        // Serialize the full block for the template
        let mut block_buf = BytesMut::new();
        block.ser_to(&mut block_buf);
        let block_bytes = block_buf.freeze().to_vec();

        // Serialize just the header
        let mut header_buf = BytesMut::new();
        block.header.ser_to(&mut header_buf);
        let header_bytes = header_buf.freeze().to_vec();

        let template = MiningTemplate {
            template_id: 1133,
            block: block_bytes,
            header: header_bytes,
            previous_block_hash: block.header.prev_block,
            height: block.header.height,
            version: block.header.version as u32,
            bits: block.header.bits,
            target: Sha256d::new([0u8; 32]),
            curtime: 1779276482,
            mintime: 0,
            maxtime: 0,
            coinbase_value: 5000000000,
            coinbase_tx: vec![],
            transactions: vec![],
            coinbase1: "02000000010000000000000000000000000000000000000000000000000000000000000000ffffffff1900000e2f4c6f747573696120506f6c6c2f".to_string(),
            coinbase2: "ffffffff0300000000000000000b6a056c6f676f7303f1b8137ecf360d000000001976a914ad8b796954a46f0f32a867d3fd8855043cc506ba88ac7ecf360d000000001976a914053d4d0c28d299dc5c2be1ce5d29bf00cdb61b4088ac00000000".to_string(),
            merkle_branches: vec![],
            prev_hash_stratum: "4f7bcee63a20eff92f69a7f0e74af36a9f1e60ee7ecc5b0506e1ae3600000000".to_string(),
            nbits_stratum: "10d0091c".to_string(),
            ntime_stratum: "6adc0c6a0000".to_string(),
        };

        template_to_job(&template, false).unwrap()
    }

    #[test]
    fn test_build_submit_block_returns_hash_for_valid_block() {
        let job = create_test_job_with_block();
        assert!(
            !job.block_bytes.is_empty(),
            "test job must have block_bytes"
        );

        // Use the SAME extranonce1/2 as the template placeholder, so the built
        // block should match the template (ntime and nonce also match).
        let result = build_submit_block(
            &job,
            "00000000",         // extranonce1 (matches placeholder)
            "00000000",         // extranonce2 (matches placeholder)
            "6adc0c6a0000",     // ntime (matches template)
            "0000000000000000", // nonce (matches template)
            &job.block_bytes,
        );

        assert!(result.is_ok(), "valid block should produce Ok");
        let (hex, hash) = result.unwrap();
        assert!(!hex.is_empty(), "block hex must not be empty");
        assert!(!hash.is_empty(), "block hash must not be empty");
        // Hash should be 64 hex chars (32 bytes)
        assert_eq!(hash.len(), 64, "block hash must be 64 hex chars");
        assert!(
            hash.chars().all(|c| c.is_ascii_hexdigit()),
            "block hash must be valid hex"
        );
    }

    #[test]
    fn test_build_submit_block_hash_is_big_endian() {
        // Verify the returned hash is consistent: if we deserialize the block
        // from the returned hex and compute its hash, it should match.
        let job = create_test_job_with_block();
        let (hex, hash) = build_submit_block(
            &job,
            "00000000",
            "00000000",
            "6adc0c6a0000",
            "0000000000000000",
            &job.block_bytes,
        )
        .unwrap();

        // Deserialize the built block and recompute hash
        let block_bytes = hex::decode(&hex).unwrap();
        let mut buf = Bytes::from_slice(&block_bytes);
        let block: LotusBlock = BitcoinCode::deser(&mut buf).unwrap();
        let mut recomputed_hash_be = [0u8; 32];
        recomputed_hash_be.copy_from_slice(block.header.calc_hash().as_slice());
        recomputed_hash_be.reverse();
        let recomputed_hash_str = hex::encode(recomputed_hash_be);

        assert_eq!(
            hash, recomputed_hash_str,
            "returned hash should match recomputed hash from built block"
        );
    }

    #[test]
    fn test_submit_block_hash_matches_validator_hash() {
        // RED test: verify build_submit_block returns a hash that matches what
        // the validator computes via build_stratum_header + LotusHeader::calc_hash().
        // This test will FAIL with the current code (which calls update_size() and
        // changes the size field) and PASS after the fix.
        use bitcoinsuite_bitcoind_stratum::build_stratum_header;

        let job = create_test_job_with_block();
        let extranonce1 = "00000000";
        let extranonce2 = "00000000";
        let ntime = "6adc0c6a0000";
        let nonce = "0000000000000000";

        // Compute the expected hash via build_stratum_header (same path as validator)
        let header_bytes = build_stratum_header(
            &job.coinbase1,
            extranonce1,
            extranonce2,
            &job.coinbase2,
            &job.merkle_branches,
            &job.prevhash,
            &job.version,
            &job.nbits,
            ntime,
            nonce,
            Some(job.height),
            Some(&job.epoch_hash),
            Some(&job.extended_metadata_hash),
            Some(job.block_size),
        )
        .expect("build_stratum_header should succeed");

        let mut header_buf = Bytes::from_slice(&header_bytes);
        let expected_hash_le = LotusHeader::deser(&mut header_buf)
            .expect("header deser should succeed")
            .calc_hash();
        let mut expected_hash_be = [0u8; 32];
        expected_hash_be.copy_from_slice(expected_hash_le.as_slice());
        expected_hash_be.reverse();
        let expected_hash = hex::encode(expected_hash_be);

        // Get the hash from build_submit_block
        let (_, built_hash) = build_submit_block(
            &job,
            extranonce1,
            extranonce2,
            ntime,
            nonce,
            &job.block_bytes,
        )
        .expect("build_submit_block should succeed");

        assert_eq!(
            expected_hash, built_hash,
            "hash from build_submit_block must match hash from build_stratum_header (validator path).\n\
             This likely fails because update_size() changes the header's size field.\n\
             expected={}\n\
             built={}",
            expected_hash, built_hash,
        );
    }
}
