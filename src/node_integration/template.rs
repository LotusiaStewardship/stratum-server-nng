use crate::stratum_protocol::job::MiningJob;
use crate::stratum_protocol::params;
use bitcoinsuite_bitcoind_nng::MiningTemplate;
use bitcoinsuite_core::{BitcoinCode, Bytes, BytesMut, Hashed, LotusBlock, LotusHeader, Tx};

/// Compute the Rust-correct block size accounting for the extranonce delta.
///
/// Lotusd's template block has a coinbase whose scriptSig may omit the
/// extranonce bytes or use different encoding than Rust's Tx::ser_to.
/// This function measures the Rust-serialized size of both the template
/// coinbase (after re-serialization) and a candidate coinbase with 8-byte
/// dummy extranonce, then adjusts the template block length accordingly.
///
/// Returns `Err` if the template block cannot be deserialized, the coinbase
/// cannot be decoded, or the coinbase transaction cannot be parsed.
/// Any of these failures means the template is corrupt and should NOT be used
/// to mine against — block_size must be accurate for valid hashes.
fn compute_block_size_with_extranonce(
    template_block: &[u8],
    coinbase1: &str,
    coinbase2: &str,
) -> anyhow::Result<u64> {
    // Deserialize the template block to access the template's coinbase
    let block = LotusBlock::deser(&mut Bytes::from_slice(template_block))
        .map_err(|e| anyhow::anyhow!(
            "failed to deserialize template block ({} bytes): {}",
            template_block.len(), e,
        ))?;

    // Get Rust-serialized size of the template's coinbase
    let template_coinbase_size = match block.txs.first() {
        Some(tx) => {
            let mut buf = BytesMut::new();
            tx.ser_to(&mut buf);
            buf.freeze().len()
        }
        None => {
            return Err(anyhow::anyhow!(
                "template block has no transactions ({} bytes)",
                template_block.len(),
            ));
        },
    };

    // Build a sample coinbase with dummy extranonce (total_extranonce_size zero bytes)
    let dummy_extranonce = hex::encode([0u8; params::EXTRANONCE_TOTAL_SIZE as usize]);
    let sample_hex = format!("{}{}{}", coinbase1, dummy_extranonce, coinbase2);
    let sample_bytes = hex::decode(&sample_hex)
        .map_err(|e| anyhow::anyhow!(
            "failed to hex-decode sample coinbase: {} (coinbase1={}B, coinbase2={}B)",
            e, coinbase1.len(), coinbase2.len(),
        ))?;
    let sample_tx = Tx::deser(&mut Bytes::from_slice(&sample_bytes))
        .map_err(|e| anyhow::anyhow!(
            "failed to deserialize sample coinbase tx ({} bytes): {}",
            sample_bytes.len(), e,
        ))?;

    // Get Rust-serialized size of the candidate coinbase
    let candidate_coinbase_size = {
        let mut buf = BytesMut::new();
        sample_tx.ser_to(&mut buf);
        buf.freeze().len()
    };

    // Compute adjusted block size
    let delta = (candidate_coinbase_size as i64) - (template_coinbase_size as i64);
    Ok(((template_block.len() as i64) + delta) as u64)
}

/// Convert a MiningTemplate from lotusd into a MiningJob for stratum protocol.
/// 
/// The coinbase1/coinbase2 from the template are used as-is. Each miner session
/// has its own extranonce1 which the miner inserts between coinbase1 and coinbase2.
///
/// # Job ID format (per UBQ §Job)
/// `job-{template_id}-{epoch}` — e.g., `job-42-1234567890`.
/// The epoch comes from the template's `curtime` field, which is a monotonically
/// increasing counter from lotusd (not a unix timestamp despite the name).
/// On subsequent `miningwrkchg` events (Slice 6), the epoch is incremented.
/// # Arguments
///
/// * `template` - The mining template from lotusd.
/// * `clean_jobs` - Whether this job should invalidate all previous jobs.
///   Pass `true` for miningwrkchg-triggered refreshes, `false` for initial startup.
pub fn template_to_job(template: &MiningTemplate, clean_jobs: bool) -> anyhow::Result<MiningJob> {
    // Extract header fields from serialized LotusHeader bytes
    let (epoch_hash, extended_metadata_hash) = if !template.header.is_empty() {
        let mut data = Bytes::from_slice(&template.header);
        match LotusHeader::deser(&mut data) {
            Ok(header) => (
                header.epoch_hash.to_hex_be(),
                header.extended_metadata_hash.to_hex_be(),
            ),
            Err(_) => (String::new(), String::new()),
        }
    } else {
        (String::new(), String::new())
    };

    // Compute the actual Rust-serialized block size by accounting for the
    // extranonce delta between the template coinbase (C++ serialized, may not
    // include the actual extranonce bytes) and the rebuilt coinbase (Rust
    // serialized, WITH extranonce).
    //
    // Formula: block_size = template.block.len() + (new_coinbase_size - old_coinbase_size)
    // where both coinbase sizes are measured using Rust serialization.
    let block_size = compute_block_size_with_extranonce(
        &template.block,
        &template.coinbase1,
        &template.coinbase2,
    )?;

    Ok(MiningJob {
        job_id: format!("job-{}-{}", template.template_id, template.curtime),
        template_id: template.template_id,
        prevhash: template.prev_hash_stratum.clone(),
        coinbase1: template.coinbase1.clone(),
        coinbase2: template.coinbase2.clone(),
        merkle_branches: template.merkle_branches.clone(),
        version: format!("{:08x}", template.version),
        nbits: template.nbits_stratum.clone(),
        ntime: template.ntime_stratum.clone(),
        network_target_hex: template.target.to_hex_be(),
        clean_jobs,
        template_epoch: template.curtime,
        height: template.height,
        epoch_hash,
        extended_metadata_hash,
        block_size,
        block_bytes: template.block.clone(),
        coinbase_value: template.coinbase_value,
    })
}

/// Verify that a mining template's coinbase has at least one spendable (non-OP_RETURN)
/// output with non-zero value. If all outputs are OP_RETURN or zero-valued, the block
/// reward would be effectively burned.
///
/// Reconstructs the full coinbase via `coinbase1 + dummy_extranonce + coinbase2`
/// and deserializes it through `bitcoinsuite_core::Tx::deser`. This mirrors the
/// same reconstruction path used by `block_builder.rs` and the share validator,
/// ensuring outputs are parsed identically to how lotusd will see them.
pub fn verify_coinbase_outputs(template: &MiningTemplate) -> Result<(), String> {
    // Reconstruct the full coinbase with an 8-byte dummy extranonce (4B en1 + 4B en2).
    // The extranonce bytes sit in the scriptSig (coinbase input) and do not affect
    // the outputs, so any placeholder value works for output verification.
    let dummy_extranonce = hex::encode([0u8; params::EXTRANONCE_TOTAL_SIZE as usize]);
    let coinbase_hex = format!(
        "{}{}{}",
        template.coinbase1, dummy_extranonce, template.coinbase2
    );
    let coinbase_bytes = hex::decode(&coinbase_hex)
        .map_err(|e| format!("failed to hex-decode reconstructed coinbase: {}", e))?;

    let mut buf = Bytes::from_slice(&coinbase_bytes);
    let coinbase_tx = Tx::deser(&mut buf)
        .map_err(|e| format!("failed to deserialize coinbase tx: {}", e))?;

    let outputs = coinbase_tx.outputs();
    if outputs.is_empty() {
        return Err("coinbase has zero outputs — entire block reward is lost".to_string());
    }

    let total_output_value: i64 = outputs.iter().map(|o| o.value).sum();
    let spendable_count = outputs
        .iter()
        .filter(|o| o.value > 0 && !o.script.is_opreturn())
        .count();

    if spendable_count == 0 {
        Err(format!(
            "all {} output(s) ({:.8} total) are OP_RETURN or zero-valued — block reward will be BURNED",
            outputs.len(),
            total_output_value as f64 / 1e8,
        ))
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stratum_protocol::params;
    use bitcoinsuite_core::{Sha256d, LotusBlock, LotusHeader, Tx, BitcoinCode, Bytes, BytesMut};

    fn create_test_template() -> MiningTemplate {
        // Build a minimal serialized LotusBlock from the template's coinbase parts.
        // We use the same coinbase1/coinbase2 throughout the test suite and reconstruct
        // the full coinbase with a dummy extranonce (8 zero bytes) to produce a valid
        // serialized block. This mirrors what lotusd would provide at runtime.
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
            merkle_branches: vec![
                "796f6be745741765f8b19cfa4209ff68447d9e76198fee5d33fbe2c944224f16".to_string(),
                "4b0ce2ddbf0f5352b721b7688109a1e1007722f96fa07f61ea8e655ac804964f".to_string(),
                "c3899f315bc3b284015819a8d77404b4e179528d62559886babf89884966a172".to_string(),
            ],
            prev_hash_stratum: "4f7bcee63a20eff92f69a7f0e74af36a9f1e60ee7ecc5b0506e1ae3600000000".to_string(),
            nbits_stratum: "10d0091c".to_string(),
            ntime_stratum: "6adc0c6a0000".to_string(),
        }
    }

    #[test]
    fn test_template_to_job_basic_conversion() {
        let template = create_test_template();
        let job = template_to_job(&template, false).unwrap();

        // Job ID format per UBQ: job-{template_id}-{epoch}
        assert_eq!(job.job_id, "job-890-100");
        assert_eq!(job.template_id, 890);
        assert_eq!(job.prevhash, template.prev_hash_stratum);
        assert_eq!(job.coinbase1, template.coinbase1);
        assert_eq!(job.coinbase2, template.coinbase2);
        assert_eq!(job.merkle_branches, template.merkle_branches);
        assert_eq!(job.version, "00000001"); // version=1 in hex
        assert_eq!(job.nbits, template.nbits_stratum);
        assert_eq!(job.ntime, template.ntime_stratum);
        assert_eq!(job.template_epoch, template.curtime);
        assert_eq!(job.clean_jobs, false);
    }

    #[test]
    fn test_template_to_job_clean_jobs_false_explicit() {
        let template = create_test_template();
        let job = template_to_job(&template, false).unwrap();
        assert_eq!(job.clean_jobs, false,
            "initial template should have clean_jobs=false"
        );
    }

    #[test]
    fn test_template_to_job_clean_jobs_true() {
        let template = create_test_template();
        let job = template_to_job(&template, true).unwrap();
        assert_eq!(job.clean_jobs, true,
            "miningwrkchg-triggered job should have clean_jobs=true"
        );
    }

    #[test]
    fn test_template_to_job_network_target() {
        let mut template = create_test_template();
        // Set raw Sha256d bytes (LE internal storage).
        // Sha256d stores bytes LSB-first. These 32 bytes are:
        //   [0x00,0x00,0x00,0x00, 0xFF,...,0xFF]
        // The U256 value represented is 0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF00000000
        // (28 most-significant bytes = 0xFF, 4 least-significant bytes = 0x00).
        // In big-endian hex (to_hex_be) this is:
        //   ffffffffffffffffffffffffffffffffffffffffffffffffffffffff00000000
        template.target = Sha256d::new([
            0x00, 0x00, 0x00, 0x00, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
            0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
            0xFF, 0xFF, 0xFF, 0xFF,
        ]);

        let job = template_to_job(&template, false).unwrap();

        assert_eq!(
            job.network_target_hex,
            "ffffffffffffffffffffffffffffffffffffffffffffffffffffffff00000000"
        );
    }

    #[test]
    fn test_template_to_job_multiple_merkle_branches() {
        let mut template = create_test_template();
        template.merkle_branches = vec![
            "branch1".to_string(),
            "branch2".to_string(),
            "branch3".to_string(),
        ];

        let job = template_to_job(&template, false).unwrap();

        assert_eq!(job.merkle_branches.len(), 3);
        assert_eq!(job.merkle_branches[0], "branch1");
        assert_eq!(job.merkle_branches[1], "branch2");
        assert_eq!(job.merkle_branches[2], "branch3");
    }

    #[test]
    fn test_template_to_job_populates_header_fields() {
        let mut template = create_test_template();
        template.height = 1292529;

        // Build a LotusHeader matching real block data at height 1292529
        let header = LotusHeader {
            version: 1,
            prev_block: Sha256d::new([0u8; 32]),
            bits: 0x1c09d010,
            timestamp: 1779227754,
            nonce: 13573272464251480634,
            size: 2588,
            height: 1292529,
            epoch_hash: Sha256d::from_hex_be(
                "00000000061fb84d2a1d30d8767f629a08904b0e70f84587008fd9e91f1583f7",
            )
            .unwrap(),
            extended_metadata_hash: Sha256d::from_hex_be(
                "9a538906e6466ebd2617d321f71bc94e56056ce213d366773699e28158e00614",
            )
            .unwrap(),
            ..Default::default()
        };

        let mut buf = BytesMut::new();
        header.ser_to(&mut buf);
        template.header = buf.as_slice().to_vec();

        let job = template_to_job(&template, false).unwrap();

        assert_eq!(job.height, 1292529);
        assert_eq!(
            job.epoch_hash,
            "00000000061fb84d2a1d30d8767f629a08904b0e70f84587008fd9e91f1583f7"
        );
        assert_eq!(
            job.extended_metadata_hash,
            "9a538906e6466ebd2617d321f71bc94e56056ce213d366773699e28158e00614"
        );
    }

    /// Helper: construct a MiningTemplate from a serialized LotusBlock and matching parts.
    /// Used by block_size tests to simulate lotusd's template output.
    fn template_from_block(
        block: &LotusBlock,
        coinbase1: &str,
        coinbase2: &str,
        merkle_branches: Vec<String>,
        prev_hash_stratum: &str,
        nbits_stratum: &str,
        ntime_stratum: &str,
    ) -> MiningTemplate {
        let block_bytes = block.ser().to_vec();
        let mut header_bytes = {
            let mut buf = BytesMut::new();
            block.header.ser_to(&mut buf);
            buf.freeze().to_vec()
        };
        // Set header.size to the ACTUAL block bytes length (simulating C++ header)
        // so that template_to_job can compute the correct Rust-correct size
        // by accounting for the extranonce delta.
        {
            // Re-deserialize header from bytes, update size, re-serialize
            let mut data = Bytes::from_slice(&header_bytes);
            let mut hdr: LotusHeader = BitcoinCode::deser(&mut data).unwrap();
            hdr.size = block_bytes.len() as u64;
            let mut buf = BytesMut::new();
            hdr.ser_to(&mut buf);
            header_bytes = buf.freeze().to_vec();
        }

        MiningTemplate {
            template_id: 42,
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
            merkle_branches,
            prev_hash_stratum: prev_hash_stratum.to_string(),
            nbits_stratum: nbits_stratum.to_string(),
            ntime_stratum: ntime_stratum.to_string(),
        }
    }

    #[test]
    fn test_block_size_matches_rust_serialized_size() {
        // Verify that template_to_job computes a block_size that matches the
        // actual Rust-serialized block size after coinbase replacement.
        // This accounts for the extranonce bytes that lotusd includes as a
        // placeholder in the template block but the stratum server replaces
        // with the session's extranonce.

        // Build a coinbase WITH extranonce placeholder (8 zero bytes).
        // This simulates a lotusd template block that DOES include the extranonce
        // placeholder, which is the standard behavior.
        let coinbase1 = "0200000001000000000000000000000000000000000000000000000000000000000000000000ffffffff0a0000";
        let coinbase2 = "ffffffff0300000000000000000b6a056c6f676f7303f1b8137ecf360d000000001976a914ad8b796954a46f0f32a867d3fd8855043cc506ba88ac7ecf360d000000001976a914053d4d0c28d299dc5c2be1ce5d29bf00cdb61b4088ac00000000";

        // Build the coinbase WITH 8-byte placeholder extranonce (as lotusd would)
        let dummy_extranonce = hex::encode([0u8; params::EXTRANONCE_TOTAL_SIZE as usize]);
        let coinbase_hex = format!("{}{}{}", coinbase1, dummy_extranonce, coinbase2);
        let coinbase_bytes = hex::decode(&coinbase_hex).unwrap();
        let coinbase_tx: Tx = BitcoinCode::deser(&mut Bytes::from_slice(&coinbase_bytes)).unwrap();

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

        // Serialize the Rust block — this is the expected size
        let expected_rust_size = block.ser().len() as u64;

        let template = template_from_block(
            &block,
            coinbase1,
            coinbase2,
            vec![],
            "4f7bcee63a20eff92f69a7f0e74af36a9f1e60ee7ecc5b0506e1ae3600000000",
            "10d0091c",
            "6adc0c6a0000",
        );

        let job = template_to_job(&template, false).unwrap();

        assert_eq!(
            job.block_size, expected_rust_size,
            "template_to_job should compute block_size matching the actual Rust-serialized block size.\n\
             expected={}, got={}",
            expected_rust_size, job.block_size,
        );
    }

    // -------------------------------------------------------------------------
    // template_to_job error propagation tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_template_to_job_fails_on_empty_block() {
        // If the template block is empty (corrupt), template_to_job must return Err
        // so the operator sees a hard stop, not a silent fallback.
        let mut template = create_test_template();
        template.block = vec![];  // empty — LotusBlock::deser will fail

        let err = template_to_job(&template, false).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("failed to deserialize template block"),
            "error should mention block deserialization failure, got: {}",
            msg,
        );
    }

    #[test]
    fn test_template_to_job_fails_on_empty_txs() {
        // If the template block has no transactions, template_to_job must return Err.
        // We construct a valid LotusBlock with an empty txs vec to trigger the
        // "no transactions" guard in compute_block_size_with_extranonce.
        let empty_tx_block = LotusBlock {
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
                ).unwrap(),
                merkle_root: Sha256d::new([0u8; 32]),
                extended_metadata_hash: Sha256d::new([0u8; 32]),
            },
            metadata: vec![],
            txs: vec![],  // no coinbase
        };
        let block_bytes = {
            let mut buf = BytesMut::new();
            empty_tx_block.ser_to(&mut buf);
            buf.freeze().to_vec()
        };

        let mut template = create_test_template();
        template.block = block_bytes;

        let err = template_to_job(&template, false).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("no transactions"),
            "error should mention no transactions, got: {}",
            msg,
        );
    }

    // -------------------------------------------------------------------------
    // verify_coinbase_outputs tests
    // -------------------------------------------------------------------------

    /// Helper: build a MiningTemplate with a specific coinbase2 for output testing.
    fn template_with_coinbase2(coinbase2: &str) -> MiningTemplate {
        let coinbase1 = "02000000010000000000000000000000000000000000000000000000000000000000000000ffffffff1900000e2f4c6f747573696120506f6f6c2f";
        MiningTemplate {
            template_id: 999,
            block: vec![],
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
            coinbase1: coinbase1.to_string(),
            coinbase2: coinbase2.to_string(),
            merkle_branches: vec![],
            prev_hash_stratum: String::new(),
            nbits_stratum: String::new(),
            ntime_stratum: String::new(),
        }
    }

    #[test]
    fn test_verify_coinbase_outputs_p2pkh_passes() {
        // coinbase2 with: sequence(ffffffff), 3 outputs (P2PKH+P2PKH+OP_RETURN)
        // Real-world values from lotusd template
        let coinbase2 = "ffffffff0300000000000000000b6a056c6f676f7303f1b8137ecf360d000000001976a914ad8b796954a46f0f32a867d3fd8855043cc506ba88ac7ecf360d000000001976a914053d4d0c28d299dc5c2be1ce5d29bf00cdb61b4088ac00000000";
        let template = template_with_coinbase2(coinbase2);
        assert!(verify_coinbase_outputs(&template).is_ok(),
            "template with P2PKH outputs should pass");
    }

    #[test]
    fn test_verify_coinbase_outputs_op_return_only_fails() {
        // coinbase2 with: sequence(ffffffff), 1 OP_RETURN output (value=0), locktime(0)
        // Script: OP_RETURN(6a) push5(05) "logos"(6c6f676f73) = 7 bytes
        // This simulates the burn scenario when coinbase_script=None
        let coinbase2 = "ffffffff010000000000000000076a056c6f676f7300000000";
        let template = template_with_coinbase2(coinbase2);
        let err = verify_coinbase_outputs(&template).unwrap_err();
        assert!(err.contains("BURNED"),
            "OP_RETURN-only template should report BURNED, got: {}", err);
    }

    #[test]
    fn test_verify_coinbase_outputs_zero_outputs_fails() {
        // coinbase2 with: sequence(ffffffff), zero outputs, locktime(0)
        let coinbase2 = "ffffffff0000000000";
        let template = template_with_coinbase2(coinbase2);
        let err = verify_coinbase_outputs(&template).unwrap_err();
        assert!(err.contains("empty") || err.contains("zero"),
            "zero-output template should report error, got: {}", err);
    }

    #[test]
    fn test_verify_coinbase_outputs_empty_fails() {
        let template = template_with_coinbase2("");
        assert!(verify_coinbase_outputs(&template).is_err());
    }

    #[test]
    fn test_verify_coinbase_outputs_mixed_passes() {
        // coinbase2 with: sequence(ffffffff) + 2 outputs + locktime(0):
        //   Output 1: OP_RETURN "logos" (value=0, script=6a056c6f676f73, 7 bytes)
        //   Output 2: P2PKH (value=221695870, script=76a914...88ac, 25 bytes)
        let coinbase2 = "ffffffff020000000000000000076a056c6f676f737ecf360d000000001976a914ad8b796954a46f0f32a867d3fd8855043cc506ba88ac00000000";
        let template = template_with_coinbase2(coinbase2);
        assert!(verify_coinbase_outputs(&template).is_ok(),
            "template with at least one P2PKH output should pass");
    }
}
