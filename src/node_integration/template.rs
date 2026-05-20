use crate::stratum_protocol::job::MiningJob;
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
/// Falls back to `template.block.len()` if deserialization fails.
fn compute_block_size_with_extranonce(
    template_block: &[u8],
    coinbase1: &str,
    coinbase2: &str,
) -> u64 {
    // Deserialize the template block to access the template's coinbase
    let block = match LotusBlock::deser(&mut Bytes::from_slice(template_block)) {
        Ok(b) => b,
        Err(_) => {
            tracing::warn!(
                block_len = template_block.len(),
                "compute_block_size: failed to deserialize template block, falling back to raw length",
            );
            return template_block.len() as u64;
        },
    };

    // Get Rust-serialized size of the template's coinbase
    let template_coinbase_size = match block.txs.first() {
        Some(tx) => {
            let mut buf = BytesMut::new();
            tx.ser_to(&mut buf);
            buf.freeze().len()
        }
        None => {
            tracing::warn!(
                block_len = template_block.len(),
                "compute_block_size: template block has no transactions, falling back to raw length",
            );
            return template_block.len() as u64;
        },
    };

    // Build a sample coinbase with dummy extranonce (0u64 = 8 zero bytes)
    let sample_hex = format!("{}{:016x}{}", coinbase1, 0u64, coinbase2);
    let sample_bytes = match hex::decode(&sample_hex) {
        Ok(b) => b,
        Err(e) => {
            tracing::warn!(
                error = %e,
                coinbase1_len = coinbase1.len(),
                coinbase2_len = coinbase2.len(),
                "compute_block_size: failed to hex-decode sample coinbase, falling back to raw length",
            );
            return template_block.len() as u64;
        },
    };
    let sample_tx = match Tx::deser(&mut Bytes::from_slice(&sample_bytes)) {
        Ok(tx) => tx,
        Err(_) => {
            tracing::warn!(
                sample_bytes_len = sample_bytes.len(),
                "compute_block_size: failed to deserialize sample coinbase tx, falling back to raw length",
            );
            return template_block.len() as u64;
        },
    };

    // Get Rust-serialized size of the candidate coinbase
    let candidate_coinbase_size = {
        let mut buf = BytesMut::new();
        sample_tx.ser_to(&mut buf);
        buf.freeze().len()
    };

    // Compute adjusted block size
    let delta = (candidate_coinbase_size as i64) - (template_coinbase_size as i64);
    ((template_block.len() as i64) + delta) as u64
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
pub fn template_to_job(template: &MiningTemplate, clean_jobs: bool) -> MiningJob {
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
    );

    MiningJob {
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
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bitcoinsuite_core::{Sha256d, LotusBlock, LotusHeader, Tx, BitcoinCode, Bytes, BytesMut};

    fn create_test_template() -> MiningTemplate {
        // Real-world template data (lotusd block height 1292529)
        MiningTemplate {
            template_id: 890,
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
            coinbase1: "02000000010000000000000000000000000000000000000000000000000000000000000000ffffffff1900000e2f4c6f747573696120506f6f6c2f".to_string(),
            coinbase2: "ffffffff0300000000000000000b6a056c6f676f7303f1b8137ecf360d000000001976a914ad8b796954a46f0f32a867d3fd8855043cc506ba88ac7ecf360d000000001976a914053d4d0c28d299dc5c2be1ce5d29bf00cdb61b4088ac00000000".to_string(),
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
        let job = template_to_job(&template, false);

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
        let job = template_to_job(&template, false);
        assert_eq!(job.clean_jobs, false,
            "initial template should have clean_jobs=false"
        );
    }

    #[test]
    fn test_template_to_job_clean_jobs_true() {
        let template = create_test_template();
        let job = template_to_job(&template, true);
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

        let job = template_to_job(&template, false);

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

        let job = template_to_job(&template, false);

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

        let job = template_to_job(&template, false);

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
        let coinbase_hex = format!("{}{:016x}{}", coinbase1, 0u64, coinbase2);
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

        let job = template_to_job(&template, false);

        assert_eq!(
            job.block_size, expected_rust_size,
            "template_to_job should compute block_size matching the actual Rust-serialized block size.\n\
             expected={}, got={}",
            expected_rust_size, job.block_size,
        );
    }
}
