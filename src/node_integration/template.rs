use crate::stratum_protocol::job::MiningJob;
use bitcoinsuite_bitcoind_nng::MiningTemplate;
use bitcoinsuite_core::{BitcoinCode, Bytes, Hashed, LotusHeader};

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
pub fn template_to_job(template: &MiningTemplate) -> MiningJob {
    // Extract header fields from serialized LotusHeader bytes
    let (epoch_hash, extended_metadata_hash, block_size) = if !template.header.is_empty() {
        let mut data = Bytes::from_slice(&template.header);
        match LotusHeader::deser(&mut data) {
            Ok(header) => (
                header.epoch_hash.to_hex_be(),
                header.extended_metadata_hash.to_hex_be(),
                header.size,
            ),
            Err(_) => (String::new(), String::new(), 0),
        }
    } else {
        (String::new(), String::new(), 0)
    };

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
        network_target_hex: hex::encode(template.target.as_slice()),
        clean_jobs: false,
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
    use bitcoinsuite_core::Sha256d;

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
        let job = template_to_job(&template);

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
    fn test_template_to_job_network_target() {
        let mut template = create_test_template();
        // Set a specific target: 0x00000000FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF
        template.target = Sha256d::new([
            0x00, 0x00, 0x00, 0x00, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
            0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
            0xFF, 0xFF, 0xFF, 0xFF,
        ]);

        let job = template_to_job(&template);

        assert_eq!(
            job.network_target_hex,
            "00000000ffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
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

        let job = template_to_job(&template);

        assert_eq!(job.merkle_branches.len(), 3);
        assert_eq!(job.merkle_branches[0], "branch1");
        assert_eq!(job.merkle_branches[1], "branch2");
        assert_eq!(job.merkle_branches[2], "branch3");
    }

    #[test]
    fn test_template_to_job_populates_header_fields() {
        use bitcoinsuite_core::{BytesMut, LotusHeader, BitcoinCode};

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

        let job = template_to_job(&template);

        assert_eq!(job.height, 1292529);
        assert_eq!(
            job.epoch_hash,
            "00000000061fb84d2a1d30d8767f629a08904b0e70f84587008fd9e91f1583f7"
        );
        assert_eq!(
            job.extended_metadata_hash,
            "9a538906e6466ebd2617d321f71bc94e56056ce213d366773699e28158e00614"
        );
        assert_eq!(job.block_size, 2588);
    }
}
