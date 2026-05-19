use crate::stratum_protocol::job::MiningJob;
use bitcoinsuite_bitcoind_nng::MiningTemplate;
use bitcoinsuite_core::Hashed;

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
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bitcoinsuite_core::Sha256d;

    fn create_test_template() -> MiningTemplate {
        MiningTemplate {
            template_id: 42,
            block: vec![],
            header: vec![],
            previous_block_hash: Sha256d::new([0u8; 32]),
            height: 1000,
            version: 536870912,
            bits: 486604799,
            target: Sha256d::new([0u8; 32]),
            curtime: 1234567890,
            mintime: 1234567800,
            maxtime: 1234567900,
            coinbase_value: 5000000000,
            coinbase_tx: vec![],
            transactions: vec![],
            coinbase1: "01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff".to_string(),
            coinbase2: "ffffffff0200f2052a010000001976a914000000000000000000000000000000000000000088ac0000000000000000266a24aa21a9ed00000000000000000000000000000000000000000000000000000000000000000000000000000000".to_string(),
            merkle_branches: vec![
                "aa21a9ed0000000000000000000000000000000000000000000000000000000000000000".to_string(),
            ],
            prev_hash_stratum: "0000000000000000000000000000000000000000000000000000000000000000".to_string(),
            nbits_stratum: "1d00ffff".to_string(),
            ntime_stratum: "5f5f5f5f".to_string(),
        }
    }

    #[test]
    fn test_template_to_job_basic_conversion() {
        let template = create_test_template();
        let job = template_to_job(&template);

        // Job ID format per UBQ: job-{template_id}-{epoch}
        assert_eq!(job.job_id, "job-42-1234567890");
        assert_eq!(job.template_id, 42);
        assert_eq!(job.prevhash, template.prev_hash_stratum);
        assert_eq!(job.coinbase1, template.coinbase1);
        assert_eq!(job.coinbase2, template.coinbase2);
        assert_eq!(job.merkle_branches, template.merkle_branches);
        assert_eq!(job.version, "20000000"); // 536870912 in hex
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
}
