use serde_json::json;

/// Parse template_id and template_epoch from a `job-{template_id}-{epoch}` format job ID.
/// Returns `None` if the format doesn't match.
pub fn parse_template_metadata_from_job_id(job_id: &str) -> Option<(i64, i64)> {
    let stripped = job_id.strip_prefix("job-")?;
    let last_hyphen = stripped.rfind('-')?;
    let template_id: i64 = stripped[..last_hyphen].parse().ok()?;
    let template_epoch: i64 = stripped[last_hyphen + 1..].parse().ok()?;
    Some((template_id, template_epoch))
}

#[derive(Debug, Clone)]
pub struct MiningJob {
    pub job_id: String,
    pub template_id: u64,
    pub prevhash: String,
    pub coinbase1: String,
    pub coinbase2: String,
    pub merkle_branches: Vec<String>,
    pub version: String,
    pub nbits: String,
    pub ntime: String,
    pub network_target_hex: String,
    pub clean_jobs: bool,
    pub template_epoch: u64,
    pub height: i32,
    pub epoch_hash: String,
    pub extended_metadata_hash: String,
    pub block_size: u64,
    /// Full serialized block bytes from the mining template.
    /// Used by `block_builder::build_submit_block` to reconstruct the full block
    /// with the miner's extranonce1, extranonce2, ntime, and nonce. The template
    /// coinbase (txs[0]) is replaced with the miner's reconstructed coinbase;
    /// the header is rebuilt via `build_stratum_header` to match the hash the
    /// validator computed.
    pub block_bytes: Vec<u8>,
    /// Total coinbase output value (subsidy + tx fees) in satoshis.
    /// From the MiningTemplate's coinbase_value field.
    pub coinbase_value: u64,
    /// Why the work changed (e.g., "new-tip", "reorg", "mempool", "manual").
    /// Empty string for initial startup jobs. Always populated when triggered
    /// by a MiningWorkChanged event (which always carries a reason).
    pub reason: String,
}

impl MiningJob {
    pub fn notify_params(&self) -> serde_json::Value {
        let mut params = json!([
            self.job_id,
            self.prevhash,
            self.coinbase1,
            self.coinbase2,
            self.merkle_branches,
            self.version,
            self.nbits,
            self.ntime,
            self.clean_jobs,
            // Lotus extension fields (params 9-12)
            self.height,
            self.epoch_hash,
            self.extended_metadata_hash,
            self.block_size,
        ]);
        // Params 13+: reason for work change (empty string = startup, omit from wire)
        if !self.reason.is_empty() {
            if let Some(arr) = params.as_array_mut() {
                arr.push(json!(&self.reason));
            }
        }
        params
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_notify_params_includes_reason_when_set() {
        let job = MiningJob {
            job_id: "job-890-100".to_string(),
            template_id: 890,
            prevhash: "4f7bcee63a20eff92f69a7f0e74af36a9f1e60ee7ecc5b0506e1ae3600000000".to_string(),
            coinbase1: "abc".to_string(),
            coinbase2: "def".to_string(),
            merkle_branches: vec!["hash1".to_string()],
            version: "00000001".to_string(),
            nbits: "10d0091c".to_string(),
            ntime: "6adc0c6a0000".to_string(),
            network_target_hex: "0000000009d01000000000000000000000000000000000000000000000000000".to_string(),
            clean_jobs: true,
            template_epoch: 100,
            height: 1292529,
            epoch_hash: "00000000061fb84d2a1d30d8767f629a08904b0e70f84587008fd9e91f1583f7".to_string(),
            extended_metadata_hash: "9a538906e6466ebd2617d321f71bc94e56056ce213d366773699e28158e00614".to_string(),
            block_size: 2588,
            block_bytes: vec![],
            coinbase_value: 5000000000,
            reason: "new-tip".to_string(),
        };
        let params = job.notify_params();
        let arr = params.as_array().unwrap();
        assert_eq!(arr.len(), 14);
        assert_eq!(arr[13], "new-tip");
    }

    #[test]
    fn test_notify_params_omits_reason_when_empty() {
        let job = MiningJob {
            job_id: "job-890-100".to_string(),
            template_id: 890,
            prevhash: "4f7bcee63a20eff92f69a7f0e74af36a9f1e60ee7ecc5b0506e1ae3600000000".to_string(),
            coinbase1: "abc".to_string(),
            coinbase2: "def".to_string(),
            merkle_branches: vec!["hash1".to_string()],
            version: "00000001".to_string(),
            nbits: "10d0091c".to_string(),
            ntime: "6adc0c6a0000".to_string(),
            network_target_hex: "0000000009d01000000000000000000000000000000000000000000000000000".to_string(),
            clean_jobs: true,
            template_epoch: 100,
            height: 1292529,
            epoch_hash: "00000000061fb84d2a1d30d8767f629a08904b0e70f84587008fd9e91f1583f7".to_string(),
            extended_metadata_hash: "9a538906e6466ebd2617d321f71bc94e56056ce213d366773699e28158e00614".to_string(),
            block_size: 2588,
            block_bytes: vec![],
            coinbase_value: 5000000000,
            reason: String::new(),
        };
        let params = job.notify_params();
        let arr = params.as_array().unwrap();
        assert_eq!(arr.len(), 13);
    }

    #[test]
    fn test_parse_template_metadata_from_job_id_standard_format() {
        let (tid, epoch) = parse_template_metadata_from_job_id("job-890-100").unwrap();
        assert_eq!(tid, 890);
        assert_eq!(epoch, 100);
    }

    #[test]
    fn test_parse_template_metadata_from_job_id_large_numbers() {
        let (tid, epoch) = parse_template_metadata_from_job_id("job-999999-1234567890").unwrap();
        assert_eq!(tid, 999999);
        assert_eq!(epoch, 1234567890);
    }

    #[test]
    fn test_parse_template_metadata_from_job_id_missing_prefix() {
        assert!(parse_template_metadata_from_job_id("not-a-job").is_none());
    }

    #[test]
    fn test_parse_template_metadata_from_job_id_no_epoch() {
        assert!(parse_template_metadata_from_job_id("job-42").is_none());
    }

    #[test]
    fn test_parse_template_metadata_from_job_id_non_numeric() {
        assert!(parse_template_metadata_from_job_id("job-abc-def").is_none());
    }
}
