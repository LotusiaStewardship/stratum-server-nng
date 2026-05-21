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
}

impl MiningJob {
    pub fn notify_params(&self) -> serde_json::Value {
        json!([
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
        ])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
