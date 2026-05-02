use serde_json::json;

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
    pub template_block: Vec<u8>,
    pub block_height: i32,
    pub epoch_hash_hex: String,
    pub extended_metadata_hash_hex: String,
    pub block_size: u64,
}

impl MiningJob {
    /// Returns mining.notify params with Lotus extensions.
    /// Standard params (9) + Lotus extensions (height, epoch_hash, extended_metadata_hash)
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
            // Lotus-specific extensions
            self.block_height,
            self.epoch_hash_hex,
            self.extended_metadata_hash_hex,
            self.block_size,
        ])
    }
}
