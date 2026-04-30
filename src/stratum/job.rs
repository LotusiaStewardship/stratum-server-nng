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
    pub template_header: Vec<u8>,
    pub template_block: Vec<u8>,
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
            self.clean_jobs
        ])
    }
}
