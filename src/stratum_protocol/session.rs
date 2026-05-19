use crate::stratum_protocol::protocol::{StratumRequest, StratumResponse};
use serde_json::{json, Value};
use std::collections::{HashSet, VecDeque};
use rand::Rng;

/// Maximum assigned jobs per session (per UBQ invariant). See UBQ §Assigned Job.
const MAX_ASSIGNED_JOBS_PER_SESSION: usize = 128;

/// A record of a job that was dispatched to the miner via mining.notify.
/// Per UBQ §Assigned Job: each assigned job captures (job_id, P_diff, ntime).
#[derive(Debug, Clone)]
pub struct AssignedJob {
    pub job_id: String,
    pub p_diff: f64,
    pub ntime: String,
}

#[derive(Debug, Clone)]
pub struct SessionState {
    pub session_id: String,
    pub extranonce1: String,
    pub extranonce2_size: u8,
    pub is_subscribed: bool,
    pub is_authorized: bool,
    pub authorized_workers: HashSet<String>,
    /// Per UBQ §Assigned Job: tracks (job_id, P_diff, ntime) for every dispatched mining.notify.
    /// Capped at MAX_ASSIGNED_JOBS_PER_SESSION (default 128) to bound memory.
    pub assigned_jobs: VecDeque<AssignedJob>,
}

impl SessionState {
    pub fn new(session_id: String) -> Self {
        // Generate random extranonce1 (4 bytes = 8 hex chars)
        let extranonce1 = format!("{:08x}", rand::thread_rng().gen::<u32>());
        Self {
            session_id,
            extranonce1,
            extranonce2_size: 4,
            is_subscribed: false,
            is_authorized: false,
            authorized_workers: HashSet::new(),
            assigned_jobs: VecDeque::new(),
        }
    }

    pub fn handle_subscribe(&mut self, req: &StratumRequest) -> StratumResponse {
        self.is_subscribed = true;
        let result = json!([
            [
                ["mining.set_difficulty", self.session_id.clone()],
                ["mining.notify", self.session_id.clone()]
            ],
            self.extranonce1.clone(),
            self.extranonce2_size
        ]);
        StratumResponse::ok(req.id.clone(), result)
    }

    pub fn handle_authorize(&mut self, req: &StratumRequest) -> StratumResponse {
        if !self.is_subscribed {
            return StratumResponse::err(req.id.clone(), 25, "not-subscribed");
        }
        let arr = req.params.as_array().cloned().unwrap_or_default();
        let worker = arr.first().and_then(|v| v.as_str()).unwrap_or_default();

        // Validate worker name format
        if parse_worker_name(worker).is_err() {
            return StratumResponse::err(req.id.clone(), 24, "unauthorized-worker");
        }

        self.authorized_workers.insert(worker.to_string());
        self.is_authorized = true;
        StratumResponse::ok(req.id.clone(), Value::Bool(true))
    }

    /// Record an assigned job (dispatched mining.notify) in the session.
    /// Per UBQ: tracks (job_id, P_diff, ntime) and caps at MAX_ASSIGNED_JOBS_PER_SESSION.
    pub fn record_assigned_job(&mut self, job_id: String, p_diff: f64, ntime: String) {
        if self.assigned_jobs.len() >= MAX_ASSIGNED_JOBS_PER_SESSION {
            self.assigned_jobs.pop_front();
        }
        self.assigned_jobs.push_back(AssignedJob {
            job_id,
            p_diff,
            ntime,
        });
    }

    /// Get an assigned job by job_id.
    pub fn get_assigned_job(&self, job_id: &str) -> Option<&AssignedJob> {
        self.assigned_jobs.iter().find(|j| j.job_id == job_id)
    }

    /// Clear all assigned jobs (called on clean_jobs=true).
    /// Per UBQ: when clean_jobs=true, ALL previous jobs become stale immediately.
    pub fn clear_assigned_jobs(&mut self) {
        self.assigned_jobs.clear();
    }

    /// Get the current P_diff (from the most recent assigned job, or 1.0 if none).
    /// Used for share difficulty recording.
    pub fn current_difficulty(&self) -> f64 {
        self.assigned_jobs
            .back()
            .map(|j| j.p_diff)
            .unwrap_or(1.0)
    }
}

pub fn parse_worker_name(input: &str) -> anyhow::Result<WorkerName> {
    let mut parts = input.splitn(2, '.');
    let address = parts.next().unwrap_or_default();
    let suffix = parts.next();

    let _: bitcoinsuite_core::LotusAddress = address
        .parse()
        .map_err(|e| anyhow::anyhow!("invalid lotus address: {e}"))?;
    if suffix.is_some_and(|s| s.is_empty()) {
        anyhow::bail!("worker suffix must not be empty when '.' is present")
    }

    Ok(WorkerName {
        payout_address: address.to_string(),
        worker_suffix: suffix.map(|s| s.to_string()),
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerName {
    pub payout_address: String,
    pub worker_suffix: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stratum_protocol::protocol::Method;

    #[test]
    fn test_subscribe_creates_session() {
        let mut session = SessionState::new("sess-1".to_string());
        let req = StratumRequest {
            id: Value::Number(1.into()),
            method: Method::Subscribe,
            params: Value::Array(vec![]),
        };
        
        let resp = session.handle_subscribe(&req);
        
        assert!(resp.error.is_null());
        assert!(session.is_subscribed);
        // extranonce1 should be 8 hex characters (4 bytes)
        assert_eq!(session.extranonce1.len(), 8);
        assert!(session.extranonce1.chars().all(|c| c.is_ascii_hexdigit()));
        assert_eq!(session.extranonce2_size, 4);
    }

    #[test]
    fn test_authorize_requires_subscribe() {
        let mut session = SessionState::new("sess-2".to_string());
        let req = StratumRequest {
            id: Value::Number(2.into()),
            method: Method::Authorize,
            params: serde_json::json!(["lotus_16PSJNf1EDEfGvaYzaXJCJZrXH4pgiTo7kyW61iGi.rig", "x"]).into(),
        };
        
        let resp = session.handle_authorize(&req);
        
        assert!(!resp.error.is_null());
        assert!(!session.is_authorized);
    }

    #[test]
    fn test_authorize_with_valid_worker() {
        let mut session = SessionState::new("sess-3".to_string());
        session.is_subscribed = true;
        
        let req = StratumRequest {
            id: Value::Number(2.into()),
            method: Method::Authorize,
            params: serde_json::json!(["lotus_16PSJNf1EDEfGvaYzaXJCJZrXH4pgiTo7kyW61iGi.rig", "x"]).into(),
        };
        
        let resp = session.handle_authorize(&req);
        
        assert!(resp.error.is_null());
        assert!(session.is_authorized);
        assert!(session.authorized_workers.contains("lotus_16PSJNf1EDEfGvaYzaXJCJZrXH4pgiTo7kyW61iGi.rig"));
    }

    #[test]
    fn test_assigned_jobs_cap() {
        let mut session = SessionState::new("sess-8".to_string());
        // Record more than MAX_ASSIGNED_JOBS_PER_SESSION jobs
        for i in 0..MAX_ASSIGNED_JOBS_PER_SESSION + 10 {
            session.record_assigned_job(
                format!("job-{}", i),
                1.0,
                format!("ntime-{}", i),
            );
        }
        
        // Should be capped at MAX_ASSIGNED_JOBS_PER_SESSION
        assert_eq!(session.assigned_jobs.len(), MAX_ASSIGNED_JOBS_PER_SESSION);
        
        // Oldest jobs should be evicted
        assert!(session.get_assigned_job("job-0").is_none());
        assert!(session.get_assigned_job("job-1").is_none());
        
        // Recent jobs should still be present
        assert!(session.get_assigned_job(
            &format!("job-{}", MAX_ASSIGNED_JOBS_PER_SESSION + 9)
        ).is_some());
    }

    #[test]
    fn test_clear_assigned_jobs() {
        let mut session = SessionState::new("sess-9".to_string());
        session.record_assigned_job("job-1".to_string(), 1.0, "ntime-1".to_string());
        session.record_assigned_job("job-2".to_string(), 1.0, "ntime-2".to_string());
        
        assert_eq!(session.assigned_jobs.len(), 2);
        
        session.clear_assigned_jobs();
        
        assert_eq!(session.assigned_jobs.len(), 0);
        assert!(session.get_assigned_job("job-1").is_none());
    }

    #[test]
    fn test_current_difficulty() {
        let mut session = SessionState::new("sess-10".to_string());
        
        // Default when no jobs assigned
        assert!((session.current_difficulty() - 1.0).abs() < f64::EPSILON);
        
        // After recording a job with specific difficulty
        session.record_assigned_job("job-1".to_string(), 512.0, "ntime".to_string());
        assert!((session.current_difficulty() - 512.0).abs() < f64::EPSILON);
        
        // After recording another job, should return latest
        session.record_assigned_job("job-2".to_string(), 256.0, "ntime2".to_string());
        assert!((session.current_difficulty() - 256.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_parse_worker_name_valid() {
        let worker = parse_worker_name("lotus_16PSJNf1EDEfGvaYzaXJCJZrXH4pgiTo7kyW61iGi.rig01").unwrap();
        assert_eq!(worker.payout_address, "lotus_16PSJNf1EDEfGvaYzaXJCJZrXH4pgiTo7kyW61iGi");
        assert_eq!(worker.worker_suffix, Some("rig01".to_string()));
    }

    #[test]
    fn test_parse_worker_name_no_suffix() {
        let worker = parse_worker_name("lotus_16PSJNf1EDEfGvaYzaXJCJZrXH4pgiTo7kyW61iGi").unwrap();
        assert_eq!(worker.payout_address, "lotus_16PSJNf1EDEfGvaYzaXJCJZrXH4pgiTo7kyW61iGi");
        assert_eq!(worker.worker_suffix, None);
    }

    #[test]
    fn test_parse_worker_name_invalid_address() {
        assert!(parse_worker_name("bad.worker").is_err());
        assert!(parse_worker_name("lotusabc.r1").is_err());
    }

    #[test]
    fn test_parse_worker_name_empty_suffix() {
        assert!(parse_worker_name("lotus_16PSJNf1EDEfGvaYzaXJCJZrXH4pgiTo7kyW61iGi.").is_err());
    }

    #[test]
    fn test_unique_extranonce1_per_session() {
        // Create multiple sessions and verify they get different extranonce1 values
        let session1 = SessionState::new("sess-1".to_string());
        let session2 = SessionState::new("sess-2".to_string());
        let session3 = SessionState::new("sess-3".to_string());
        
        // All should have valid 8-char hex extranonce1
        assert_eq!(session1.extranonce1.len(), 8);
        assert_eq!(session2.extranonce1.len(), 8);
        assert_eq!(session3.extranonce1.len(), 8);
        
        // They should be different (probability of collision is extremely low)
        assert_ne!(session1.extranonce1, session2.extranonce1);
        assert_ne!(session2.extranonce1, session3.extranonce1);
        assert_ne!(session1.extranonce1, session3.extranonce1);
    }
}
