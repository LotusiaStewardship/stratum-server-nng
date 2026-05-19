use crate::stratum_protocol::protocol::{Method, StratumRequest, StratumResponse};
use serde_json::{json, Value};
use std::collections::HashSet;
use rand::Rng;

#[derive(Debug, Clone)]
pub struct SessionState {
    pub session_id: String,
    pub extranonce1: String,
    pub extranonce2_size: u8,
    pub is_subscribed: bool,
    pub is_authorized: bool,
    pub authorized_workers: HashSet<String>,
    pub active_jobs: HashSet<String>,
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
            active_jobs: HashSet::new(),
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

    pub fn handle_submit(&self, req: &StratumRequest) -> StratumResponse {
        if !self.is_subscribed {
            return StratumResponse::err(req.id.clone(), 25, "not-subscribed");
        }
        let arr = req.params.as_array().cloned().unwrap_or_default();
        let worker = arr.first().and_then(|v| v.as_str()).unwrap_or_default();
        
        if !self.authorized_workers.contains(worker) {
            return StratumResponse::rejected(req.id.clone(), 24, "unauthorized-worker");
        }
        
        // Validate submit shape (5 params minimum)
        if arr.len() < 5 {
            return StratumResponse::rejected(req.id.clone(), 20, "invalid-submit-shape");
        }
        
        // For Slice 1, accept all valid submits (validation in Slice 3)
        StratumResponse::ok(req.id.clone(), Value::Bool(true))
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
    fn test_submit_requires_authorize() {
        let mut session = SessionState::new("sess-4".to_string());
        session.is_subscribed = true;
        
        let req = StratumRequest {
            id: Value::Number(3.into()),
            method: Method::Submit,
            params: serde_json::json!(["worker", "job1", "00112233", "001122334455", "0011223344556677"]).into(),
        };
        
        let resp = session.handle_submit(&req);
        
        assert!(!resp.error.is_null());
        assert_eq!(resp.result, Value::Bool(false));
    }

    #[test]
    fn test_submit_with_authorized_worker() {
        let mut session = SessionState::new("sess-5".to_string());
        session.is_subscribed = true;
        session.is_authorized = true;
        session.authorized_workers.insert("lotus_16PSJNf1EDEfGvaYzaXJCJZrXH4pgiTo7kyW61iGi.rig".to_string());
        
        let req = StratumRequest {
            id: Value::Number(3.into()),
            method: Method::Submit,
            params: serde_json::json!(["lotus_16PSJNf1EDEfGvaYzaXJCJZrXH4pgiTo7kyW61iGi.rig", "job1", "00112233", "001122334455", "0011223344556677"]).into(),
        };
        
        let resp = session.handle_submit(&req);
        
        assert!(resp.error.is_null());
        assert_eq!(resp.result, Value::Bool(true));
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
