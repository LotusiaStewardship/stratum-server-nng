use crate::stratum::job::MiningJob;
use crate::stratum::protocol::{
    decode_request_line, Method, StratumError, StratumRequest, StratumResponse,
};
use crate::stratum::worker::parse_worker_name;
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};

#[derive(Debug, Clone)]
pub struct SessionState {
    pub session_id: String,
    pub extranonce1: String,
    pub extranonce2_size: u8,
    pub authorized_workers: HashSet<String>,
    pub worker_difficulty: HashMap<String, f64>,
    pub active_jobs: HashSet<String>,
}

impl SessionState {
    pub fn new(session_id: String) -> Self {
        Self {
            session_id,
            extranonce1: "00000000".to_string(),
            extranonce2_size: 4,
            authorized_workers: HashSet::new(),
            worker_difficulty: HashMap::new(),
            active_jobs: HashSet::new(),
        }
    }
}

pub fn apply_notify(session: &mut SessionState, job: &MiningJob) {
    if job.clean_jobs {
        session.active_jobs.clear();
    }
    session.active_jobs.insert(job.job_id.clone());
}

pub fn handle_line(
    session: &mut SessionState,
    line: &str,
    max_request_line_bytes: usize,
) -> Result<Option<StratumResponse>, StratumError> {
    let req = decode_request_line(line, max_request_line_bytes)?;
    Ok(handle_request(session, req))
}

pub fn handle_request(session: &mut SessionState, req: StratumRequest) -> Option<StratumResponse> {
    match req.method {
        Method::Subscribe => {
            let result = json!([
                [
                    ["mining.set_difficulty", session.session_id],
                    ["mining.notify", session.session_id]
                ],
                session.extranonce1,
                session.extranonce2_size
            ]);
            Some(StratumResponse::ok(req.id, result))
        }
        Method::Authorize => {
            let arr = req.params.as_array().cloned().unwrap_or_default();
            let worker = arr.first().and_then(|v| v.as_str()).unwrap_or_default();
            if parse_worker_name(worker).is_err() {
                return Some(StratumResponse::err(req.id, 24, "unauthorized-worker"));
            }
            session.authorized_workers.insert(worker.to_string());
            session
                .worker_difficulty
                .entry(worker.to_string())
                .or_insert(1.0);
            Some(StratumResponse::ok(req.id, Value::Bool(true)))
        }
        Method::Submit => {
            let arr = req.params.as_array().cloned().unwrap_or_default();
            let worker = arr.first().and_then(|v| v.as_str()).unwrap_or_default();
            let job_id = arr.get(1).and_then(|v| v.as_str()).unwrap_or_default();
            if !session.authorized_workers.contains(worker) {
                return Some(StratumResponse::err(req.id, 24, "unauthorized-worker"));
            }
            if arr.len() < 5 {
                return Some(StratumResponse::err(req.id, 20, "invalid-submit-shape"));
            }
            if !session.active_jobs.contains(job_id) {
                return Some(StratumResponse::err(req.id, 21, "stale-job"));
            }
            Some(StratumResponse::ok(req.id, Value::Bool(true)))
        }
        Method::Ping => Some(StratumResponse::ok(req.id, Value::Bool(true))),
        Method::ExtranonceSubscribe => Some(StratumResponse::ok(req.id, Value::Bool(true))),
        Method::SuggestDifficulty | Method::SetExtranonce | Method::Unknown(_) => {
            Some(StratumResponse::err(req.id, 20, "unsupported"))
        }
        Method::Notify | Method::SetDifficulty => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_subscribe_and_authorize() {
        let mut s = SessionState::new("sess-1".to_string());
        let sub = handle_line(
            &mut s,
            r#"{"id":1,"method":"mining.subscribe","params":[]}"#,
            8192,
        )
        .unwrap()
        .unwrap();
        assert!(sub.error.is_null());

        let auth = handle_line(
            &mut s,
            r#"{"id":2,"method":"mining.authorize","params":["lotus_abc.rig","x"]}"#,
            8192,
        )
        .unwrap()
        .unwrap();
        assert_eq!(auth.result, Value::Bool(true));
    }
}
