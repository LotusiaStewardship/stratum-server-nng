use crate::http_api::server::AppState;
use axum::{extract::State, response::Json};
use serde::Serialize;
use std::collections::HashMap;

#[derive(Serialize)]
pub struct StatsResponse {
    pub total_shares: i64,
    pub accepted_shares: i64,
    pub rejected_shares: i64,
    pub accepted_pct: f64,
    pub rejection_breakdown: HashMap<String, i64>,
    pub network_difficulty: Option<String>,
}

pub async fn stats_handler(State(state): State<AppState>) -> Json<StatsResponse> {
    let stats = state.stats.read().await;

    // Query share outcome counts from repository
    let (total_shares, accepted_shares, rejected_shares, rejection_breakdown) =
        if let Some(ref share_repo) = state.share_repo {
            let total = share_repo.total_count().unwrap_or(0);
            let accepted = share_repo.count_outcomes_by_status("accepted").unwrap_or(0);
            let rejected = share_repo.count_outcomes_by_status("rejected").unwrap_or(0);
            let reasons = share_repo.count_rejected_by_reason().unwrap_or_default();
            (total, accepted, rejected, reasons)
        } else {
            (0, 0, 0, HashMap::new())
        };

    let accepted_pct = if total_shares > 0 {
        (accepted_shares as f64 / total_shares as f64) * 100.0
    } else {
        0.0
    };

    Json(StatsResponse {
        total_shares,
        accepted_shares,
        rejected_shares,
        accepted_pct,
        rejection_breakdown,
        network_difficulty: stats.network_difficulty.clone(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::accounting::{init_schema, ShareRepository};
    use crate::http_api::ServerStats;
    use parking_lot::Mutex;
    use rusqlite::Connection;
    use std::sync::Arc;
    use tempfile::NamedTempFile;
    use tokio::sync::RwLock;

    #[tokio::test]
    async fn test_stats_response_correct_without_db() {
        // Without a share_repo, all counts return 0
        let stats = ServerStats::default();

        let response = stats_handler(State(AppState {
            stats: Arc::new(RwLock::new(stats)),
            share_repo: None,
            worker_repo: None,
            round_repo: None,
            found_block_repo: None,
            payout_repo: None,
            accounting_service: None,
            payout_config: None,
            api_token: "test".to_string(),
        }))
        .await;

        assert_eq!(response.total_shares, 0);
        assert_eq!(response.accepted_shares, 0);
        assert_eq!(response.rejected_shares, 0);
        assert_eq!(response.accepted_pct, 0.0);
        assert!(response.rejection_breakdown.is_empty());
    }

    #[tokio::test]
    async fn test_stats_response_with_network_difficulty() {
        let stats = ServerStats {
            network_difficulty: Some("ffffffff".to_string()),
            ..Default::default()
        };

        let response = stats_handler(State(AppState {
            stats: Arc::new(RwLock::new(stats)),
            share_repo: None,
            worker_repo: None,
            round_repo: None,
            found_block_repo: None,
            payout_repo: None,
            accounting_service: None,
            payout_config: None,
            api_token: "test".to_string(),
        }))
        .await;

        assert_eq!(response.network_difficulty, Some("ffffffff".to_string()));
    }

    #[tokio::test]
    async fn test_stats_response_queries_repository() {
        // Create test database with known share outcome data
        let temp_file = NamedTempFile::new().unwrap();
        let db_conn = Connection::open(temp_file.path()).unwrap();
        init_schema(&db_conn).unwrap();

        // Insert a worker
        db_conn.execute(
            "INSERT INTO workers (id, payout_address, worker_suffix) VALUES (1, 'test_addr', 'rig1')",
            [],
        ).unwrap();

        // Insert raw shares (one per outcome)
        db_conn.execute(
            "INSERT INTO shares (worker_id, session_id, job_id, template_id, template_epoch, extranonce1, extranonce2, ntime_hex_6b, nonce_hex_8b, difficulty, dedupe_key) 
             VALUES (1, 'sess-1', 'job-1', 1, 100, '00000001', '00112233', '001122334455', '0011223344556677', 1.0, 'dk1')",
            [],
        ).unwrap();
        db_conn.execute(
            "INSERT INTO shares (worker_id, session_id, job_id, template_id, template_epoch, extranonce1, extranonce2, ntime_hex_6b, nonce_hex_8b, difficulty, dedupe_key) 
             VALUES (1, 'sess-1', 'job-1', 1, 101, '00000001', '00112234', '001122334456', '0011223344556678', 1.0, 'dk2')",
            [],
        ).unwrap();
        db_conn.execute(
            "INSERT INTO shares (worker_id, session_id, job_id, template_id, template_epoch, extranonce1, extranonce2, ntime_hex_6b, nonce_hex_8b, difficulty, dedupe_key) 
             VALUES (1, 'sess-1', 'job-1', 1, 102, '00000001', '00112235', '001122334457', '0011223344556679', 1.0, 'dk3')",
            [],
        ).unwrap();

        // Insert share outcomes (what stats queries)
        db_conn.execute(
            "INSERT INTO share_outcomes (share_id, session_id, worker_id, job_id, dedupe_key, status, low_diff_ok, network_target_ok) 
             VALUES (1, 'sess-1', 1, 'job-1', 'dk1', 'accepted', 1, 0)",
            [],
        ).unwrap();
        db_conn.execute(
            "INSERT INTO share_outcomes (share_id, session_id, worker_id, job_id, dedupe_key, status, low_diff_ok, network_target_ok) 
             VALUES (2, 'sess-1', 1, 'job-1', 'dk2', 'accepted', 1, 0)",
            [],
        ).unwrap();
        db_conn.execute(
            "INSERT INTO share_outcomes (share_id, session_id, worker_id, job_id, dedupe_key, status, reject_reason, low_diff_ok, network_target_ok) 
             VALUES (3, 'sess-1', 1, 'job-1', 'dk3', 'rejected', 'low-difficulty-share', 0, 0)",
            [],
        ).unwrap();

        let share_repo = ShareRepository::new(Arc::new(Mutex::new(db_conn)));
        let stats = ServerStats::default();

        let response = stats_handler(State(AppState {
            stats: Arc::new(RwLock::new(stats)),
            share_repo: Some(share_repo),
            worker_repo: None,
            round_repo: None,
            found_block_repo: None,
            payout_repo: None,
            accounting_service: None,
            payout_config: None,
            api_token: "test".to_string(),
        }))
        .await;

        // Should query actual database counts from share_outcomes
        assert_eq!(response.total_shares, 3);
        assert_eq!(response.accepted_shares, 2);
        assert_eq!(response.rejected_shares, 1);
        assert!((response.accepted_pct - 66.67).abs() < 0.01);
        assert_eq!(
            response.rejection_breakdown.get("low-difficulty-share"),
            Some(&1)
        );
    }
}
