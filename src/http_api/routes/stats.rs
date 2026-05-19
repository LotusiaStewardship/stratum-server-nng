use axum::{
    extract::State,
    response::Json,
};
use serde::Serialize;
use crate::http_api::server::AppState;

#[derive(Serialize)]
pub struct StatsResponse {
    pub total_shares: i64,
    pub accepted_shares: i64,
    pub rejected_shares: i64,
    pub accepted_pct: f64,
    pub network_difficulty: Option<String>,
}

pub async fn stats_handler(State(state): State<AppState>) -> Json<StatsResponse> {
    let stats = state.stats.read().await;
    
    // Query actual share counts from repository if available
    let (total_shares, accepted_shares, rejected_shares) = if let Some(ref share_repo) = state.share_repo {
        let total = share_repo.total_count().unwrap_or(0);
        let accepted = share_repo.count_by_status("accepted").unwrap_or(0);
        let rejected = share_repo.count_by_status("rejected").unwrap_or(0);
        (total, accepted, rejected)
    } else {
        (stats.total_shares, stats.accepted_shares, stats.rejected_shares)
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
    async fn test_stats_response() {
        let stats = ServerStats {
            total_shares: 100,
            accepted_shares: 95,
            rejected_shares: 5,
            ..Default::default()
        };

        let response = stats_handler(State(AppState {
            stats: Arc::new(RwLock::new(stats)),
            share_repo: None,
        }))
        .await;

        assert_eq!(response.total_shares, 100);
        assert_eq!(response.accepted_shares, 95);
        assert_eq!(response.rejected_shares, 5);
        assert!((response.accepted_pct - 95.0).abs() < 0.01);
    }

    #[tokio::test]
    async fn test_stats_response_zero_shares() {
        let stats = ServerStats::default();

        let response = stats_handler(State(AppState {
            stats: Arc::new(RwLock::new(stats)),
            share_repo: None,
        }))
        .await;

        assert_eq!(response.total_shares, 0);
        assert_eq!(response.accepted_pct, 0.0);
    }

    #[tokio::test]
    async fn test_stats_response_with_network_difficulty() {
        let stats = ServerStats {
            total_shares: 100,
            accepted_shares: 95,
            rejected_shares: 5,
            network_difficulty: Some("ffffffff".to_string()),
            ..Default::default()
        };

        let response = stats_handler(State(AppState {
            stats: Arc::new(RwLock::new(stats)),
            share_repo: None,
        }))
        .await;

        assert_eq!(response.network_difficulty, Some("ffffffff".to_string()));
    }

    #[tokio::test]
    async fn test_stats_response_queries_repository() {
        // Create test database with known share data
        let temp_file = NamedTempFile::new().unwrap();
        let db_conn = Connection::open(temp_file.path()).unwrap();
        init_schema(&db_conn).unwrap();
        
        // Insert a worker first
        db_conn.execute(
            "INSERT INTO workers (id, payout_address, worker_suffix) VALUES (1, 'test_addr', 'rig1')",
            [],
        ).unwrap();
        
        // Insert shares with different statuses
        db_conn.execute(
            "INSERT INTO shares (worker_id, session_id, job_id, extranonce2, ntime_hex_6b, nonce_hex_8b, difficulty, status, reject_reason) 
             VALUES (1, 'sess-1', 'job-1', '00112233', '001122334455', '0011223344556677', 1.0, 'accepted', NULL)",
            [],
        ).unwrap();
        db_conn.execute(
            "INSERT INTO shares (worker_id, session_id, job_id, extranonce2, ntime_hex_6b, nonce_hex_8b, difficulty, status, reject_reason) 
             VALUES (1, 'sess-1', 'job-1', '00112234', '001122334456', '0011223344556678', 1.0, 'accepted', NULL)",
            [],
        ).unwrap();
        db_conn.execute(
            "INSERT INTO shares (worker_id, session_id, job_id, extranonce2, ntime_hex_6b, nonce_hex_8b, difficulty, status, reject_reason) 
             VALUES (1, 'sess-1', 'job-1', '00112235', '001122334457', '0011223344556679', 1.0, 'rejected', 'low-difficulty-share')",
            [],
        ).unwrap();

        let share_repo = ShareRepository::new(Arc::new(Mutex::new(db_conn)));
        let stats = ServerStats::default();

        let response = stats_handler(State(AppState {
            stats: Arc::new(RwLock::new(stats)),
            share_repo: Some(share_repo),
        }))
        .await;

        // Should query actual database counts
        assert_eq!(response.total_shares, 3);
        assert_eq!(response.accepted_shares, 2);
        assert_eq!(response.rejected_shares, 1);
        assert!((response.accepted_pct - 66.67).abs() < 0.01);
    }
}
