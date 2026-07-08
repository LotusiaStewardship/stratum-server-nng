use crate::http_api::pagination::{PaginatedResponse, PaginationParams};
use crate::http_api::server::AppState;
use axum::{
    extract::{Path, Query, State},
    response::Json,
};
use serde::{Deserialize, Serialize};

#[derive(Serialize)]
pub struct RoundSummary {
    pub id: i64,
    pub start_template_id: String,
    pub end_template_id: Option<String>,
    pub status: String,
    pub found_block_hash: Option<String>,
}

#[derive(Deserialize)]
pub struct ListRoundsParams {
    pub status: Option<String>,
    pub limit: Option<i64>,
    pub offset: Option<i64>,
}

impl ListRoundsParams {
    fn pagination(&self) -> PaginationParams {
        PaginationParams {
            limit: self.limit,
            offset: self.offset,
        }
    }
}

/// Share breakdown within a round, grouped by worker.
#[derive(Debug, Clone, Serialize)]
pub struct WorkerShareBreakdown {
    pub worker_id: i64,
    pub payout_address: String,
    pub worker_suffix: Option<String>,
    pub total_shares: i64,
    pub accepted_shares: i64,
    pub rejected_shares: i64,
}

/// Detailed round information with share breakdown by worker.
#[derive(Debug, Clone, Serialize)]
pub struct RoundDetail {
    pub id: i64,
    pub start_template_id: String,
    pub end_template_id: Option<String>,
    pub status: String,
    pub found_block_hash: Option<String>,
    pub share_breakdown: Vec<WorkerShareBreakdown>,
    pub total_shares: i64,
}

pub async fn get_round(
    State(state): State<AppState>,
    Path(id): Path<i64>,
) -> Json<Option<RoundDetail>> {
    let round = match state.round_repo.as_ref() {
        Some(repo) => match repo.get_by_id(id) {
            Ok(Some(r)) => r,
            _ => return Json(None),
        },
        None => return Json(None),
    };

    let (share_breakdown, total_shares) = if let Some(ref share_repo) = state.share_repo {
        let rows = share_repo
            .count_outcomes_by_round_with_workers(id)
            .unwrap_or_default();
        let breakdown: Vec<WorkerShareBreakdown> = rows
            .into_iter()
            .map(
                |(worker_id, payout_address, worker_suffix, total, accepted, rejected)| {
                    WorkerShareBreakdown {
                        worker_id,
                        payout_address,
                        worker_suffix,
                        total_shares: total,
                        accepted_shares: accepted,
                        rejected_shares: rejected,
                    }
                },
            )
            .collect();
        let total: i64 = breakdown.iter().map(|w| w.total_shares).sum();
        (breakdown, total)
    } else {
        (Vec::new(), 0)
    };

    Json(Some(RoundDetail {
        id: round.id,
        start_template_id: round.start_template_id.to_string(),
        end_template_id: round.end_template_id.map(|v| v.to_string()),
        status: round.status,
        found_block_hash: round.found_block_hash,
        share_breakdown,
        total_shares,
    }))
}

pub async fn list_rounds(
    State(state): State<AppState>,
    Query(params): Query<ListRoundsParams>,
) -> Json<PaginatedResponse<RoundSummary>> {
    let all = if let Some(ref round_repo) = state.round_repo {
        round_repo
            .list(params.status.as_deref())
            .unwrap_or_default()
            .into_iter()
            .map(|r| RoundSummary {
                id: r.id,
                start_template_id: r.start_template_id.to_string(),
                end_template_id: r.end_template_id.map(|v| v.to_string()),
                status: r.status,
                found_block_hash: r.found_block_hash,
            })
            .collect()
    } else {
        Vec::new()
    };
    let total = all.len() as i64;
    Json(PaginatedResponse::new(all, total, &params.pagination()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::accounting::{init_schema, RoundRepository};
    use crate::http_api::ServerStats;
    use parking_lot::Mutex;
    use rusqlite::Connection;
    use std::sync::Arc;
    use tempfile::NamedTempFile;
    use tokio::sync::RwLock;

    #[tokio::test]
    async fn test_get_round_not_found() {
        let stats = ServerStats::default();
        let response = get_round(
            State(AppState {
                stats: Arc::new(RwLock::new(stats)),
                share_repo: None,
                worker_repo: None,
                round_repo: None,
                found_block_repo: None,
                payout_repo: None,
                accounting_service: None,
                payout_config: None,
                api_token: "test".to_string(),
            }),
            Path(999),
        )
        .await;

        assert!(response.0.is_none());
    }

    #[tokio::test]
    async fn test_get_round_with_share_breakdown() {
        let f = NamedTempFile::new().unwrap();
        let conn = Connection::open(f.path()).unwrap();
        init_schema(&conn).unwrap();
        let conn_arc = Arc::new(Mutex::new(conn));
        let round_repo = RoundRepository::new(conn_arc.clone());
        let worker_repo = crate::accounting::WorkerRepository::new(conn_arc.clone());
        let share_repo = crate::accounting::ShareRepository::new(conn_arc.clone());

        // Create a round
        let (round, _) = round_repo.get_or_create_current_round(1).unwrap();

        // Create workers
        let w1 = worker_repo.upsert("addr1", Some("rig1")).unwrap();
        let w2 = worker_repo.upsert("addr2", Some("rig2")).unwrap();

        // Insert shares and outcomes for the round
        let conn = conn_arc.clone();
        let dedupe_base = |i: i64| format!("round_detail_{}", i);
        for i in 1..=3 {
            let dedupe = dedupe_base(i);
            conn.lock().execute(
                "INSERT INTO shares (worker_id, session_id, job_id, template_id, template_epoch, extranonce1, extranonce2, ntime_hex_6b, nonce_hex_8b, difficulty, dedupe_key)
                 VALUES (?1, 'sess-1', 'job-1', 1, 100, '00000001', '00112233', '001122334455', '0011223344556677', 1.0, ?2)",
                rusqlite::params![w1.id, dedupe],
            ).unwrap();
            conn.lock().execute(
                "INSERT INTO share_outcomes (share_id, session_id, worker_id, job_id, round_id, dedupe_key, status, low_diff_ok, network_target_ok)
                 VALUES (?1, 'sess-1', ?2, 'job-1', ?3, ?4, 'accepted', 1, 0)",
                rusqlite::params![i as i64, w1.id, round.id, dedupe],
            ).unwrap();
        }
        // One rejected for w2 (share_id = 4, the next sequential ID)
        let dedupe_r = dedupe_base(10);
        conn.lock().execute(
            "INSERT INTO shares (worker_id, session_id, job_id, template_id, template_epoch, extranonce1, extranonce2, ntime_hex_6b, nonce_hex_8b, difficulty, dedupe_key)
             VALUES (?1, 'sess-1', 'job-1', 1, 100, '00000001', '00112233', '001122334455', '0011223344556677', 1.0, ?2)",
            rusqlite::params![w2.id, dedupe_r],
        ).unwrap();
        let share_id_4: i64 = 4;
        conn.lock().execute(
            "INSERT INTO share_outcomes (share_id, session_id, worker_id, job_id, round_id, dedupe_key, status, reject_reason, low_diff_ok, network_target_ok)
             VALUES (?1, 'sess-1', ?2, 'job-1', ?3, ?4, 'rejected', 'low-difficulty-share', 0, 0)",
            rusqlite::params![share_id_4, w2.id, round.id, dedupe_r],
        ).unwrap();

        let stats = ServerStats::default();
        let response = get_round(
            State(AppState {
                stats: Arc::new(RwLock::new(stats)),
                share_repo: Some(share_repo),
                worker_repo: Some(worker_repo),
                round_repo: Some(round_repo),
                found_block_repo: None,
                payout_repo: None,
                accounting_service: None,
                payout_config: None,
                api_token: "test".to_string(),
            }),
            Path(round.id),
        )
        .await;

        let detail = match &response.0 {
            Some(d) => d,
            None => panic!("round should be found"),
        };
        assert_eq!(detail.id, round.id);
        assert_eq!(detail.total_shares, 4);
        assert_eq!(detail.share_breakdown.len(), 2);

        // w1 has 3 accepted
        let w1_row = detail
            .share_breakdown
            .iter()
            .find(|w| w.worker_id == w1.id)
            .unwrap();
        assert_eq!(w1_row.total_shares, 3);
        assert_eq!(w1_row.accepted_shares, 3);
        assert_eq!(w1_row.rejected_shares, 0);

        // w2 has 1 rejected
        let w2_row = detail
            .share_breakdown
            .iter()
            .find(|w| w.worker_id == w2.id)
            .unwrap();
        assert_eq!(w2_row.total_shares, 1);
        assert_eq!(w2_row.accepted_shares, 0);
        assert_eq!(w2_row.rejected_shares, 1);
    }

    #[tokio::test]
    async fn test_list_rounds_empty() {
        let stats = ServerStats::default();
        let params = ListRoundsParams {
            status: None,
            limit: None,
            offset: None,
        };
        let response = list_rounds(
            State(AppState {
                stats: Arc::new(RwLock::new(stats)),
                share_repo: None,
                worker_repo: None,
                round_repo: None,
                found_block_repo: None,
                payout_repo: None,
                accounting_service: None,
                payout_config: None,
                api_token: "test".to_string(),
            }),
            Query(params),
        )
        .await;

        assert!(response.data.is_empty());
    }

    #[tokio::test]
    async fn test_list_rounds_with_data() {
        let f = NamedTempFile::new().unwrap();
        let conn = Connection::open(f.path()).unwrap();
        init_schema(&conn).unwrap();
        let round_repo = RoundRepository::new(Arc::new(Mutex::new(conn)));

        let _ = round_repo.get_or_create_current_round(1).unwrap();
        let _ = round_repo.get_or_create_current_round(10).unwrap(); // no-op, same round

        let stats = ServerStats::default();
        let params = ListRoundsParams {
            status: None,
            limit: None,
            offset: None,
        };
        let response = list_rounds(
            State(AppState {
                stats: Arc::new(RwLock::new(stats)),
                share_repo: None,
                worker_repo: None,
                round_repo: Some(round_repo),
                found_block_repo: None,
                payout_repo: None,
                accounting_service: None,
                payout_config: None,
                api_token: "test".to_string(),
            }),
            Query(params),
        )
        .await;

        assert_eq!(response.data.len(), 1);
        assert_eq!(response.total, 1);
        assert_eq!(response.data[0].status, "open");
    }

    #[tokio::test]
    async fn test_list_rounds_filter_by_status() {
        let f = NamedTempFile::new().unwrap();
        let conn = Connection::open(f.path()).unwrap();
        init_schema(&conn).unwrap();
        let round_repo = RoundRepository::new(Arc::new(Mutex::new(conn)));

        let (r1, _) = round_repo.get_or_create_current_round(1).unwrap();
        round_repo.close_round(r1.id, 5, "found").unwrap();
        let _ = round_repo.get_or_create_current_round(6).unwrap(); // new open round

        let stats = ServerStats::default();

        // Filter by 'open'
        let params = ListRoundsParams {
            status: Some("open".to_string()),
            limit: None,
            offset: None,
        };
        let open_rounds = list_rounds(
            State(AppState {
                stats: Arc::new(RwLock::new(stats.clone())),
                share_repo: None,
                worker_repo: None,
                round_repo: Some(round_repo.clone()),
                found_block_repo: None,
                payout_repo: None,
                accounting_service: None,
                payout_config: None,
                api_token: "test".to_string(),
            }),
            Query(params),
        )
        .await;
        assert_eq!(open_rounds.data.len(), 1);
        assert_eq!(open_rounds.data[0].start_template_id, "6");

        // Filter by 'found'
        let params = ListRoundsParams {
            status: Some("found".to_string()),
            limit: None,
            offset: None,
        };
        let found_rounds = list_rounds(
            State(AppState {
                stats: Arc::new(RwLock::new(stats)),
                share_repo: None,
                worker_repo: None,
                round_repo: Some(round_repo),
                found_block_repo: None,
                payout_repo: None,
                accounting_service: None,
                payout_config: None,
                api_token: "test".to_string(),
            }),
            Query(params),
        )
        .await;
        assert_eq!(found_rounds.data.len(), 1);
        assert_eq!(found_rounds.data[0].start_template_id, "1");
    }
}
