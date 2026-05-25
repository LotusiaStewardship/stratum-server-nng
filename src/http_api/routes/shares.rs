use axum::{
    extract::{Query, State},
    response::Json,
};
use serde::Serialize;
use crate::http_api::pagination::{PaginationParams, PaginatedResponse};
use crate::http_api::server::AppError;
use crate::http_api::server::AppState;

#[derive(Serialize)]
pub struct ShareResponse {
    pub id: i64,
    pub worker_id: i64,
    pub session_id: String,
    pub job_id: String,
    pub template_id: String,
    pub template_epoch: String,
    pub extranonce1: String,
    pub extranonce2: String,
    pub ntime_hex_6b: String,
    pub nonce_hex_8b: String,
    pub difficulty: f64,
    pub dedupe_key: String,
}

#[derive(Serialize)]
pub struct ShareOutcomeResponse {
    pub id: i64,
    pub share_id: i64,
    pub session_id: String,
    pub worker_id: i64,
    pub job_id: String,
    pub round_id: Option<i64>,
    pub dedupe_key: String,
    pub status: String,
    pub reject_reason: Option<String>,
    pub node_result: Option<String>,
    pub low_diff_ok: Option<bool>,
    pub network_target_ok: Option<bool>,
    pub block_hash: Option<String>,
}

#[derive(serde::Deserialize)]
pub struct ListSharesParams {
    pub worker_id: Option<i64>,
    pub status: Option<String>,
    pub from: Option<String>,
    pub to: Option<String>,
    pub limit: Option<i64>,
    pub offset: Option<i64>,
}

/// Build a PaginationParams from ListSharesParams fields.
fn pagination_from(params: &ListSharesParams) -> PaginationParams {
    PaginationParams {
        limit: params.limit,
        offset: params.offset,
    }
}

/// GET /api/v1/shares — list shares with optional filters and pagination.
pub async fn list_shares(
    State(state): State<AppState>,
    Query(params): Query<ListSharesParams>,
) -> Result<Json<PaginatedResponse<ShareResponse>>, AppError> {
    let repo = state.share_repo.ok_or(AppError::DbNotConfigured)?;
    let pagination = pagination_from(&params);

    let all: Vec<ShareResponse> = repo
        .list_shares(
            params.worker_id,
            params.status.as_deref(),
            params.from.as_deref(),
            params.to.as_deref(),
            None,
            None,
        )?
        .into_iter()
        .map(|s| ShareResponse {
            id: s.id,
            worker_id: s.worker_id,
            session_id: s.session_id,
            job_id: s.job_id,
            template_id: s.template_id.to_string(),
            template_epoch: s.template_epoch.to_string(),
            extranonce1: s.extranonce1,
            extranonce2: s.extranonce2,
            ntime_hex_6b: s.ntime_hex_6b,
            nonce_hex_8b: s.nonce_hex_8b,
            difficulty: s.difficulty,
            dedupe_key: s.dedupe_key,
        })
        .collect();

    let total = repo
        .count_shares(params.worker_id, params.status.as_deref(), params.from.as_deref(), params.to.as_deref())
        .unwrap_or(all.len() as i64);

    Ok(Json(PaginatedResponse::new(all, total, &pagination)))
}

/// GET /api/v1/share-outcomes — list share outcomes with optional filters and pagination.
pub async fn list_share_outcomes(
    State(state): State<AppState>,
    Query(params): Query<ListSharesParams>,
) -> Result<Json<PaginatedResponse<ShareOutcomeResponse>>, AppError> {
    let repo = state.share_repo.ok_or(AppError::DbNotConfigured)?;
    let pagination = pagination_from(&params);

    let all: Vec<ShareOutcomeResponse> = repo
        .list_outcomes(
            params.worker_id,
            params.status.as_deref(),
            params.from.as_deref(),
            params.to.as_deref(),
            None,
            None,
        )?
        .into_iter()
        .map(|o| ShareOutcomeResponse {
            id: o.id,
            share_id: o.share_id,
            session_id: o.session_id,
            worker_id: o.worker_id,
            job_id: o.job_id,
            round_id: o.round_id,
            dedupe_key: o.dedupe_key,
            status: o.status,
            reject_reason: o.reject_reason,
            node_result: o.node_result,
            low_diff_ok: o.low_diff_ok,
            network_target_ok: o.network_target_ok,
            block_hash: o.block_hash,
        })
        .collect();

    let total = repo
        .count_outcomes(params.worker_id, params.status.as_deref(), params.from.as_deref(), params.to.as_deref())
        .unwrap_or(all.len() as i64);

    Ok(Json(PaginatedResponse::new(all, total, &pagination)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::accounting::{schema::init_schema, ShareRepository, WorkerRepository};
    use crate::http_api::server::{AppState, ServerStats};
    use parking_lot::Mutex;
    use rusqlite::Connection;
    use std::sync::Arc;
    use tempfile::NamedTempFile;
    use tokio::sync::RwLock;

    fn test_state() -> AppState {
        let f = NamedTempFile::new().unwrap();
        let conn = Connection::open(f.path()).unwrap();
        init_schema(&conn).unwrap();
        let conn_arc = Arc::new(Mutex::new(conn));
        let share_repo = ShareRepository::new(conn_arc.clone());
        let _worker_repo = WorkerRepository::new(conn_arc);

        AppState {
            stats: Arc::new(RwLock::new(ServerStats::default())),
            share_repo: Some(share_repo),
            worker_repo: None,
            round_repo: None,
            found_block_repo: None,
            payout_repo: None,
            accounting_service: None,
            payout_config: None,
            api_token: "test".to_string(),
        }
    }

    #[tokio::test]
    async fn test_list_shares_empty() {
        let state = test_state();
        let params = ListSharesParams {
            worker_id: None,
            status: None,
            from: None,
            to: None,
            limit: None,
            offset: None,
        };
        let response = list_shares(State(state), Query(params))
            .await
            .unwrap();
        assert!(response.data.is_empty());
        assert_eq!(response.total, 0);
    }

    #[tokio::test]
    async fn test_list_share_outcomes_empty() {
        let state = test_state();
        let params = ListSharesParams {
            worker_id: None,
            status: None,
            from: None,
            to: None,
            limit: None,
            offset: None,
        };
        let response = list_share_outcomes(State(state), Query(params))
            .await
            .unwrap();
        assert!(response.data.is_empty());
        assert_eq!(response.total, 0);
    }
}
