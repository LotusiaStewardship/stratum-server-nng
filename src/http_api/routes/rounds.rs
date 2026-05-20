use axum::{
    extract::{Query, State},
    response::Json,
};
use serde::{Deserialize, Serialize};
use crate::http_api::server::AppState;

#[derive(Serialize)]
pub struct RoundSummary {
    pub id: i64,
    pub start_template_id: i64,
    pub end_template_id: Option<i64>,
    pub status: String,
    pub found_block_hash: Option<String>,
}

#[derive(Deserialize)]
pub struct ListRoundsParams {
    pub status: Option<String>,
}

pub async fn list_rounds(
    State(state): State<AppState>,
    Query(params): Query<ListRoundsParams>,
) -> Json<Vec<RoundSummary>> {
    let rounds = if let Some(ref round_repo) = state.round_repo {
        round_repo
            .list(params.status.as_deref())
            .unwrap_or_default()
            .into_iter()
            .map(|r| RoundSummary {
                id: r.id,
                start_template_id: r.start_template_id,
                end_template_id: r.end_template_id,
                status: r.status,
                found_block_hash: r.found_block_hash,
            })
            .collect()
    } else {
        Vec::new()
    };

    Json(rounds)
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
    async fn test_list_rounds_empty() {
        let stats = ServerStats::default();
        let params = ListRoundsParams { status: None };
        let response = list_rounds(State(AppState {
            stats: Arc::new(RwLock::new(stats)),
            share_repo: None,
            worker_repo: None,
            round_repo: None,
        }), Query(params))
        .await;

        assert!(response.is_empty());
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
        let params = ListRoundsParams { status: None };
        let response = list_rounds(State(AppState {
            stats: Arc::new(RwLock::new(stats)),
            share_repo: None,
            worker_repo: None,
            round_repo: Some(round_repo),
        }), Query(params))
        .await;

        assert_eq!(response.len(), 1);
        assert_eq!(response[0].status, "open");
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
        let params = ListRoundsParams { status: Some("open".to_string()) };
        let open_rounds = list_rounds(State(AppState {
            stats: Arc::new(RwLock::new(stats.clone())),
            share_repo: None,
            worker_repo: None,
            round_repo: Some(round_repo.clone()),
        }), Query(params))
        .await;
        assert_eq!(open_rounds.len(), 1);
        assert_eq!(open_rounds[0].start_template_id, 6);

        // Filter by 'found'
        let params = ListRoundsParams { status: Some("found".to_string()) };
        let found_rounds = list_rounds(State(AppState {
            stats: Arc::new(RwLock::new(stats)),
            share_repo: None,
            worker_repo: None,
            round_repo: Some(round_repo),
        }), Query(params))
        .await;
        assert_eq!(found_rounds.len(), 1);
        assert_eq!(found_rounds[0].start_template_id, 1);
    }
}
