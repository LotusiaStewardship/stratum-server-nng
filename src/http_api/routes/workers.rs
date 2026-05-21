use axum::{
    extract::{Path, State},
    response::Json,
};
use serde::Serialize;
use crate::http_api::server::AppState;

#[derive(Serialize)]
pub struct WorkerSummary {
    pub id: i64,
    pub payout_address: String,
    pub worker_suffix: Option<String>,
    pub total_shares: i64,
}

#[derive(Serialize, Clone)]
pub struct WorkerDetail {
    pub id: i64,
    pub payout_address: String,
    pub worker_suffix: Option<String>,
    pub total_shares: i64,
}

pub async fn list_workers(State(state): State<AppState>) -> Json<Vec<WorkerSummary>> {
    let workers = if let Some(ref worker_repo) = state.worker_repo {
        worker_repo
            .list_all()
            .unwrap_or_default()
            .into_iter()
            .map(|w| {
                let count = state
                    .share_repo
                    .as_ref()
                    .map(|r| r.count_by_worker(w.id).unwrap_or(0))
                    .unwrap_or(0);
                WorkerSummary {
                    id: w.id,
                    payout_address: w.payout_address,
                    worker_suffix: w.worker_suffix,
                    total_shares: count,
                }
            })
            .collect()
    } else {
        Vec::new()
    };

    Json(workers)
}

pub async fn get_worker(
    State(state): State<AppState>,
    Path(id): Path<i64>,
) -> Json<Option<WorkerDetail>> {
    let result = state.worker_repo.as_ref().and_then(|repo| {
        repo.get_by_id(id).ok()?
    });

    match result {
        Some(worker) => {
            let count = state.share_repo.as_ref()
                .map(|r| r.count_by_worker(worker.id).unwrap_or(0))
                .unwrap_or(0);
            Json(Some(WorkerDetail {
                id: worker.id,
                payout_address: worker.payout_address,
                worker_suffix: worker.worker_suffix,
                total_shares: count,
            }))
        }
        None => Json(None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::accounting::{init_schema, WorkerRepository, ShareRepository};
    use crate::http_api::ServerStats;
    use parking_lot::Mutex;
    use rusqlite::Connection;
    use std::sync::Arc;
    use tempfile::NamedTempFile;
    use tokio::sync::RwLock;

    #[tokio::test]
    async fn test_list_workers_empty() {
        let stats = ServerStats::default();
        let response = list_workers(State(AppState {
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

        assert!(response.is_empty());
    }

    #[tokio::test]
    async fn test_list_workers_with_data() {
        let f = NamedTempFile::new().unwrap();
        let conn = Connection::open(f.path()).unwrap();
        init_schema(&conn).unwrap();
        let conn_arc = Arc::new(Mutex::new(conn));
        let worker_repo = WorkerRepository::new(conn_arc.clone());
        let share_repo = ShareRepository::new(conn_arc);

        // Create workers
        worker_repo.upsert("addr1", Some("rig1")).unwrap();
        worker_repo.upsert("addr2", Some("rig2")).unwrap();

        let stats = ServerStats::default();
        let response = list_workers(State(AppState {
            stats: Arc::new(RwLock::new(stats)),
            share_repo: Some(share_repo),
            worker_repo: Some(worker_repo),
            round_repo: None,
            found_block_repo: None,
            payout_repo: None,
            accounting_service: None,
            payout_config: None,
            api_token: "test".to_string(),
        }))
        .await;

        assert_eq!(response.len(), 2);
        assert_eq!(response[0].payout_address, "addr1");
        assert_eq!(response[1].payout_address, "addr2");
    }

    #[tokio::test]
    async fn test_get_worker_by_id() {
        let f = NamedTempFile::new().unwrap();
        let conn = Connection::open(f.path()).unwrap();
        init_schema(&conn).unwrap();
        let conn_arc = Arc::new(Mutex::new(conn));
        let worker_repo = WorkerRepository::new(conn_arc.clone());
        let share_repo = ShareRepository::new(conn_arc);

        let worker = worker_repo.upsert("addr1", Some("rig1")).unwrap();

        let stats = ServerStats::default();
        let response = get_worker(State(AppState {
            stats: Arc::new(RwLock::new(stats)),
            share_repo: Some(share_repo),
            worker_repo: Some(worker_repo),
            round_repo: None,
            found_block_repo: None,
            payout_repo: None,
            accounting_service: None,
            payout_config: None,
            api_token: "test".to_string(),
        }), Path(worker.id))
        .await;

        assert!(response.0.is_some());
        assert_eq!(response.0.unwrap().payout_address, "addr1");
    }

    #[tokio::test]
    async fn test_get_worker_not_found() {
        let f = NamedTempFile::new().unwrap();
        let conn = Connection::open(f.path()).unwrap();
        init_schema(&conn).unwrap();
        let conn_arc = Arc::new(Mutex::new(conn));
        let worker_repo = WorkerRepository::new(conn_arc.clone());
        let share_repo = ShareRepository::new(conn_arc);

        let stats = ServerStats::default();
        let response = get_worker(State(AppState {
            stats: Arc::new(RwLock::new(stats)),
            share_repo: Some(share_repo),
            worker_repo: Some(worker_repo),
            round_repo: None,
            found_block_repo: None,
            payout_repo: None,
            accounting_service: None,
            payout_config: None,
            api_token: "test".to_string(),
        }), Path(999))
        .await;

        assert!(response.0.is_none());
    }
}
