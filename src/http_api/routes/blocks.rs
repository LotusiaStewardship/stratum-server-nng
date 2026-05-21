use axum::{
    extract::{Path, Query, State},
    response::Json,
};
use serde::{Deserialize, Serialize};
use crate::http_api::server::AppState;

#[derive(Serialize)]
pub struct BlockSummary {
    pub id: i64,
    pub round_id: i64,
    pub block_hash: String,
    pub height: i64,
    pub status: String,
    pub coinbase_value: i64,
    pub network_target_hex: String,
}

#[derive(Deserialize)]
pub struct ListBlocksParams {
    pub status: Option<String>,
}

#[derive(Serialize)]
pub struct BlockDetail {
    pub id: i64,
    pub round_id: i64,
    pub block_hash: String,
    pub height: i64,
    pub status: String,
    pub worker_id: Option<i64>,
    pub template_id: Option<i64>,
    pub persist_source: Option<String>,
    pub orphan_reason: Option<String>,
    pub coinbase_value: i64,
    pub network_target_hex: String,
}

pub async fn list_blocks(
    State(state): State<AppState>,
    Query(params): Query<ListBlocksParams>,
) -> Json<Vec<BlockSummary>> {
    let blocks = if let Some(ref repo) = state.found_block_repo {
        repo.list(params.status.as_deref())
            .unwrap_or_default()
            .into_iter()
            .map(|b| BlockSummary {
                id: b.id,
                round_id: b.round_id,
                block_hash: b.block_hash,
                height: b.height,
                status: b.status,
                coinbase_value: b.coinbase_value,
                network_target_hex: b.network_target_hex.clone(),
            })
            .collect()
    } else {
        Vec::new()
    };

    Json(blocks)
}

pub async fn get_block(
    State(state): State<AppState>,
    Path(hash): Path<String>,
) -> Json<Option<BlockDetail>> {
    let block = match state.found_block_repo.as_ref() {
        Some(repo) => match repo.get_by_hash(&hash) {
            Ok(Some(b)) => Some(BlockDetail {
                id: b.id,
                round_id: b.round_id,
                block_hash: b.block_hash,
                height: b.height,
                status: b.status,
                worker_id: b.worker_id,
                template_id: b.template_id,
                persist_source: b.persist_source,
                orphan_reason: b.orphan_reason,
                coinbase_value: b.coinbase_value,
                network_target_hex: b.network_target_hex.clone(),
            }),
            _ => None,
        },
        None => None,
    };

    Json(block)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::accounting::{init_schema, FoundBlockRepository};
    use crate::http_api::ServerStats;
    use parking_lot::Mutex;
    use rusqlite::Connection;
    use std::sync::Arc;
    use tempfile::NamedTempFile;
    use tokio::sync::RwLock;

    fn test_state(repo: Option<FoundBlockRepository>) -> AppState {
        AppState {
            stats: Arc::new(RwLock::new(ServerStats::default())),
            share_repo: None,
            worker_repo: None,
            round_repo: None,
            found_block_repo: repo,
            payout_repo: None,
            accounting_service: None,
            payout_config: None,
            api_token: "test".to_string(),
        }
    }

    fn setup_db() -> (Connection, FoundBlockRepository) {
        let f = NamedTempFile::new().unwrap();
        let conn = Connection::open(f.path()).unwrap();
        init_schema(&conn).unwrap();
        // Create a round for foreign key
        conn.execute(
            "INSERT INTO rounds (id, start_template_id, status) VALUES (1, 42, 'open')",
            [],
        ).unwrap();
        // Create a worker for foreign key (used by test_get_block_by_hash with worker_id=42)
        conn.execute(
            "INSERT INTO workers (id, payout_address) VALUES (42, 'lotus_test')",
            [],
        ).unwrap();
        let repo = FoundBlockRepository::new(Arc::new(Mutex::new(conn)));
        (Connection::open(f.path()).unwrap(), repo)
    }

    #[tokio::test]
    async fn test_list_blocks_empty() {
        let state = test_state(None);
        let params = ListBlocksParams { status: None };
        let response = list_blocks(State(state), Query(params)).await;
        assert!(response.is_empty());
    }

    #[tokio::test]
    async fn test_list_blocks_with_data() {
        let (_conn, repo) = setup_db();
        repo.record_found_block(1, "block1", 100, None, None, None, 0, "").unwrap();
        repo.record_found_block(1, "block2", 101, None, None, None, 0, "").unwrap();

        let state = test_state(Some(repo));
        let params = ListBlocksParams { status: None };
        let response = list_blocks(State(state), Query(params)).await;
        assert_eq!(response.len(), 2);
        assert_eq!(response[0].block_hash, "block2"); // DESC order
        assert_eq!(response[1].block_hash, "block1");
    }

    #[tokio::test]
    async fn test_list_blocks_by_status() {
        let (_conn, repo) = setup_db();
        repo.record_found_block(1, "block1", 100, None, None, None, 0, "").unwrap();
        repo.record_found_block(1, "block2", 101, None, None, None, 0, "").unwrap();
        repo.mark_orphaned("block1", "reorg").unwrap();

        let state = test_state(Some(repo));

        let params = ListBlocksParams { status: Some("confirmed".to_string()) };
        let confirmed = list_blocks(State(state.clone()), Query(params)).await;
        assert_eq!(confirmed.len(), 1);
        assert_eq!(confirmed[0].block_hash, "block2");

        let params = ListBlocksParams { status: Some("orphaned".to_string()) };
        let orphaned = list_blocks(State(state), Query(params)).await;
        assert_eq!(orphaned.len(), 1);
        assert_eq!(orphaned[0].block_hash, "block1");
    }

    #[tokio::test]
    async fn test_get_block_by_hash() {
        let (_conn, repo) = setup_db();
        repo.record_found_block(1, "blockhash123", 500, Some(42), Some(100), Some("json-rpc"), 0, "").unwrap();

        let state = test_state(Some(repo));
        let response = get_block(State(state), Path("blockhash123".to_string())).await;

        let detail = response.0.expect("block should be found");
        assert_eq!(detail.block_hash, "blockhash123");
        assert_eq!(detail.height, 500);
        assert_eq!(detail.status, "confirmed");
        assert_eq!(detail.worker_id, Some(42));
        assert_eq!(detail.persist_source, Some("json-rpc".to_string()));
    }

    #[tokio::test]
    async fn test_get_block_not_found() {
        let (_conn, repo) = setup_db();
        let state = test_state(Some(repo));
        let response = get_block(State(state), Path("nonexistent".to_string())).await;
        assert!(response.0.is_none());
    }
}
