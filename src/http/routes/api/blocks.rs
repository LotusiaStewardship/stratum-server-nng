//! GET /api/blocks - List found blocks

use axum::{extract::{Query, State}, Json};
use crate::http::{AppState, PublicDb};
use tracing::{error, info};

#[derive(Debug, serde::Deserialize)]
pub struct BlocksQuery {
    #[serde(default = "default_limit")]
    limit: u32,
    status: Option<String>,
}

fn default_limit() -> u32 {
    50
}

pub async fn list_blocks(
    State(state): State<AppState>,
    Query(query): Query<BlocksQuery>,
) -> Json<Vec<crate::http::models::BlockInfo>> {
    info!(
        limit = query.limit,
        status = ?query.status,
        "API request: GET /api/blocks"
    );
    let db = PublicDb::new(state.db);
    match db.list_found_blocks(query.limit, query.status.as_deref()) {
        Ok(blocks) => {
            info!(count = blocks.len(), "blocks retrieved");
            Json(blocks)
        }
        Err(e) => {
            error!(error = %e, "failed to list blocks");
            Json(vec![])
        }
    }
}
