//! GET /api/workers - List workers
//! GET /api/miner/:address - Get miner details by payout address

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use crate::http::{AppState, PublicDb};
use tracing::{error, info};

/// Query parameters for /api/workers
#[derive(Debug, serde::Deserialize)]
pub struct WorkersQuery {
    #[serde(default = "default_limit")]
    limit: u32,
    #[serde(default)]
    offset: u32,
}

fn default_limit() -> u32 {
    100
}

/// GET /api/workers
pub async fn list_workers(
    State(state): State<AppState>,
    Query(query): Query<WorkersQuery>,
) -> Json<Vec<crate::http::models::WorkerStats>> {
    info!(
        limit = query.limit,
        offset = query.offset,
        "API request: GET /api/workers"
    );
    let db = PublicDb::new(state.db);
    match db.list_workers(query.limit, query.offset) {
        Ok(workers) => {
            info!(count = workers.len(), "workers retrieved");
            Json(workers)
        }
        Err(e) => {
            error!(error = %e, "failed to list workers");
            Json(vec![])
        }
    }
}

/// GET /api/miner/:address
pub async fn get_miner(
    State(state): State<AppState>,
    Path(address): Path<String>,
) -> impl IntoResponse {
    info!(address = %address, "API request: GET /api/miner/:address");
    let db = PublicDb::new(state.db);
    match db.get_worker_by_address(&address) {
        Ok(Some(miner)) => {
            info!(
                workers = miner.workers.len(),
                "miner details retrieved"
            );
            Json(miner).into_response()
        }
        Ok(None) => {
            info!("miner not found");
            (StatusCode::NOT_FOUND, "miner not found").into_response()
        }
        Err(e) => {
            error!(error = %e, "failed to get miner details");
            (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response()
        }
    }
}
