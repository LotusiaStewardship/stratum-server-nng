//! GET /api/rounds - List recent rounds

use axum::{extract::{Query, State}, Json};
use crate::http::{AppState, PublicDb};
use tracing::{error, info};

#[derive(Debug, serde::Deserialize)]
pub struct RoundsQuery {
    #[serde(default = "default_limit")]
    limit: u32,
}

fn default_limit() -> u32 {
    50
}

pub async fn list_rounds(
    State(state): State<AppState>,
    Query(query): Query<RoundsQuery>,
) -> Json<Vec<crate::http::models::RoundStats>> {
    info!(limit = query.limit, "API request: GET /api/rounds");
    let db = PublicDb::new(state.db, state.stats.clone());
    match db.list_recent_rounds(query.limit) {
        Ok(rounds) => {
            info!(count = rounds.len(), "rounds retrieved");
            Json(rounds)
        }
        Err(e) => {
            error!(error = %e, "failed to list rounds");
            Json(vec![])
        }
    }
}
