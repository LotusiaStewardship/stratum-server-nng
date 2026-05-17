//! GET /api/payouts - List payout batches

use axum::{extract::{Query, State}, Json};
use crate::http::{AppState, PublicDb};
use tracing::{error, info};

#[derive(Debug, serde::Deserialize)]
pub struct PayoutsQuery {
    #[serde(default = "default_limit")]
    limit: u32,
}

fn default_limit() -> u32 {
    50
}

pub async fn list_payouts(
    State(state): State<AppState>,
    Query(query): Query<PayoutsQuery>,
) -> Json<Vec<crate::http::models::PayoutInfo>> {
    info!(limit = query.limit, "API request: GET /api/payouts");
    let db = PublicDb::new(state.db);
    match db.list_payout_batches(query.limit) {
        Ok(payouts) => {
            info!(count = payouts.len(), "payouts retrieved");
            Json(payouts)
        }
        Err(e) => {
            error!(error = %e, "failed to list payouts");
            Json(vec![])
        }
    }
}
