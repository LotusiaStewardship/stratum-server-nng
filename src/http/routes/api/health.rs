//! GET /api/health - Health check

use axum::{extract::State, Json};
use crate::http::{AppState, PublicDb};
use tracing::{error, info};

pub async fn health_check(State(state): State<AppState>) -> Json<crate::http::models::HealthResponse> {
    info!("API request: GET /api/health");
    let db = PublicDb::new(state.db, state.stats.clone());
    match db.health_check() {
        Ok(health) => {
            info!(status = %health.status, "health check passed");
            Json(health)
        }
        Err(e) => {
            error!(error = %e, "health check failed");
            Json(crate::http::models::HealthResponse {
                status: "error".to_string(),
                database: "error".to_string(),
                last_share: None,
            })
        }
    }
}
