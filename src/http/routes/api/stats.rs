//! GET /api/stats - Pool statistics

use axum::{extract::State, Json};
use crate::http::AppState;
use tracing::{error, info};

pub async fn get_stats(State(state): State<AppState>) -> Json<crate::http::models::PoolStats> {
    info!("API request: GET /api/stats");
    
    // Get network difficulty directly from DifficultyCache (live from NNG MiningTemplate)
    let network_difficulty = state.diff_cache.network_diff();
    
    // Use cached_db for consistent data with WebSocket broadcasts
    match state.cached_db.get_pool_stats(network_difficulty).await {
        Ok(stats) => {
            info!(
                active_miners = stats.active_miners,
                blocks_found = stats.blocks_found_total,
                network_difficulty = stats.network_difficulty,
                "pool stats retrieved"
            );
            Json(stats)
        }
        Err(e) => {
            error!(error = %e, "failed to get pool stats");
            Json(crate::http::models::PoolStats::default())
        }
    }
}
