use axum::{
    routing::get,
    Router,
};
use serde::Serialize;
use std::sync::Arc;
use tokio::sync::RwLock;
use crate::accounting::ShareRepository;

#[derive(Clone)]
pub struct AppState {
    pub stats: Arc<RwLock<ServerStats>>,
    pub share_repo: Option<ShareRepository>,
}

#[derive(Clone, Default, Serialize)]
pub struct ServerStats {
    pub uptime_secs: u64,
    pub connected_miners: u64,
    pub total_shares: i64,
    pub accepted_shares: i64,
    pub rejected_shares: i64,
    pub network_difficulty: Option<String>,
}

pub fn create_router(state: AppState) -> Router {
    Router::new()
        .route("/api/v1/health", get(crate::http_api::routes::health_handler))
        .route("/api/v1/stats", get(crate::http_api::routes::stats_handler))
        .with_state(state)
}
