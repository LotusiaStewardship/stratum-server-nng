use axum::{
    extract::State,
    response::Json,
};
use serde::Serialize;
use crate::http_api::server::AppState;

#[derive(Serialize)]
pub struct HealthResponse {
    pub status: String,
    pub uptime_secs: u64,
    pub connected_miners: u64,
}

pub async fn health_handler(State(state): State<AppState>) -> Json<HealthResponse> {
    let stats = state.stats.read().await;
    Json(HealthResponse {
        status: "ok".to_string(),
        uptime_secs: stats.uptime_secs,
        connected_miners: stats.connected_miners,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::http_api::ServerStats;
    use std::sync::Arc;
    use tokio::sync::RwLock;

    #[tokio::test]
    async fn test_health_response() {
        let stats = ServerStats {
            uptime_secs: 100,
            connected_miners: 5,
            ..Default::default()
        };

        let response = health_handler(State(AppState {
            stats: Arc::new(RwLock::new(stats)),
            share_repo: None,
        }))
        .await;

        assert_eq!(response.status, "ok");
        assert_eq!(response.uptime_secs, 100);
        assert_eq!(response.connected_miners, 5);
    }
}
