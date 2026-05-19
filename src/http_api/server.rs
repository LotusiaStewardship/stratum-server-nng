use axum::{
    extract::State,
    http::StatusCode,
    response::Json,
    routing::get,
    Router,
};
use serde::Serialize;
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Clone)]
pub struct AppState {
    pub stats: Arc<RwLock<ServerStats>>,
}

#[derive(Clone, Default, Serialize)]
pub struct ServerStats {
    pub uptime_secs: u64,
    pub connected_miners: u64,
    pub total_shares: i64,
    pub accepted_shares: i64,
    pub rejected_shares: i64,
}

#[derive(Serialize)]
pub struct HealthResponse {
    pub status: String,
    pub uptime_secs: u64,
    pub connected_miners: u64,
}

#[derive(Serialize)]
pub struct StatsResponse {
    pub total_shares: i64,
    pub accepted_shares: i64,
    pub rejected_shares: i64,
    pub accepted_pct: f64,
}

pub fn create_router(state: AppState) -> Router {
    Router::new()
        .route("/api/v1/health", get(health_handler))
        .route("/api/v1/stats", get(stats_handler))
        .with_state(state)
}

async fn health_handler(State(state): State<AppState>) -> Json<HealthResponse> {
    let stats = state.stats.read().await;
    Json(HealthResponse {
        status: "ok".to_string(),
        uptime_secs: stats.uptime_secs,
        connected_miners: stats.connected_miners,
    })
}

async fn stats_handler(State(state): State<AppState>) -> Json<StatsResponse> {
    let stats = state.stats.read().await;
    let accepted_pct = if stats.total_shares > 0 {
        (stats.accepted_shares as f64 / stats.total_shares as f64) * 100.0
    } else {
        0.0
    };

    Json(StatsResponse {
        total_shares: stats.total_shares,
        accepted_shares: stats.accepted_shares,
        rejected_shares: stats.rejected_shares,
        accepted_pct,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_health_response() {
        let stats = ServerStats {
            uptime_secs: 100,
            connected_miners: 5,
            ..Default::default()
        };

        let response = health_handler(State(AppState {
            stats: Arc::new(RwLock::new(stats)),
        }))
        .await;

        assert_eq!(response.status, "ok");
        assert_eq!(response.uptime_secs, 100);
        assert_eq!(response.connected_miners, 5);
    }

    #[tokio::test]
    async fn test_stats_response() {
        let stats = ServerStats {
            total_shares: 100,
            accepted_shares: 95,
            rejected_shares: 5,
            ..Default::default()
        };

        let response = stats_handler(State(AppState {
            stats: Arc::new(RwLock::new(stats)),
        }))
        .await;

        assert_eq!(response.total_shares, 100);
        assert_eq!(response.accepted_shares, 95);
        assert_eq!(response.rejected_shares, 5);
        assert!((response.accepted_pct - 95.0).abs() < 0.01);
    }

    #[tokio::test]
    async fn test_stats_response_zero_shares() {
        let stats = ServerStats::default();

        let response = stats_handler(State(AppState {
            stats: Arc::new(RwLock::new(stats)),
        }))
        .await;

        assert_eq!(response.total_shares, 0);
        assert_eq!(response.accepted_pct, 0.0);
    }
}
