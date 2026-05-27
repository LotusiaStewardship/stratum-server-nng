use crate::accounting::{
    AccountingService, FoundBlockRepository, PayoutRepository, RoundRepository, ShareRepository,
    WorkerRepository,
};
use axum::routing::{get, post};
use axum::{
    extract::{Request, State},
    http::StatusCode,
    middleware::{self, Next},
    response::{IntoResponse, Response},
    Router,
};
use serde::Serialize;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Payout configuration needed by the trigger endpoint.
#[derive(Clone, Debug)]
pub struct PayoutConfig {
    pub fee_bps: u32,
    pub fee_address: Option<String>,
    pub min_payout_sat: i64,
    pub n_multiplier: f64,
}

#[derive(Clone)]
pub struct AppState {
    pub stats: Arc<RwLock<ServerStats>>,
    pub share_repo: Option<ShareRepository>,
    pub worker_repo: Option<WorkerRepository>,
    pub round_repo: Option<RoundRepository>,
    pub found_block_repo: Option<FoundBlockRepository>,
    pub payout_repo: Option<PayoutRepository>,
    pub api_token: String,
    pub accounting_service: Option<AccountingService>,
    pub payout_config: Option<PayoutConfig>,
}

#[derive(Clone, Default, Serialize)]
pub struct ServerStats {
    pub uptime_secs: u64,
    pub connected_miners: u64,
    pub network_difficulty: Option<String>,
}

/// Authentication middleware that checks Bearer token from Authorization header.
/// Returns 401 Unauthorized if the token is missing or doesn't match.
pub async fn auth_middleware(State(state): State<AppState>, req: Request, next: Next) -> Response {
    let token = req
        .headers()
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .unwrap_or("");

    if token != state.api_token {
        return StatusCode::UNAUTHORIZED.into_response();
    }

    next.run(req).await
}

/// Errors that can be returned from API handlers.
#[derive(Debug)]
pub enum AppError {
    NotFound(String),
    BadRequest(String),
    DbNotConfigured,
    Internal(String),
}

impl axum::response::IntoResponse for AppError {
    fn into_response(self) -> Response {
        let (status, body) = match self {
            AppError::NotFound(msg) => (StatusCode::NOT_FOUND, msg),
            AppError::BadRequest(msg) => (StatusCode::BAD_REQUEST, msg),
            AppError::DbNotConfigured => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "database not configured".to_string(),
            ),
            AppError::Internal(msg) => (StatusCode::INTERNAL_SERVER_ERROR, msg),
        };
        (status, body).into_response()
    }
}

impl From<anyhow::Error> for AppError {
    fn from(e: anyhow::Error) -> Self {
        AppError::Internal(e.to_string())
    }
}

impl From<rusqlite::Error> for AppError {
    fn from(e: rusqlite::Error) -> Self {
        AppError::Internal(e.to_string())
    }
}

pub fn create_router(state: AppState) -> Router {
    // Public routes (no auth required)
    let public = Router::new().route(
        "/api/v1/health",
        get(crate::http_api::routes::health_handler),
    );

    // Protected routes (require Bearer token)
    let protected = Router::new()
        .route("/api/v1/stats", get(crate::http_api::routes::stats_handler))
        .route(
            "/api/v1/workers",
            get(crate::http_api::routes::list_workers),
        )
        .route(
            "/api/v1/workers/{id}",
            get(crate::http_api::routes::get_worker),
        )
        .route("/api/v1/rounds", get(crate::http_api::routes::list_rounds))
        .route(
            "/api/v1/rounds/{id}",
            get(crate::http_api::routes::get_round),
        )
        .route("/api/v1/blocks", get(crate::http_api::routes::list_blocks))
        .route(
            "/api/v1/blocks/{hash}",
            get(crate::http_api::routes::get_block),
        )
        .route(
            "/api/v1/payouts",
            get(crate::http_api::routes::list_payouts),
        )
        .route(
            "/api/v1/payouts/{id}",
            get(crate::http_api::routes::get_payout),
        )
        .route(
            "/api/v1/admin/payouts/trigger/{block_hash}",
            post(crate::http_api::routes::trigger_payout),
        )
        .route("/api/v1/shares", get(crate::http_api::routes::list_shares))
        .route(
            "/api/v1/share-outcomes",
            get(crate::http_api::routes::list_share_outcomes),
        )
        .layer(middleware::from_fn_with_state(
            state.clone(),
            auth_middleware,
        ));

    Router::new()
        .merge(public)
        .merge(protected)
        .with_state(state)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::SocketAddr;
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    use tokio::net::TcpStream;

    fn test_state(token: &str) -> AppState {
        AppState {
            stats: Arc::new(RwLock::new(ServerStats::default())),
            share_repo: None,
            worker_repo: None,
            round_repo: None,
            found_block_repo: None,
            payout_repo: None,
            api_token: token.to_string(),
            accounting_service: None,
            payout_config: None,
        }
    }

    async fn spawn_test_server(state: AppState) -> SocketAddr {
        let router = create_router(state);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let _ = axum::serve(listener, router).await;
        });
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        addr
    }

    async fn http_get(addr: SocketAddr, path: &str, token: Option<&str>) -> u16 {
        let stream = TcpStream::connect(addr).await.unwrap();
        let (reader, mut writer) = stream.into_split();
        let mut buf_reader = BufReader::new(reader);

        let request = match token {
            Some(t) => format!(
                "GET {} HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer {}\r\nConnection: close\r\n\r\n",
                path, t
            ),
            None => format!(
                "GET {} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
                path
            ),
        };
        writer.write_all(request.as_bytes()).await.unwrap();
        writer.flush().await.unwrap();

        // Read just the status line
        let mut status_line = String::new();
        buf_reader.read_line(&mut status_line).await.unwrap();

        let parts: Vec<&str> = status_line.split(' ').collect();
        let status_code: u16 = parts.get(1).and_then(|s| s.parse().ok()).unwrap_or(0);
        status_code
    }

    #[tokio::test]
    async fn test_auth_health_without_token() {
        let addr = spawn_test_server(test_state("secret")).await;
        let status = http_get(addr, "/api/v1/health", None).await;
        assert_eq!(status, 200, "health should not require auth");
    }

    #[tokio::test]
    async fn test_auth_stats_without_token() {
        let addr = spawn_test_server(test_state("secret")).await;
        let status = http_get(addr, "/api/v1/stats", None).await;
        assert_eq!(status, 401, "stats should require auth");
    }

    #[tokio::test]
    async fn test_auth_stats_with_wrong_token() {
        let addr = spawn_test_server(test_state("secret")).await;
        let status = http_get(addr, "/api/v1/stats", Some("wrong")).await;
        assert_eq!(status, 401, "wrong token should be rejected");
    }

    #[tokio::test]
    async fn test_auth_stats_with_correct_token() {
        let addr = spawn_test_server(test_state("secret")).await;
        let status = http_get(addr, "/api/v1/stats", Some("secret")).await;
        assert_eq!(status, 200, "correct token should be accepted");
    }
}
