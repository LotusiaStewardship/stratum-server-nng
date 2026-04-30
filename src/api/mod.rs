use crate::accounting::AccountingDb;
use crate::stratum::server::RuntimeStats;
use anyhow::Result;
use axum::{
    extract::State,
    http::{header, HeaderMap, StatusCode},
    response::IntoResponse,
    routing::get,
    Json, Router,
};
use serde::Serialize;
use tokio::net::TcpListener;

#[derive(Clone)]
struct ApiState {
    token: String,
    db: AccountingDb,
    stats: std::sync::Arc<RuntimeStats>,
}

#[derive(Serialize)]
struct StatusResp<'a> {
    status: &'a str,
    payout_method: Option<String>,
    idle_disconnects: u64,
    rate_limit_disconnects: u64,
    template_payout_mismatch_total: u64,
    candidate_payout_mismatch_total: u64,
    found_block_persist_ok_total: u64,
    found_block_persist_error_total: u64,
    found_block_observed_not_persisted_total: u64,
}

fn check_auth(headers: &HeaderMap, token: &str) -> bool {
    let Some(value) = headers.get(header::AUTHORIZATION) else {
        return false;
    };
    let Ok(value) = value.to_str() else {
        return false;
    };
    value == format!("Bearer {token}")
}

async fn healthz() -> impl IntoResponse {
    (StatusCode::OK, "ok")
}

async fn readyz(State(state): State<ApiState>) -> impl IntoResponse {
    if state.db.active_payout_method().is_ok() {
        (StatusCode::OK, "ready")
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, "db_unavailable")
    }
}

async fn status(State(state): State<ApiState>, headers: HeaderMap) -> impl IntoResponse {
    if !check_auth(&headers, &state.token) {
        return (StatusCode::UNAUTHORIZED, "unauthorized").into_response();
    }
    let payout_method = state.db.active_payout_method().ok().flatten();
    let snap = state.stats.snapshot();
    tracing::info!(
        idle_disconnects = snap.idle_disconnects,
        rate_limit_disconnects = snap.rate_limit_disconnects,
        "operator status requested"
    );
    Json(StatusResp {
        status: "ok",
        payout_method,
        idle_disconnects: snap.idle_disconnects,
        rate_limit_disconnects: snap.rate_limit_disconnects,
        template_payout_mismatch_total: snap.template_payout_mismatch_total,
        candidate_payout_mismatch_total: snap.candidate_payout_mismatch_total,
        found_block_persist_ok_total: snap.found_block_persist_ok_total,
        found_block_persist_error_total: snap.found_block_persist_error_total,
        found_block_observed_not_persisted_total: snap.found_block_observed_not_persisted_total,
    })
    .into_response()
}

async fn workers(State(state): State<ApiState>, headers: HeaderMap) -> impl IntoResponse {
    if !check_auth(&headers, &state.token) {
        return (StatusCode::UNAUTHORIZED, "unauthorized").into_response();
    }
    match state.db.list_workers(100) {
        Ok(v) => Json(v).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

async fn rounds(State(state): State<ApiState>, headers: HeaderMap) -> impl IntoResponse {
    if !check_auth(&headers, &state.token) {
        return (StatusCode::UNAUTHORIZED, "unauthorized").into_response();
    }
    match state.db.list_recent_rounds(100) {
        Ok(v) => Json(v).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

async fn recent_shares(State(state): State<ApiState>, headers: HeaderMap) -> impl IntoResponse {
    if !check_auth(&headers, &state.token) {
        return (StatusCode::UNAUTHORIZED, "unauthorized").into_response();
    }
    match state.db.list_recent_shares(100) {
        Ok(v) => Json(v).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

async fn payouts(State(state): State<ApiState>, headers: HeaderMap) -> impl IntoResponse {
    if !check_auth(&headers, &state.token) {
        return (StatusCode::UNAUTHORIZED, "unauthorized").into_response();
    }
    match state.db.list_recent_payout_batches(100) {
        Ok(v) => Json(v).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

pub async fn start_operator_api(
    bind: String,
    token: String,
    db: AccountingDb,
    stats: std::sync::Arc<RuntimeStats>,
) -> Result<()> {
    let state = ApiState { token, db, stats };
    let app = Router::new()
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz))
        .route("/status", get(status))
        .route("/workers", get(workers))
        .route("/rounds", get(rounds))
        .route("/shares", get(recent_shares))
        .route("/payouts", get(payouts))
        .with_state(state);

    let listener = TcpListener::bind(&bind).await?;
    axum::serve(listener, app).await?;
    Ok(())
}
