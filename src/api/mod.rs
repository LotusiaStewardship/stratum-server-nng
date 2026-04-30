use crate::accounting::{
    AccountingDb, FoundBlockStateSummary, MissingFoundBlock, PayoutBatchStateSummary,
    SchedulerHealthSummary,
};
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
    found_blocks: FoundBlockStateSummary,
    payout_batches: PayoutBatchStateSummary,
    latest_payout_batch_status: Option<String>,
    latest_payout_batch_txid: Option<String>,
    scheduler: SchedulerHealthSummary,
}

#[derive(Serialize)]
struct ReconciliationResp {
    missing_found_blocks: Vec<MissingFoundBlock>,
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
    let found_blocks = state
        .db
        .found_block_state_summary()
        .unwrap_or(FoundBlockStateSummary {
            pending: 0,
            matured: 0,
            orphaned: 0,
            paid: 0,
        });
    let latest_payout = state
        .db
        .list_recent_payout_batches(1)
        .ok()
        .and_then(|mut v| v.pop());
    let payout_batches = state
        .db
        .payout_batch_state_summary()
        .unwrap_or(PayoutBatchStateSummary {
            planned: 0,
            signed: 0,
            submitted: 0,
            confirmed: 0,
            invalidated_orphan: 0,
            failed: 0,
        });
    let scheduler = state
        .db
        .scheduler_health_summary()
        .unwrap_or(SchedulerHealthSummary {
            matured_found_blocks_ready: 0,
            retry_ready_batches: 0,
            next_retry_at: None,
        });

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
        found_blocks,
        payout_batches,
        latest_payout_batch_status: latest_payout.as_ref().map(|v| v.status.clone()),
        latest_payout_batch_txid: latest_payout
            .as_ref()
            .and_then(|v| v.submitted_txid.clone()),
        scheduler,
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

async fn reconciliation_missing_found_blocks(
    State(state): State<ApiState>,
    headers: HeaderMap,
) -> impl IntoResponse {
    if !check_auth(&headers, &state.token) {
        return (StatusCode::UNAUTHORIZED, "unauthorized").into_response();
    }
    match state.db.reconcile_missing_found_blocks_detail() {
        Ok(v) => Json(ReconciliationResp {
            missing_found_blocks: v,
        })
        .into_response(),
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
        .route(
            "/reconciliation/missing-found-blocks",
            get(reconciliation_missing_found_blocks),
        )
        .with_state(state);

    let listener = TcpListener::bind(&bind).await?;
    axum::serve(listener, app).await?;
    Ok(())
}
