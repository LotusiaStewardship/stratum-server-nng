use crate::accounting::AccountingDb;
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
}

#[derive(Serialize)]
struct StatusResp<'a> {
    status: &'a str,
    payout_method: Option<String>,
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

async fn status(State(state): State<ApiState>, headers: HeaderMap) -> impl IntoResponse {
    if !check_auth(&headers, &state.token) {
        return (StatusCode::UNAUTHORIZED, "unauthorized").into_response();
    }
    let payout_method = state.db.active_payout_method().ok().flatten();
    Json(StatusResp {
        status: "ok",
        payout_method,
    })
    .into_response()
}

async fn recent_shares(State(state): State<ApiState>, headers: HeaderMap) -> impl IntoResponse {
    if !check_auth(&headers, &state.token) {
        return (StatusCode::UNAUTHORIZED, "unauthorized").into_response();
    }
    match state.db.list_recent_shares(50) {
        Ok(v) => Json(v).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

pub async fn start_operator_api(bind: String, token: String, db: AccountingDb) -> Result<()> {
    let state = ApiState { token, db };
    let app = Router::new()
        .route("/status", get(status))
        .route("/shares", get(recent_shares))
        .with_state(state);

    let listener = TcpListener::bind(&bind).await?;
    axum::serve(listener, app).await?;
    Ok(())
}
