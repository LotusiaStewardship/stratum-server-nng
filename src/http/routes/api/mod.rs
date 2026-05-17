//! API Routes - JSON endpoints

mod stats;
mod workers;
mod rounds;
mod blocks;
mod payouts;
mod health;

use axum::{routing::get, Router};
use crate::http::AppState;

pub fn api_routes() -> Router<AppState> {
    Router::new()
        .route("/stats", get(stats::get_stats))
        .route("/workers", get(workers::list_workers))
        .route("/miner/:address", get(workers::get_miner))
        .route("/rounds", get(rounds::list_rounds))
        .route("/blocks", get(blocks::list_blocks))
        .route("/payouts", get(payouts::list_payouts))
        .route("/health", get(health::health_check))
}
