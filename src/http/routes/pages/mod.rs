//! Page Routes - HTML pages (server-side rendered via Askama)

mod blocks;
mod faq;
mod home;
mod miner_detail;
mod miners;
mod payouts;

use axum::{routing::get, Router};
use crate::http::AppState;

pub fn page_routes() -> Router<AppState> {
    Router::new()
        .route("/", get(home::home_page))
        .route("/miners", get(miners::miners_page))
        .route("/miner/:address", get(miner_detail::miner_detail_page))
        .route("/blocks", get(blocks::blocks_page))
        .route("/payouts", get(payouts::payouts_page))
        .route("/faq", get(faq::faq_page))
}
