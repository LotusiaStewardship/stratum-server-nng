//! GET /miners - Miner leaderboard page

use askama::Template;
use axum::{extract::State, response::IntoResponse};

use crate::http::{AppState, PublicDb};

struct FooterCtx {
    pool_name: String,
    pool_fee_bps: String,
    contact: String,
}

struct WorkerRow {
    id: i64,
    payout_address: String,
    payout_address_short: String,
    worker_suffix: String,
    shares_accepted: String,
    shares_rejected: String,
    shares_stale: String,
    blocks_found: String,
}

#[derive(Template)]
#[template(path = "http/miners.html")]
struct MinersPage {
    footer: FooterCtx,
    workers: Vec<WorkerRow>,
}

pub async fn miners_page(State(state): State<AppState>) -> impl IntoResponse {
    let db = PublicDb::new(state.db.clone(), state.stats.clone());
    let workers = db.list_workers(200, 0).unwrap_or_default();

    let fee_str = if state.pool_config.fee.enabled {
        state.pool_config.fee.fee_bps.to_string()
    } else {
        String::from("0")
    };

    let template = MinersPage {
        footer: FooterCtx {
            pool_name: state.pool_config.name.clone(),
            pool_fee_bps: fee_str,
            contact: String::new(),
        },
        workers: workers
            .into_iter()
            .map(|w| WorkerRow {
                id: w.id,
                payout_address: w.payout_address.clone(),
                payout_address_short: if w.payout_address.len() > 24 {
                    format!("{}...", &w.payout_address[..24])
                } else {
                    w.payout_address
                },
                worker_suffix: w.worker_suffix.unwrap_or_else(|| String::from("—")),
                shares_accepted: w.shares_accepted.to_string(),
                shares_rejected: w.shares_rejected.to_string(),
                shares_stale: w.shares_stale.to_string(),
                blocks_found: w.blocks_found.to_string(),
            })
            .collect(),
    };

    template.into_response()
}
