//! GET /miner/:address - Individual miner detail page

use askama::Template;
use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
};

use crate::http::{AppState, PublicDb};

struct FooterCtx {
    pool_name: String,
    pool_fee_bps: String,
    contact: String,
}

struct WorkerRow {
    worker_suffix: String,
    shares_accepted: String,
    shares_rejected: String,
    shares_stale: String,
    blocks_found: String,
}

#[derive(Template)]
#[template(path = "http/miner_detail.html")]
struct MinerDetailPage {
    footer: FooterCtx,
    payout_address: String,
    total_shares_accepted: String,
    total_blocks_found: String,
    worker_count: String,
    workers: Vec<WorkerRow>,
}

pub async fn miner_detail_page(
    State(state): State<AppState>,
    Path(address): Path<String>,
) -> impl IntoResponse {
    let db = PublicDb::new(state.db.clone());

    match db.get_worker_by_address(&address) {
        Ok(Some(miner)) => {
            let fee_str = if state.pool_config.fee.enabled {
                state.pool_config.fee.fee_bps.to_string()
            } else {
                String::from("0")
            };

            let template = MinerDetailPage {
                footer: FooterCtx {
                    pool_name: state.pool_config.name.clone(),
                    pool_fee_bps: fee_str,
                    contact: String::new(),
                },
                payout_address: miner.payout_address,
                total_shares_accepted: miner.total_shares_accepted.to_string(),
                total_blocks_found: miner.total_blocks_found.to_string(),
                worker_count: miner.workers.len().to_string(),
                workers: miner
                    .workers
                    .into_iter()
                    .map(|w| WorkerRow {
                        worker_suffix: w.worker_suffix.unwrap_or_else(|| String::from("default")),
                        shares_accepted: w.shares_accepted.to_string(),
                        shares_rejected: w.shares_rejected.to_string(),
                        shares_stale: w.shares_stale.to_string(),
                        blocks_found: w.blocks_found.to_string(),
                    })
                    .collect(),
            };
            template.into_response()
        }
        Ok(None) => (StatusCode::NOT_FOUND, "Miner not found").into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}
