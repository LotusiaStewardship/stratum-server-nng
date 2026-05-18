//! GET /blocks - Block finder history page

use askama::Template;
use axum::{extract::State, response::IntoResponse};

use crate::http::{AppState, PublicDb};

struct FooterCtx {
    pool_name: String,
    pool_fee_bps: String,
    contact: String,
}

struct BlockRow {
    height: String,
    hash: String,
    status: String,
    confirmations: String,
    found_by: String,
    found_at: String,
}

#[derive(Template)]
#[template(path = "http/blocks.html")]
struct BlocksPage {
    footer: FooterCtx,
    blocks: Vec<BlockRow>,
}

pub async fn blocks_page(State(state): State<AppState>) -> impl IntoResponse {
    let db = PublicDb::new(state.db.clone(), state.stats.clone());
    let blocks = db.list_found_blocks(50, None).unwrap_or_default();

    let fee_str = if state.pool_config.fee.enabled {
        state.pool_config.fee.fee_bps.to_string()
    } else {
        String::from("0")
    };

    let template = BlocksPage {
        footer: FooterCtx {
            pool_name: state.pool_config.name.clone(),
            pool_fee_bps: fee_str,
            contact: String::new(),
        },
        blocks: blocks
            .into_iter()
            .map(|b| BlockRow {
                height: b.height.to_string(),
                hash: if b.hash.len() > 20 {
                    format!("{}...", &b.hash[..20])
                } else {
                    b.hash
                },
                status: b.status,
                confirmations: b.confirmations.to_string(),
                found_by: b.found_by.unwrap_or_else(|| String::from("Unknown")),
                found_at: b.found_at.to_string(),
            })
            .collect(),
    };

    template.into_response()
}
