//! GET /payouts - Payout history page

use askama::Template;
use axum::{extract::State, response::IntoResponse};

use crate::http::{AppState, PublicDb};

struct FooterCtx {
    pool_name: String,
    pool_fee_bps: String,
    contact: String,
}

struct PayoutRow {
    id: String,
    method: String,
    status: String,
    txid: String,
    created_at: String,
}

#[derive(Template)]
#[template(path = "http/payouts.html")]
struct PayoutsPage {
    footer: FooterCtx,
    payouts: Vec<PayoutRow>,
}

pub async fn payouts_page(State(state): State<AppState>) -> impl IntoResponse {
    let db = PublicDb::new(state.db.clone(), state.stats.clone());
    let payouts = db.list_payout_batches(50).unwrap_or_default();

    let fee_str = if state.pool_config.fee.enabled {
        state.pool_config.fee.fee_bps.to_string()
    } else {
        String::from("0")
    };

    let template = PayoutsPage {
        footer: FooterCtx {
            pool_name: state.pool_config.name.clone(),
            pool_fee_bps: fee_str,
            contact: String::new(),
        },
        payouts: payouts
            .into_iter()
            .map(|p| PayoutRow {
                id: p.id.to_string(),
                method: p.method,
                status: p.status,
                txid: p.submitted_txid.map(|t| {
                    if t.len() > 20 {
                        format!("{}...", &t[..20])
                    } else {
                        t
                    }
                }).unwrap_or_else(|| String::from("—")),
                created_at: p.created_at.to_string(),
            })
            .collect(),
    };

    template.into_response()
}
