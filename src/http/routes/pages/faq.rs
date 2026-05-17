//! GET /faq - FAQ page

use askama::Template;
use axum::{extract::State, response::IntoResponse};

use crate::http::AppState;

struct FooterCtx {
    pool_name: String,
    pool_fee_bps: String,
    contact: String,
}

#[derive(Template)]
#[template(path = "http/faq.html")]
struct FaqPage {
    footer: FooterCtx,
    n_multiplier: String,
    min_confirmations: String,
}

pub async fn faq_page(State(state): State<AppState>) -> impl IntoResponse {
    let fee_str = if state.pool_config.fee.enabled {
        state.pool_config.fee.fee_bps.to_string()
    } else {
        String::from("0")
    };

    let template = FaqPage {
        footer: FooterCtx {
            pool_name: state.pool_config.name.clone(),
            pool_fee_bps: fee_str,
            contact: String::new(),
        },
        n_multiplier: state.pool_config.pplns.n_multiplier.to_string(),
        min_confirmations: state.pool_config.pplns.min_confirmations.to_string(),
    };

    template.into_response()
}
