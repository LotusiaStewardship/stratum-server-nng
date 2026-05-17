//! GET / - Home dashboard page

use askama::Template;
use axum::{extract::State, response::IntoResponse};

use crate::http::{AppState, PublicDb};

struct FooterCtx {
    pool_name: String,
    pool_fee_bps: String,
    contact: String,
}

#[derive(Template)]
#[template(path = "http/home.html")]
struct HomePage {
    footer: FooterCtx,
    pool_hashrate: String,
    active_miners: String,
    network_difficulty: String,
    blocks_found_24h: String,
    blocks: Vec<BlockRow>,
    workers: Vec<WorkerRow>,
}

struct BlockRow {
    height: String,
    hash: String,
    status: String,
    found_by: String,
    found_at: String,
}

struct WorkerRow {
    payout_address: String,
    payout_address_short: String,
    worker_suffix: String,
    shares_accepted: String,
    shares_rejected: String,
    blocks_found: String,
}

pub async fn home_page(State(state): State<AppState>) -> impl IntoResponse {
    let db = PublicDb::new(state.db.clone());

    // Get network difficulty directly from DifficultyCache (live from NNG MiningTemplate)
    let network_difficulty = state.diff_cache.network_diff();

    let stats = db.get_pool_stats(network_difficulty).unwrap_or_default();
    let blocks = db.list_found_blocks(10, None).unwrap_or_default();
    let workers = db.list_workers(10, 0).unwrap_or_default();

    let fee_str = if state.pool_config.fee.enabled {
        state.pool_config.fee.fee_bps.to_string()
    } else {
        String::from("0")
    };

    let template = HomePage {
        footer: FooterCtx {
            pool_name: state.pool_config.name.clone(),
            pool_fee_bps: fee_str,
            contact: String::new(),
        },
        pool_hashrate: format_hashrate(stats.pool_hashrate),
        active_miners: stats.active_miners.to_string(),
        network_difficulty: format_difficulty(stats.network_difficulty),
        blocks_found_24h: stats.blocks_found_24h.to_string(),
        blocks: blocks
            .into_iter()
            .map(|b| BlockRow {
                height: b.height.to_string(),
                hash: short_hash(&b.hash),
                status: b.status,
                found_by: b.found_by.unwrap_or_else(|| String::from("Unknown")),
                found_at: b.found_at.to_string(),
            })
            .collect(),
        workers: workers
            .into_iter()
            .map(|w| WorkerRow {
                payout_address: w.payout_address.clone(),
                payout_address_short: short_address(&w.payout_address),
                worker_suffix: w.worker_suffix.unwrap_or_else(|| String::from("—")),
                shares_accepted: w.shares_accepted.to_string(),
                shares_rejected: w.shares_rejected.to_string(),
                blocks_found: w.blocks_found.to_string(),
            })
            .collect(),
    };

    template.into_response()
}

fn format_hashrate(h: f64) -> String {
    if h >= 1e12 {
        format!("{:.2} TH/s", h / 1e12)
    } else if h >= 1e9 {
        format!("{:.2} GH/s", h / 1e9)
    } else if h >= 1e6 {
        format!("{:.2} MH/s", h / 1e6)
    } else if h >= 1e3 {
        format!("{:.2} KH/s", h / 1e3)
    } else {
        format!("{:.2} H/s", h)
    }
}

fn format_difficulty(d: f64) -> String {
    if d >= 1e12 {
        format!("{:.2} T", d / 1e12)
    } else if d >= 1e9 {
        format!("{:.2} G", d / 1e9)
    } else if d >= 1e6 {
        format!("{:.2} M", d / 1e6)
    } else {
        format!("{:.2}", d)
    }
}

fn short_hash(h: &str) -> String {
    if h.len() > 16 {
        format!("{}...", &h[..16])
    } else {
        h.to_string()
    }
}

fn short_address(a: &str) -> String {
    if a.len() > 20 {
        format!("{}...", &a[..20])
    } else {
        a.to_string()
    }
}
