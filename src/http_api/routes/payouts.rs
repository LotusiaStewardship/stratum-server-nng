use axum::{
    extract::{Path, Query, State},
    Json,
};
use serde::Serialize;

use super::super::{AppState, AppError};

#[derive(Debug, Serialize)]
pub struct PayoutBatchResponse {
    pub id: i64,
    pub round_id: i64,
    pub status: String,
    pub total_amount: i64,
    pub pool_fee_amount: i64,
    pub pool_fee_address: Option<String>,
    pub miner_count: i64,
    pub retry_key: Option<String>,
    pub submitted_txid: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct PayoutDetailResponse {
    pub id: i64,
    pub worker_id: i64,
    pub payout_address: String,
    pub amount: i64,
    pub dust_carried_forward: i64,
}

#[derive(Debug, Serialize)]
pub struct PayoutBatchDetailResponse {
    pub batch: PayoutBatchResponse,
    pub payouts: Vec<PayoutDetailResponse>,
}

#[derive(Debug, serde::Deserialize)]
pub struct ListPayoutsParams {
    pub status: Option<String>,
}

/// GET /api/v1/payouts — list payout batches with optional ?status= filter.
pub async fn list_payouts(
    State(state): State<AppState>,
    Query(params): Query<ListPayoutsParams>,
) -> Result<Json<Vec<PayoutBatchResponse>>, AppError> {
    let repo = state.payout_repo.ok_or(AppError::DbNotConfigured)?;
    let batches = repo.list_batches(params.status.as_deref())?;
    let response: Vec<PayoutBatchResponse> = batches
        .into_iter()
        .map(|b| PayoutBatchResponse {
            id: b.id,
            round_id: b.round_id,
            status: b.status,
            total_amount: b.total_amount,
            pool_fee_amount: b.pool_fee_amount,
            pool_fee_address: b.pool_fee_address,
            miner_count: b.miner_count,
            retry_key: b.retry_key,
            submitted_txid: b.submitted_txid,
        })
        .collect();
    Ok(Json(response))
}

/// POST /api/v1/admin/payouts/trigger/{block_hash}
/// — Manually trigger payout calculation for a confirmed found block.
pub async fn trigger_payout(
    State(state): State<AppState>,
    Path(block_hash): Path<String>,
) -> Result<Json<serde_json::Value>, AppError> {
    let accounting = state
        .accounting_service
        .ok_or(AppError::DbNotConfigured)?;
    let config = state
        .payout_config
        .ok_or(AppError::Internal("payout config not loaded".to_string()))?;

    // Look up found block by hash
    let found_block = state
        .found_block_repo
        .ok_or(AppError::DbNotConfigured)?
        .get_by_hash(&block_hash)?
        .ok_or(AppError::NotFound(format!("found block {}", block_hash)))?;

    // Verify block is eligible
    if found_block.status == "orphaned" {
        return Err(AppError::BadRequest(
            "cannot trigger payout for orphaned block".to_string(),
        ));
    }
    if found_block.status == "paid" {
        return Err(AppError::BadRequest(
            "block already paid".to_string(),
        ));
    }

    // Calculate payout using the accounting service.
    // coinbase_value and network_target_hex are read from the found_block record,
    // which was populated from the MiningJob at the time the block was submitted.
    let batch_id = accounting.create_payout_for_found_block(
        &found_block,
        config.fee_bps,
        config.fee_address.as_deref(),
        config.min_payout_sat,
        config.n_multiplier,
    )?;

    // Mark the block as paid
    // TODO: update found_block status to 'paid' after successful payout

    Ok(Json(serde_json::json!({
        "batch_id": batch_id,
        "status": "pending",
        "message": "payout batch created, waiting for signing (Slice 9)",
    })))
}

/// GET /api/v1/payouts/{id} — batch details with miner payouts.
pub async fn get_payout(
    State(state): State<AppState>,
    Path(id): Path<i64>,
) -> Result<Json<PayoutBatchDetailResponse>, AppError> {
    let repo = state.payout_repo.ok_or(AppError::DbNotConfigured)?;
    let batch = repo
        .get_batch_by_id(id)?
        .ok_or(AppError::NotFound(format!("payout batch {}", id)))?;
    let payouts = repo.get_payouts_by_batch(id)?;

    Ok(Json(PayoutBatchDetailResponse {
        batch: PayoutBatchResponse {
            id: batch.id,
            round_id: batch.round_id,
            status: batch.status,
            total_amount: batch.total_amount,
            pool_fee_amount: batch.pool_fee_amount,
            pool_fee_address: batch.pool_fee_address,
            miner_count: batch.miner_count,
            retry_key: batch.retry_key,
            submitted_txid: batch.submitted_txid,
        },
        payouts: payouts
            .into_iter()
            .map(|p| PayoutDetailResponse {
                id: p.id,
                worker_id: p.worker_id,
                payout_address: p.payout_address,
                amount: p.amount,
                dust_carried_forward: p.dust_carried_forward,
            })
            .collect(),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::accounting::{schema::init_schema, PayoutRepository};
    use rusqlite::Connection;
    use std::sync::Arc;
    use tokio::sync::RwLock;
    use parking_lot::Mutex;
    
    fn setup_repo() -> PayoutRepository {
        let conn = Arc::new(Mutex::new(Connection::open_in_memory().unwrap()));
        let c = conn.lock();
        init_schema(&c).unwrap();
        std::mem::drop(c);
        PayoutRepository::new(conn)
    }

    #[tokio::test]
    async fn test_list_payouts_empty() {
        let repo = setup_repo();
        let state = AppState {
            stats: Arc::new(RwLock::new(crate::http_api::ServerStats::default())),
            share_repo: None,
            worker_repo: None,
            round_repo: None,
            found_block_repo: None,
            payout_repo: Some(repo),
            accounting_service: None,
            payout_config: None,
            api_token: "test".to_string(),
        };
        let result = list_payouts(State(state), Query(ListPayoutsParams { status: None }))
            .await
            .unwrap();
        assert!(result.0.is_empty());
    }
}
