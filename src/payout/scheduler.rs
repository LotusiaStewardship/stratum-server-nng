use anyhow::Result;
use tracing::{error, info, warn};

use bitcoinsuite_core::Hashed;

use crate::{
    accounting::AccountingDb,
    config::Config,
    payout::{build_pplns_payout_plan, WeightedShare},
};

const COINBASE_MATURITY_BLOCKS: u32 = 100;
const DEFAULT_GROSS_REWARD_SAT: i64 = 1_300_000_000;

pub async fn run_payout_scheduler(cfg: Config, db: AccountingDb) -> Result<()> {
    if cfg.pool.signing.mode != "internal" {
        warn!(mode = %cfg.pool.signing.mode, "payout scheduler disabled: only internal signer mode is currently supported");
        return Ok(());
    }

    let interval = std::time::Duration::from_secs(cfg.pool.pplns.payout_interval_secs.max(30));
    loop {
        tokio::time::sleep(interval).await;

        if let Err(err) = db.advance_found_block_confirmations(cfg.pool.pplns.min_confirmations.max(COINBASE_MATURITY_BLOCKS)) {
            error!(error = %err, "failed advancing found block confirmations");
            continue;
        }

        let Some((found_block_id, block_hash)) = db.take_next_matured_found_block()? else {
            continue;
        };

        let shares = db
            .list_weighted_shares_for_pplns(20_000)?
            .into_iter()
            .map(|(payout_address, work_units)| WeightedShare {
                payout_address,
                work_units,
            })
            .collect::<Vec<_>>();

        let fee_address = cfg.pool.fee.fee_address.as_deref();
        let plan = build_pplns_payout_plan(
            DEFAULT_GROSS_REWARD_SAT,
            if cfg.pool.fee.enabled { cfg.pool.fee.fee_bps } else { 0 },
            fee_address,
            &shares,
            cfg.pool.pplns.min_payout_sat,
        );

        let retry_key = format!("{}:{}", block_hash, plan.outputs.len());
        let batch_id = db.create_payout_batch(
            &retry_key,
            plan.gross_reward_sat,
            plan.fee_sat,
            plan.net_reward_sat,
            &plan.outputs,
        )?;

        db.update_payout_batch_state(batch_id, "signed", Some("internal:simulated"), None, None)?;
        // first-draft submit path: mark as submitted + confirmed deterministically
        let simulated_txid = hex::encode(bitcoinsuite_core::Sha256::digest(retry_key.into_bytes().into()).as_ref());
        db.update_payout_batch_state(batch_id, "submitted", None, Some(&simulated_txid), None)?;
        db.update_payout_batch_state(batch_id, "confirmed", None, Some(&simulated_txid), None)?;
        db.mark_found_block_paid(found_block_id)?;

        info!(batch_id, found_block_id, block_hash, outputs = plan.outputs.len(), txid = %simulated_txid, "payout batch lifecycle completed");
    }
}
