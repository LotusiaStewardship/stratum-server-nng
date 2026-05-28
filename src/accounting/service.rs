use crate::accounting_error;
use crate::accounting_info;
use crate::accounting_warn;
use crate::payout_info;
use anyhow::Result;
use parking_lot::Mutex;
use rusqlite::Connection;
use std::sync::Arc;

use super::{
    AccountingEvent, AccountingEventRepository, FoundBlock, FoundBlockRepository, PayoutRepository,
    Round, RoundRepository, Share, ShareOutcome, ShareRepository, WorkerRepository,
};

/// Facade that orchestrates accounting operations across multiple repositories.
///
/// Per UBQ §Accounting Service: owns multi-step accounting operations and
/// provides a single interface for share recording, round management, and
/// event tracking.
#[derive(Clone)]
pub struct AccountingService {
    pub share_repo: ShareRepository,
    pub worker_repo: WorkerRepository,
    pub round_repo: RoundRepository,
    pub event_repo: AccountingEventRepository,
    pub found_block_repo: FoundBlockRepository,
    pub payout_repo: PayoutRepository,
    conn: Arc<Mutex<Connection>>,
}

impl AccountingService {
    pub fn new(conn: Arc<Mutex<Connection>>) -> Self {
        Self {
            share_repo: ShareRepository::new(conn.clone()),
            worker_repo: WorkerRepository::new(conn.clone()),
            round_repo: RoundRepository::new(conn.clone()),
            event_repo: AccountingEventRepository::new(conn.clone()),
            found_block_repo: FoundBlockRepository::new(conn.clone()),
            payout_repo: PayoutRepository::new(conn.clone()),
            conn,
        }
    }

    /// Record a share submission with full accounting: worker upsert, round resolution,
    /// atomic share+outcome insert, and accounting event recording.
    ///
    /// Takes the already-parsed payout_address and optional worker_suffix
    /// (from `parse_worker_name`). Records accounting events automatically.
    ///
    /// Returns (share_id, outcome_id, round_id, dedupe_key) or (None, None, None, "")
    /// if the share was a duplicate (dedupe key collision).
    pub fn record_share(
        &self,
        payout_address: &str,
        worker_suffix: Option<&str>,
        session_id: &str,
        job_id: &str,
        template_id: u64,
        template_epoch: u64,
        extranonce1: &str,
        extranonce2: &str,
        ntime_hex: &str,
        nonce_hex: &str,
        difficulty: f64,
        status: &str,
        reject_reason: Option<&str>,
        low_diff_ok: bool,
        network_target_ok: bool,
        block_hash: Option<&str>,
    ) -> Result<(Option<i64>, Option<i64>, Option<i64>, String)> {
        // 1. Upsert worker
        let worker = self.worker_repo.upsert(payout_address, worker_suffix)?;

        // 2. Resolve round for this template (records round_opened event if new round created)
        let round = self.resolve_round_for_template(template_id)?;
        let round_id = Some(round.id);

        // 3. Build dedupe key
        let dedupe_key = ShareRepository::build_dedupe_key(
            worker.id,
            template_id,
            template_epoch,
            extranonce2,
            ntime_hex,
            nonce_hex,
        );

        // 4. Create raw share record
        let share = Share {
            id: 0,
            worker_id: worker.id,
            session_id: session_id.to_string(),
            job_id: job_id.to_string(),
            template_id,
            template_epoch,
            extranonce1: extranonce1.to_string(),
            extranonce2: extranonce2.to_string(),
            ntime_hex_6b: ntime_hex.to_string(),
            nonce_hex_8b: nonce_hex.to_string(),
            difficulty,
            dedupe_key: dedupe_key.clone(),
        };

        // 5. Create share outcome
        let outcome = ShareOutcome {
            id: 0,
            share_id: 0, // Will be set by atomic insert
            session_id: session_id.to_string(),
            worker_id: worker.id,
            job_id: job_id.to_string(),
            round_id,
            dedupe_key: dedupe_key.clone(),
            status: status.to_string(),
            reject_reason: reject_reason.map(|s| s.to_string()),
            node_result: None,
            low_diff_ok: Some(low_diff_ok),
            network_target_ok: Some(network_target_ok),
            block_hash: block_hash.map(|s| s.to_string()),
        };

        // 6. Insert atomically
        let (share_id, outcome_id, is_new) = self
            .share_repo
            .insert_share_and_outcome_atomic(&share, &outcome)?;

        // 7. Record accounting event (only for fresh inserts, not duplicates)
        if is_new {
            if let (Some(_sid), Some(_oid)) = (share_id, outcome_id) {
                // Reconstruct full worker name from payout_address and suffix
                let full_worker_name = match &worker.worker_suffix {
                    Some(suffix) => format!("{}.{}", worker.payout_address, suffix),
                    None => worker.payout_address.clone(),
                };
                let event = AccountingEvent {
                    id: 0,
                    event_type: "share_outcome".to_string(),
                    status: status.to_string(),
                    session_id: Some(session_id.to_string()),
                    worker_id: Some(worker.id),
                    worker_name: Some(full_worker_name),
                    payout_address: Some(worker.payout_address.clone()),
                    round_id,
                    template_id: Some(template_id),
                    template_epoch: Some(template_epoch),
                    job_id: Some(job_id.to_string()),
                    block_hash: block_hash.map(|s| s.to_string()),
                    height: None,
                    payload_json: None,
                };
                self.event_repo.record_event(&event)?;
            }
        }

        Ok((share_id, outcome_id, round_id, dedupe_key))
    }

    /// Get the current open round, creating one if needed.
    /// Records a `round_opened` accounting event when a new round is created.
    pub fn get_or_create_current_round(&self, start_template_id: u64) -> Result<Round> {
        let (round, is_new) = self
            .round_repo
            .get_or_create_current_round(start_template_id)?;
        if is_new {
            let event = AccountingEvent {
                id: 0,
                event_type: "round_opened".to_string(),
                status: "open".to_string(),
                session_id: None,
                worker_id: None,
                worker_name: None,
                payout_address: None,
                round_id: Some(round.id),
                template_id: Some(start_template_id),
                template_epoch: None,
                job_id: None,
                block_hash: None,
                height: None,
                payload_json: None,
            };
            let _ = self.event_repo.record_event(&event);
        }
        Ok(round)
    }

    /// Resolve which round a template belongs to.
    /// Records a `round_opened` accounting event when a new round is created.
    pub fn resolve_round_for_template(&self, template_id: u64) -> Result<Round> {
        let (round, is_new) = self.round_repo.resolve_round_for_template(template_id)?;
        if is_new {
            let event = AccountingEvent {
                id: 0,
                event_type: "round_opened".to_string(),
                status: "open".to_string(),
                session_id: None,
                worker_id: None,
                worker_name: None,
                payout_address: None,
                round_id: Some(round.id),
                template_id: Some(template_id),
                template_epoch: None,
                job_id: None,
                block_hash: None,
                height: None,
                payload_json: None,
            };
            let _ = self.event_repo.record_event(&event);
        }
        Ok(round)
    }

    /// Close a round. Records a `round_closed` accounting event.
    pub fn close_round(&self, id: i64, end_template_id: u64, status: &str) -> Result<()> {
        self.round_repo.close_round(id, end_template_id, status)?;
        let event = AccountingEvent {
            id: 0,
            event_type: "round_closed".to_string(),
            status: status.to_string(),
            session_id: None,
            worker_id: None,
            worker_name: None,
            payout_address: None,
            round_id: Some(id),
            template_id: Some(end_template_id),
            template_epoch: None,
            job_id: None,
            block_hash: None,
            height: None,
            payload_json: None,
        };
        let _ = self.event_repo.record_event(&event);
        Ok(())
    }

    /// Update the node_result field on a share_outcome after block submission.
    pub fn update_share_outcome_node_result(
        &self,
        dedupe_key: &str,
        node_result: &str,
    ) -> Result<usize> {
        self.share_repo
            .update_outcome_node_result(dedupe_key, node_result)
    }

    /// Record a found block after successful lotusd submission.
    pub fn record_found_block(
        &self,
        round_id: i64,
        block_hash: &str,
        height: i32,
        worker_id: Option<i64>,
        template_id: Option<u64>,
        persist_source: Option<&str>,
        coinbase_value: u64,
        network_target_hex: &str,
    ) -> Result<FoundBlock> {
        let block = self.found_block_repo.record_found_block(
            round_id,
            block_hash,
            height,
            worker_id,
            template_id,
            persist_source,
            coinbase_value,
            network_target_hex,
        )?;
        // Record accounting event
        let event = AccountingEvent {
            id: 0,
            event_type: "found_block_observed".to_string(),
            status: "confirmed".to_string(),
            session_id: None,
            worker_id,
            worker_name: None,
            payout_address: None,
            round_id: Some(round_id),
            template_id,
            template_epoch: None,
            job_id: None,
            block_hash: Some(block_hash.to_string()),
            height: Some(height),
            payload_json: None,
        };
        let _ = self.event_repo.record_event(&event);
        Ok(block)
    }

    /// Record an accounting event.
    pub fn record_event(&self, event: &AccountingEvent) -> Result<i64> {
        self.event_repo.record_event(event)
    }

    /// Calculate PPLNS payout for a found block and create the payout batch atomically.
    ///
    /// 1. Reads coinbase_value and network_target_hex from the found_block record
    /// 2. Looks up the found_at timestamp from the block's share_outcome
    /// 3. Calculates the PPLNS window using the converted network difficulty
    /// 4. Builds the payout plan with fee and dust
    /// 5. Creates the payout batch, payouts, snapshots, and updates dust balances
    /// 6. Records accounting events
    ///
    /// Returns the PayoutBatch ID.
    pub fn create_payout_for_found_block(
        &self,
        found_block: &FoundBlock,
        fee_bps: u32,
        fee_address: Option<&str>,
        min_payout_sat: i64,
        n_multiplier: f64,
    ) -> Result<i64> {
        use crate::payout::plan::build_payout_plan;
        use crate::payout::pplns::calculate_pplns_window;
        use crate::share_processing::network_target_hex_to_difficulty;

        // NNG raw coinbase_value (u64) → CAmount/i64 (lotusd monetary domain).
        // Safety: Lotus coinbase values are < 10^12 satoshis, well below i64::MAX.
        // The coinbase_value is the total block reward (subsidy + fees), but the
        // miner only receives the portion remaining after minerfund deduction.
        // See payout::reward_from_coinbase for the mirror of lotusd's split logic.
        let gross_reward = crate::payout::reward_from_coinbase(found_block.coinbase_value);
        let network_difficulty =
            network_target_hex_to_difficulty(&found_block.network_target_hex).unwrap_or(1.0);
        if found_block.coinbase_value == 0 {
            accounting_warn!(
                hash = %found_block.block_hash,
                "create_payout_for_found_block: coinbase_value is 0 — block reward may be unset",
            );
        }

        let conn = self.conn.lock();

        // 2. Find the found_at timestamp — the share_outcome that found this block.
        // Query by block_hash from share_outcomes for this found block.
        let found_at: Option<String> = {
            let mut stmt = conn.prepare(
                "SELECT so.created_at
                 FROM share_outcomes so
                 WHERE so.block_hash = ?1
                   AND so.status = 'accepted'
                   AND so.network_target_ok = 1
                 LIMIT 1",
            )?;
            let result = stmt.query_row(rusqlite::params![&found_block.block_hash], |row| {
                row.get::<_, String>(0)
            });
            match result {
                Ok(ts) => Some(ts),
                Err(rusqlite::Error::QueryReturnedNoRows) => None,
                Err(e) => return Err(e.into()),
            }
        };

        let found_at = match found_at {
            Some(ts) => ts,
            None => {
                anyhow::bail!(
                    "no accepted share_outcome with network_target_ok=1 for block hash {}",
                    found_block.block_hash,
                );
            }
        };

        // 2. Calculate PPLNS window
        let shares = calculate_pplns_window(&conn, &found_at, n_multiplier, network_difficulty)?;

        // 3. Read existing dust balances
        let dust_balances: Vec<(String, i64)> = {
            let mut stmt = conn.prepare("SELECT payout_address, balance FROM dust_balances")?;
            let rows = stmt.query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
            })?;
            let mut balances = Vec::new();
            for row in rows {
                balances.push(row?);
            }
            balances
        };

        // 4. Build payout plan
        let plan = build_payout_plan(
            found_block.round_id,
            found_block.height,
            &found_block.block_hash,
            network_difficulty,
            gross_reward,
            fee_bps,
            fee_address,
            min_payout_sat,
            &dust_balances,
            &shares,
        );

        // 5. Guard: reject if a payout batch already exists for this block_hash.
        // Uses retry_key prefix match (same pattern as the scheduler).
        // Prevents duplicate batches regardless of caller.
        {
            let mut stmt =
                conn.prepare("SELECT COUNT(*) FROM payout_batches WHERE retry_key LIKE ?1")?;
            let existing: i64 = stmt.query_row(
                rusqlite::params![format!("{}%", found_block.block_hash)],
                |row| row.get(0),
            )?;
            if existing > 0 {
                anyhow::bail!(
                    "payout batch already exists for block hash {}",
                    found_block.block_hash,
                );
            }
        }

        // 6. Atomically create batch, payouts, snapshots, and update dust.
        // All SQL is inlined through the already-locked `conn` rather than calling
        // repository methods (which would try to re-lock and deadlock).
        conn.execute_batch("BEGIN TRANSACTION")?;

        let result = (|| -> Result<i64> {
            // Create payout batch
            conn.execute(
                "INSERT INTO payout_batches
                 (round_id, total_amount, pool_fee_amount, pool_fee_address, miner_count, retry_key, status)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'pending')",
                rusqlite::params![
                    found_block.round_id,
                    plan.gross_reward,
                    plan.pool_fee_amount,
                    plan.pool_fee_address.as_deref(),
                    plan.outputs.len() as i64,
                    &plan.retry_key,
                ],
            )?;
            let batch_id = conn.last_insert_rowid();

            // Record individual payouts and build snapshots
            let mut snapshots = Vec::new();
            let mut payout_stmt = conn.prepare(
                "INSERT INTO payouts (batch_id, worker_id, payout_address, amount, dust_carried_forward)
                 VALUES (?1, ?2, ?3, ?4, ?5)"
            )?;
            for output in &plan.outputs {
                // Record payout (even zero-amount dust entries)
                payout_stmt.execute(rusqlite::params![
                    batch_id,
                    output.worker_id,
                    &output.payout_address,
                    output.amount,
                    output.dust_carried_forward,
                ])?;

                // Snapshot ALL miner window shares for audit trail (including dusted)
                for share in &shares {
                    if share.payout_address == output.payout_address {
                        snapshots.push(crate::accounting::PayoutShareSnapshot {
                            id: 0,
                            batch_id,
                            share_id: share.share_id,
                            share_outcome_id: share.share_outcome_id,
                            payout_address: share.payout_address.clone(),
                            work_units: share.difficulty,
                            share_created_at: share.created_at.clone(),
                        });
                    }
                }
            }

            // Insert snapshots
            if !snapshots.is_empty() {
                let mut snap_stmt = conn.prepare(
                    "INSERT INTO payout_share_snapshots
                     (batch_id, share_id, share_outcome_id, payout_address, work_units, share_created_at)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6)"
                )?;
                for snap in &snapshots {
                    snap_stmt.execute(rusqlite::params![
                        snap.batch_id,
                        snap.share_id,
                        snap.share_outcome_id,
                        &snap.payout_address,
                        snap.work_units,
                        &snap.share_created_at,
                    ])?;
                }
            }

            // Update dust balances
            let outputs_by_addr: std::collections::BTreeMap<&str, i64> = plan
                .outputs
                .iter()
                .map(|o| (o.payout_address.as_str(), o.dust_carried_forward))
                .collect();
            let mut dust_upsert = conn.prepare(
                "INSERT INTO dust_balances (payout_address, balance)
                 VALUES (?1, ?2)
                 ON CONFLICT(payout_address) DO UPDATE SET balance = balance + ?2, updated_at = CURRENT_TIMESTAMP"
            )?;
            for (addr, dust) in &outputs_by_addr {
                if *dust > 0 {
                    dust_upsert.execute(rusqlite::params![addr, dust])?;
                }
            }

            // Record accounting event
            conn.execute(
                "INSERT INTO accounting_events (event_type, status, round_id, block_hash, height)
                 VALUES ('payout_batch_created', 'pending', ?1, ?2, ?3)",
                rusqlite::params![
                    found_block.round_id,
                    &found_block.block_hash,
                    found_block.height,
                ],
            )?;

            Ok(batch_id)
        })();

        match result {
            Ok(id) => {
                conn.execute_batch("COMMIT")?;
                Ok(id)
            }
            Err(e) => {
                conn.execute_batch("ROLLBACK")?;
                Err(e)
            }
        }
    }

    /// Rebuild a payout batch's miner payouts and share snapshots from the
    /// original PPLNS window data. This is a recovery path for cases where
    /// payouts were accidentally deleted from the DB.
    ///
    /// The rebuild re-runs `calculate_pplns_window` and `build_payout_plan`,
    /// then atomically replaces the batch's payouts and snapshots with fresh
    /// data. The batch status must be `pending` — already-submitted batches
    /// cannot be rebuilt (their tx is on-chain).
    ///
    /// Uses current `dust_balances`, which may include dust carried forward
    /// from the original plan. This means the miner receives a negligibly
    /// larger weight (a few extra satoshis) — an acceptable overpayment.
    pub fn rebuild_payout_plan_for_batch(
        &self,
        batch_id: i64,
        fee_bps: u32,
        fee_address: Option<&str>,
        min_payout_sat: i64,
        n_multiplier: f64,
    ) -> Result<Vec<crate::payout::plan::PayoutOutput>> {
        use crate::payout::plan::build_payout_plan;
        use crate::payout::pplns::calculate_pplns_window;
        use crate::share_processing::network_target_hex_to_difficulty;

        // 1. Load batch — verify status
        let batch = self
            .payout_repo
            .get_batch_by_id(batch_id)?
            .ok_or_else(|| anyhow::anyhow!("batch {} not found", batch_id))?;
        if batch.status != "pending" {
            anyhow::bail!(
                "batch {} status is '{}', only pending batches can be rebuilt",
                batch_id,
                batch.status,
            );
        }

        // 2. Load found_block
        let found_block = self
            .found_block_repo
            .get_by_round_id(batch.round_id)?
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "no found_block for round {}",
                    batch.round_id
                )
            })?;

        // 3. Compute gross_reward and network_difficulty
        let gross_reward = crate::payout::reward_from_coinbase(found_block.coinbase_value);
        let network_difficulty =
            network_target_hex_to_difficulty(&found_block.network_target_hex).unwrap_or(1.0);

        let conn = self.conn.lock();

        // 4. Find the found_at timestamp
        let found_at: String = {
            let mut stmt = conn.prepare(
                "SELECT so.created_at
                 FROM share_outcomes so
                 WHERE so.block_hash = ?1
                   AND so.status = 'accepted'
                   AND so.network_target_ok = 1
                 LIMIT 1",
            )?;
            stmt.query_row(rusqlite::params![&found_block.block_hash], |row| {
                row.get::<_, String>(0)
            })
            .map_err(|e| {
                anyhow::anyhow!(
                    "no accepted share_outcome with network_target_ok=1 for block hash {}: {}",
                    found_block.block_hash,
                    e,
                )
            })?
        };

        // 5. Recalculate PPLNS window
        let shares = calculate_pplns_window(&conn, &found_at, n_multiplier, network_difficulty)?;
        if shares.is_empty() {
            anyhow::bail!(
                "PPLNS window is empty for round {} — cannot rebuild payout plan",
                found_block.round_id,
            );
        }

        // 6. Read dust balances
        let dust_balances: Vec<(String, i64)> = {
            let mut stmt = conn.prepare(
                "SELECT payout_address, balance FROM dust_balances",
            )?;
            let rows = stmt.query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
            })?;
            rows.collect::<std::result::Result<Vec<_>, _>>()?
        };

        // 7. Build plan
        let plan = build_payout_plan(
            found_block.round_id,
            found_block.height,
            &found_block.block_hash,
            network_difficulty,
            gross_reward,
            fee_bps,
            fee_address,
            min_payout_sat,
            &dust_balances,
            &shares,
        );

        if plan.outputs.is_empty() {
            anyhow::bail!(
                "rebuild produced empty outputs for batch {} — cannot proceed",
                batch_id,
            );
        }

        // 8. Atomically: delete old + insert new
        conn.execute_batch("BEGIN TRANSACTION")?;
        let result = (|| -> Result<Vec<crate::payout::plan::PayoutOutput>> {
            // Delete old payouts and snapshots
            conn.execute(
                "DELETE FROM payouts WHERE batch_id = ?1",
                rusqlite::params![batch_id],
            )?;
            conn.execute(
                "DELETE FROM payout_share_snapshots WHERE batch_id = ?1",
                rusqlite::params![batch_id],
            )?;

            // Re-insert payouts and build snapshots
            let mut snapshots = Vec::new();
            let mut payout_stmt = conn.prepare(
                "INSERT INTO payouts (batch_id, worker_id, payout_address, amount, dust_carried_forward)
                 VALUES (?1, ?2, ?3, ?4, ?5)",
            )?;
            for output in &plan.outputs {
                payout_stmt.execute(rusqlite::params![
                    batch_id,
                    output.worker_id,
                    &output.payout_address,
                    output.amount,
                    output.dust_carried_forward,
                ])?;

                for share in &shares {
                    if share.payout_address == output.payout_address {
                        snapshots.push(crate::accounting::PayoutShareSnapshot {
                            id: 0,
                            batch_id,
                            share_id: share.share_id,
                            share_outcome_id: share.share_outcome_id,
                            payout_address: share.payout_address.clone(),
                            work_units: share.difficulty,
                            share_created_at: share.created_at.clone(),
                        });
                    }
                }
            }

            // Insert snapshots
            if !snapshots.is_empty() {
                let mut snap_stmt = conn.prepare(
                    "INSERT INTO payout_share_snapshots
                     (batch_id, share_id, share_outcome_id, payout_address, work_units, share_created_at)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                )?;
                for snap in &snapshots {
                    snap_stmt.execute(rusqlite::params![
                        snap.batch_id,
                        snap.share_id,
                        snap.share_outcome_id,
                        &snap.payout_address,
                        snap.work_units,
                        &snap.share_created_at,
                    ])?;
                }
            }

            // Update dust balances (same ON CONFLICT pattern as create_payout_for_found_block)
            let outputs_by_addr: std::collections::BTreeMap<&str, i64> = plan
                .outputs
                .iter()
                .map(|o| (o.payout_address.as_str(), o.dust_carried_forward))
                .collect();
            let mut dust_upsert = conn.prepare(
                "INSERT INTO dust_balances (payout_address, balance)
                 VALUES (?1, ?2)
                 ON CONFLICT(payout_address) DO UPDATE SET balance = balance + ?2, updated_at = CURRENT_TIMESTAMP",
            )?;
            for (addr, dust) in &outputs_by_addr {
                if *dust > 0 {
                    dust_upsert.execute(rusqlite::params![addr, dust])?;
                }
            }

            Ok(plan.outputs)
        })();

        match result {
            Ok(outputs) => {
                conn.execute_batch("COMMIT")?;
                payout_info!(batch_id = batch_id, "rebuilt payout plan with {} outputs", outputs.len());
                Ok(outputs)
            }
            Err(e) => {
                conn.execute_batch("ROLLBACK")?;
                Err(e)
            }
        }
    }

    /// Process pending payout batches through the configured signer.
    pub async fn process_pending_payouts(
        &self,
        signer: &dyn crate::payout::signer::Signer,
        rpc_client: &crate::node_integration::JsonRpcClient,
        // Payout config params needed for auto-rebuild when payouts are missing:
        fee_bps: u32,
        fee_address: Option<String>,
        min_payout_sat: i64,
        n_multiplier: f64,
    ) -> Result<()> {
        use crate::payout::plan::PayoutOutput;
        use crate::payout::signer::SignedBatchData;

        let pending = self.payout_repo.list_batches(Some("pending"))?;

        for batch in &pending {
            let Some(found_block) = self.found_block_repo.get_by_round_id(batch.round_id)? else {
                accounting_warn!(
                    round_id = batch.round_id,
                    "no found_block for pending batch"
                );
                continue;
            };

            // Resolve coinbase txid
            let coinbase_txid = match &found_block.coinbase_txid {
                Some(txid) => txid.clone(),
                None => match rpc_client.get_block(&found_block.block_hash).await {
                    Ok(block_data) => {
                        let tx_list = match block_data["tx"].as_array() {
                            Some(txs) => txs,
                            None => {
                                accounting_warn!(hash = %found_block.block_hash, "getblock missing 'tx'");
                                continue;
                            }
                        };
                        let cb_txid = match tx_list.first() {
                            Some(tx_entry) => tx_entry
                                .as_object()
                                .and_then(|o| o.get("txid").and_then(|v| v.as_str()))
                                .or_else(|| tx_entry.as_str())
                                .unwrap_or(""),
                            None => {
                                accounting_warn!(hash = %found_block.block_hash, "empty tx list");
                                continue;
                            }
                        };
                        if cb_txid.is_empty() {
                            continue;
                        }
                        let _ = self
                            .found_block_repo
                            .update_coinbase_txid(found_block.id, cb_txid);
                        cb_txid.to_string()
                    }
                    Err(e) => {
                        accounting_warn!(hash = %found_block.block_hash, error = %e, "getblock failed");
                        continue;
                    }
                },
            };

            // Fetch coinbase output details. Lotus: vout[0] = OP_RETURN metadata,
            // so find first non-OP_RETURN spendable output.
            let (coinbase_amount, coinbase_vout, coinbase_script) = match rpc_client
                .get_raw_transaction(&coinbase_txid)
                .await
            {
                Ok(raw_tx) => {
                    let vouts = match raw_tx["vout"].as_array() {
                        Some(v) => v,
                        None => {
                            accounting_warn!(txid = %coinbase_txid, "no vout");
                            continue;
                        }
                    };
                    let spendable = vouts.iter().enumerate().find(|(_idx, vout)| {
                        let is_nulldata = vout["scriptPubKey"]["type"].as_str() == Some("nulldata");
                        let value = vout["value"].as_f64().unwrap_or(0.0);
                        !is_nulldata && value > 0.0
                    });
                    let (vout_idx, vout) = match spendable {
                        Some(v) => v,
                        None => {
                            tracing::warn!(txid = %coinbase_txid, "no spendable vout");
                            continue;
                        }
                    };
                    let value_xpi = vout["value"].as_f64().unwrap_or(0.0);
                    let script = vout["scriptPubKey"]["hex"]
                        .as_str()
                        .unwrap_or("")
                        .to_string();
                    let amount_sat = (value_xpi * crate::constants::SATS_PER_XPI_F64) as i64;
                    (amount_sat, vout_idx as u32, script)
                }
                Err(e) => {
                    tracing::warn!(txid = %coinbase_txid, error = %e, "getrawtransaction failed");
                    continue;
                }
            };

            let payouts = match self.payout_repo.get_payouts_by_batch(batch.id) {
                Ok(p) => p,
                Err(e) => {
                    tracing::warn!(batch_id = batch.id, error = %e, "load payouts failed");
                    continue;
                }
            };

            let outputs: Vec<PayoutOutput> = if payouts.is_empty() {
                payout_info!(
                    batch_id = batch.id,
                    "payouts table empty for batch — rebuilding from PPLNS window",
                );
                match self.rebuild_payout_plan_for_batch(
                    batch.id,
                    fee_bps,
                    fee_address.as_deref(),
                    min_payout_sat,
                    n_multiplier,
                ) {
                    Ok(out) => out,
                    Err(e) => {
                        accounting_warn!(
                            batch_id = batch.id,
                            error = %e,
                            "rebuild failed — skipping batch",
                        );
                        continue;
                    }
                }
            } else {
                payouts
                    .iter()
                    .map(|p| PayoutOutput {
                        payout_address: p.payout_address.clone(),
                        worker_id: p.worker_id,
                        amount: p.amount,
                        dust_carried_forward: p.dust_carried_forward,
                    })
                    .collect()
            };

            let plan = crate::payout::plan::PayoutPlan {
                round_id: batch.round_id,
                block_height: found_block.height,
                block_hash: found_block.block_hash.clone(),
                network_difficulty: 0.0,
                total_work_units: 0.0,
                gross_reward: batch.total_amount,
                pool_fee_amount: batch.pool_fee_amount,
                pool_fee_address: batch.pool_fee_address.clone(),
                outputs,
                dust_carried_forward_total: 0,
                retry_key: batch.retry_key.clone().unwrap_or_default(),
            };

            let signed_data = SignedBatchData {
                plan,
                coinbase_txid: coinbase_txid.clone(),
                coinbase_vout,
                coinbase_amount,
                coinbase_script_pubkey_hex: coinbase_script,
            };

            match signer.sign_and_submit(&signed_data).await {
                Ok(txid) => {
                    let _ = self.payout_repo.mark_batch_submitted(batch.id, &txid);
                    let _ = self.found_block_repo.update_status(found_block.id, "paid");
                    accounting_info!(batch_id = batch.id, txid = %txid, "payout submitted");
                }
                Err(e) => {
                    accounting_warn!(batch_id = batch.id, error = %e, "sign/submit failed (will retry)");
                }
            }
        }
        Ok(())
    }

    /// Reconcile confirmed found_blocks against the current chain state at startup.
    ///
    /// For each confirmed block:
    /// 1. If `block.height > tip_height` → orphan with reason "block_not_found"
    /// 2. If `block.height <= tip_height` → fetch canonical hash at that height
    /// 3. If `canonical_hash != stored_hash` → orphan with reason "reorg_detected"
    ///
    /// `get_block_hash` is an async closure that queries the node (e.g., via JSON-RPC getblockhash).
    /// It receives a height and returns `Ok(Some(hash))` if the block exists at that height,
    /// `Ok(None)` if the height is above tip, or `Err` if the query failed.
    pub async fn reconcile_found_blocks<F, Fut>(
        &self,
        tip_height: i32,
        get_block_hash: F,
    ) -> Result<()>
    where
        F: Fn(i32) -> Fut,
        Fut: std::future::Future<Output = Result<Option<String>>>,
    {
        let confirmed = self.found_block_repo.list(Some("immature"))?;

        for block in &confirmed {
            if block.height > tip_height {
                accounting_warn!(
                    hash = %block.block_hash,
                    height = block.height,
                    tip = tip_height,
                    "found_block above chain tip at startup — orphaning",
                );
                if let Err(e) = self
                    .found_block_repo
                    .mark_orphaned(&block.block_hash, "block_not_found")
                {
                    accounting_error!(error = %e, "failed to orphan block above tip");
                }
                if let Err(e) = self.close_round(block.round_id, 0, "orphaned") {
                    accounting_error!(error = %e, "failed to close orphaned round");
                }
                continue;
            }

            // Block height <= tip — fetch the canonical hash at this height
            let canonical_hash = match get_block_hash(block.height).await {
                Ok(Some(hash)) => hash,
                Ok(None) => {
                    // Height exists but no block at this height — unlikely but handle gracefully
                    accounting_warn!(
                        hash = %block.block_hash,
                        height = block.height,
                        "no canonical block at height during reconciliation",
                    );
                    continue;
                }
                Err(e) => {
                    accounting_warn!(
                        error = %e,
                        height = block.height,
                        "failed to fetch canonical block hash during reconciliation",
                    );
                    continue;
                }
            };

            if canonical_hash != block.block_hash {
                accounting_warn!(
                    stored = %block.block_hash,
                    canonical = %canonical_hash,
                    height = block.height,
                    "found_block hash mismatch at startup — orphaning (reorg detected)",
                );
                if let Err(e) = self
                    .found_block_repo
                    .mark_orphaned(&block.block_hash, "reorg_detected")
                {
                    accounting_error!(error = %e, "failed to orphan reorged block");
                }
                if let Err(e) = self.close_round(block.round_id, 0, "orphaned") {
                    accounting_error!(error = %e, "failed to close orphaned round");
                }
            }
        }

        Ok(())
    }

    /// Check all immature found blocks for maturation.
    ///
    /// For each immature block, compute confirmations as
    /// `tip_height - block.height + 1`. If confirmations >= `min_confirmations`,
    /// promote the block to `matured` status.
    ///
    /// Returns the list of newly matured blocks so the caller can send
    /// maturation events to the payout handler.
    pub fn check_maturation(
        &self,
        tip_height: i32,
        min_confirmations: u64,
    ) -> Result<Vec<FoundBlock>> {
        let immature = self.found_block_repo.list(Some("immature"))?;
        let mut matured = Vec::new();
        for block in &immature {
            let confirms = tip_height - block.height + 1;
            if confirms >= min_confirmations as i32 {
                self.found_block_repo.mark_matured(block.id)?;
                accounting_info!(
                    hash = %block.block_hash,
                    height = block.height,
                    confirms = confirms,
                    threshold = min_confirmations,
                    "block matured",
                );
                matured.push(block.clone());
            }
        }
        Ok(matured)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::accounting::schema::init_schema;
    use tempfile::NamedTempFile;

    #[test]
    fn test_record_share_full_accounting() {
        let f = NamedTempFile::new().unwrap();
        let conn = Connection::open(f.path()).unwrap();
        init_schema(&conn).unwrap();
        let svc = AccountingService::new(Arc::new(Mutex::new(conn)));

        let (share_id, outcome_id, round_id, _dedupe) = svc
            .record_share(
                "lotus_16PSJNf1EDEfGvaYzaXJCJZrXH4pgiTo7kyW61iGi",
                Some("rig1"),
                "sess-1",
                "job-42-100",
                42,
                100,
                "00000001",
                "00112233",
                "001122334455",
                "0011223344556677",
                1.0,
                "accepted",
                None,
                true,
                false,
                None,
            )
            .unwrap();

        assert!(share_id.is_some(), "share should be persisted");
        assert!(outcome_id.is_some(), "outcome should be persisted");
        assert!(round_id.is_some(), "round should be resolved");
        assert_eq!(round_id, Some(1));

        // Verify accounting event was recorded
        let events = svc.event_repo.list_by_type("share_outcome", 10, 0).unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].status, "accepted");
        assert_eq!(events[0].worker_id, Some(1));
        assert_eq!(events[0].round_id, round_id);
    }

    #[test]
    fn test_record_share_rejected_still_persisted() {
        let f = NamedTempFile::new().unwrap();
        let conn = Connection::open(f.path()).unwrap();
        init_schema(&conn).unwrap();
        let svc = AccountingService::new(Arc::new(Mutex::new(conn)));

        let (share_id, outcome_id, round_id, _dedupe) = svc
            .record_share(
                "lotus_16PSJNf1EDEfGvaYzaXJCJZrXH4pgiTo7kyW61iGi",
                None,
                "sess-2",
                "job-42-100",
                42,
                100,
                "00000001",
                "00112233",
                "001122334455",
                "0011223344556677",
                1.0,
                "rejected",
                Some("low-difficulty-share"),
                false,
                false,
                None,
            )
            .unwrap();

        assert!(share_id.is_some(), "rejected share should be persisted");
        assert!(outcome_id.is_some(), "rejected outcome should be persisted");
        assert!(round_id.is_some(), "round should be resolved");

        let events = svc.event_repo.list_by_type("share_outcome", 10, 0).unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].status, "rejected");
    }

    #[test]
    fn test_record_found_block_uses_correct_round_id() {
        let f = NamedTempFile::new().unwrap();
        let conn = Connection::open(f.path()).unwrap();
        init_schema(&conn).unwrap();
        let svc = AccountingService::new(Arc::new(Mutex::new(conn)));

        // Record a share which creates a round (template_id=42)
        let (_, _, round_id, _) = svc
            .record_share(
                "lotus_16PSJNf1EDEfGvaYzaXJCJZrXH4pgiTo7kyW61iGi",
                Some("rig1"),
                "sess-1",
                "job-42-100",
                42,
                100,
                "00000001",
                "00112233",
                "001122334455",
                "0011223344556677",
                1.0,
                "accepted",
                None,
                true,
                false,
                None,
            )
            .unwrap();
        assert!(round_id.is_some(), "a round should exist");

        // Resolve the round for this template (as handle_submit should)
        let round = svc.resolve_round_for_template(42).unwrap();
        assert_eq!(round.id, round_id.unwrap(), "resolved round should match");

        // Record a found block using the resolved round.id (NOT template_id)
        let found = svc
            .record_found_block(
                round.id,
                "0000abc",
                1292529,
                None,
                Some(42),
                Some("json-rpc"),
                0,
                "",
            )
            .unwrap();

        // The found_block.round_id must equal the resolved round.id
        assert_eq!(
            found.round_id, round.id,
            "found_block.round_id should be the resolved round's id, not template_id"
        );
        assert_eq!(found.block_hash, "0000abc");
        assert_eq!(found.status, "immature");

        // Verify accounting event was recorded
        let events = svc
            .event_repo
            .list_by_type("found_block_observed", 10, 0)
            .unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].round_id, Some(round.id));
    }

    #[tokio::test]
    async fn test_reconcile_found_blocks_orphans_when_hash_mismatch() {
        let f = NamedTempFile::new().unwrap();
        let conn = Connection::open(f.path()).unwrap();
        init_schema(&conn).unwrap();
        let svc = AccountingService::new(Arc::new(Mutex::new(conn)));

        // Create a round and record a found_block
        let _round = svc.get_or_create_current_round(42).unwrap();
        svc.record_found_block(
            1,
            "0000abcdef12345678900000000000000000000000000000000000000000000000",
            1000,
            None,
            Some(42),
            Some("json-rpc"),
            0,
            "",
        )
        .unwrap();

        // Reconcile with a different canonical hash at height 1000
        // The mock returns a different hash, simulating a reorg
        // Use AtomicI64 so the async closure can share ownership via `async move`
        let call_count = std::sync::Arc::new(std::sync::atomic::AtomicI64::new(0));
        let call_count_closure = call_count.clone();

        svc.reconcile_found_blocks(
            2000, // tip well above block height
            |height| {
                let call_count = call_count_closure.clone();
                async move {
                    call_count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    assert_eq!(height, 1000);
                    Ok(Some(
                        "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
                            .to_string(),
                    ))
                }
            },
        )
        .await
        .unwrap();

        assert_eq!(
            call_count.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "get_block_hash should be called once"
        );

        // Verify block was orphaned
        let block = svc
            .found_block_repo
            .get_by_hash("0000abcdef12345678900000000000000000000000000000000000000000000000")
            .unwrap()
            .unwrap();
        assert_eq!(block.status, "orphaned");
        assert_eq!(block.orphan_reason, Some("reorg_detected".to_string()));

        // Verify round was orphaned
        let round = svc.round_repo.get_by_id(1).unwrap().unwrap();
        assert_eq!(round.status, "orphaned");
    }

    #[tokio::test]
    async fn test_reconcile_found_blocks_orphans_when_above_tip() {
        let f = NamedTempFile::new().unwrap();
        let conn = Connection::open(f.path()).unwrap();
        init_schema(&conn).unwrap();
        let svc = AccountingService::new(Arc::new(Mutex::new(conn)));

        // Create a round and record a found_block with height above tip
        let _round = svc.get_or_create_current_round(42).unwrap();
        svc.record_found_block(
            1,
            "0000abcdef12345678900000000000000000000000000000000000000000000000",
            999,
            None,
            Some(42),
            Some("json-rpc"),
            0,
            "",
        )
        .unwrap();

        // Reconcile with tip = 100 — block at height 999 is above tip
        svc.reconcile_found_blocks(100, |_height| async {
            unreachable!("should not be called for blocks above tip");
        })
        .await
        .unwrap();

        // Verify block was orphaned
        let block = svc
            .found_block_repo
            .get_by_hash("0000abcdef12345678900000000000000000000000000000000000000000000000")
            .unwrap()
            .unwrap();
        assert_eq!(block.status, "orphaned");
        assert_eq!(block.orphan_reason, Some("block_not_found".to_string()));

        // Verify round was orphaned
        let round = svc.round_repo.get_by_id(1).unwrap().unwrap();
        assert_eq!(round.status, "orphaned");
    }

    #[test]
    fn test_check_maturation_below_threshold() {
        let f = NamedTempFile::new().unwrap();
        let conn = Connection::open(f.path()).unwrap();
        init_schema(&conn).unwrap();
        let svc = AccountingService::new(Arc::new(Mutex::new(conn)));

        // Create a round
        let (_, _, round_id, _) = svc
            .record_share(
                "lotus_16PSJNf1EDEfGvaYzaXJCJZrXH4pgiTo7kyW61iGi",
                Some("rig1"),
                "sess-1",
                "job-42-100",
                42,
                100,
                "00000001",
                "00112233",
                "001122334455",
                "0011223344556677",
                1.0,
                "accepted",
                None,
                true,
                false,
                None,
            )
            .unwrap();
        let round_id = round_id.unwrap();

        // Record a block at height 950
        let block = svc
            .record_found_block(
                round_id,
                "blockhash1",
                950,
                None,
                Some(42),
                Some("json-rpc"),
                50000,
                "",
            )
            .unwrap();
        assert_eq!(block.status, "immature");

        // Tip = 999 → 50 confirmations, below threshold of 100
        let matured = svc.check_maturation(999, 100).unwrap();
        assert!(
            matured.is_empty(),
            "block should NOT mature below threshold"
        );

        let block = svc
            .found_block_repo
            .get_by_hash("blockhash1")
            .unwrap()
            .unwrap();
        assert_eq!(block.status, "immature", "block should still be immature");
    }

    #[test]
    fn test_check_maturation_at_threshold() {
        let f = NamedTempFile::new().unwrap();
        let conn = Connection::open(f.path()).unwrap();
        init_schema(&conn).unwrap();
        let svc = AccountingService::new(Arc::new(Mutex::new(conn)));

        let (_, _, round_id, _) = svc
            .record_share(
                "lotus_16PSJNf1EDEfGvaYzaXJCJZrXH4pgiTo7kyW61iGi",
                Some("rig1"),
                "sess-1",
                "job-42-100",
                42,
                100,
                "00000001",
                "00112233",
                "001122334455",
                "0011223344556677",
                1.0,
                "accepted",
                None,
                true,
                false,
                None,
            )
            .unwrap();
        let round_id = round_id.unwrap();

        // Block at height 950, tip = 1049 → 100 confirmations (at threshold)
        let block = svc
            .record_found_block(
                round_id,
                "blockhash2",
                950,
                None,
                Some(42),
                Some("json-rpc"),
                50000,
                "",
            )
            .unwrap();
        assert_eq!(block.status, "immature");

        let matured = svc.check_maturation(1049, 100).unwrap();
        assert_eq!(matured.len(), 1, "block should mature at threshold");
        assert_eq!(matured[0].block_hash, "blockhash2");

        let block = svc
            .found_block_repo
            .get_by_hash("blockhash2")
            .unwrap()
            .unwrap();
        assert_eq!(block.status, "matured");
        assert!(block.matured_at.is_some(), "matured_at should be set");
    }

    #[test]
    fn test_check_maturation_above_threshold() {
        let f = NamedTempFile::new().unwrap();
        let conn = Connection::open(f.path()).unwrap();
        init_schema(&conn).unwrap();
        let svc = AccountingService::new(Arc::new(Mutex::new(conn)));

        let (_, _, round_id, _) = svc
            .record_share(
                "lotus_16PSJNf1EDEfGvaYzaXJCJZrXH4pgiTo7kyW61iGi",
                Some("rig1"),
                "sess-1",
                "job-42-100",
                42,
                100,
                "00000001",
                "00112233",
                "001122334455",
                "0011223344556677",
                1.0,
                "accepted",
                None,
                true,
                false,
                None,
            )
            .unwrap();
        let round_id = round_id.unwrap();

        // Block at height 950, tip = 1100 → 151 confirmations (above threshold)
        let block = svc
            .record_found_block(
                round_id,
                "blockhash3",
                950,
                None,
                Some(42),
                Some("json-rpc"),
                50000,
                "",
            )
            .unwrap();
        assert_eq!(block.status, "immature");

        let matured = svc.check_maturation(1100, 100).unwrap();
        assert_eq!(matured.len(), 1);
        assert_eq!(matured[0].block_hash, "blockhash3");

        // Verify DB was updated
        let block = svc
            .found_block_repo
            .get_by_hash("blockhash3")
            .unwrap()
            .unwrap();
        assert_eq!(block.status, "matured");
    }

    #[test]
    fn test_check_maturation_no_immature_blocks() {
        let f = NamedTempFile::new().unwrap();
        let conn = Connection::open(f.path()).unwrap();
        init_schema(&conn).unwrap();
        let svc = AccountingService::new(Arc::new(Mutex::new(conn)));

        // No blocks at all
        let matured = svc.check_maturation(999, 100).unwrap();
        assert!(matured.is_empty());
    }

    #[test]
    fn test_check_maturation_does_not_mature_orphaned() {
        let f = NamedTempFile::new().unwrap();
        let conn = Connection::open(f.path()).unwrap();
        init_schema(&conn).unwrap();
        let svc = AccountingService::new(Arc::new(Mutex::new(conn)));

        let (_, _, round_id, _) = svc
            .record_share(
                "lotus_16PSJNf1EDEfGvaYzaXJCJZrXH4pgiTo7kyW61iGi",
                Some("rig1"),
                "sess-1",
                "job-42-100",
                42,
                100,
                "00000001",
                "00112233",
                "001122334455",
                "0011223344556677",
                1.0,
                "accepted",
                None,
                true,
                false,
                None,
            )
            .unwrap();
        let round_id = round_id.unwrap();

        svc.record_found_block(
            round_id,
            "orphanedblock",
            950,
            None,
            Some(42),
            Some("json-rpc"),
            50000,
            "",
        )
        .unwrap();
        svc.found_block_repo
            .mark_orphaned("orphanedblock", "reorg")
            .unwrap();

        // Tip is 1100, which would mature if block were still immature
        let matured = svc.check_maturation(1100, 100).unwrap();
        assert!(matured.is_empty(), "orphaned blocks should not be matured");
    }

    #[test]
    fn test_round_closed_on_block_found() {
        let f = NamedTempFile::new().unwrap();
        let conn = Connection::open(f.path()).unwrap();
        init_schema(&conn).unwrap();
        let svc = AccountingService::new(Arc::new(Mutex::new(conn)));

        // Record a share which creates a round (template_id=42)
        let (_, _, round_id, _) = svc
            .record_share(
                "lotus_16PSJNf1EDEfGvaYzaXJCJZrXH4pgiTo7kyW61iGi",
                None,
                "sess-1",
                "job-42-100",
                42,
                100,
                "00000001",
                "00112233",
                "001122334455",
                "0011223344556677",
                1.0,
                "accepted",
                None,
                true,
                false,
                None,
            )
            .unwrap();
        assert!(round_id.is_some(), "round should exist");

        // Verify round is open before block found
        let round_before = svc
            .round_repo
            .get_by_id(round_id.unwrap())
            .unwrap()
            .unwrap();
        assert_eq!(round_before.status, "open", "round should start as 'open'");

        // Record a found block and close the round (as handle_submit should)
        let template_id: u64 = 42;
        let round = svc.resolve_round_for_template(template_id).unwrap();
        let _found = svc
            .record_found_block(
                round.id,
                "foundblockhash",
                1000,
                None,
                Some(template_id),
                Some("json-rpc"),
                0,
                "",
            )
            .unwrap();
        svc.close_round(round.id, template_id, "found").unwrap();

        // Verify round is now 'found'
        let round_after = svc.round_repo.get_by_id(round.id).unwrap().unwrap();
        assert_eq!(
            round_after.status, "found",
            "round should transition to 'found'"
        );
        assert_eq!(
            round_after.end_template_id,
            Some(template_id),
            "end_template_id should be set"
        );

        // Verify accounting event was recorded
        let events = svc.event_repo.list_by_type("round_closed", 10, 0).unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].round_id, Some(round.id));
        assert_eq!(events[0].status, "found");
    }

    #[test]
    fn test_record_share_returns_dedupe_key_with_real_worker_id() {
        let f = NamedTempFile::new().unwrap();
        let conn = Connection::open(f.path()).unwrap();
        init_schema(&conn).unwrap();
        let svc = AccountingService::new(Arc::new(Mutex::new(conn)));

        // Record a share — the service upserts a worker and returns the actual dedupe key
        let (share_id, outcome_id, round_id, dedupe_key) = svc
            .record_share(
                "lotus_16PSJNf1EDEfGvaYzaXJCJZrXH4pgiTo7kyW61iGi",
                Some("rig1"),
                "sess-1",
                "job-42-100",
                42,
                100,
                "00000001",
                "00112233",
                "001122334455",
                "0011223344556677",
                1.0,
                "accepted",
                None,
                true,
                false,
                None,
            )
            .unwrap();

        assert!(share_id.is_some(), "share should be persisted");
        assert!(outcome_id.is_some(), "outcome should be persisted");
        assert!(round_id.is_some(), "round should be resolved");

        // The dedupe key must start with the real worker_id (1, since this is the first worker)
        // Format: worker_id:template_id:template_epoch:extranonce2:ntime:nonce
        assert!(
            dedupe_key.starts_with("1:"),
            "dedupe_key should start with the real worker_id (1), got: {}",
            dedupe_key
        );
        assert!(
            dedupe_key.contains("42:100:00112233:001122334455:0011223344556677"),
            "dedupe_key should contain template_id, epoch, extranonce2, ntime, nonce, got: {}",
            dedupe_key
        );
    }

    #[test]
    fn test_record_share_dedupe_correctly() {
        let f = NamedTempFile::new().unwrap();
        let conn = Connection::open(f.path()).unwrap();
        init_schema(&conn).unwrap();
        let svc = AccountingService::new(Arc::new(Mutex::new(conn)));

        // First insert with worker suffix (ensures worker dedupe works)
        let (sid1, _, _, _) = svc
            .record_share(
                "lotus_16PSJNf1EDEfGvaYzaXJCJZrXH4pgiTo7kyW61iGi",
                Some("rig1"),
                "sess-1",
                "job-42-100",
                42,
                100,
                "00000001",
                "00112233",
                "001122334455",
                "0011223344556677",
                1.0,
                "accepted",
                None,
                true,
                false,
                None,
            )
            .unwrap();
        assert!(sid1.is_some(), "first insert should succeed");

        // Duplicate insert (same dedupe key) — should not create a new share
        let (sid2, _, _, _) = svc
            .record_share(
                "lotus_16PSJNf1EDEfGvaYzaXJCJZrXH4pgiTo7kyW61iGi",
                Some("rig1"),
                "sess-1",
                "job-42-100",
                42,
                100,
                "00000001",
                "00112233",
                "001122334455",
                "0011223344556677",
                1.0,
                "accepted",
                None,
                true,
                false,
                None,
            )
            .unwrap();
        assert!(sid2.is_some(), "duplicate insert should still return an ID");

        // Total share count should be 1, not 2
        let total_shares = svc.share_repo.total_count().unwrap();
        assert_eq!(
            total_shares, 1,
            "only one share should exist despite two insert attempts"
        );

        // Only one accounting event
        let events = svc.event_repo.list_by_type("share_outcome", 10, 0).unwrap();
        assert_eq!(events.len(), 1, "only one accounting event should exist");
    }

    #[test]
    fn test_snapshot_includes_dusted_miner_shares() {
        // Verify that the payout share snapshot captures ALL miner shares in the
        // PPLNS window, even when a miner's payout falls below min_payout_sat
        // and their output is clipped to dust.
        let f = NamedTempFile::new().unwrap();
        let db_path = f.path().to_path_buf();

        // Set up schema and test data BEFORE creating the service.
        // Use a dedicated setup connection so we don't interfere with the
        // service's connection pool.
        {
            let setup = Connection::open(&db_path).unwrap();
            init_schema(&setup).unwrap();

            setup
                .execute(
                    "INSERT INTO workers (id, payout_address) VALUES (1, 'big_addr')",
                    [],
                )
                .unwrap();
            setup
                .execute(
                    "INSERT INTO workers (id, payout_address) VALUES (2, 'small_addr')",
                    [],
                )
                .unwrap();
            setup
                .execute(
                    "INSERT INTO rounds (id, start_template_id, status) VALUES (1, 100, 'open')",
                    [],
                )
                .unwrap();

            // Big miner share (diff 500)
            setup.execute(
                "INSERT INTO shares (id, worker_id, session_id, job_id, template_id, template_epoch,
                                     extranonce1, extranonce2, ntime_hex_6b, nonce_hex_8b, difficulty, dedupe_key)
                 VALUES (1, 1, 's1', 'j1', 100, 1, 'e1', 'en2', 'ntime', 'nonce', 500.0, 'dk1')",
                [],
            ).unwrap();
            setup.execute(
                "INSERT INTO share_outcomes (id, share_id, session_id, worker_id, job_id, round_id, dedupe_key,
                                             status, low_diff_ok, network_target_ok, created_at)
                 VALUES (1, 1, 's1', 1, 'j1', 1, 'dk1', 'accepted', 1, 0, '2026-05-20T12:00:01')",
                [],
            ).unwrap();

            // Small miner share (diff 1)
            setup.execute(
                "INSERT INTO shares (id, worker_id, session_id, job_id, template_id, template_epoch,
                                     extranonce1, extranonce2, ntime_hex_6b, nonce_hex_8b, difficulty, dedupe_key)
                 VALUES (2, 2, 's2', 'j2', 100, 2, 'e1', 'en2', 'ntime', 'nonce', 1.0, 'dk2')",
                [],
            ).unwrap();
            setup.execute(
                "INSERT INTO share_outcomes (id, share_id, session_id, worker_id, job_id, round_id, dedupe_key,
                                             status, low_diff_ok, network_target_ok, created_at)
                 VALUES (2, 2, 's2', 2, 'j2', 1, 'dk2', 'accepted', 1, 0, '2026-05-20T12:00:02')",
                [],
            ).unwrap();

            // Block-finding share (belongs to big miner)
            let block_hash = "00000000deadbeef00000000000000000000000000000000000000000000000000";
            setup.execute(
                "INSERT INTO shares (id, worker_id, session_id, job_id, template_id, template_epoch,
                                     extranonce1, extranonce2, ntime_hex_6b, nonce_hex_8b, difficulty, dedupe_key)
                 VALUES (3, 1, 's1', 'j3', 100, 3, 'e1', 'en2', 'ntime', 'nonce', 1.0, 'dk3')",
                [],
            ).unwrap();
            setup.execute(
                "INSERT INTO share_outcomes (id, share_id, session_id, worker_id, job_id, round_id, dedupe_key,
                                             status, low_diff_ok, network_target_ok, block_hash, created_at)
                 VALUES (3, 3, 's1', 1, 'j3', 1, 'dk3', 'accepted', 1, 1, ?1, '2026-05-20T12:00:03')",
                rusqlite::params![block_hash],
            ).unwrap();
        }
        // setup connection dropped: schema + test data committed

        // Now create the service on a fresh connection to the same db file
        let conn = Connection::open(&db_path).unwrap();
        let svc = AccountingService::new(Arc::new(Mutex::new(conn)));

        // Record the found block with a modest coinbase_value
        let block_hash = "00000000deadbeef00000000000000000000000000000000000000000000000000";
        let found_block = svc
            .record_found_block(
                1, // round_id
                block_hash,
                5000,
                Some(1), // big miner found it
                Some(100),
                Some("json-rpc"),
                1000, // coinbase_value = 1000 sat
                "0000ffff0000000000000000000000000000000000000000000000000000000000",
            )
            .unwrap();

        // Calculate payout with min_payout_sat high enough that small miner gets dusted
        let batch_id = svc
            .create_payout_for_found_block(
                &found_block,
                0,    // 0 bps fee
                None, // no fee address
                500,  // min_payout_sat = 500
                10.0, // n_multiplier = 10.0 (window covers all shares)
            )
            .unwrap();

        // Query snapshots for this batch
        let snapshots = svc.payout_repo.get_snapshots_by_batch(batch_id).unwrap();

        // Assert BOTH miners have snapshot entries
        let big_snaps: Vec<_> = snapshots
            .iter()
            .filter(|s| s.payout_address == "big_addr")
            .collect();
        let small_snaps: Vec<_> = snapshots
            .iter()
            .filter(|s| s.payout_address == "small_addr")
            .collect();

        assert!(
            !big_snaps.is_empty(),
            "big miner should have snapshot entries (amount >= min_payout_sat)"
        );
        assert!(
            !small_snaps.is_empty(),
            "small miner should have snapshot entries even though payout was dusted"
        );

        // Verify the payouts show the dusted amount correctly
        let payouts = svc.payout_repo.get_payouts_by_batch(batch_id).unwrap();
        let small_payout = payouts
            .iter()
            .find(|p| p.payout_address == "small_addr")
            .unwrap();
        assert_eq!(
            small_payout.amount, 0,
            "dusted miner's payout amount should be 0"
        );
        assert!(
            small_payout.dust_carried_forward > 0,
            "dusted miner should have dust carried forward"
        );
    }

    #[test]
    fn test_full_payout_flow_integration() {
        // Full end-to-end integration test for create_payout_for_found_block:
        //   workers, shares, found block → payout plan → batch → payouts →
        //   snapshots → dust tracking → accounting event.
        let f = NamedTempFile::new().unwrap();
        let db_path = f.path().to_path_buf();

        {
            let setup = Connection::open(&db_path).unwrap();
            init_schema(&setup).unwrap();

            // Workers
            setup
                .execute_batch(
                    "INSERT INTO workers (id, payout_address) VALUES (1, 'alice');
                 INSERT INTO workers (id, payout_address) VALUES (2, 'bob');",
                )
                .unwrap();

            // Round
            setup
                .execute(
                    "INSERT INTO rounds (id, start_template_id, status) VALUES (1, 200, 'open')",
                    [],
                )
                .unwrap();

            // Alice: diff 300 (75% of work)
            setup.execute(
                "INSERT INTO shares (id, worker_id, session_id, job_id, template_id, template_epoch,
                                     extranonce1, extranonce2, ntime_hex_6b, nonce_hex_8b, difficulty, dedupe_key)
                 VALUES (1, 1, 's1', 'j1', 200, 1, 'e1', 'en2', 'ntime', 'nonce', 300.0, 'dk1')",
                [],
            ).unwrap();
            setup.execute(
                "INSERT INTO share_outcomes (id, share_id, session_id, worker_id, job_id, round_id, dedupe_key,
                                             status, low_diff_ok, network_target_ok, created_at)
                 VALUES (1, 1, 's1', 1, 'j1', 1, 'dk1', 'accepted', 1, 0, '2026-05-20T12:00:01')",
                [],
            ).unwrap();

            // Bob: diff 100 (25% of work)
            setup.execute(
                "INSERT INTO shares (id, worker_id, session_id, job_id, template_id, template_epoch,
                                     extranonce1, extranonce2, ntime_hex_6b, nonce_hex_8b, difficulty, dedupe_key)
                 VALUES (2, 2, 's2', 'j2', 200, 2, 'e1', 'en2', 'ntime', 'nonce', 100.0, 'dk2')",
                [],
            ).unwrap();
            setup.execute(
                "INSERT INTO share_outcomes (id, share_id, session_id, worker_id, job_id, round_id, dedupe_key,
                                             status, low_diff_ok, network_target_ok, created_at)
                 VALUES (2, 2, 's2', 2, 'j2', 1, 'dk2', 'accepted', 1, 0, '2026-05-20T12:00:02')",
                [],
            ).unwrap();

            // Block-finding share (found by Alice)
            let block_hash = "00000000cafebabe00000000000000000000000000000000000000000000000000";
            setup.execute(
                "INSERT INTO shares (id, worker_id, session_id, job_id, template_id, template_epoch,
                                     extranonce1, extranonce2, ntime_hex_6b, nonce_hex_8b, difficulty, dedupe_key)
                 VALUES (3, 1, 's1', 'j3', 200, 3, 'e1', 'en2', 'ntime', 'nonce', 1.0, 'dk3')",
                [],
            ).unwrap();
            setup.execute(
                "INSERT INTO share_outcomes (id, share_id, session_id, worker_id, job_id, round_id, dedupe_key,
                                             status, low_diff_ok, network_target_ok, block_hash, created_at)
                 VALUES (3, 3, 's1', 1, 'j3', 1, 'dk3', 'accepted', 1, 1, ?1, '2026-05-20T12:00:03')",
                rusqlite::params![block_hash],
            ).unwrap();

            // Pre-existing dust for Alice (50 sat carried from previous round)
            setup
                .execute(
                    "INSERT INTO dust_balances (payout_address, balance) VALUES ('alice', 50)",
                    [],
                )
                .unwrap();
        }

        // Create the service on a fresh connection
        let conn = Connection::open(&db_path).unwrap();
        let svc = AccountingService::new(Arc::new(Mutex::new(conn)));

        let block_hash = "00000000cafebabe00000000000000000000000000000000000000000000000000";
        let found_block = svc
            .record_found_block(
                1,
                block_hash,
                9999,
                Some(1),
                Some(200),
                Some("json-rpc"),
                10000, // coinbase_value = 10000 sat
                "0000ffff0000000000000000000000000000000000000000000000000000000000",
            )
            .unwrap();

        // Payout: 200 bps (2%) fee, n_multiplier covers all shares
        let batch_id = svc
            .create_payout_for_found_block(
                &found_block,
                200,              // 2% fee
                Some("fee_pool"), // fee address
                1,                // min_payout_sat = 1 (no dust)
                10.0,             // window covers everything
            )
            .unwrap();

        let batch = svc.payout_repo.get_batch_by_id(batch_id).unwrap().unwrap();
        assert_eq!(
            batch.status, "pending",
            "fresh payout batch should be pending"
        );
        assert_eq!(batch.round_id, 1);

        // Fee = 5000 * 200 / 10000 = 100 sat (after minerfund deduction)
        assert_eq!(batch.pool_fee_amount, 100);
        assert_eq!(batch.pool_fee_address.as_deref(), Some("fee_pool"));

        // Gross = 5000, Fee = 100, Net = 4900
        // Alice: (300 + 50 dust_weight) / (400 + 50 dust_weight) ≈ 0.7778 of net
        // Bob: 100 / (400 + 50 dust_weight) ≈ 0.2222 of net
        // We don't check exact amounts (dust weight makes it fuzzy); verify sums instead.
        let payouts = svc.payout_repo.get_payouts_by_batch(batch_id).unwrap();
        let total_payouts: i64 = payouts.iter().map(|p| p.amount).sum();
        let total_dust: i64 = payouts.iter().map(|p| p.dust_carried_forward).sum();
        assert_eq!(
            total_payouts + batch.pool_fee_amount + total_dust,
            5000,
            "gross reward should equal sum of payouts + fee + dust",
        );

        // Fee is tracked in payout_batches.pool_fee_amount, not in individual payouts.
        assert_eq!(batch.pool_fee_amount, 100);
        assert_eq!(batch.pool_fee_address.as_deref(), Some("fee_pool"));

        // Verify Alice (worker 1) and Bob (worker 2) each have a payout
        assert!(
            payouts.iter().any(|p| p.worker_id == 1),
            "Alice should have a payout"
        );
        assert!(
            payouts.iter().any(|p| p.worker_id == 2),
            "Bob should have a payout"
        );

        // Snapshots cover all shares
        let snapshots = svc.payout_repo.get_snapshots_by_batch(batch_id).unwrap();
        assert_eq!(snapshots.len(), 2, "one snapshot per miner (2 share rows)");
        assert!(snapshots.iter().any(|s| s.payout_address == "alice"));
        assert!(snapshots.iter().any(|s| s.payout_address == "bob"));

        // Retry_key format: "{block_hash}:{num_outputs}"
        assert!(
            batch
                .retry_key
                .as_deref()
                .unwrap()
                .starts_with("00000000cafebabe00000000000000000000000000000000000000000000000000:"),
            "retry_key should start with block_hash, got: {:?}",
            batch.retry_key,
        );

        // Dust: Alice had 50 pre-existing dust, which was consumed as bonus weight.
        // Bob had no pre-existing dust, so no new dust was created for him.
        // Alice's payout > min_payout_sat so no new dust for her either.
        // Total dust carried forward = sum of dust_carried_forward across payouts.
        // Since min_payout_sat=1, no payouts were clipped to dust.
        assert!(
            total_dust == 0,
            "no dust should be generated when min_payout_sat=1 and all miners get >=1 sat",
        );

        // Verify dust_balances table: Alice's pre-existing dust (50) was used as bonus
        // weight in the payout calculation but is NOT decremented in the DB — the dust
        // ledger is additive only (dust is never removed, only accumulated).
        let (alice_dust, _) = svc.payout_repo.get_or_create_dust_balance("alice").unwrap();
        assert_eq!(
            alice_dust, 50,
            "Alice's pre-existing dust remains in balance (additive-only ledger)"
        );
    }

    #[test]
    fn test_create_payout_rejects_duplicate_block() {
        // Verify that create_payout_for_found_block rejects a second call
        // with the same block_hash (prevents duplicate payout batches).
        let f = NamedTempFile::new().unwrap();
        let db_path = f.path().to_path_buf();

        {
            let setup = Connection::open(&db_path).unwrap();
            init_schema(&setup).unwrap();

            // Workers
            setup
                .execute_batch(
                    "INSERT INTO workers (id, payout_address) VALUES (1, 'alice');
                 INSERT INTO workers (id, payout_address) VALUES (2, 'bob');",
                )
                .unwrap();

            // Round
            setup
                .execute(
                    "INSERT INTO rounds (id, start_template_id, status) VALUES (1, 200, 'open')",
                    [],
                )
                .unwrap();

            // Alice: diff 300 (75% of work)
            setup.execute(
                "INSERT INTO shares (id, worker_id, session_id, job_id, template_id, template_epoch,
                                     extranonce1, extranonce2, ntime_hex_6b, nonce_hex_8b, difficulty, dedupe_key)
                 VALUES (1, 1, 's1', 'j1', 200, 1, 'e1', 'en2', 'ntime', 'nonce', 300.0, 'dk1')",
                [],
            ).unwrap();
            setup.execute(
                "INSERT INTO share_outcomes (id, share_id, session_id, worker_id, job_id, round_id, dedupe_key,
                                             status, low_diff_ok, network_target_ok, created_at)
                 VALUES (1, 1, 's1', 1, 'j1', 1, 'dk1', 'accepted', 1, 0, '2026-05-20T12:00:01')",
                [],
            ).unwrap();

            // Bob: diff 100 (25% of work)
            setup.execute(
                "INSERT INTO shares (id, worker_id, session_id, job_id, template_id, template_epoch,
                                     extranonce1, extranonce2, ntime_hex_6b, nonce_hex_8b, difficulty, dedupe_key)
                 VALUES (2, 2, 's2', 'j2', 200, 2, 'e1', 'en2', 'ntime', 'nonce', 100.0, 'dk2')",
                [],
            ).unwrap();
            setup.execute(
                "INSERT INTO share_outcomes (id, share_id, session_id, worker_id, job_id, round_id, dedupe_key,
                                             status, low_diff_ok, network_target_ok, created_at)
                 VALUES (2, 2, 's2', 2, 'j2', 1, 'dk2', 'accepted', 1, 0, '2026-05-20T12:00:02')",
                [],
            ).unwrap();

            // Block-finding share (network_target_ok=1, block_hash set)
            let block_hash = "0000deadbeef000000000000000000000000000000000000000000000000000000";
            setup.execute(
                "INSERT INTO shares (id, worker_id, session_id, job_id, template_id, template_epoch,
                                     extranonce1, extranonce2, ntime_hex_6b, nonce_hex_8b, difficulty, dedupe_key)
                 VALUES (3, 1, 's1', 'j3', 200, 3, 'e1', 'en2', 'ntime', 'nonce', 1.0, 'dk3')",
                [],
            ).unwrap();
            setup.execute(
                "INSERT INTO share_outcomes (id, share_id, session_id, worker_id, job_id, round_id, dedupe_key,
                                             status, low_diff_ok, network_target_ok, block_hash, created_at)
                 VALUES (3, 3, 's1', 1, 'j3', 1, 'dk3', 'accepted', 1, 1, ?1, '2026-05-20T12:00:03')",
                rusqlite::params![block_hash],
            ).unwrap();
        }

        let conn = Connection::open(&db_path).unwrap();
        let svc = AccountingService::new(Arc::new(Mutex::new(conn)));

        let block_hash = "0000deadbeef000000000000000000000000000000000000000000000000000000";
        let found_block = svc
            .record_found_block(
                1,
                block_hash,
                10000,
                Some(1),
                Some(200),
                Some("json-rpc"),
                10000,
                "0000ffff0000000000000000000000000000000000000000000000000000000000",
            )
            .unwrap();

        // First call — succeeds
        let _batch_id = svc
            .create_payout_for_found_block(&found_block, 200, Some("fee_pool"), 1, 10.0)
            .unwrap();

        // Second call with same block — fails
        let err = svc
            .create_payout_for_found_block(&found_block, 200, Some("fee_pool"), 1, 10.0)
            .unwrap_err();
        assert!(
            err.to_string().contains("already exists"),
            "should reject duplicate block, got: {err}",
        );
    }

    #[test]
    fn test_dust_accumulates_across_multiple_rounds() {
        // Verify that dust_balances grow additively across multiple payout rounds
        // (additive-only ledger: dust is never decremented).
        let f = NamedTempFile::new().unwrap();
        let db_path = f.path().to_path_buf();

        {
            let setup = Connection::open(&db_path).unwrap();
            init_schema(&setup).unwrap();

            // Workers
            setup
                .execute_batch(
                    "INSERT INTO workers (id, payout_address) VALUES (1, 'big_miner');
                 INSERT INTO workers (id, payout_address) VALUES (2, 'small_miner');",
                )
                .unwrap();

            // Two rounds
            setup
                .execute_batch(
                    "INSERT INTO rounds (id, start_template_id, status) VALUES (1, 100, 'open');
                 INSERT INTO rounds (id, start_template_id, status) VALUES (2, 200, 'open');",
                )
                .unwrap();

            // === Round 1 shares ===
            // Big miner: diff 8.0 (80%)
            setup.execute(
                "INSERT INTO shares (id, worker_id, session_id, job_id, template_id, template_epoch,
                                     extranonce1, extranonce2, ntime_hex_6b, nonce_hex_8b, difficulty, dedupe_key)
                 VALUES (1, 1, 's1', 'j1', 100, 1, 'e1', 'en2', 'ntime', 'nonce', 8.0, 'dk1')",
                [],
            ).unwrap();
            setup.execute(
                "INSERT INTO share_outcomes (id, share_id, session_id, worker_id, job_id, round_id, dedupe_key,
                                             status, low_diff_ok, network_target_ok, created_at)
                 VALUES (1, 1, 's1', 1, 'j1', 1, 'dk1', 'accepted', 1, 0, '2026-01-01T12:00:00')",
                [],
            ).unwrap();

            // Small miner: diff 2.0 (20%)
            setup.execute(
                "INSERT INTO shares (id, worker_id, session_id, job_id, template_id, template_epoch,
                                     extranonce1, extranonce2, ntime_hex_6b, nonce_hex_8b, difficulty, dedupe_key)
                 VALUES (2, 2, 's2', 'j2', 100, 2, 'e1', 'en2', 'ntime', 'nonce', 2.0, 'dk2')",
                [],
            ).unwrap();
            setup.execute(
                "INSERT INTO share_outcomes (id, share_id, session_id, worker_id, job_id, round_id, dedupe_key,
                                             status, low_diff_ok, network_target_ok, created_at)
                 VALUES (2, 2, 's2', 2, 'j2', 1, 'dk2', 'accepted', 1, 0, '2026-01-01T12:00:01')",
                [],
            ).unwrap();

            // Block-finding share for R1 (big miner, network_target_ok=1, block_hash set)
            let r1_hash = "00000000000000000000000000000000000000000000000000000000000000aa";
            setup.execute(
                "INSERT INTO shares (id, worker_id, session_id, job_id, template_id, template_epoch,
                                     extranonce1, extranonce2, ntime_hex_6b, nonce_hex_8b, difficulty, dedupe_key)
                 VALUES (3, 1, 's1', 'j3', 100, 3, 'e1', 'en2', 'ntime', 'nonce', 1.0, 'dk3')",
                [],
            ).unwrap();
            setup.execute(
                "INSERT INTO share_outcomes (id, share_id, session_id, worker_id, job_id, round_id, dedupe_key,
                                             status, low_diff_ok, network_target_ok, block_hash, created_at)
                 VALUES (3, 3, 's1', 1, 'j3', 1, 'dk3', 'accepted', 1, 1, ?1, '2026-01-01T12:00:02')",
                rusqlite::params![r1_hash],
            ).unwrap();

            // === Round 2 shares (same distribution, later timestamps) ===
            // Big miner: diff 8.0
            setup.execute(
                "INSERT INTO shares (id, worker_id, session_id, job_id, template_id, template_epoch,
                                     extranonce1, extranonce2, ntime_hex_6b, nonce_hex_8b, difficulty, dedupe_key)
                 VALUES (4, 1, 's1', 'j4', 200, 4, 'e1', 'en2', 'ntime', 'nonce', 8.0, 'dk4')",
                [],
            ).unwrap();
            setup.execute(
                "INSERT INTO share_outcomes (id, share_id, session_id, worker_id, job_id, round_id, dedupe_key,
                                             status, low_diff_ok, network_target_ok, created_at)
                 VALUES (4, 4, 's1', 1, 'j4', 2, 'dk4', 'accepted', 1, 0, '2026-01-02T12:00:00')",
                [],
            ).unwrap();

            // Small miner: diff 2.0
            setup.execute(
                "INSERT INTO shares (id, worker_id, session_id, job_id, template_id, template_epoch,
                                     extranonce1, extranonce2, ntime_hex_6b, nonce_hex_8b, difficulty, dedupe_key)
                 VALUES (5, 2, 's2', 'j5', 200, 5, 'e1', 'en2', 'ntime', 'nonce', 2.0, 'dk5')",
                [],
            ).unwrap();
            setup.execute(
                "INSERT INTO share_outcomes (id, share_id, session_id, worker_id, job_id, round_id, dedupe_key,
                                             status, low_diff_ok, network_target_ok, created_at)
                 VALUES (5, 5, 's2', 2, 'j5', 2, 'dk5', 'accepted', 1, 0, '2026-01-02T12:00:01')",
                [],
            ).unwrap();

            // Block-finding share for R2 (big miner, different block_hash)
            let r2_hash = "00000000000000000000000000000000000000000000000000000000000000bb";
            setup.execute(
                "INSERT INTO shares (id, worker_id, session_id, job_id, template_id, template_epoch,
                                     extranonce1, extranonce2, ntime_hex_6b, nonce_hex_8b, difficulty, dedupe_key)
                 VALUES (6, 1, 's1', 'j6', 200, 6, 'e1', 'en2', 'ntime', 'nonce', 1.0, 'dk6')",
                [],
            ).unwrap();
            setup.execute(
                "INSERT INTO share_outcomes (id, share_id, session_id, worker_id, job_id, round_id, dedupe_key,
                                             status, low_diff_ok, network_target_ok, block_hash, created_at)
                 VALUES (6, 6, 's1', 1, 'j6', 2, 'dk6', 'accepted', 1, 1, ?1, '2026-01-02T12:00:02')",
                rusqlite::params![r2_hash],
            ).unwrap();
        }

        let conn = Connection::open(&db_path).unwrap();
        let svc = AccountingService::new(Arc::new(Mutex::new(conn)));

        let r1_hash = "00000000000000000000000000000000000000000000000000000000000000aa";
        let r2_hash = "00000000000000000000000000000000000000000000000000000000000000bb";

        // Record both found blocks
        let found_block_1 = svc
            .record_found_block(
                1,
                r1_hash,
                10000,
                Some(1),
                Some(100),
                Some("json-rpc"),
                1000,
                "0000ffff0000000000000000000000000000000000000000000000000000000000",
            )
            .unwrap();
        let found_block_2 = svc
            .record_found_block(
                2,
                r2_hash,
                10001,
                Some(1),
                Some(200),
                Some("json-rpc"),
                1000,
                "0000ffff0000000000000000000000000000000000000000000000000000000000",
            )
            .unwrap();

        // Round 1 payout: small_miner gets < 500 sat → dusted
        let _batch_1 = svc
            .create_payout_for_found_block(
                &found_block_1,
                0,    // no fee
                None, // no fee address
                500,  // min_payout_sat
                10.0, // large n_multiplier ensures all shares in window
            )
            .unwrap();

        let (dust_after_1, _) = svc
            .payout_repo
            .get_or_create_dust_balance("small_miner")
            .unwrap();
        assert!(
            dust_after_1 > 0,
            "small_miner should have dust after round 1, got: {dust_after_1}",
        );

        // Round 2 payout: small_miner's dust balance grows (additive-only)
        let _batch_2 = svc
            .create_payout_for_found_block(&found_block_2, 0, None, 500, 10.0)
            .unwrap();

        let (dust_after_2, _) = svc
            .payout_repo
            .get_or_create_dust_balance("small_miner")
            .unwrap();
        assert!(
            dust_after_2 > dust_after_1,
            "dust should grow across rounds (additive-only): after_1={dust_after_1}, after_2={dust_after_2}",
        );
    }

    #[test]
    fn test_rebuild_payout_plan_repopulates_payouts_and_snapshots() {
        let f = NamedTempFile::new().unwrap();
        let db_path = f.path().to_path_buf();

        {
            let setup = Connection::open(&db_path).unwrap();
            init_schema(&setup).unwrap();
            setup.execute_batch(
                "INSERT INTO workers (id, payout_address) VALUES (1, 'alice');
                 INSERT INTO workers (id, payout_address) VALUES (2, 'bob');",
            )
            .unwrap();
            setup.execute(
                "INSERT INTO rounds (id, start_template_id, status) VALUES (1, 200, 'open')",
                [],
            )
            .unwrap();
            // Alice: diff 300
            setup.execute(
                "INSERT INTO shares (id, worker_id, session_id, job_id, template_id, template_epoch,
                                     extranonce1, extranonce2, ntime_hex_6b, nonce_hex_8b, difficulty, dedupe_key)
                 VALUES (1, 1, 's1', 'j1', 200, 1, 'e1', 'en2', 'ntime', 'nonce', 300.0, 'dk1')",
                [],
            ).unwrap();
            setup.execute(
                "INSERT INTO share_outcomes (id, share_id, session_id, worker_id, job_id, round_id, dedupe_key,
                                             status, low_diff_ok, network_target_ok, created_at)
                 VALUES (1, 1, 's1', 1, 'j1', 1, 'dk1', 'accepted', 1, 0, '2026-06-01T12:00:01')",
                [],
            ).unwrap();
            // Bob: diff 100
            setup.execute(
                "INSERT INTO shares (id, worker_id, session_id, job_id, template_id, template_epoch,
                                     extranonce1, extranonce2, ntime_hex_6b, nonce_hex_8b, difficulty, dedupe_key)
                 VALUES (2, 2, 's2', 'j2', 200, 2, 'e1', 'en2', 'ntime', 'nonce', 100.0, 'dk2')",
                [],
            ).unwrap();
            setup.execute(
                "INSERT INTO share_outcomes (id, share_id, session_id, worker_id, job_id, round_id, dedupe_key,
                                             status, low_diff_ok, network_target_ok, created_at)
                 VALUES (2, 2, 's2', 2, 'j2', 1, 'dk2', 'accepted', 1, 0, '2026-06-01T12:00:02')",
                [],
            ).unwrap();
            // Block-finding share (Alice, network_target_ok=1)
            let block_hash = "00000000aaaa000000000000000000000000000000000000000000000000000000";
            setup.execute(
                "INSERT INTO shares (id, worker_id, session_id, job_id, template_id, template_epoch,
                                     extranonce1, extranonce2, ntime_hex_6b, nonce_hex_8b, difficulty, dedupe_key)
                 VALUES (3, 1, 's1', 'j3', 200, 3, 'e1', 'en2', 'ntime', 'nonce', 1.0, 'dk3')",
                [],
            ).unwrap();
            setup.execute(
                "INSERT INTO share_outcomes (id, share_id, session_id, worker_id, job_id, round_id, dedupe_key,
                                             status, low_diff_ok, network_target_ok, block_hash, created_at)
                 VALUES (3, 3, 's1', 1, 'j3', 1, 'dk3', 'accepted', 1, 1, ?1, '2026-06-01T12:00:03')",
                rusqlite::params![block_hash],
            ).unwrap();
        }

        let conn = Connection::open(&db_path).unwrap();
        let svc = AccountingService::new(Arc::new(Mutex::new(conn)));

        let block_hash = "00000000aaaa000000000000000000000000000000000000000000000000000000";
        let found_block = svc
            .record_found_block(
                1,
                block_hash,
                10000,
                Some(1),
                Some(200),
                Some("json-rpc"),
                10000,
                "0000ffff0000000000000000000000000000000000000000000000000000000000",
            )
            .unwrap();

        let batch_id = svc
            .create_payout_for_found_block(
                &found_block,
                200,
                Some("fee_pool"),
                1,
                10.0,
            )
            .unwrap();

        // Verify original payouts exist
        let original_payouts = svc.payout_repo.get_payouts_by_batch(batch_id).unwrap();
        assert_eq!(original_payouts.len(), 2, "should have 2 miner payouts originally");
        let original_total: i64 = original_payouts.iter().map(|p| p.amount).sum();

        // Simulate user accidentally deleting payouts (not snapshots)
        svc.payout_repo.delete_payouts_by_batch(batch_id).unwrap();
        let deleted = svc.payout_repo.get_payouts_by_batch(batch_id).unwrap();
        assert!(deleted.is_empty(), "payouts should be empty after delete");

        // Rebuild
        let outputs = svc
            .rebuild_payout_plan_for_batch(batch_id, 200, Some("fee_pool"), 1, 10.0)
            .unwrap();

        // Verify payouts re-created
        let rebuilt_payouts = svc.payout_repo.get_payouts_by_batch(batch_id).unwrap();
        assert_eq!(rebuilt_payouts.len(), 2, "rebuild should recreate 2 miner payouts");
        let rebuilt_total: i64 = rebuilt_payouts.iter().map(|p| p.amount).sum();
        assert_eq!(
            rebuilt_total, original_total,
            "rebuilt payout total should match original"
        );
        assert_eq!(outputs.len(), rebuilt_payouts.len());

        // Verify snapshots re-created
        let snapshots = svc.payout_repo.get_snapshots_by_batch(batch_id).unwrap();
        assert!(!snapshots.is_empty(), "snapshots should be re-created");

        // Verify each miner got a payout and total sums match gross - fee
        let batch = svc.payout_repo.get_batch_by_id(batch_id).unwrap().unwrap();
        let total_payouts: i64 = rebuilt_payouts.iter().map(|p| p.amount).sum();
        let total_dust: i64 = rebuilt_payouts.iter().map(|p| p.dust_carried_forward).sum();
        // gross = 5000 (after minerfund), fee = 100, net = 4900 to miners
        assert_eq!(
            total_payouts + batch.pool_fee_amount + total_dust,
            5000,
            "gross reward = payouts + fee + dust"
        );
        assert!(rebuilt_payouts.iter().any(|p| p.worker_id == 1), "alice payout");
        assert!(rebuilt_payouts.iter().any(|p| p.worker_id == 2), "bob payout");
    }
}
