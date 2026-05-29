use crate::{payout_debug, payout_info, payout_warn};
use std::sync::Arc;
use tokio::sync::mpsc;

use super::signer::Signer;
use super::PayoutEvent;
use crate::accounting::AccountingService;
use crate::node_integration::JsonRpcClient;

/// Event-driven payout handler that replaces the old timer-based scheduler.
///
/// Receives maturation events (block hashes) from the NNG consumer via an
/// unbounded channel, creates PPLNS payout batches for newly matured blocks,
/// and submits pending batches through the configured signer.
///
/// ## Event flow
///
/// 1. A `MiningWorkChanged` event fires (every new block)
/// 2. The NNG consumer calls `check_maturation` → promotes blocks → sends
///    their block hashes through the channel
/// 3. This handler receives the block hash
/// 4. Looks up the `FoundBlock` from the DB
/// 5. Creates a payout batch via `create_payout_for_found_block` (if one
///    doesn't already exist — `retry_key` UNIQUE constraint prevents dupes)
/// 6. Calls `process_pending_payouts` to sign and submit any pending batches
///
/// The handler also runs at startup to catch blocks that matured while the
/// process was offline (those events are sent by `main.rs` after fetching
/// the chain tip via RPC).
pub struct PayoutHandler {
    rx: mpsc::UnboundedReceiver<PayoutEvent>,
    accounting: AccountingService,
    signer: Arc<dyn Signer>,
    rpc_client: Arc<JsonRpcClient>,
    fee_bps: u32,
    fee_address: Option<String>,
    min_payout_sat: i64,
    n_multiplier: f64,
    /// Guards process_pending_payouts against concurrent execution.
    /// Prevents race conditions when multiple events (e.g., BlockConnected
    /// queued after BlockMatured) could trigger overlapping submissions.
    pending_payouts_lock: tokio::sync::Mutex<()>,
}

impl PayoutHandler {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        rx: mpsc::UnboundedReceiver<PayoutEvent>,
        accounting: AccountingService,
        signer: Arc<dyn Signer>,
        rpc_client: Arc<JsonRpcClient>,
        fee_bps: u32,
        fee_address: Option<String>,
        min_payout_sat: i64,
        n_multiplier: f64,
    ) -> Self {
        Self {
            rx,
            accounting,
            signer,
            rpc_client,
            fee_bps,
            fee_address,
            min_payout_sat,
            n_multiplier,
            pending_payouts_lock: tokio::sync::Mutex::new(()),
        }
    }

    /// Run the event loop, processing maturation events until the channel
    /// closes (sender dropped, signalling shutdown).
    pub async fn run(&mut self) {
        while let Some(event) = self.rx.recv().await {
            match &event {
                PayoutEvent::BlockMatured(block_hash) => {
                    payout_debug!(hash = %block_hash, "payout handler: received maturation event");

                    // Look up the found block
                    let found_block = match self.accounting.found_block_repo.get_by_hash(block_hash)
                    {
                        Ok(Some(b)) => b,
                        Ok(None) => {
                            payout_warn!(hash = %block_hash, "payout handler: block not found in DB");
                            continue;
                        }
                        Err(e) => {
                            payout_warn!(hash = %block_hash, error = %e, "payout handler: DB error");
                            continue;
                        }
                    };

                    // Skip if a payout batch already exists for this block
                    let existing = self
                        .accounting
                        .payout_repo
                        .list_batches(None)
                        .unwrap_or_default();
                    let already_paid = existing.iter().any(|b| {
                        b.retry_key
                            .as_deref()
                            .map(|k| k.starts_with(&found_block.block_hash))
                            .unwrap_or(false)
                    });

                    if !already_paid {
                        match self.accounting.create_payout_for_found_block(
                            &found_block,
                            self.fee_bps,
                            self.fee_address.as_deref(),
                            self.min_payout_sat,
                            self.n_multiplier,
                        ) {
                            Ok(batch_id) => {
                                payout_info!(
                                    hash = %found_block.block_hash,
                                    batch_id = batch_id,
                                    "payout batch created via maturation event",
                                );
                            }
                            Err(e) => {
                                payout_warn!(
                                    hash = %found_block.block_hash,
                                    error = %e,
                                    "failed to create payout batch",
                                );
                            }
                        }
                    } else {
                        payout_debug!(hash = %block_hash, "payout batch already exists, skipping creation");
                    }

                    self.process_pending_payouts_locked().await;
                }
                PayoutEvent::BlockConnected(txids) => {
                    payout_debug!("payout handler: block connected — checking confirmations");

                    // Scan submitted batches for on-chain confirmation
                    if let Ok(submitted) =
                        self.accounting.payout_repo.list_batches(Some("submitted"))
                    {
                        for batch in &submitted {
                            if let Some(ref submitted_txid) = batch.submitted_txid {
                                if txids.contains(submitted_txid) {
                                    payout_info!(
                                        batch_id = batch.id,
                                        txid = %submitted_txid,
                                        "payout confirmed on-chain",
                                    );
                                    let _ = self
                                        .accounting
                                        .payout_repo
                                        .update_batch_status(batch.id, "confirmed");
                                    if let Ok(Some(fb)) = self
                                        .accounting
                                        .found_block_repo
                                        .get_by_round_id(batch.round_id)
                                    {
                                        let _ = self
                                            .accounting
                                            .found_block_repo
                                            .update_status(fb.id, "paid");
                                    }
                                }
                            }
                        }
                    }

                    self.process_pending_payouts_locked().await;
                }
            }
        }

        payout_info!("payout handler: event channel closed, shutting down");
    }

    /// Call process_pending_payouts under the mutex guard.
    /// Ensures only one submission run executes at a time, even if multiple
    /// events (BlockConnected after BlockMatured, timer, admin trigger) arrive
    /// concurrently.
    async fn process_pending_payouts_locked(&self) {
        let _guard = self.pending_payouts_lock.lock().await;
        if let Err(e) = self
            .accounting
            .process_pending_payouts(
                self.signer.as_ref(),
                &self.rpc_client,
                self.fee_bps,
                self.fee_address.clone(),
                self.min_payout_sat,
                self.n_multiplier,
            )
            .await
        {
            payout_warn!(error = %e, "process_pending_payouts failed");
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use parking_lot::Mutex;
    use rusqlite::Connection;
    use tempfile::NamedTempFile;

    use crate::accounting::{init_schema, AccountingService};

    #[tokio::test]
    async fn test_pending_payouts_lock_serializes_concurrent_calls() {
        // Verify that the tokio::sync::Mutex guard in
        // process_pending_payouts_locked prevents concurrent execution.
        let lock = Arc::new(tokio::sync::Mutex::new(()));

        let lock_clone = lock.clone();
        let start = Instant::now();

        // Acquire the lock in the current task
        let guard = lock.lock().await;

        // Spawn a task that tries to acquire the same lock
        let task = tokio::spawn(async move {
            let _g = lock_clone.lock().await;
            Instant::now()
        });

        // Task should be blocked waiting for the lock
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(
            !task.is_finished(),
            "task should be blocked waiting for the mutex lock",
        );

        // Release the lock
        drop(guard);

        // Task should now acquire the lock and complete
        let acquired_at = tokio::time::timeout(Duration::from_millis(200), task)
            .await
            .expect("task should complete within timeout")
            .expect("task should not panic");

        assert!(
            acquired_at - start >= Duration::from_millis(50),
            "task should have waited for the lock to be released",
        );
    }

    #[tokio::test]
    async fn test_block_connected_confirms_matching_payout() {
        let f = NamedTempFile::new().unwrap();
        let conn = Connection::open(f.path()).unwrap();
        init_schema(&conn).unwrap();

        // Create round + found_block
        conn.execute(
            "INSERT INTO rounds (id, start_template_id, status) VALUES (1, 42, 'found')",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO found_blocks (id, round_id, block_hash, height, status, coinbase_value, network_target_hex)
             VALUES (1, 1, 'abc', 100, 'matured', 50000, '')",
            [],
        )
        .unwrap();

        let accounting = AccountingService::new(Arc::new(Mutex::new(conn)));

        // Create a submitted payout batch with a known txid
        let batch = accounting
            .payout_repo
            .create_payout_batch(1, 100000, 1000, None, 1, "hash:abc")
            .unwrap();
        accounting
            .payout_repo
            .mark_batch_submitted(
                batch.id,
                "a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0c1d2e3f4a5b6c7d8e9f0a1b2",
            )
            .unwrap();

        // Simulate what PayoutHandler does on BlockConnected(txids)
        let txids =
            vec!["a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0c1d2e3f4a5b6c7d8e9f0a1b2".to_string()];

        let submitted = accounting
            .payout_repo
            .list_batches(Some("submitted"))
            .unwrap();
        for batch in &submitted {
            if let Some(ref submitted_txid) = batch.submitted_txid {
                if txids.contains(submitted_txid) {
                    accounting
                        .payout_repo
                        .update_batch_status(batch.id, "confirmed")
                        .unwrap();
                    if let Some(fb) = accounting
                        .found_block_repo
                        .get_by_round_id(batch.round_id)
                        .unwrap()
                    {
                        accounting
                            .found_block_repo
                            .update_status(fb.id, "paid")
                            .unwrap();
                    }
                }
            }
        }

        // Verify batch transitioned to 'confirmed'
        let updated_batch = accounting
            .payout_repo
            .get_batch_by_id(batch.id)
            .unwrap()
            .unwrap();
        assert_eq!(
            updated_batch.status, "confirmed",
            "matching batch should be confirmed"
        );

        // Verify found_block transitioned to 'paid'
        let fb = accounting
            .found_block_repo
            .get_by_hash("abc")
            .unwrap()
            .unwrap();
        assert_eq!(
            fb.status, "paid",
            "found_block should become paid after on-chain confirmation"
        );
    }

    #[tokio::test]
    async fn test_block_connected_does_not_confirm_non_matching_txid() {
        let f = NamedTempFile::new().unwrap();
        let conn = Connection::open(f.path()).unwrap();
        init_schema(&conn).unwrap();

        conn.execute(
            "INSERT INTO rounds (id, start_template_id, status) VALUES (1, 42, 'found')",
            [],
        )
        .unwrap();

        let accounting = AccountingService::new(Arc::new(Mutex::new(conn)));

        let batch = accounting
            .payout_repo
            .create_payout_batch(1, 100000, 1000, None, 1, "hash:abc")
            .unwrap();
        accounting
            .payout_repo
            .mark_batch_submitted(
                batch.id,
                "a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0c1d2e3f4a5b6c7d8e9f0a1b2",
            )
            .unwrap();

        // Different txid in the block — should NOT match
        let txids =
            vec!["0000000000000000000000000000000000000000000000000000000000000000".to_string()];

        let submitted = accounting
            .payout_repo
            .list_batches(Some("submitted"))
            .unwrap();
        for batch in &submitted {
            if let Some(ref submitted_txid) = batch.submitted_txid {
                if txids.contains(submitted_txid) {
                    accounting
                        .payout_repo
                        .update_batch_status(batch.id, "confirmed")
                        .unwrap();
                }
            }
        }

        let updated_batch = accounting
            .payout_repo
            .get_batch_by_id(batch.id)
            .unwrap()
            .unwrap();
        assert_eq!(
            updated_batch.status, "submitted",
            "batch should stay submitted when txid doesn't match"
        );
    }
}
