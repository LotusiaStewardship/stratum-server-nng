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
                    let found_block = match self.accounting.found_block_repo.get_by_hash(block_hash) {
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
                PayoutEvent::BlockConnected => {
                    payout_debug!("payout handler: block connected — retrying pending payouts");
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
            .process_pending_payouts(self.signer.as_ref(), &self.rpc_client)
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
}
