use std::sync::Arc;
use std::time::Duration;
use anyhow::Result;
use bitcoinsuite_bitcoind_nng::{
    Message, MiningWorkChanged, BlockDisconnected, PubInterface,
};
use tokio::sync::{broadcast, mpsc};
use tracing::{debug, error, info, warn};

use bitcoinsuite_core::Hashed;
use crate::accounting::{AccountingEvent, AccountingService};
use crate::node_integration::{NngRpcClient, JobCache, template_to_job};
use crate::stratum_protocol::job::MiningJob;

/// Debounce duration for miningwrkchg events: 100ms.
const MINING_WORK_COALESCE_MS: u64 = 100;

/// Consumes NNG pub/sub events and orchestrates the downstream effects.
///
/// Standalone task spawned in main.rs (not owned by StratumServer).
pub struct NngEventConsumer {
    interface: PubInterface,
    nng_rpc: Arc<NngRpcClient>,
    job_cache: Arc<JobCache>,
    accounting: Option<AccountingService>,
    job_tx: broadcast::Sender<Arc<MiningJob>>,
}

impl NngEventConsumer {
    /// Open a connection to the NNG pub socket and subscribe to relevant topics.
    pub fn new(
        pub_url: &str,
        nng_rpc: Arc<NngRpcClient>,
        job_cache: Arc<JobCache>,
        accounting: Option<AccountingService>,
        job_tx: broadcast::Sender<Arc<MiningJob>>,
    ) -> Result<Self> {
        let interface = PubInterface::open(pub_url)
            .map_err(|e| anyhow::anyhow!("failed to open NNG pub interface: {}", e))?;
        interface.subscribe("miningwrkchg")
            .map_err(|e| anyhow::anyhow!("failed to subscribe to miningwrkchg: {}", e))?;
        interface.subscribe("blkconnected")
            .map_err(|e| anyhow::anyhow!("failed to subscribe to blkconnected: {}", e))?;
        interface.subscribe("blkdisconctd")
            .map_err(|e| anyhow::anyhow!("failed to subscribe to blkdisconctd: {}", e))?;
        info!(pub_url, "NNG pub/sub consumer subscribed to events");
        Ok(Self { interface, nng_rpc, job_cache, accounting, job_tx })
    }

    /// Run the event loop until shutdown signal is received.
    pub async fn run(
        self,
        mut shutdown_signal: broadcast::Receiver<()>,
    ) -> Result<()> {
        info!("NNG pub/sub consumer starting event loop");

        let (raw_tx, raw_rx) = mpsc::channel::<MiningWorkChanged>(32);
        let (coalesced_tx, mut coalesced_rx) = mpsc::channel::<MiningWorkChanged>(32);

        let coalescer_shutdown = shutdown_signal.resubscribe();
        tokio::spawn(async move {
            coalesce_events(raw_rx, coalesced_tx, coalescer_shutdown).await;
        });

        loop {
            tokio::select! {
                msg = self.interface.recv_async() => {
                    match msg {
                        Ok(Message::MiningWorkChanged(event)) => {
                            debug!(
                                reason = ?event.reason,
                                height = event.height,
                                epoch = event.template_epoch,
                                "received miningwrkchg event",
                            );
                            if raw_tx.send(event).await.is_err() {
                                warn!("coalescer input channel closed");
                            }
                        }
                        Ok(Message::BlockDisconnected(event)) => {
                            let block_hash = event.block.header.hash.to_hex_be();
                            debug!(block_hash, "received blkdisconctd event");
                            if let Some(ref acct) = self.accounting {
                                handle_block_disconnected(event, acct).await;
                            }
                        }
                        Ok(_) => {}
                        Err(e) => {
                            error!(error = %e, "NNG pub/sub recv error");
                        }
                    }
                }
                Some(event) = coalesced_rx.recv() => {
                    self.on_mining_work_changed(event).await;
                }
                _ = shutdown_signal.recv() => {
                    info!("NNG pub/sub consumer shutting down");
                    break;
                }
            }
        }

        Ok(())
    }

    /// Handle a coalesced mining work change event.
    async fn on_mining_work_changed(&self, event: MiningWorkChanged) {
        info!(
            reason = ?event.reason,
            height = event.height,
            epoch = event.template_epoch,
            "processing mining work change",
        );

        let template = match self.nng_rpc.get_mining_template().await {
            Ok(t) => t,
            Err(e) => {
                error!(error = %e, "failed to fetch mining template after miningwrkchg");
                return;
            }
        };

        // Per UBQ: ALL miningwrkchg events trigger clean_jobs=true because
        // Lotus header includes block_size, which changes with every mempool update.
        // The reason code (NewTip/Reorg/MempoolRefresh/ManualInvalidation) is for
        // logging and observability only — not for behavioral branching.
        let job = Arc::new(template_to_job(&template, true));
        self.job_cache.insert((*job).clone()).await;

        debug!(job_id = %job.job_id, "broadcasting new job to all sessions");
        if self.job_tx.send(job).is_err() {
            warn!("no active session consumers for new job broadcast");
        }
    }
}

// ---- Standalone handler functions (testable without PubInterface) ----

/// Handle a block disconnected event: if the disconnected block matches a
/// found_block, mark it orphaned and close the associated round.
pub(crate) async fn handle_block_disconnected(
    event: BlockDisconnected,
    accounting: &AccountingService,
) {
    let block_hash = event.block.header.hash.to_hex_be();

    let found = match accounting.found_block_repo.get_by_hash(&block_hash) {
        Ok(Some(f)) => f,
        Ok(None) => {
            debug!(block_hash, "disconnected block not in found_blocks, ignoring");
            return;
        }
        Err(e) => {
            error!(block_hash, error = %e, "error querying found_blocks");
            return;
        }
    };

    info!(
        block_hash,
        height = found.height,
        round_id = found.round_id,
        "block disconnected — marking found_block as orphaned",
    );

    if let Err(e) = accounting.found_block_repo.mark_orphaned(&block_hash, "reorg_detected") {
        error!(error = %e, "failed to mark found_block as orphaned");
        return;
    }

    if let Err(e) = accounting.close_round(found.round_id, 0, "orphaned") {
        error!(error = %e, "failed to close orphaned round");
        return;
    }

    let acct_event = AccountingEvent {
        id: 0,
        event_type: "found_block_orphaned".to_string(),
        status: "orphaned".to_string(),
        session_id: None,
        worker_id: found.worker_id,
        worker_name: None,
        payout_address: None,
        round_id: Some(found.round_id),
        template_id: found.template_id,
        template_epoch: None,
        job_id: None,
        block_hash: Some(block_hash),
        height: Some(found.height),
        payload_json: None,
    };
    if let Err(e) = accounting.event_repo.record_event(&acct_event) {
        error!(error = %e, "failed to record found_block_orphaned accounting event");
    }
}

/// Coalesce rapid miningwrkchg events.
///
/// Reads raw events from `input`. Each time an event arrives, waits up to 100ms
/// for additional events. Only the latest event in each 100ms window is forwarded
/// to `output`. This prevents excessive template refreshes during mempool churn.
async fn coalesce_events(
    mut input: mpsc::Receiver<MiningWorkChanged>,
    output: mpsc::Sender<MiningWorkChanged>,
    mut shutdown: broadcast::Receiver<()>,
) {
    loop {
        let mut latest = match input.recv().await {
            Some(event) => event,
            None => break,
        };

        loop {
            tokio::select! {
                Some(event) = input.recv() => {
                    latest = event;
                }
                _ = tokio::time::sleep(Duration::from_millis(MINING_WORK_COALESCE_MS)) => {
                    break;
                }
                _ = shutdown.recv() => {
                    return;
                }
            }
        }

        if output.send(latest).await.is_err() {
            break;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bitcoinsuite_core::{Hashed, Sha256d};
    use bitcoinsuite_bitcoind_nng::{Block, BlockHeader};
    use parking_lot::Mutex;
    use rusqlite::Connection;
    use tempfile::NamedTempFile;
    use tokio::sync::broadcast;

    use crate::accounting::{init_schema, FoundBlockRepository};

    fn make_block_disconnected(hash_hex: &str) -> BlockDisconnected {
        let hash = Sha256d::from_hex_be(hash_hex)
            .unwrap_or_else(|_| Sha256d::new([0u8; 32]));
        BlockDisconnected {
            block: Block {
                header: BlockHeader {
                    raw: vec![],
                    hash,
                    prev_hash: Sha256d::new([0u8; 32]),
                    n_bits: 0,
                    timestamp: 0,
                },
                metadata: vec![],
                txs: vec![],
                file_num: 0,
                data_pos: 0,
                undo_pos: 0,
            },
        }
    }

    // ---- Coalescing tests ----

    #[tokio::test]
    async fn test_coalesce_single_event_passthrough() {
        let (raw_tx, raw_rx) = mpsc::channel::<MiningWorkChanged>(32);
        let (coalesced_tx, mut coalesced_rx) = mpsc::channel::<MiningWorkChanged>(32);
        let (_shutdown_tx, shutdown_rx) = broadcast::channel::<()>(1);

        tokio::spawn(async move {
            coalesce_events(raw_rx, coalesced_tx, shutdown_rx).await;
        });

        let event = MiningWorkChanged {
            reason: bitcoinsuite_bitcoind_nng::MiningWorkChangedReason::NewTip,
            block_hash: Sha256d::new([0u8; 32]),
            height: 1000,
            node_time: 12345,
            template_epoch: 42,
        };
        raw_tx.send(event.clone()).await.unwrap();

        let received = tokio::time::timeout(
            Duration::from_millis(200),
            coalesced_rx.recv(),
        )
        .await
        .expect("should receive coalesced event within 200ms")
        .expect("coalesced channel should not be closed");

        assert_eq!(received.height, 1000);
        assert_eq!(received.template_epoch, 42);
    }

    #[tokio::test]
    async fn test_coalesce_multiple_events_merged() {
        let (raw_tx, raw_rx) = mpsc::channel::<MiningWorkChanged>(32);
        let (coalesced_tx, mut coalesced_rx) = mpsc::channel::<MiningWorkChanged>(32);
        let (_shutdown_tx, shutdown_rx) = broadcast::channel::<()>(1);

        tokio::spawn(async move {
            coalesce_events(raw_rx, coalesced_tx, shutdown_rx).await;
        });

        for epoch in 1..=3 {
            raw_tx
                .send(MiningWorkChanged {
                    reason: bitcoinsuite_bitcoind_nng::MiningWorkChangedReason::MempoolRefresh,
                    block_hash: Sha256d::new([0u8; 32]),
                    height: 1000,
                    node_time: 12345 + epoch as i64,
                    template_epoch: epoch,
                })
                .await
                .unwrap();
        }

        let received = tokio::time::timeout(
            Duration::from_millis(200),
            coalesced_rx.recv(),
        )
        .await
        .expect("should receive coalesced event within 200ms")
        .expect("coalesced channel should not be closed");

        assert_eq!(
            received.template_epoch, 3,
            "should receive the latest event (epoch=3)"
        );

        let extra = tokio::time::timeout(Duration::from_millis(150), coalesced_rx.recv()).await;
        assert!(
            extra.is_err() || extra.unwrap().is_none(),
            "should not receive a second coalesced event"
        );
    }

    // ---- BlockDisconnected handler tests ----

    #[tokio::test]
    async fn test_handle_block_disconnected_orphans_matching_block() {
        let f = NamedTempFile::new().unwrap();
        let conn = Connection::open(f.path()).unwrap();
        init_schema(&conn).unwrap();
        let conn_arc = Arc::new(Mutex::new(conn));

        conn_arc.lock().execute(
            "INSERT INTO rounds (id, start_template_id, status) VALUES (1, 42, 'open')",
            [],
        )
        .unwrap();
        let repo = FoundBlockRepository::new(conn_arc.clone());
        repo.record_found_block(1, "000000000000000000000000000000000000000000000000000000000000abc1", 1000, None, Some(42), Some("json-rpc"))
            .unwrap();

        let accounting = AccountingService::new(conn_arc);
        let event = make_block_disconnected("000000000000000000000000000000000000000000000000000000000000abc1");

        handle_block_disconnected(event, &accounting).await;

        let found = accounting.found_block_repo.get_by_hash("000000000000000000000000000000000000000000000000000000000000abc1").unwrap().unwrap();
        assert_eq!(found.status, "orphaned");
        assert_eq!(found.orphan_reason, Some("reorg_detected".to_string()));

        let round = accounting.round_repo.get_by_id(1).unwrap().unwrap();
        assert_eq!(round.status, "orphaned");

        let events = accounting.event_repo.list_by_type("found_block_orphaned", 10, 0).unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].block_hash, Some("000000000000000000000000000000000000000000000000000000000000abc1".to_string()));
        assert_eq!(events[0].status, "orphaned");
    }

    #[tokio::test]
    async fn test_handle_block_disconnected_ignores_unknown_block() {
        let f = NamedTempFile::new().unwrap();
        let conn = Connection::open(f.path()).unwrap();
        init_schema(&conn).unwrap();
        let conn_arc = Arc::new(Mutex::new(conn));

        conn_arc.lock().execute(
            "INSERT INTO rounds (id, start_template_id, status) VALUES (1, 42, 'open')",
            [],
        )
        .unwrap();
        let repo = FoundBlockRepository::new(conn_arc.clone());
        repo.record_found_block(1, "000000000000000000000000000000000000000000000000000000000000000a", 1000, None, Some(42), Some("json-rpc"))
            .unwrap();

        let accounting = AccountingService::new(conn_arc);
        let event = make_block_disconnected("ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff");

        handle_block_disconnected(event, &accounting).await;

        let found = accounting.found_block_repo.get_by_hash("000000000000000000000000000000000000000000000000000000000000000a").unwrap().unwrap();
        assert_eq!(found.status, "confirmed", "known block should NOT be orphaned");

        let events = accounting.event_repo.list_by_type("found_block_orphaned", 10, 0).unwrap();
        assert!(events.is_empty(), "no orphan event should be recorded");
    }

    /// Stub: needs real-world NNG flatbuffer data to test the full miningwrkchg
    /// event processing (fetch template → convert → broadcast through job_tx).
    ///
    /// Steps to populate:
    ///   1. `PubInterface::open(pub_url)`, subscribe "miningwrkchg"
    ///   2. Trigger template change (new block or `lotusd invalidatetemplate`)
    ///   3. `recv_raw()` to capture prefix + payload bytes
    ///   4. Embed in this test, call `PubInterface::parse_msg()` to verify
    #[ignore]
    #[tokio::test]
    async fn test_miningwrkchg_deserialization() {
        unimplemented!("test requires real-world NNG flatbuffer data");
    }
}
