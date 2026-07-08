use crate::{node_int_debug, node_int_error, node_int_info, node_int_warn};
use anyhow::Result;
use bitcoinsuite_bitcoind_nng::{BlockDisconnected, Message, MiningWorkChanged, PubInterface};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{broadcast, mpsc};

use crate::accounting::{AccountingEvent, AccountingService, ChainTip};
use crate::node_integration::{template_to_job, JobCache, NngRpcClient};
use crate::payout::PayoutEvent;
use crate::stratum_protocol::job::MiningJob;
use bitcoinsuite_core::{BitcoinCode, Bytes, Hashed, LotusHeader};

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
    chain_tip: ChainTip,
    maturation_tx: mpsc::UnboundedSender<PayoutEvent>,
    min_confirmations: u64,
}

impl NngEventConsumer {
    /// Open a connection to the NNG pub socket and subscribe to relevant topics.
    pub fn new(
        pub_url: &str,
        nng_rpc: Arc<NngRpcClient>,
        job_cache: Arc<JobCache>,
        accounting: Option<AccountingService>,
        job_tx: broadcast::Sender<Arc<MiningJob>>,
        chain_tip: ChainTip,
        maturation_tx: mpsc::UnboundedSender<PayoutEvent>,
        min_confirmations: u64,
    ) -> Result<Self> {
        let interface = PubInterface::open(pub_url)
            .map_err(|e| anyhow::anyhow!("failed to open NNG pub interface: {}", e))?;
        interface
            .subscribe("miningwrkchg")
            .map_err(|e| anyhow::anyhow!("failed to subscribe to miningwrkchg: {}", e))?;
        interface
            .subscribe("blkconnected")
            .map_err(|e| anyhow::anyhow!("failed to subscribe to blkconnected: {}", e))?;
        interface
            .subscribe("blkdisconctd")
            .map_err(|e| anyhow::anyhow!("failed to subscribe to blkdisconctd: {}", e))?;
        node_int_info!(pub_url, "NNG pub/sub consumer subscribed to events");
        Ok(Self {
            interface,
            nng_rpc,
            job_cache,
            accounting,
            job_tx,
            chain_tip,
            maturation_tx,
            min_confirmations,
        })
    }

    /// Run the event loop until shutdown signal is received.
    pub async fn run(self, mut shutdown_signal: broadcast::Receiver<()>) -> Result<()> {
        node_int_info!("NNG pub/sub consumer starting event loop");

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
                            node_int_debug!(
                                reason = ?event.reason,
                                height = event.height,
                                epoch = event.template_epoch,
                                "received miningwrkchg event",
                            );
                            if raw_tx.send(event).await.is_err() {
                                node_int_warn!("coalescer input channel closed");
                            }
                        }
                        Ok(Message::BlockDisconnected(event)) => {
                            let block_hash = event.block.header.hash.to_hex_be();
                            node_int_debug!(block_hash, "received blkdisconctd event");
                            node_int_debug!(
                                block_hash = %block_hash,
                                prev_hash = %event.block.header.prev_hash.to_hex_be(),
                                n_bits = event.block.header.n_bits,
                                "block disconnected event payload",
                            );
                            if let Some(ref acct) = self.accounting {
                                handle_block_disconnected(event, acct).await;
                            }
                        }
                        Ok(Message::BlockConnected(event)) => {
                            let block_hash = event.block.header.hash.to_hex_be();
                            node_int_debug!(
                                block_hash = %block_hash,
                                prev_hash = %event.block.header.prev_hash.to_hex_be(),
                                n_bits = event.block.header.n_bits,
                                timestamp = event.block.header.timestamp,
                                "block connected event payload",
                            );
                            if let Some(ref acct) = self.accounting {
                                handle_block_connected(
                                    event,
                                    acct,
                                    &self.chain_tip,
                                    &self.maturation_tx,
                                    self.min_confirmations,
                                ).await;
                            }
                        }
                        Ok(_) => {}
                        Err(e) => {
                            node_int_error!(error = %e, "NNG pub/sub recv error");
                        }
                    }
                }
                Some(event) = coalesced_rx.recv() => {
                    self.on_mining_work_changed(event).await;
                }
                _ = shutdown_signal.recv() => {
                    node_int_info!("NNG pub/sub consumer shutting down");
                    break;
                }
            }
        }

        Ok(())
    }

    /// Handle a coalesced mining work change event.
    async fn on_mining_work_changed(&self, event: MiningWorkChanged) {
        node_int_info!(
            reason = ?event.reason,
            height = event.height,
            epoch = event.template_epoch,
            "processing mining work change",
        );

        let template = match self.nng_rpc.get_mining_template().await {
            Ok(t) => t,
            Err(e) => {
                node_int_error!(error = %e, "failed to fetch mining template after miningwrkchg");
                return;
            }
        };

        // Per UBQ: ALL miningwrkchg events trigger clean_jobs=true because
        // Lotus header includes block_size, which changes with every mempool update.
        // The reason code (NewTip/Reorg/MempoolRefresh/ManualInvalidation) is also
        // forwarded to miners via MiningJob.reason for UX transparency.
        let mut job = match template_to_job(&template, true) {
            Ok(job) => job,
            Err(e) => {
                node_int_error!(
                    error = %e,
                    template_id = template.template_id,
                    height = template.height,
                    "failed to convert mining template to job — skipping refresh",
                );
                return;
            }
        };
        job.reason = miningwrkchg_reason_to_string(event.reason).to_string();
        let job = Arc::new(job);
        self.job_cache.insert((*job).clone()).await;

        node_int_debug!(
            job_id = %job.job_id,
            template_id = job.template_id,
            prevhash = %job.prevhash,
            coinbase1 = %job.coinbase1,
            coinbase2 = %job.coinbase2,
            merkle_branches = %serde_json::to_string(&job.merkle_branches).unwrap_or_default(),
            version = %job.version,
            nbits = %job.nbits,
            ntime = %job.ntime,
            network_target_hex = %job.network_target_hex,
            clean_jobs = job.clean_jobs,
            template_epoch = job.template_epoch,
            height = job.height,
            epoch_hash = %job.epoch_hash,
            extended_metadata_hash = %job.extended_metadata_hash,
            block_size = job.block_size,
            reason = ?event.reason,
            "mining job from wrkchg event",
        );

        node_int_debug!(job_id = %job.job_id, "broadcasting new job to all sessions");
        if self.job_tx.send(job).is_err() {
            node_int_warn!("no active session consumers for new job broadcast");
        }
    }
}

// ---- Work change reason helpers ----

/// Convert a MiningWorkChangedReason to the short string sent to miners.
pub fn miningwrkchg_reason_to_string(
    reason: bitcoinsuite_bitcoind_nng::MiningWorkChangedReason,
) -> &'static str {
    use bitcoinsuite_bitcoind_nng::MiningWorkChangedReason::*;
    match reason {
        NewTip => "new-tip",
        Reorg => "reorg",
        MempoolRefresh => "mempool",
        ManualInvalidation => "manual",
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
            node_int_debug!(
                block_hash,
                "disconnected block not in found_blocks, ignoring"
            );
            return;
        }
        Err(e) => {
            node_int_error!(block_hash, error = %e, "error querying found_blocks");
            return;
        }
    };

    node_int_info!(
        block_hash,
        height = found.height,
        round_id = found.round_id,
        "block disconnected — marking found_block as orphaned",
    );

    if let Err(e) = accounting
        .found_block_repo
        .mark_orphaned(&block_hash, "reorg_detected")
    {
        node_int_error!(error = %e, "failed to mark found_block as orphaned");
        return;
    }

    if let Err(e) = accounting.close_round(found.round_id, 0, "orphaned") {
        node_int_error!(error = %e, "failed to close orphaned round");
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
        node_int_error!(error = %e, "failed to record found_block_orphaned accounting event");
    }
}

/// Handle a block connected event: update chain tip, check for block maturation,
/// and send maturation events to the payout handler.
///
/// The block height is decoded from the serialized Lotus header bytes in
/// `event.block.header.raw`.
pub(crate) async fn handle_block_connected(
    event: bitcoinsuite_bitcoind_nng::BlockConnected,
    accounting: &AccountingService,
    chain_tip: &ChainTip,
    maturation_tx: &mpsc::UnboundedSender<PayoutEvent>,
    min_confirmations: u64,
) {
    // Decode height from the serialized Lotus header
    let height = match LotusHeader::deser(&mut Bytes::from_slice(&event.block.header.raw)) {
        Ok(header) => header.height,
        Err(e) => {
            node_int_error!(
                hash = %event.block.header.hash.to_hex_be(),
                error = %e,
                "failed to decode Lotus header from BlockConnected event"
            );
            return;
        }
    };

    chain_tip.update_block_connected(height);
    let tip = chain_tip.get();

    // Check if this block reconnects an orphaned pool block (reorg reversal).
    let block_hash = event.block.header.hash.to_hex_be();
    if let Ok(Some(fb)) = accounting.found_block_repo.get_by_hash(&block_hash) {
        if fb.status == "orphaned" {
            match accounting
                .payout_repo
                .get_batches_by_block_hash(&block_hash)
            {
                Ok(batches) => {
                    let has_submitted = batches.iter().any(|b| b.status == "submitted");
                    if has_submitted {
                        node_int_warn!(
                            hash = %block_hash,
                            "orphaned block reconnected but has submitted payout — cannot un-orphan",
                        );
                    } else {
                        let has_pending = batches.iter().any(|b| b.status == "pending");
                        if let Err(e) = accounting.found_block_repo.un_orphan(&block_hash) {
                            node_int_error!(hash = %block_hash, error = %e, "failed to un-orphan block");
                        } else {
                            if has_pending {
                                // Block was already matured with a payout batch — restore to matured
                                let _ = accounting.found_block_repo.mark_matured(fb.id);
                            }
                            node_int_info!(
                                hash = %block_hash,
                                unorphaned_to = if has_pending { "matured" } else { "immature" },
                                "reconnected orphaned block",
                            );
                        }
                    }
                }
                Err(e) => {
                    node_int_error!(
                        hash = %block_hash,
                        error = %e,
                        "failed to query batches for orphan check",
                    );
                }
            }
        }
    }

    match accounting.check_maturation(tip, min_confirmations) {
        Ok(matured_blocks) => {
            for matured in &matured_blocks {
                node_int_debug!(
                    hash = %matured.block_hash,
                    height = matured.height,
                    "block matured via blkconnected event",
                );
                let _ = maturation_tx.send(PayoutEvent::BlockMatured(matured.block_hash.clone()));
            }
        }
        Err(e) => {
            node_int_warn!(
                error = %e,
                "maturation check failed after blkconnected",
            );
        }
    }

    // Extract transaction IDs from the connected block for payout confirmation scanning.
    let txids: Vec<String> = event
        .block
        .txs
        .iter()
        .map(|bt| bt.tx.txid.to_hex_be())
        .collect();

    // Send BlockConnected with txids so the payout handler can check for on-chain
    // confirmation of submitted payouts and retry any pending submissions.
    let _ = maturation_tx.send(PayoutEvent::BlockConnected(txids));
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
    use bitcoinsuite_bitcoind_nng::{Block, BlockHeader};
    use bitcoinsuite_core::{BitcoinCode, Bytes, BytesMut, Hashed, LotusHeader, Sha256d};
    use parking_lot::Mutex;
    use rusqlite::Connection;
    use tempfile::NamedTempFile;
    use tokio::sync::broadcast;

    use crate::accounting::{init_schema, FoundBlockRepository};

    fn make_header_bytes(height: i32) -> Vec<u8> {
        let header = LotusHeader {
            prev_block: Sha256d::new([0u8; 32]),
            bits: 0x1d00ffff,
            timestamp: 1_700_000_000,
            reserved: 0,
            nonce: 0,
            version: 1,
            size: 1000,
            height,
            epoch_hash: Sha256d::new([0u8; 32]),
            merkle_root: Sha256d::new([0u8; 32]),
            extended_metadata_hash: Sha256d::new([0u8; 32]),
        };
        let mut buf = BytesMut::new();
        header.ser_to(&mut buf);
        buf.freeze().to_vec()
    }

    fn make_block_connected(height: i32) -> bitcoinsuite_bitcoind_nng::BlockConnected {
        let raw = make_header_bytes(height);
        bitcoinsuite_bitcoind_nng::BlockConnected {
            block: Block {
                header: BlockHeader {
                    raw,
                    hash: Sha256d::new([0u8; 32]),
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

    fn make_block_disconnected(hash_hex: &str) -> BlockDisconnected {
        let hash = Sha256d::from_hex_be(hash_hex).unwrap_or_else(|_| Sha256d::new([0u8; 32]));
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

        let received = tokio::time::timeout(Duration::from_millis(200), coalesced_rx.recv())
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

        let received = tokio::time::timeout(Duration::from_millis(200), coalesced_rx.recv())
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

        conn_arc
            .lock()
            .execute(
                "INSERT INTO rounds (id, start_template_id, status) VALUES (1, 42, 'open')",
                [],
            )
            .unwrap();
        let repo = FoundBlockRepository::new(conn_arc.clone());
        repo.record_found_block(
            1,
            "000000000000000000000000000000000000000000000000000000000000abc1",
            1000,
            None,
            Some(42),
            Some("json-rpc"),
            0,
            "",
        )
        .unwrap();

        let accounting = AccountingService::new(conn_arc);
        let event = make_block_disconnected(
            "000000000000000000000000000000000000000000000000000000000000abc1",
        );

        handle_block_disconnected(event, &accounting).await;

        let found = accounting
            .found_block_repo
            .get_by_hash("000000000000000000000000000000000000000000000000000000000000abc1")
            .unwrap()
            .unwrap();
        assert_eq!(found.status, "orphaned");
        assert_eq!(found.orphan_reason, Some("reorg_detected".to_string()));

        let round = accounting.round_repo.get_by_id(1).unwrap().unwrap();
        assert_eq!(round.status, "orphaned");

        let events = accounting
            .event_repo
            .list_by_type("found_block_orphaned", 10, 0)
            .unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(
            events[0].block_hash,
            Some("000000000000000000000000000000000000000000000000000000000000abc1".to_string())
        );
        assert_eq!(events[0].status, "orphaned");
    }

    #[tokio::test]
    async fn test_handle_block_disconnected_ignores_unknown_block() {
        let f = NamedTempFile::new().unwrap();
        let conn = Connection::open(f.path()).unwrap();
        init_schema(&conn).unwrap();
        let conn_arc = Arc::new(Mutex::new(conn));

        conn_arc
            .lock()
            .execute(
                "INSERT INTO rounds (id, start_template_id, status) VALUES (1, 42, 'open')",
                [],
            )
            .unwrap();
        let repo = FoundBlockRepository::new(conn_arc.clone());
        repo.record_found_block(
            1,
            "000000000000000000000000000000000000000000000000000000000000000a",
            1000,
            None,
            Some(42),
            Some("json-rpc"),
            0,
            "",
        )
        .unwrap();

        let accounting = AccountingService::new(conn_arc);
        let event = make_block_disconnected(
            "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
        );

        handle_block_disconnected(event, &accounting).await;

        let found = accounting
            .found_block_repo
            .get_by_hash("000000000000000000000000000000000000000000000000000000000000000a")
            .unwrap()
            .unwrap();
        assert_eq!(
            found.status, "immature",
            "known block should NOT be orphaned"
        );

        let events = accounting
            .event_repo
            .list_by_type("found_block_orphaned", 10, 0)
            .unwrap();
        assert!(events.is_empty(), "no orphan event should be recorded");
    }

    #[test]
    fn test_miningwrkchg_reason_to_string_new_tip() {
        use bitcoinsuite_bitcoind_nng::MiningWorkChangedReason;
        assert_eq!(
            miningwrkchg_reason_to_string(MiningWorkChangedReason::NewTip),
            "new-tip"
        );
    }

    #[test]
    fn test_miningwrkchg_reason_to_string_reorg() {
        use bitcoinsuite_bitcoind_nng::MiningWorkChangedReason;
        assert_eq!(
            miningwrkchg_reason_to_string(MiningWorkChangedReason::Reorg),
            "reorg"
        );
    }

    #[test]
    fn test_miningwrkchg_reason_to_string_mempool() {
        use bitcoinsuite_bitcoind_nng::MiningWorkChangedReason;
        assert_eq!(
            miningwrkchg_reason_to_string(MiningWorkChangedReason::MempoolRefresh),
            "mempool"
        );
    }

    #[test]
    fn test_miningwrkchg_reason_to_string_manual() {
        use bitcoinsuite_bitcoind_nng::MiningWorkChangedReason;
        assert_eq!(
            miningwrkchg_reason_to_string(MiningWorkChangedReason::ManualInvalidation),
            "manual"
        );
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

    // ---- BlockConnected handler tests ----

    #[tokio::test]
    async fn test_blkconnected_height_parsing() {
        let event = make_block_connected(1000);
        let height = LotusHeader::deser(&mut Bytes::from_slice(&event.block.header.raw))
            .unwrap()
            .height;
        assert_eq!(height, 1000);
    }

    #[tokio::test]
    async fn test_blkconnected_updates_chain_tip() {
        let f = NamedTempFile::new().unwrap();
        let conn = Connection::open(f.path()).unwrap();
        init_schema(&conn).unwrap();
        let accounting = AccountingService::new(Arc::new(Mutex::new(conn)));
        let chain_tip = ChainTip::new(0);
        let (maturation_tx, _maturation_rx) = mpsc::unbounded_channel::<PayoutEvent>();

        let event = make_block_connected(500);
        handle_block_connected(event, &accounting, &chain_tip, &maturation_tx, 100).await;

        assert_eq!(chain_tip.get(), 500);
    }

    #[tokio::test]
    async fn test_blkconnected_triggers_maturation() {
        let f = NamedTempFile::new().unwrap();
        let conn = Connection::open(f.path()).unwrap();
        init_schema(&conn).unwrap();
        let accounting = AccountingService::new(Arc::new(Mutex::new(conn)));
        let chain_tip = ChainTip::new(0);
        let (maturation_tx, mut maturation_rx) = mpsc::unbounded_channel::<PayoutEvent>();

        // Record a found_block at height 100 (immature)
        let round = accounting.resolve_round_for_template(42).unwrap();
        accounting
            .record_found_block(
                round.id,
                "block1",
                100,
                None,
                Some(42),
                Some("json-rpc"),
                50000,
                "00000000ffff0000000000000000000000000000000000000000000000000000",
            )
            .unwrap();

        // BlockConnected at height 200 → confirmations = 200 - 100 + 1 = 101 >= 100
        let event = make_block_connected(200);
        handle_block_connected(event, &accounting, &chain_tip, &maturation_tx, 100).await;

        // The immature block should be matured
        let found = accounting
            .found_block_repo
            .get_by_hash("block1")
            .unwrap()
            .unwrap();
        assert_eq!(found.status, "matured");

        // The hash should appear on the maturation channel
        let received = tokio::time::timeout(Duration::from_millis(100), maturation_rx.recv())
            .await
            .expect("should receive maturation event")
            .expect("channel should not be closed");
        assert_eq!(received, PayoutEvent::BlockMatured("block1".to_string()));
    }

    #[tokio::test]
    async fn test_blkconnected_sends_both_matured_and_connected() {
        let f = NamedTempFile::new().unwrap();
        let conn = Connection::open(f.path()).unwrap();
        init_schema(&conn).unwrap();
        let accounting = AccountingService::new(Arc::new(Mutex::new(conn)));
        let chain_tip = ChainTip::new(0);
        let (maturation_tx, mut maturation_rx) = mpsc::unbounded_channel::<PayoutEvent>();

        // Record a found_block at height 100 (immature)
        let round = accounting.resolve_round_for_template(42).unwrap();
        accounting
            .record_found_block(
                round.id,
                "block1",
                100,
                None,
                Some(42),
                Some("json-rpc"),
                50000,
                "00000000ffff0000000000000000000000000000000000000000000000000000",
            )
            .unwrap();

        // BlockConnected at height 200 → 101 confirmations → matures block1
        let event = make_block_connected(200);
        handle_block_connected(event, &accounting, &chain_tip, &maturation_tx, 100).await;

        // Should receive BlockMatured first, then BlockConnected
        let first = tokio::time::timeout(Duration::from_millis(100), maturation_rx.recv())
            .await
            .expect("should receive first event")
            .expect("channel should not be closed");
        assert!(
            matches!(&first, PayoutEvent::BlockMatured(h) if h == "block1"),
            "expected BlockMatured first, got: {:?}",
            first,
        );

        let second = tokio::time::timeout(Duration::from_millis(100), maturation_rx.recv())
            .await
            .expect("should receive second event")
            .expect("channel should not be closed");
        assert!(
            matches!(second, PayoutEvent::BlockConnected(_)),
            "expected BlockConnected second, got: {:?}",
            second,
        );
    }

    #[tokio::test]
    async fn test_blkconnected_no_pool_blocks() {
        let f = NamedTempFile::new().unwrap();
        let conn = Connection::open(f.path()).unwrap();
        init_schema(&conn).unwrap();
        // Set up a round (required for record_found_block to work if we add one)
        let _ = conn.execute(
            "INSERT INTO rounds (id, start_template_id, status) VALUES (1, 42, 'open')",
            [],
        );
        let accounting = AccountingService::new(Arc::new(Mutex::new(conn)));
        let chain_tip = ChainTip::new(0);
        let (maturation_tx, mut maturation_rx) = mpsc::unbounded_channel::<PayoutEvent>();

        // BlockConnected at a high height with no pool blocks in DB
        let event = make_block_connected(99999);
        handle_block_connected(event, &accounting, &chain_tip, &maturation_tx, 100).await;

        // Chain tip should still advance
        assert_eq!(chain_tip.get(), 99999);

        // Should receive a BlockConnected signal (every blkconnected triggers retry)
        let result = tokio::time::timeout(Duration::from_millis(50), maturation_rx.recv()).await;
        match result {
            Ok(Some(PayoutEvent::BlockConnected(_))) => { /* expected */ }
            other => panic!("expected BlockConnected signal, got: {:?}", other,),
        }
    }

    #[tokio::test]
    async fn test_blkconnected_un_orphans_block_without_batch() {
        let f = NamedTempFile::new().unwrap();
        let conn = Connection::open(f.path()).unwrap();
        init_schema(&conn).unwrap();
        conn.execute(
            "INSERT INTO rounds (id, start_template_id, status) VALUES (1, 42, 'found')",
            [],
        )
        .unwrap();
        let accounting = AccountingService::new(Arc::new(Mutex::new(conn)));
        let chain_tip = ChainTip::new(0);
        let (maturation_tx, _maturation_rx) = mpsc::unbounded_channel::<PayoutEvent>();

        // Create a found_block with the same hash the default make_block_connected produces
        let zero_hash = Sha256d::new([0u8; 32]).to_hex_be();
        accounting
            .found_block_repo
            .record_found_block(1, &zero_hash, 100, None, None, None, 50000, "")
            .unwrap();
        accounting
            .found_block_repo
            .mark_orphaned(&zero_hash, "reorg_detected")
            .unwrap();

        let event = make_block_connected(200);
        handle_block_connected(event, &accounting, &chain_tip, &maturation_tx, 100).await;

        let fb = accounting
            .found_block_repo
            .get_by_hash(&zero_hash)
            .unwrap()
            .unwrap();
        // The orphan check restores to immature, then the maturation check immediately
        // promotes it because it already meets maturity depth (100+100 <= 200).
        assert_eq!(
            fb.status, "matured",
            "orphaned block should be restored and matured in same event"
        );
        assert_eq!(fb.orphan_reason, None, "orphan_reason should be cleared");
    }

    #[tokio::test]
    async fn test_blkconnected_un_orphans_block_with_pending_batch() {
        let f = NamedTempFile::new().unwrap();
        let conn = Connection::open(f.path()).unwrap();
        init_schema(&conn).unwrap();
        conn.execute(
            "INSERT INTO rounds (id, start_template_id, status) VALUES (1, 42, 'found')",
            [],
        )
        .unwrap();
        let accounting = AccountingService::new(Arc::new(Mutex::new(conn)));
        let chain_tip = ChainTip::new(0);
        let (maturation_tx, _maturation_rx) = mpsc::unbounded_channel::<PayoutEvent>();

        // Create a found_block and mature it
        let zero_hash = Sha256d::new([0u8; 32]).to_hex_be();
        let fb = accounting
            .found_block_repo
            .record_found_block(1, &zero_hash, 100, None, None, None, 50000, "")
            .unwrap();
        accounting.found_block_repo.mark_matured(fb.id).unwrap();

        // Create a pending payout batch
        accounting
            .payout_repo
            .create_payout_batch(1, 100000, 1000, None, 1, &format!("hash:{}", &zero_hash))
            .unwrap();

        // Now orphan the block
        accounting
            .found_block_repo
            .mark_orphaned(&zero_hash, "reorg_detected")
            .unwrap();

        let event = make_block_connected(200);
        handle_block_connected(event, &accounting, &chain_tip, &maturation_tx, 100).await;

        let fb = accounting
            .found_block_repo
            .get_by_hash(&zero_hash)
            .unwrap()
            .unwrap();
        assert_eq!(
            fb.status, "matured",
            "orphaned block with pending batch should be restored to matured"
        );
        assert_eq!(fb.orphan_reason, None, "orphan_reason should be cleared");
    }

    #[tokio::test]
    async fn test_blkconnected_does_not_un_orphan_with_submitted_batch() {
        let f = NamedTempFile::new().unwrap();
        let conn = Connection::open(f.path()).unwrap();
        init_schema(&conn).unwrap();
        conn.execute(
            "INSERT INTO rounds (id, start_template_id, status) VALUES (1, 42, 'found')",
            [],
        )
        .unwrap();
        let accounting = AccountingService::new(Arc::new(Mutex::new(conn)));
        let chain_tip = ChainTip::new(0);
        let (maturation_tx, _maturation_rx) = mpsc::unbounded_channel::<PayoutEvent>();

        // Create a found_block and mature it
        let zero_hash = Sha256d::new([0u8; 32]).to_hex_be();
        let fb = accounting
            .found_block_repo
            .record_found_block(1, &zero_hash, 100, None, None, None, 50000, "")
            .unwrap();
        accounting.found_block_repo.mark_matured(fb.id).unwrap();

        // Create a submitted payout batch
        let batch = accounting
            .payout_repo
            .create_payout_batch(1, 100000, 1000, None, 1, &format!("hash:{}", &zero_hash))
            .unwrap();
        accounting
            .payout_repo
            .mark_batch_submitted(
                batch.id,
                "a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0c1d2e3f4a5b6c7d8e9f0a1b2",
            )
            .unwrap();

        // Now orphan the block
        accounting
            .found_block_repo
            .mark_orphaned(&zero_hash, "reorg_detected")
            .unwrap();

        let event = make_block_connected(200);
        handle_block_connected(event, &accounting, &chain_tip, &maturation_tx, 100).await;

        let fb = accounting
            .found_block_repo
            .get_by_hash(&zero_hash)
            .unwrap()
            .unwrap();
        assert_eq!(
            fb.status, "orphaned",
            "orphaned block with submitted payout should stay orphaned"
        );
        assert_eq!(
            fb.orphan_reason,
            Some("reorg_detected".to_string()),
            "orphan_reason should be preserved"
        );
    }
}
