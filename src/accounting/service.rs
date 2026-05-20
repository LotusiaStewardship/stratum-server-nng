use anyhow::Result;
use std::sync::Arc;
use parking_lot::Mutex;
use rusqlite::Connection;

use super::{
    ShareRepository, WorkerRepository, RoundRepository,
    FoundBlockRepository, AccountingEventRepository,
    Share, ShareOutcome, Round, AccountingEvent, FoundBlock,
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
}

impl AccountingService {
    pub fn new(conn: Arc<Mutex<Connection>>) -> Self {
        Self {
            share_repo: ShareRepository::new(conn.clone()),
            worker_repo: WorkerRepository::new(conn.clone()),
            round_repo: RoundRepository::new(conn.clone()),
            event_repo: AccountingEventRepository::new(conn.clone()),
            found_block_repo: FoundBlockRepository::new(conn.clone()),
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
        template_id: i64,
        template_epoch: i64,
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
        let (share_id, outcome_id, is_new) = self.share_repo.insert_share_and_outcome_atomic(&share, &outcome)?;

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
    pub fn get_or_create_current_round(&self, start_template_id: i64) -> Result<Round> {
        let (round, is_new) = self.round_repo.get_or_create_current_round(start_template_id)?;
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
    pub fn resolve_round_for_template(&self, template_id: i64) -> Result<Round> {
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
    pub fn close_round(&self, id: i64, end_template_id: i64, status: &str) -> Result<()> {
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
    pub fn update_share_outcome_node_result(&self, dedupe_key: &str, node_result: &str) -> Result<usize> {
        self.share_repo.update_outcome_node_result(dedupe_key, node_result)
    }

    /// Record a found block after successful lotusd submission.
    pub fn record_found_block(
        &self,
        round_id: i64,
        block_hash: &str,
        height: i64,
        worker_id: Option<i64>,
        template_id: Option<i64>,
        persist_source: Option<&str>,
    ) -> Result<FoundBlock> {
        let block = self.found_block_repo.record_found_block(
            round_id, block_hash, height, worker_id, template_id, persist_source,
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
        tip_height: i64,
        get_block_hash: F,
    ) -> Result<()>
    where
        F: Fn(i64) -> Fut,
        Fut: std::future::Future<Output = Result<Option<String>>>,
    {
        let confirmed = self.found_block_repo.list(Some("confirmed"))?;

        for block in &confirmed {
            if block.height > tip_height {
                tracing::warn!(
                    hash = %block.block_hash,
                    height = block.height,
                    tip = tip_height,
                    "found_block above chain tip at startup — orphaning",
                );
                if let Err(e) = self.found_block_repo.mark_orphaned(&block.block_hash, "block_not_found") {
                    tracing::error!(error = %e, "failed to orphan block above tip");
                }
                if let Err(e) = self.close_round(block.round_id, 0, "orphaned") {
                    tracing::error!(error = %e, "failed to close orphaned round");
                }
                continue;
            }

            // Block height <= tip — fetch the canonical hash at this height
            let canonical_hash = match get_block_hash(block.height).await {
                Ok(Some(hash)) => hash,
                Ok(None) => {
                    // Height exists but no block at this height — unlikely but handle gracefully
                    tracing::warn!(
                        hash = %block.block_hash,
                        height = block.height,
                        "no canonical block at height during reconciliation",
                    );
                    continue;
                }
                Err(e) => {
                    tracing::warn!(
                        error = %e,
                        height = block.height,
                        "failed to fetch canonical block hash during reconciliation",
                    );
                    continue;
                }
            };

            if canonical_hash != block.block_hash {
                tracing::warn!(
                    stored = %block.block_hash,
                    canonical = %canonical_hash,
                    height = block.height,
                    "found_block hash mismatch at startup — orphaning (reorg detected)",
                );
                if let Err(e) = self.found_block_repo.mark_orphaned(&block.block_hash, "reorg_detected") {
                    tracing::error!(error = %e, "failed to orphan reorged block");
                }
                if let Err(e) = self.close_round(block.round_id, 0, "orphaned") {
                    tracing::error!(error = %e, "failed to close orphaned round");
                }
            }
        }

        Ok(())
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
            .record_found_block(round.id, "0000abc", 1292529, None, Some(42), Some("json-rpc"))
            .unwrap();

        // The found_block.round_id must equal the resolved round.id
        assert_eq!(
            found.round_id, round.id,
            "found_block.round_id should be the resolved round's id, not template_id"
        );
        assert_eq!(found.block_hash, "0000abc");
        assert_eq!(found.status, "confirmed");

        // Verify accounting event was recorded
        let events = svc.event_repo.list_by_type("found_block_observed", 10, 0).unwrap();
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
        svc.record_found_block(1, "0000abcdef12345678900000000000000000000000000000000000000000000000", 1000, None, Some(42), Some("json-rpc")).unwrap();

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
                    Ok(Some("ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff".to_string()))
                }
            },
        ).await.unwrap();

        assert_eq!(call_count.load(std::sync::atomic::Ordering::SeqCst), 1, "get_block_hash should be called once");

        // Verify block was orphaned
        let block = svc.found_block_repo.get_by_hash("0000abcdef12345678900000000000000000000000000000000000000000000000").unwrap().unwrap();
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
        svc.record_found_block(1, "0000abcdef12345678900000000000000000000000000000000000000000000000", 999, None, Some(42), Some("json-rpc")).unwrap();

        // Reconcile with tip = 100 — block at height 999 is above tip
        svc.reconcile_found_blocks(100, |_height| async {
            unreachable!("should not be called for blocks above tip");
        }).await.unwrap();

        // Verify block was orphaned
        let block = svc.found_block_repo.get_by_hash("0000abcdef12345678900000000000000000000000000000000000000000000000").unwrap().unwrap();
        assert_eq!(block.status, "orphaned");
        assert_eq!(block.orphan_reason, Some("block_not_found".to_string()));

        // Verify round was orphaned
        let round = svc.round_repo.get_by_id(1).unwrap().unwrap();
        assert_eq!(round.status, "orphaned");
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
        let round_before = svc.round_repo.get_by_id(round_id.unwrap()).unwrap().unwrap();
        assert_eq!(round_before.status, "open", "round should start as 'open'");

        // Record a found block and close the round (as handle_submit should)
        let template_id: i64 = 42;
        let round = svc.resolve_round_for_template(template_id).unwrap();
        let _found = svc
            .record_found_block(round.id, "foundblockhash", 1000, None, Some(template_id), Some("json-rpc"))
            .unwrap();
        svc.close_round(round.id, template_id, "found").unwrap();

        // Verify round is now 'found'
        let round_after = svc.round_repo.get_by_id(round.id).unwrap().unwrap();
        assert_eq!(round_after.status, "found", "round should transition to 'found'");
        assert_eq!(round_after.end_template_id, Some(template_id), "end_template_id should be set");

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
        assert_eq!(total_shares, 1, "only one share should exist despite two insert attempts");

        // Only one accounting event
        let events = svc.event_repo.list_by_type("share_outcome", 10, 0).unwrap();
        assert_eq!(events.len(), 1, "only one accounting event should exist");
    }
}
