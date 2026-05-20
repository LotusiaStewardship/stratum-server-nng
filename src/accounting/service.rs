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
    /// Returns (share_id, outcome_id, round_id) or (None, None, None) if the share
    /// was a duplicate (dedupe key collision).
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
    ) -> Result<(Option<i64>, Option<i64>, Option<i64>)> {
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

        Ok((share_id, outcome_id, round_id))
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

        let (share_id, outcome_id, round_id) = svc
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

        let (share_id, outcome_id, round_id) = svc
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
    fn test_record_share_dedupe_correctly() {
        let f = NamedTempFile::new().unwrap();
        let conn = Connection::open(f.path()).unwrap();
        init_schema(&conn).unwrap();
        let svc = AccountingService::new(Arc::new(Mutex::new(conn)));

        // First insert with worker suffix (ensures worker dedupe works)
        let (sid1, _, _) = svc
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
        let (sid2, _, _) = svc
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
