use anyhow::Result;
use rusqlite::{Connection, params};
use std::sync::Arc;
use parking_lot::Mutex;

/// An append-only audit log entry recording significant pool operations.
/// Per UBQ §Accounting Event: provides a complete chronological record for financial auditing.
#[derive(Debug, Clone)]
pub struct AccountingEvent {
    pub id: i64,
    pub event_type: String,
    pub status: String,
    pub session_id: Option<String>,
    pub worker_id: Option<i64>,
    pub worker_name: Option<String>,
    pub payout_address: Option<String>,
    pub round_id: Option<i64>,
    pub template_id: Option<i64>,
    pub template_epoch: Option<i64>,
    pub job_id: Option<String>,
    pub block_hash: Option<String>,
    pub height: Option<i64>,
    pub payload_json: Option<String>,
}

#[derive(Clone)]
pub struct AccountingEventRepository {
    conn: Arc<Mutex<Connection>>,
}

impl AccountingEventRepository {
    pub fn new(conn: Arc<Mutex<Connection>>) -> Self {
        Self { conn }
    }

    /// Record an accounting event (append-only).
    ///
    /// Returns the ID of the newly inserted event.
    pub fn record_event(&self, event: &AccountingEvent) -> Result<i64> {
        let conn = self.conn.lock();
        let mut stmt = conn.prepare(
            "INSERT INTO accounting_events
             (event_type, status, session_id, worker_id, worker_name, payout_address,
              round_id, template_id, template_epoch, job_id, block_hash, height, payload_json)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)"
        )?;

        let id = stmt.insert(params![
            event.event_type,
            event.status,
            event.session_id,
            event.worker_id,
            event.worker_name,
            event.payout_address,
            event.round_id,
            event.template_id,
            event.template_epoch,
            event.job_id,
            event.block_hash,
            event.height,
            event.payload_json,
        ])?;

        Ok(id as i64)
    }

    /// List events by type, most recent first.
    pub fn list_by_type(&self, event_type: &str, limit: i64, offset: i64) -> Result<Vec<AccountingEvent>> {
        let conn = self.conn.lock();
        let mut stmt = conn.prepare(
            "SELECT id, event_type, status, session_id, worker_id, worker_name, payout_address,
                    round_id, template_id, template_epoch, job_id, block_hash, height, payload_json
             FROM accounting_events
             WHERE event_type = ?1
             ORDER BY id DESC
             LIMIT ?2 OFFSET ?3"
        )?;

        let rows = stmt.query_map(params![event_type, limit, offset], |row| {
            Ok(AccountingEvent {
                id: row.get(0)?,
                event_type: row.get(1)?,
                status: row.get(2)?,
                session_id: row.get(3)?,
                worker_id: row.get(4)?,
                worker_name: row.get(5)?,
                payout_address: row.get(6)?,
                round_id: row.get(7)?,
                template_id: row.get(8)?,
                template_epoch: row.get(9)?,
                job_id: row.get(10)?,
                block_hash: row.get(11)?,
                height: row.get(12)?,
                payload_json: row.get(13)?,
            })
        })?;

        let mut events = Vec::new();
        for row in rows {
            events.push(row?);
        }
        Ok(events)
    }

    /// Count events by type.
    pub fn count_by_type(&self, event_type: &str) -> Result<i64> {
        let conn = self.conn.lock();
        let mut stmt = conn.prepare(
            "SELECT COUNT(*) FROM accounting_events WHERE event_type = ?1"
        )?;
        let count: i64 = stmt.query_row(params![event_type], |row| row.get(0))?;
        Ok(count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::accounting::schema::init_schema;
    use tempfile::NamedTempFile;

    fn make_event(event_type: &str, status: &str, round_id: Option<i64>) -> AccountingEvent {
        AccountingEvent {
            id: 0,
            event_type: event_type.to_string(),
            status: status.to_string(),
            session_id: None,
            worker_id: None,
            worker_name: None,
            payout_address: None,
            round_id,
            template_id: None,
            template_epoch: None,
            job_id: None,
            block_hash: None,
            height: None,
            payload_json: None,
        }
    }

    #[test]
    fn test_record_event_creates_event_with_correct_fields() {
        let f = NamedTempFile::new().unwrap();
        let conn = Connection::open(f.path()).unwrap();
        init_schema(&conn).unwrap();
        let repo = AccountingEventRepository::new(Arc::new(Mutex::new(conn)));

        let event = make_event("share_outcome", "accepted", Some(1));
        let id = repo.record_event(&event).unwrap();

        assert_eq!(id, 1);
    }

    #[test]
    fn test_list_by_type_returns_matching_events() {
        let f = NamedTempFile::new().unwrap();
        let conn = Connection::open(f.path()).unwrap();
        init_schema(&conn).unwrap();
        let repo = AccountingEventRepository::new(Arc::new(Mutex::new(conn)));

        repo.record_event(&make_event("share_outcome", "accepted", Some(1))).unwrap();
        repo.record_event(&make_event("round_opened", "open", None)).unwrap();
        repo.record_event(&make_event("share_outcome", "rejected", Some(1))).unwrap();

        let outcomes = repo.list_by_type("share_outcome", 10, 0).unwrap();
        assert_eq!(outcomes.len(), 2);

        let opened = repo.list_by_type("round_opened", 10, 0).unwrap();
        assert_eq!(opened.len(), 1);
    }

    #[test]
    fn test_count_by_type() {
        let f = NamedTempFile::new().unwrap();
        let conn = Connection::open(f.path()).unwrap();
        init_schema(&conn).unwrap();
        let repo = AccountingEventRepository::new(Arc::new(Mutex::new(conn)));

        repo.record_event(&make_event("share_outcome", "accepted", None)).unwrap();
        repo.record_event(&make_event("share_outcome", "accepted", None)).unwrap();

        assert_eq!(repo.count_by_type("share_outcome").unwrap(), 2);
        assert_eq!(repo.count_by_type("round_opened").unwrap(), 0);
    }

    #[test]
    fn test_list_by_type_respects_limit_and_offset() {
        let f = NamedTempFile::new().unwrap();
        let conn = Connection::open(f.path()).unwrap();
        init_schema(&conn).unwrap();
        let repo = AccountingEventRepository::new(Arc::new(Mutex::new(conn)));

        for i in 0..5 {
            let mut e = make_event("test_event", "ok", None);
            e.payload_json = Some(format!("event-{}", i));
            repo.record_event(&e).unwrap();
        }

        let page1 = repo.list_by_type("test_event", 2, 0).unwrap();
        assert_eq!(page1.len(), 2);
        // Most recent first: event-4, event-3

        let page2 = repo.list_by_type("test_event", 2, 2).unwrap();
        assert_eq!(page2.len(), 2);
    }

    #[test]
    fn test_record_event_stores_context_fields() {
        let f = NamedTempFile::new().unwrap();
        let conn = Connection::open(f.path()).unwrap();
        init_schema(&conn).unwrap();
        let repo = AccountingEventRepository::new(Arc::new(Mutex::new(conn)));

        let event = AccountingEvent {
            id: 0,
            event_type: "found_block_observed".to_string(),
            status: "confirmed".to_string(),
            session_id: Some("sess-1".to_string()),
            worker_id: Some(42),
            worker_name: Some("miner.rig".to_string()),
            payout_address: Some("lotus_addr".to_string()),
            round_id: Some(1),
            template_id: Some(100),
            template_epoch: Some(5),
            job_id: Some("job-100-5".to_string()),
            block_hash: Some("0000abc".to_string()),
            height: Some(1292529),
            payload_json: Some(r#"{"reason":"new_tip"}"#.to_string()),
        };

        let id = repo.record_event(&event).unwrap();

        let events = repo.list_by_type("found_block_observed", 10, 0).unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].id, id);
        assert_eq!(events[0].worker_id, Some(42));
        assert_eq!(events[0].block_hash.as_deref(), Some("0000abc"));
        assert_eq!(events[0].height, Some(1292529));
    }
}
