use anyhow::Result;
use rusqlite::Connection;
use rusqlite::ToSql;
use std::sync::Arc;
use parking_lot::Mutex;

/// Raw share submission record (per UBQ §Share).
/// Captures the fact that a miner submitted work. Immutable once persisted.
#[derive(Debug, Clone)]
pub struct Share {
    pub id: i64,
    pub worker_id: i64,
    pub session_id: String,
    pub job_id: String,
    pub template_id: i64,
    pub template_epoch: i64,
    pub extranonce2: String,
    pub ntime_hex_6b: String,
    pub nonce_hex_8b: String,
    pub difficulty: f64,
    pub dedupe_key: String,
}

/// Share outcome (validation pipeline result, per UBQ §Share Outcome).
/// Captures the full validation result linked to the raw share.
#[derive(Debug, Clone)]
pub struct ShareOutcome {
    pub id: i64,
    pub share_id: i64,
    pub session_id: String,
    pub worker_id: i64,
    pub job_id: String,
    pub round_id: Option<i64>,
    pub dedupe_key: String,
    pub status: String,
    pub reject_reason: Option<String>,
    pub node_result: Option<String>,
    pub low_diff_ok: Option<bool>,
    pub network_target_ok: Option<bool>,
    pub block_hash: Option<String>,
}

/// Authorization event record (per UBQ §Authorization Event).
/// Every mining.authorize attempt produces one record, regardless of outcome.
#[derive(Debug, Clone)]
pub struct AuthorizationEvent {
    pub id: i64,
    pub session_id: String,
    pub worker_name: String,
    pub payout_address: String,
    pub worker_suffix: Option<String>,
    pub authorized: bool,
    pub reason: Option<String>,
}

#[derive(Clone)]
pub struct ShareRepository {
    conn: Arc<Mutex<Connection>>,
}

impl ShareRepository {
    pub fn new(conn: Arc<Mutex<Connection>>) -> Self {
        Self { conn }
    }

    /// Compute dedupe key per UBQ format: worker_id:template_id:template_epoch:extranonce2:ntime:nonce
    pub fn build_dedupe_key(
        worker_id: i64,
        template_id: i64,
        template_epoch: i64,
        extranonce2: &str,
        ntime: &str,
        nonce: &str,
    ) -> String {
        format!(
            "{}:{}:{}:{}:{}:{}",
            worker_id, template_id, template_epoch, extranonce2, ntime, nonce
        )
    }

    /// Insert a raw share record (immutable).
    /// Uses INSERT OR IGNORE for dedupe key idempotency.
    /// Returns the share ID, or None if the record was a duplicate.
    pub fn insert_share(&self, share: &Share) -> Result<Option<i64>> {
        let conn = self.conn.lock();
        let mut stmt = conn.prepare(
            "INSERT OR IGNORE INTO shares
             (worker_id, session_id, job_id, template_id, template_epoch,
              extranonce2, ntime_hex_6b, nonce_hex_8b, difficulty, dedupe_key)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
        )?;

        let rows_affected = stmt.execute([
            share.worker_id.to_sql()?,
            share.session_id.to_sql()?,
            share.job_id.to_sql()?,
            share.template_id.to_sql()?,
            share.template_epoch.to_sql()?,
            share.extranonce2.to_sql()?,
            share.ntime_hex_6b.to_sql()?,
            share.nonce_hex_8b.to_sql()?,
            share.difficulty.to_sql()?,
            share.dedupe_key.to_sql()?,
        ])?;

        if rows_affected == 0 {
            // Duplicate — get the existing ID
            let mut query = conn.prepare(
                "SELECT id FROM shares WHERE dedupe_key = ?1"
            )?;
            let id: i64 = query.query_row([&share.dedupe_key], |row| row.get(0))?;
            Ok(Some(id))
        } else {
            // Get the last inserted row ID
            let id = conn.last_insert_rowid();
            Ok(Some(id))
        }
    }

    /// Insert a share outcome record.
    /// Uses INSERT OR IGNORE for dedupe key idempotency.
    pub fn insert_share_outcome(&self, outcome: &ShareOutcome) -> Result<Option<i64>> {
        let conn = self.conn.lock();
        let mut stmt = conn.prepare(
            "INSERT OR IGNORE INTO share_outcomes
             (share_id, session_id, worker_id, job_id, round_id, dedupe_key,
              status, reject_reason, node_result, low_diff_ok, network_target_ok, block_hash)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
        )?;

        let rows_affected = stmt.execute([
            outcome.share_id.to_sql()?,
            outcome.session_id.to_sql()?,
            outcome.worker_id.to_sql()?,
            outcome.job_id.to_sql()?,
            outcome.round_id.to_sql()?,
            outcome.dedupe_key.to_sql()?,
            outcome.status.to_sql()?,
            outcome.reject_reason.to_sql()?,
            outcome.node_result.to_sql()?,
            outcome.low_diff_ok.to_sql()?,
            outcome.network_target_ok.to_sql()?,
            outcome.block_hash.to_sql()?,
        ])?;

        if rows_affected == 0 {
            // Duplicate
            let mut query = conn.prepare(
                "SELECT id FROM share_outcomes WHERE dedupe_key = ?1"
            )?;
            let id: i64 = query.query_row([&outcome.dedupe_key], |row| row.get(0))?;
            Ok(Some(id))
        } else {
            Ok(Some(conn.last_insert_rowid()))
        }
    }

    /// Record an authorization event (immutable audit log per UBQ).
    /// Every mining.authorize attempt produces one record, regardless of success/failure.
    pub fn insert_authorization_event(&self, event: &AuthorizationEvent) -> Result<i64> {
        let conn = self.conn.lock();
        let mut stmt = conn.prepare(
            "INSERT INTO authorization_events
             (session_id, worker_name, payout_address, worker_suffix, authorized, reason)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        )?;

        let id = stmt.insert([
            event.session_id.to_sql()?,
            event.worker_name.to_sql()?,
            event.payout_address.to_sql()?,
            event.worker_suffix.to_sql()?,
            (event.authorized as i64).to_sql()?,
            event.reason.to_sql()?,
        ])?;

        Ok(id as i64)
    }

    /// Count share_outcomes by status (used for stats).
    pub fn count_outcomes_by_status(&self, status: &str) -> Result<i64> {
        let conn = self.conn.lock();
        let mut stmt = conn
            .prepare("SELECT COUNT(*) FROM share_outcomes WHERE status = ?1")?;
        let count: i64 = stmt.query_row([status], |row| row.get(0))?;
        Ok(count)
    }

    /// Total share_outcomes count.
    pub fn total_outcome_count(&self) -> Result<i64> {
        let conn = self.conn.lock();
        let mut stmt = conn.prepare("SELECT COUNT(*) FROM share_outcomes")?;
        let count: i64 = stmt.query_row([], |row| row.get(0))?;
        Ok(count)
    }

    /// Count shares by worker (legacy — queries shares table).
    pub fn count_by_worker(&self, worker_id: i64) -> Result<i64> {
        let conn = self.conn.lock();
        let mut stmt = conn
            .prepare("SELECT COUNT(*) FROM shares WHERE worker_id = ?1")?;
        let count: i64 = stmt.query_row([worker_id], |row| row.get(0))?;
        Ok(count)
    }

    /// Total number of raw share records.
    pub fn total_count(&self) -> Result<i64> {
        let conn = self.conn.lock();
        let mut stmt = conn.prepare("SELECT COUNT(*) FROM shares")?;
        let count: i64 = stmt.query_row([], |row| row.get(0))?;
        Ok(count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::accounting::schema::init_schema;
    use tempfile::NamedTempFile;

    fn create_test_share(worker_id: i64, template_id: i64, template_epoch: i64, dedupe_key: &str) -> Share {
        Share {
            id: 0,
            worker_id,
            session_id: "sess-1".to_string(),
            job_id: format!("job-{}-{}", template_id, template_epoch),
            template_id,
            template_epoch,
            extranonce2: "00112233".to_string(),
            ntime_hex_6b: "001122334455".to_string(),
            nonce_hex_8b: "0011223344556677".to_string(),
            difficulty: 1.0,
            dedupe_key: dedupe_key.to_string(),
        }
    }

    fn create_test_outcome(share_id: i64, worker_id: i64, dedupe_key: &str, status: &str) -> ShareOutcome {
        ShareOutcome {
            id: 0,
            share_id,
            session_id: "sess-1".to_string(),
            worker_id,
            job_id: "job-1-12345".to_string(),
            round_id: None,
            dedupe_key: dedupe_key.to_string(),
            status: status.to_string(),
            reject_reason: None,
            node_result: None,
            low_diff_ok: Some(true),
            network_target_ok: Some(false),
            block_hash: None,
        }
    }

    fn setup_worker(conn: &Connection, id: i64, address: &str, suffix: Option<&str>) {
        conn.execute(
            "INSERT INTO workers (id, payout_address, worker_suffix) VALUES (?1, ?2, ?3)",
            rusqlite::params![id as i64, address, suffix],
        )
        .unwrap();
    }

    #[test]
    fn test_insert_raw_share() {
        let f = NamedTempFile::new().unwrap();
        let conn = Connection::open(f.path()).unwrap();
        init_schema(&conn).unwrap();
        setup_worker(&conn, 1, "test_address", None);

        let repo = ShareRepository::new(Arc::new(Mutex::new(conn)));
        let share = create_test_share(1, 42, 12345, "1:42:12345:00112233:001122334455:0011223344556677");

        let id = repo.insert_share(&share).unwrap();
        assert!(id.is_some());
        assert!(id.unwrap() > 0);
    }

    #[test]
    fn test_dedupe_key_prevents_duplicate_shares() {
        let f = NamedTempFile::new().unwrap();
        let conn = Connection::open(f.path()).unwrap();
        init_schema(&conn).unwrap();
        setup_worker(&conn, 1, "test_address", None);

        let repo = ShareRepository::new(Arc::new(Mutex::new(conn)));
        let dedupe = "1:42:12345:00112233:001122334455:0011223344556677";
        let share = create_test_share(1, 42, 12345, dedupe);

        // First insert should succeed
        let id1 = repo.insert_share(&share).unwrap();
        assert!(id1.is_some());

        // Second insert with same dedupe key should also return an ID (existing)
        let id2 = repo.insert_share(&share).unwrap();
        assert!(id2.is_some());

        // Both should return the same ID (idempotent)
        assert_eq!(id1, id2);
    }

    #[test]
    fn test_insert_share_outcome() {
        let f = NamedTempFile::new().unwrap();
        let conn = Connection::open(f.path()).unwrap();
        init_schema(&conn).unwrap();
        setup_worker(&conn, 1, "test_address", None);

        let repo = ShareRepository::new(Arc::new(Mutex::new(conn)));
        let share = create_test_share(1, 42, 12345, "1:42:12345:00112233:001122334455:0011223344556677");
        let share_id = repo.insert_share(&share).unwrap().unwrap();

        let outcome = create_test_outcome(share_id, 1, "1:42:12345:00112233:001122334455:0011223344556677", "accepted");
        let outcome_id = repo.insert_share_outcome(&outcome).unwrap();
        assert!(outcome_id.is_some());
        assert!(outcome_id.unwrap() > 0);
    }

    #[test]
    fn test_insert_authorization_event() {
        let f = NamedTempFile::new().unwrap();
        let conn = Connection::open(f.path()).unwrap();
        init_schema(&conn).unwrap();

        let repo = ShareRepository::new(Arc::new(Mutex::new(conn)));
        let event = AuthorizationEvent {
            id: 0,
            session_id: "sess-1".to_string(),
            worker_name: "lotus_16PSJNf1EDEfGvaYzaXJCJZrXH4pgiTo7kyW61iGi.rig".to_string(),
            payout_address: "lotus_16PSJNf1EDEfGvaYzaXJCJZrXH4pgiTo7kyW61iGi".to_string(),
            worker_suffix: Some("rig".to_string()),
            authorized: true,
            reason: None,
        };

        let id = repo.insert_authorization_event(&event).unwrap();
        assert_eq!(id, 1);
    }

    #[test]
    fn test_insert_auth_event_unauthorized() {
        let f = NamedTempFile::new().unwrap();
        let conn = Connection::open(f.path()).unwrap();
        init_schema(&conn).unwrap();

        let repo = ShareRepository::new(Arc::new(Mutex::new(conn)));
        let event = AuthorizationEvent {
            id: 0,
            session_id: "sess-2".to_string(),
            worker_name: "bad.worker".to_string(),
            payout_address: "bad".to_string(),
            worker_suffix: Some("worker".to_string()),
            authorized: false,
            reason: Some("invalid lotus address".to_string()),
        };

        let id = repo.insert_authorization_event(&event).unwrap();
        assert_eq!(id, 1);
    }

    #[test]
    fn test_count_by_worker() {
        let f = NamedTempFile::new().unwrap();
        let conn = Connection::open(f.path()).unwrap();
        init_schema(&conn).unwrap();
        setup_worker(&conn, 1, "addr1", None);
        setup_worker(&conn, 2, "addr2", None);

        let repo = ShareRepository::new(Arc::new(Mutex::new(conn)));
        repo.insert_share(&create_test_share(1, 1, 100, "dk1")).unwrap();
        repo.insert_share(&create_test_share(1, 1, 101, "dk2")).unwrap();
        repo.insert_share(&create_test_share(2, 1, 102, "dk3")).unwrap();

        assert_eq!(repo.count_by_worker(1).unwrap(), 2);
        assert_eq!(repo.count_by_worker(2).unwrap(), 1);
    }

    #[test]
    fn test_count_outcomes_by_status() {
        let f = NamedTempFile::new().unwrap();
        let conn = Connection::open(f.path()).unwrap();
        init_schema(&conn).unwrap();
        setup_worker(&conn, 1, "test_address", None);

        let repo = ShareRepository::new(Arc::new(Mutex::new(conn)));

        // Insert shares and outcomes
        let s1 = create_test_share(1, 1, 100, "dk1");
        let sid1 = repo.insert_share(&s1).unwrap().unwrap();
        repo.insert_share_outcome(&create_test_outcome(sid1, 1, "dk1", "accepted")).unwrap();

        let s2 = create_test_share(1, 1, 101, "dk2");
        let sid2 = repo.insert_share(&s2).unwrap().unwrap();
        repo.insert_share_outcome(&create_test_outcome(sid2, 1, "dk2", "accepted")).unwrap();

        let s3 = create_test_share(1, 1, 102, "dk3");
        let sid3 = repo.insert_share(&s3).unwrap().unwrap();
        let mut o3 = create_test_outcome(sid3, 1, "dk3", "rejected");
        o3.reject_reason = Some("low-difficulty-share".to_string());
        repo.insert_share_outcome(&o3).unwrap();

        assert_eq!(repo.count_outcomes_by_status("accepted").unwrap(), 2);
        assert_eq!(repo.count_outcomes_by_status("rejected").unwrap(), 1);
    }

    #[test]
    fn test_total_count() {
        let f = NamedTempFile::new().unwrap();
        let conn = Connection::open(f.path()).unwrap();
        init_schema(&conn).unwrap();
        setup_worker(&conn, 1, "test_address", None);

        let repo = ShareRepository::new(Arc::new(Mutex::new(conn)));
        assert_eq!(repo.total_count().unwrap(), 0);

        repo.insert_share(&create_test_share(1, 1, 100, "dk1")).unwrap();
        repo.insert_share(&create_test_share(1, 1, 101, "dk2")).unwrap();

        assert_eq!(repo.total_count().unwrap(), 2);
    }

    #[test]
    fn test_build_dedupe_key() {
        let key = ShareRepository::build_dedupe_key(1, 42, 12345, "00112233", "001122334455", "0011223344556677");
        assert_eq!(key, "1:42:12345:00112233:001122334455:0011223344556677");
    }
}
