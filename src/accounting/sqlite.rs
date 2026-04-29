use crate::accounting::{PayoutMethod, Share, Worker};
use anyhow::{anyhow, Result};
use chrono::{DateTime, Utc};
use rusqlite::{params, Connection, OptionalExtension};
use std::sync::{Arc, Mutex};

/// Thread-safe SQLite wrapper for authoritative pool accounting state.
///
/// Notes:
/// - Uses WAL mode for durability/performance balance.
/// - Exposes idempotent insert semantics for share dedupe.
#[derive(Clone)]
pub struct AccountingDb {
    conn: Arc<Mutex<Connection>>,
}

impl AccountingDb {
    pub fn open(path: &str) -> Result<Self> {
        let conn = Connection::open(path)?;
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.pragma_update(None, "synchronous", "NORMAL")?;
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
        })
    }

    pub fn init_schema(&self) -> Result<()> {
        let conn = self.conn.lock().map_err(|_| anyhow!("db mutex poisoned"))?;
        conn.execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS meta (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS workers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                payout_address TEXT NOT NULL,
                worker_suffix TEXT,
                created_at TEXT NOT NULL,
                UNIQUE(payout_address, worker_suffix)
            );

            CREATE TABLE IF NOT EXISTS shares (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                worker_id INTEGER NOT NULL,
                template_id INTEGER NOT NULL,
                difficulty REAL NOT NULL,
                accepted INTEGER NOT NULL,
                stale INTEGER NOT NULL,
                dedupe_key TEXT NOT NULL UNIQUE,
                created_at TEXT NOT NULL,
                FOREIGN KEY(worker_id) REFERENCES workers(id)
            );

            CREATE TABLE IF NOT EXISTS rounds (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                start_template_id INTEGER NOT NULL,
                end_template_id INTEGER,
                found_block_hash TEXT,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS payout_batches (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                method TEXT NOT NULL,
                status TEXT NOT NULL,
                created_at TEXT NOT NULL
            );
            "#,
        )?;
        Ok(())
    }

    pub fn set_active_payout_method(&self, method: PayoutMethod) -> Result<()> {
        let conn = self.conn.lock().map_err(|_| anyhow!("db mutex poisoned"))?;
        conn.execute(
            "INSERT INTO meta(key, value) VALUES('payout_method', ?1)
             ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            params![method.as_str()],
        )?;
        Ok(())
    }

    pub fn upsert_worker(
        &self,
        payout_address: &str,
        worker_suffix: Option<&str>,
    ) -> Result<Worker> {
        let now = Utc::now();
        let conn = self.conn.lock().map_err(|_| anyhow!("db mutex poisoned"))?;
        conn.execute(
            "INSERT INTO workers(payout_address, worker_suffix, created_at) VALUES(?1, ?2, ?3)
             ON CONFLICT(payout_address, worker_suffix) DO NOTHING",
            params![payout_address, worker_suffix, now.to_rfc3339()],
        )?;

        let (id, created_at): (i64, String) = conn
            .query_row(
                "SELECT id, created_at FROM workers WHERE payout_address=?1 AND worker_suffix IS ?2",
                params![payout_address, worker_suffix],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )?;

        Ok(Worker {
            id,
            payout_address: payout_address.to_string(),
            worker_suffix: worker_suffix.map(|s| s.to_string()),
            created_at: DateTime::parse_from_rfc3339(&created_at)?.with_timezone(&Utc),
        })
    }

    /// Insert share if it hasn't been seen before.
    /// Returns `Ok(true)` if inserted, `Ok(false)` if duplicate (idempotent).
    pub fn insert_share_idempotent(
        &self,
        worker_id: i64,
        template_id: u64,
        difficulty: f64,
        accepted: bool,
        stale: bool,
        dedupe_key: &str,
    ) -> Result<bool> {
        let now = Utc::now();
        let conn = self.conn.lock().map_err(|_| anyhow!("db mutex poisoned"))?;
        let rows = conn.execute(
            "INSERT OR IGNORE INTO shares(worker_id, template_id, difficulty, accepted, stale, dedupe_key, created_at)
             VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![
                worker_id,
                template_id as i64,
                difficulty,
                if accepted { 1 } else { 0 },
                if stale { 1 } else { 0 },
                dedupe_key,
                now.to_rfc3339()
            ],
        )?;
        Ok(rows > 0)
    }

    pub fn list_recent_shares(&self, limit: u32) -> Result<Vec<Share>> {
        let conn = self.conn.lock().map_err(|_| anyhow!("db mutex poisoned"))?;
        let mut stmt = conn.prepare(
            "SELECT id, worker_id, template_id, difficulty, accepted, stale, dedupe_key, created_at
             FROM shares ORDER BY id DESC LIMIT ?1",
        )?;
        let rows = stmt.query_map(params![limit as i64], |r| {
            Ok((
                r.get::<_, i64>(0)?,
                r.get::<_, i64>(1)?,
                r.get::<_, i64>(2)?,
                r.get::<_, f64>(3)?,
                r.get::<_, i64>(4)?,
                r.get::<_, i64>(5)?,
                r.get::<_, String>(6)?,
                r.get::<_, String>(7)?,
            ))
        })?;

        let mut out = Vec::new();
        for row in rows {
            let (id, worker_id, template_id, difficulty, accepted, stale, dedupe_key, created_at) =
                row?;
            out.push(Share {
                id,
                worker_id,
                template_id: template_id as u64,
                difficulty,
                accepted: accepted != 0,
                stale: stale != 0,
                dedupe_key,
                created_at: DateTime::parse_from_rfc3339(&created_at)?.with_timezone(&Utc),
            });
        }
        Ok(out)
    }

    pub fn active_payout_method(&self) -> Result<Option<String>> {
        let conn = self.conn.lock().map_err(|_| anyhow!("db mutex poisoned"))?;
        let v = conn
            .query_row(
                "SELECT value FROM meta WHERE key='payout_method'",
                [],
                |r| r.get::<_, String>(0),
            )
            .optional()?;
        Ok(v)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    #[test]
    fn test_share_idempotency() {
        let f = NamedTempFile::new().unwrap();
        let db = AccountingDb::open(f.path().to_str().unwrap()).unwrap();
        db.init_schema().unwrap();

        let worker = db.upsert_worker("lotus_abc", Some("rig1")).unwrap();
        assert!(db
            .insert_share_idempotent(worker.id, 1, 1.0, true, false, "k1")
            .unwrap());
        assert!(!db
            .insert_share_idempotent(worker.id, 1, 1.0, true, false, "k1")
            .unwrap());
    }
}
