use crate::accounting::{PayoutBatch, PayoutMethod, Round, Share, Worker};
use anyhow::{anyhow, Result};
use chrono::{DateTime, Utc};
use rusqlite::{params, Connection, OptionalExtension};
use std::sync::{Arc, Mutex};

#[derive(Debug, Clone, serde::Serialize)]
pub struct MissingFoundBlock {
    pub block_hash: String,
    pub template_id: Option<i64>,
    pub worker_id: Option<i64>,
    pub worker_name: Option<String>,
    pub payout_address: Option<String>,
    pub created_at: String,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct FoundBlockStateSummary {
    pub pending: u64,
    pub matured: u64,
    pub orphaned: u64,
    pub paid: u64,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct PayoutBatchStateSummary {
    pub planned: u64,
    pub signed: u64,
    pub submitted: u64,
    pub confirmed: u64,
    pub invalidated_orphan: u64,
    pub failed: u64,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct SchedulerHealthSummary {
    pub matured_found_blocks_ready: u64,
    pub retry_ready_batches: u64,
    pub next_retry_at: Option<String>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct PplnsWindowShare {
    pub share_id: i64,
    pub payout_address: String,
    pub work_units: f64,
    pub created_at: String,
}

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

    /// Initialize schema and run simple forward-only migrations.
    pub fn init_schema(&self) -> Result<()> {
        let mut conn = self.conn.lock().map_err(|_| anyhow!("db mutex poisoned"))?;
        let tx = conn.transaction()?;
        tx.execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS schema_migrations (
                version INTEGER PRIMARY KEY,
                applied_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS meta (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            "#,
        )?;

        let has_v1: Option<i64> = tx
            .query_row(
                "SELECT version FROM schema_migrations WHERE version=1",
                [],
                |r| r.get(0),
            )
            .optional()?;
        if has_v1.is_none() {
            tx.execute_batch(
                r#"
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

                CREATE TABLE IF NOT EXISTS found_blocks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    round_id INTEGER NOT NULL,
                    block_hash TEXT NOT NULL UNIQUE,
                    height INTEGER,
                    status TEXT NOT NULL DEFAULT 'pending',
                    confirmations INTEGER NOT NULL DEFAULT 0,
                    template_id INTEGER,
                    worker_id INTEGER,
                    worker_name TEXT,
                    payout_address TEXT,
                    persist_source TEXT,
                    created_at TEXT NOT NULL,
                    FOREIGN KEY(round_id) REFERENCES rounds(id)
                );

                CREATE TABLE IF NOT EXISTS submit_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    block_hash TEXT NOT NULL,
                    template_id INTEGER,
                    worker_id INTEGER,
                    worker_name TEXT,
                    payout_address TEXT,
                    node_result TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    UNIQUE(block_hash, worker_id, node_result)
                );

                CREATE TABLE IF NOT EXISTS payout_batches (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    method TEXT NOT NULL,
                    status TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS payout_entries (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    payout_batch_id INTEGER NOT NULL,
                    address TEXT NOT NULL,
                    amount_sat INTEGER NOT NULL,
                    created_at TEXT NOT NULL,
                    FOREIGN KEY(payout_batch_id) REFERENCES payout_batches(id)
                );
                "#,
            )?;
            tx.execute(
                "INSERT INTO schema_migrations(version, applied_at) VALUES(1, ?1)",
                params![Utc::now().to_rfc3339()],
            )?;
        }

        // forward-safe backfill for existing DB files
        Self::ensure_column(
            &tx,
            "found_blocks",
            "status",
            "TEXT NOT NULL DEFAULT 'pending'",
        )?;
        Self::ensure_column(
            &tx,
            "found_blocks",
            "confirmations",
            "INTEGER NOT NULL DEFAULT 0",
        )?;
        Self::ensure_column(&tx, "found_blocks", "template_id", "INTEGER")?;
        Self::ensure_column(&tx, "found_blocks", "worker_id", "INTEGER")?;
        Self::ensure_column(&tx, "found_blocks", "worker_name", "TEXT")?;
        Self::ensure_column(&tx, "found_blocks", "payout_address", "TEXT")?;
        Self::ensure_column(&tx, "found_blocks", "persist_source", "TEXT")?;
        Self::ensure_column(
            &tx,
            "found_blocks",
            "coinbase_maturity_blocks",
            "INTEGER NOT NULL DEFAULT 100",
        )?;
        Self::ensure_column(&tx, "found_blocks", "matured_at", "TEXT")?;
        Self::ensure_column(&tx, "found_blocks", "disconnected_at", "TEXT")?;
        Self::ensure_column(
            &tx,
            "found_blocks",
            "chain_state",
            "TEXT NOT NULL DEFAULT 'pending'",
        )?;
        tx.execute_batch("CREATE TABLE IF NOT EXISTS submit_events (id INTEGER PRIMARY KEY AUTOINCREMENT, block_hash TEXT NOT NULL, template_id INTEGER, worker_id INTEGER, worker_name TEXT, payout_address TEXT, node_result TEXT NOT NULL, created_at TEXT NOT NULL, UNIQUE(block_hash, worker_id, node_result));")?;
        tx.execute_batch("CREATE TABLE IF NOT EXISTS payout_scheduler_lease (id INTEGER PRIMARY KEY CHECK(id=1), owner TEXT NOT NULL, expires_at TEXT NOT NULL);")?;

        Self::ensure_column(&tx, "payout_batches", "retry_key", "TEXT")?;
        Self::ensure_column(&tx, "payout_batches", "signed_payload_ref", "TEXT")?;
        Self::ensure_column(&tx, "payout_batches", "submitted_txid", "TEXT")?;
        Self::ensure_column(&tx, "payout_batches", "last_error", "TEXT")?;
        Self::ensure_column(&tx, "payout_batches", "next_retry_at", "TEXT")?;
        Self::ensure_column(
            &tx,
            "payout_batches",
            "attempt_count",
            "INTEGER NOT NULL DEFAULT 0",
        )?;
        Self::ensure_column(&tx, "payout_batches", "gross_reward_sat", "INTEGER")?;
        Self::ensure_column(&tx, "payout_batches", "fee_sat", "INTEGER")?;
        Self::ensure_column(&tx, "payout_batches", "net_reward_sat", "INTEGER")?;
        Self::ensure_column(&tx, "payout_batches", "confirmed_at", "TEXT")?;
        Self::ensure_column(&tx, "payout_batches", "found_block_id", "INTEGER")?;

        tx.execute_batch("CREATE UNIQUE INDEX IF NOT EXISTS idx_payout_batches_retry_key_unique ON payout_batches(retry_key) WHERE retry_key IS NOT NULL;")?;
        tx.execute_batch(
            "CREATE TABLE IF NOT EXISTS payout_share_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                payout_batch_id INTEGER NOT NULL,
                share_id INTEGER NOT NULL,
                payout_address TEXT NOT NULL,
                work_units REAL NOT NULL,
                share_created_at TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY(payout_batch_id) REFERENCES payout_batches(id)
            );",
        )?;

        tx.commit()?;
        Ok(())
    }

    fn ensure_column(
        tx: &rusqlite::Transaction<'_>,
        table: &str,
        col: &str,
        decl: &str,
    ) -> Result<()> {
        let mut stmt = tx.prepare(&format!("PRAGMA table_info({table})"))?;
        let mut rows = stmt.query([])?;
        while let Some(row) = rows.next()? {
            let name: String = row.get(1)?;
            if name == col {
                return Ok(());
            }
        }
        tx.execute(&format!("ALTER TABLE {table} ADD COLUMN {col} {decl}"), [])?;
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

        let (id, created_at): (i64, String) = conn.query_row(
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

    pub fn list_workers(&self, limit: u32) -> Result<Vec<Worker>> {
        let conn = self.conn.lock().map_err(|_| anyhow!("db mutex poisoned"))?;
        let mut stmt = conn.prepare(
            "SELECT id, payout_address, worker_suffix, created_at FROM workers ORDER BY id DESC LIMIT ?1",
        )?;
        let rows = stmt.query_map(params![limit as i64], |r| {
            Ok((
                r.get::<_, i64>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, Option<String>>(2)?,
                r.get::<_, String>(3)?,
            ))
        })?;
        let mut out = Vec::new();
        for row in rows {
            let (id, payout_address, worker_suffix, created_at) = row?;
            out.push(Worker {
                id,
                payout_address,
                worker_suffix,
                created_at: DateTime::parse_from_rfc3339(&created_at)?.with_timezone(&Utc),
            });
        }
        Ok(out)
    }

    /// Insert share if it hasn't been seen before.
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

    pub fn list_recent_rounds(&self, limit: u32) -> Result<Vec<Round>> {
        let conn = self.conn.lock().map_err(|_| anyhow!("db mutex poisoned"))?;
        let mut stmt = conn.prepare(
            "SELECT id, start_template_id, end_template_id, found_block_hash, created_at
             FROM rounds ORDER BY id DESC LIMIT ?1",
        )?;
        let rows = stmt.query_map(params![limit as i64], |r| {
            Ok((
                r.get::<_, i64>(0)?,
                r.get::<_, i64>(1)?,
                r.get::<_, Option<i64>>(2)?,
                r.get::<_, Option<String>>(3)?,
                r.get::<_, String>(4)?,
            ))
        })?;
        let mut out = Vec::new();
        for row in rows {
            let (id, start_template_id, end_template_id, found_block_hash, created_at) = row?;
            out.push(Round {
                id,
                start_template_id: start_template_id as u64,
                end_template_id: end_template_id.map(|v| v as u64),
                found_block_hash,
                created_at: DateTime::parse_from_rfc3339(&created_at)?.with_timezone(&Utc),
            });
        }
        Ok(out)
    }

    pub fn list_recent_payout_batches(&self, limit: u32) -> Result<Vec<PayoutBatch>> {
        let conn = self.conn.lock().map_err(|_| anyhow!("db mutex poisoned"))?;
        let mut stmt = conn.prepare(
            "SELECT id, method, status, submitted_txid, created_at FROM payout_batches ORDER BY id DESC LIMIT ?1",
        )?;
        let rows = stmt.query_map(params![limit as i64], |r| {
            Ok((
                r.get::<_, i64>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, String>(2)?,
                r.get::<_, Option<String>>(3)?,
                r.get::<_, String>(4)?,
            ))
        })?;
        let mut out = Vec::new();
        for row in rows {
            let (id, method, status, submitted_txid, created_at) = row?;
            out.push(PayoutBatch {
                id,
                method,
                status,
                submitted_txid,
                created_at: DateTime::parse_from_rfc3339(&created_at)?.with_timezone(&Utc),
            });
        }
        Ok(out)
    }

    pub fn record_found_block(
        &self,
        block_hash: &str,
        template_id: u64,
        block_height: i64,
        worker_id: i64,
        worker_name: &str,
        payout_address: &str,
        persist_source: &str,
    ) -> Result<()> {
        let now = Utc::now().to_rfc3339();
        let conn = self.conn.lock().map_err(|_| anyhow!("db mutex poisoned"))?;
        conn.execute(
            "INSERT OR IGNORE INTO rounds(start_template_id, created_at) VALUES(?1, ?2)",
            params![template_id as i64, now],
        )?;
        let round_id: i64 = conn.query_row(
            "SELECT id FROM rounds WHERE start_template_id=?1 ORDER BY id DESC LIMIT 1",
            params![template_id as i64],
            |r| r.get(0),
        )?;

        conn.execute(
            "INSERT INTO found_blocks(round_id, block_hash, height, template_id, worker_id, worker_name, payout_address, persist_source, coinbase_maturity_blocks, chain_state, status, confirmations, created_at)
             VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, 100, 'pending', 'pending', 0, ?9)
             ON CONFLICT(block_hash) DO UPDATE SET
                round_id=excluded.round_id,
                height=excluded.height,
                template_id=excluded.template_id,
                worker_id=excluded.worker_id,
                worker_name=excluded.worker_name,
                payout_address=excluded.payout_address,
                persist_source=excluded.persist_source",
            params![round_id, block_hash, block_height, template_id as i64, worker_id, worker_name, payout_address, persist_source, now],
        )?;
        conn.execute(
            "INSERT OR IGNORE INTO submit_events(block_hash, template_id, worker_id, worker_name, payout_address, node_result, created_at)
             VALUES(?1, ?2, ?3, ?4, ?5, 'accepted', ?6)",
            params![block_hash, template_id as i64, worker_id, worker_name, payout_address, now],
        )?;
        Ok(())
    }

    pub fn reconcile_missing_found_blocks(&self) -> Result<u64> {
        Ok(self.reconcile_missing_found_blocks_detail()?.len() as u64)
    }

    pub fn reconcile_missing_found_blocks_detail(&self) -> Result<Vec<MissingFoundBlock>> {
        let conn = self.conn.lock().map_err(|_| anyhow!("db mutex poisoned"))?;
        let mut stmt = conn.prepare(
            "SELECT se.block_hash, se.template_id, se.worker_id, se.worker_name, se.payout_address, se.created_at
             FROM submit_events se
             LEFT JOIN found_blocks fb ON fb.block_hash = se.block_hash
             WHERE se.node_result='accepted' AND fb.id IS NULL
             ORDER BY se.id DESC",
        )?;
        let rows = stmt.query_map([], |r| {
            Ok(MissingFoundBlock {
                block_hash: r.get(0)?,
                template_id: r.get(1)?,
                worker_id: r.get(2)?,
                worker_name: r.get(3)?,
                payout_address: r.get(4)?,
                created_at: r.get(5)?,
            })
        })?;
        let mut out = Vec::new();
        for row in rows {
            out.push(row?);
        }
        Ok(out)
    }

    pub fn list_weighted_shares_for_pplns_window(
        &self,
        found_block_id: i64,
        target_work_units: f64,
        hard_limit: u32,
    ) -> Result<Vec<PplnsWindowShare>> {
        let conn = self.conn.lock().map_err(|_| anyhow!("db mutex poisoned"))?;
        let cutoff: String = conn.query_row(
            "SELECT created_at FROM found_blocks WHERE id=?1",
            params![found_block_id],
            |r| r.get(0),
        )?;

        let mut stmt = conn.prepare(
            "SELECT s.id, w.payout_address, s.difficulty, s.created_at
             FROM shares s
             JOIN workers w ON w.id = s.worker_id
             WHERE s.accepted=1 AND s.stale=0 AND s.created_at <= ?1
             ORDER BY s.id DESC
             LIMIT ?2",
        )?;
        let rows = stmt.query_map(params![cutoff, hard_limit as i64], |r| {
            Ok(PplnsWindowShare {
                share_id: r.get::<_, i64>(0)?,
                payout_address: r.get::<_, String>(1)?,
                work_units: r.get::<_, f64>(2)?,
                created_at: r.get::<_, String>(3)?,
            })
        })?;
        let mut out = Vec::new();
        let mut cumulative_work = 0.0;
        for row in rows {
            let row = row?;
            cumulative_work += row.work_units;
            out.push(row);
            if cumulative_work >= target_work_units {
                break;
            }
        }
        Ok(out)
    }

    pub fn take_next_matured_found_block(&self) -> Result<Option<(i64, String)>> {
        let conn = self.conn.lock().map_err(|_| anyhow!("db mutex poisoned"))?;
        let row = conn
            .query_row(
                "SELECT id, block_hash FROM found_blocks WHERE chain_state='matured' AND status='matured' ORDER BY id ASC LIMIT 1",
                [],
                |r| Ok((r.get::<_, i64>(0)?, r.get::<_, String>(1)?)),
            )
            .optional()?;
        Ok(row)
    }

    pub fn mark_found_block_payout_submitted(&self, id: i64) -> Result<()> {
        let conn = self.conn.lock().map_err(|_| anyhow!("db mutex poisoned"))?;
        conn.execute(
            "UPDATE found_blocks SET status='payout_submitted' WHERE id=?1",
            params![id],
        )?;
        Ok(())
    }

    pub fn mark_found_block_paid(&self, id: i64) -> Result<()> {
        let conn = self.conn.lock().map_err(|_| anyhow!("db mutex poisoned"))?;
        conn.execute(
            "UPDATE found_blocks SET status='paid' WHERE id=?1",
            params![id],
        )?;
        Ok(())
    }

    pub fn create_payout_batch(
        &self,
        found_block_id: i64,
        retry_key: &str,
        gross_reward_sat: i64,
        fee_sat: i64,
        net_reward_sat: i64,
        outputs: &[(String, i64)],
        snapshot_shares: &[PplnsWindowShare],
    ) -> Result<i64> {
        let now = Utc::now().to_rfc3339();
        let mut conn = self.conn.lock().map_err(|_| anyhow!("db mutex poisoned"))?;
        let tx = conn.transaction()?;
        let existing = tx
            .query_row(
                "SELECT id FROM payout_batches WHERE retry_key=?1",
                params![retry_key],
                |r| r.get::<_, i64>(0),
            )
            .optional()?;
        if let Some(existing_id) = existing {
            return Ok(existing_id);
        }
        tx.execute(
            "INSERT INTO payout_batches(method, status, retry_key, found_block_id, gross_reward_sat, fee_sat, net_reward_sat, created_at)
             VALUES('pplns', 'planned', ?1, ?2, ?3, ?4, ?5, ?6)",
            params![retry_key, found_block_id, gross_reward_sat, fee_sat, net_reward_sat, now],
        )?;
        let batch_id = tx.last_insert_rowid();
        for (addr, sat) in outputs {
            tx.execute(
                "INSERT INTO payout_entries(payout_batch_id, address, amount_sat, created_at) VALUES(?1, ?2, ?3, ?4)",
                params![batch_id, addr, sat, now],
            )?;
        }
        for share in snapshot_shares {
            tx.execute(
                "INSERT INTO payout_share_snapshots(payout_batch_id, share_id, payout_address, work_units, share_created_at, created_at)
                 VALUES(?1, ?2, ?3, ?4, ?5, ?6)",
                params![batch_id, share.share_id, share.payout_address, share.work_units, share.created_at, now],
            )?;
        }
        tx.commit()?;
        Ok(batch_id)
    }

    pub fn update_payout_batch_state(
        &self,
        batch_id: i64,
        status: &str,
        signed_payload_ref: Option<&str>,
        submitted_txid: Option<&str>,
        last_error: Option<&str>,
    ) -> Result<()> {
        let conn = self.conn.lock().map_err(|_| anyhow!("db mutex poisoned"))?;
        conn.execute(
            "UPDATE payout_batches
             SET status=?2,
                 signed_payload_ref=COALESCE(?3, signed_payload_ref),
                 submitted_txid=COALESCE(?4, submitted_txid),
                 last_error=?5,
                 attempt_count=attempt_count+1,
                 next_retry_at=?6,
                 confirmed_at=CASE WHEN ?2='confirmed' THEN ?6 ELSE confirmed_at END
             WHERE id=?1",
            params![
                batch_id,
                status,
                signed_payload_ref,
                submitted_txid,
                last_error,
                Utc::now().to_rfc3339()
            ],
        )?;
        Ok(())
    }

    pub fn sync_found_block_confirmations(
        &self,
        tip_height: i64,
        min_confirmations: u32,
    ) -> Result<()> {
        let now = Utc::now().to_rfc3339();
        let conn = self.conn.lock().map_err(|_| anyhow!("db mutex poisoned"))?;
        conn.execute(
            "UPDATE found_blocks
             SET confirmations = CASE
                WHEN height IS NULL THEN confirmations
                WHEN ?1 - height + 1 < 0 THEN 0
                ELSE (?1 - height + 1)
             END
             WHERE chain_state='pending'",
            params![tip_height],
        )?;
        conn.execute(
            "UPDATE found_blocks
             SET chain_state='matured', status='matured', matured_at=?1
             WHERE chain_state='pending' AND confirmations >= CASE
                WHEN coinbase_maturity_blocks > ?2 THEN coinbase_maturity_blocks ELSE ?2 END",
            params![now, min_confirmations as i64],
        )?;
        Ok(())
    }

    pub fn mark_pending_blocks_orphaned(&self) -> Result<u64> {
        let now = Utc::now().to_rfc3339();
        let conn = self.conn.lock().map_err(|_| anyhow!("db mutex poisoned"))?;
        let changed = conn.execute(
            "UPDATE found_blocks
             SET chain_state='orphaned', status='orphaned', disconnected_at=?1
             WHERE chain_state='pending'",
            params![now],
        )?;
        conn.execute(
            "UPDATE payout_batches
             SET status='invalidated_orphan', last_error='found block orphaned'
             WHERE status IN ('planned','signed','submitted')
               AND found_block_id IN (SELECT id FROM found_blocks WHERE chain_state='orphaned')",
            [],
        )?;
        Ok(changed as u64)
    }

    pub fn found_block_state_summary(&self) -> Result<FoundBlockStateSummary> {
        let conn = self.conn.lock().map_err(|_| anyhow!("db mutex poisoned"))?;
        let pending: i64 = conn.query_row(
            "SELECT COUNT(*) FROM found_blocks WHERE chain_state='pending'",
            [],
            |r| r.get(0),
        )?;
        let matured: i64 = conn.query_row(
            "SELECT COUNT(*) FROM found_blocks WHERE chain_state='matured'",
            [],
            |r| r.get(0),
        )?;
        let orphaned: i64 = conn.query_row(
            "SELECT COUNT(*) FROM found_blocks WHERE chain_state='orphaned'",
            [],
            |r| r.get(0),
        )?;
        let paid: i64 = conn.query_row(
            "SELECT COUNT(*) FROM found_blocks WHERE status='paid'",
            [],
            |r| r.get(0),
        )?;
        Ok(FoundBlockStateSummary {
            pending: pending as u64,
            matured: matured as u64,
            orphaned: orphaned as u64,
            paid: paid as u64,
        })
    }

    pub fn payout_batch_state_summary(&self) -> Result<PayoutBatchStateSummary> {
        let conn = self.conn.lock().map_err(|_| anyhow!("db mutex poisoned"))?;
        let planned: i64 = conn.query_row(
            "SELECT COUNT(*) FROM payout_batches WHERE status='planned'",
            [],
            |r| r.get(0),
        )?;
        let signed: i64 = conn.query_row(
            "SELECT COUNT(*) FROM payout_batches WHERE status='signed'",
            [],
            |r| r.get(0),
        )?;
        let submitted: i64 = conn.query_row(
            "SELECT COUNT(*) FROM payout_batches WHERE status='submitted'",
            [],
            |r| r.get(0),
        )?;
        let confirmed: i64 = conn.query_row(
            "SELECT COUNT(*) FROM payout_batches WHERE status='confirmed'",
            [],
            |r| r.get(0),
        )?;
        let invalidated_orphan: i64 = conn.query_row(
            "SELECT COUNT(*) FROM payout_batches WHERE status='invalidated_orphan'",
            [],
            |r| r.get(0),
        )?;
        let failed: i64 = conn.query_row(
            "SELECT COUNT(*) FROM payout_batches WHERE last_error IS NOT NULL AND status NOT IN ('invalidated_orphan')",
            [],
            |r| r.get(0),
        )?;

        Ok(PayoutBatchStateSummary {
            planned: planned as u64,
            signed: signed as u64,
            submitted: submitted as u64,
            confirmed: confirmed as u64,
            invalidated_orphan: invalidated_orphan as u64,
            failed: failed as u64,
        })
    }

    pub fn scheduler_health_summary(&self) -> Result<SchedulerHealthSummary> {
        let conn = self.conn.lock().map_err(|_| anyhow!("db mutex poisoned"))?;
        let matured_found_blocks_ready: i64 = conn.query_row(
            "SELECT COUNT(*) FROM found_blocks WHERE chain_state='matured' AND status='matured'",
            [],
            |r| r.get(0),
        )?;
        let retry_ready_batches: i64 = conn.query_row(
            "SELECT COUNT(*) FROM payout_batches WHERE status='failed'",
            [],
            |r| r.get(0),
        )?;
        let next_retry_at: Option<String> = conn
            .query_row(
                "SELECT MIN(next_retry_at) FROM payout_batches WHERE status='failed' AND next_retry_at IS NOT NULL",
                [],
                |r| r.get(0),
            )
            .optional()?
            .flatten();

        Ok(SchedulerHealthSummary {
            matured_found_blocks_ready: matured_found_blocks_ready as u64,
            retry_ready_batches: retry_ready_batches as u64,
            next_retry_at,
        })
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

    #[test]
    fn test_found_block_record_is_idempotent_and_updates_attribution() {
        let f = NamedTempFile::new().unwrap();
        let db = AccountingDb::open(f.path().to_str().unwrap()).unwrap();
        db.init_schema().unwrap();

        let worker = db.upsert_worker("lotus_abc", Some("rig1")).unwrap();
        db.record_found_block(
            "bh1",
            9,
            100,
            worker.id,
            "lotus_abc.rig1",
            &worker.payout_address,
            "submit_flow",
        )
        .unwrap();
        db.record_found_block(
            "bh1",
            9,
            100,
            worker.id,
            "lotus_abc.rig1",
            &worker.payout_address,
            "submit_flow",
        )
        .unwrap();

        let conn = db.conn.lock().unwrap();
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM found_blocks WHERE block_hash='bh1'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(count, 1);
        let status: String = conn
            .query_row(
                "SELECT status FROM found_blocks WHERE block_hash='bh1'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(status, "pending");
    }

    #[test]
    fn test_found_block_maturity_and_orphaning() {
        let f = NamedTempFile::new().unwrap();
        let db = AccountingDb::open(f.path().to_str().unwrap()).unwrap();
        db.init_schema().unwrap();

        let worker = db.upsert_worker("lotus_abc", None).unwrap();
        db.record_found_block(
            "bh2",
            10,
            120,
            worker.id,
            "lotus_abc",
            &worker.payout_address,
            "submit_flow",
        )
        .unwrap();
        assert_eq!(db.mark_pending_blocks_orphaned().unwrap(), 1);
        db.record_found_block(
            "bh3",
            11,
            120,
            worker.id,
            "lotus_abc",
            &worker.payout_address,
            "submit_flow",
        )
        .unwrap();
        for _ in 0..100 {
            db.sync_found_block_confirmations(220, 100).unwrap();
        }
        let conn = db.conn.lock().unwrap();
        let status: String = conn
            .query_row(
                "SELECT status FROM found_blocks WHERE block_hash='bh3'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(status, "matured");
    }
}
