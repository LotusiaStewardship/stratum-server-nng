use crate::accounting::{FoundBlock, PayoutBatch, PayoutMethod, Round, Share, Worker};
use anyhow::{anyhow, Result};
use chrono::{DateTime, Duration, Utc};
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
    pub confirmed: u64,
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

/// Information about the current scheduler lease
#[derive(Debug, Clone, serde::Serialize)]
pub struct LeaseInfo {
    pub owner: String,
    pub expires_at: String,
    pub is_valid: bool,
    pub is_expired: bool,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct PplnsWindowShare {
    pub share_id: i64,
    pub payout_address: String,
    pub work_units: f64,
    pub created_at: String,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct WorkerAccountingSummary {
    pub worker_id: i64,
    pub payout_address: String,
    pub worker_suffix: Option<String>,
    pub accepted: u64,
    pub rejected: u64,
    pub stale: u64,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct RejectedReasonSummary {
    pub reason: String,
    pub count: u64,
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
                    status TEXT NOT NULL DEFAULT 'confirmed',
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
            "TEXT NOT NULL DEFAULT 'confirmed'",
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
            "orphan_reason",
            "TEXT",
        )?;
        tx.execute_batch("CREATE TABLE IF NOT EXISTS submit_events (id INTEGER PRIMARY KEY AUTOINCREMENT, block_hash TEXT NOT NULL, template_id INTEGER, worker_id INTEGER, worker_name TEXT, payout_address TEXT, node_result TEXT NOT NULL, created_at TEXT NOT NULL, UNIQUE(block_hash, worker_id, node_result));")?;
        tx.execute_batch("CREATE TABLE IF NOT EXISTS payout_scheduler_lease (id INTEGER PRIMARY KEY CHECK(id=1), owner TEXT NOT NULL, expires_at TEXT NOT NULL);")?;
        tx.execute_batch(
            "CREATE TABLE IF NOT EXISTS accounting_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_type TEXT NOT NULL,
                status TEXT,
                session_id TEXT,
                worker_id INTEGER,
                worker_name TEXT,
                payout_address TEXT,
                share_id INTEGER,
                round_id INTEGER,
                template_id INTEGER,
                template_epoch INTEGER,
                job_id TEXT,
                block_hash TEXT,
                height INTEGER,
                payload_json TEXT,
                created_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_accounting_events_type_created ON accounting_events(event_type, created_at);
            CREATE INDEX IF NOT EXISTS idx_accounting_events_worker_created ON accounting_events(worker_id, created_at);
            CREATE INDEX IF NOT EXISTS idx_accounting_events_round_created ON accounting_events(round_id, created_at);
            CREATE INDEX IF NOT EXISTS idx_accounting_events_blockhash ON accounting_events(block_hash);

            CREATE TABLE IF NOT EXISTS authorization_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id TEXT NOT NULL,
                worker_name TEXT NOT NULL,
                payout_address TEXT,
                worker_suffix TEXT,
                authorized INTEGER NOT NULL,
                reason TEXT,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS share_outcomes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id TEXT,
                worker_id INTEGER,
                worker_name TEXT,
                payout_address TEXT,
                template_id INTEGER,
                template_epoch INTEGER,
                job_id TEXT,
                round_id INTEGER,
                dedupe_key TEXT,
                status TEXT NOT NULL,
                reject_reason TEXT,
                node_result TEXT,
                low_diff_ok INTEGER,
                network_target_ok INTEGER,
                block_hash TEXT,
                share_id INTEGER,
                created_at TEXT NOT NULL,
                UNIQUE(dedupe_key)
            );

            CREATE TABLE IF NOT EXISTS payout_dust_ledger (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                payout_batch_id INTEGER,
                found_block_id INTEGER,
                address TEXT NOT NULL,
                amount_sat INTEGER NOT NULL,
                policy TEXT NOT NULL,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS round_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                round_id INTEGER NOT NULL,
                event_type TEXT NOT NULL,
                reason TEXT,
                block_hash TEXT,
                template_id INTEGER,
                created_at TEXT NOT NULL
            );
            ")?;

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
                ordering_criterion TEXT,
                truncation_reason TEXT,
                created_at TEXT NOT NULL,
                FOREIGN KEY(payout_batch_id) REFERENCES payout_batches(id)
            );",
        )?;
        Self::ensure_column(&tx, "payout_share_snapshots", "ordering_criterion", "TEXT")?;
        Self::ensure_column(&tx, "payout_share_snapshots", "truncation_reason", "TEXT")?;
        Self::ensure_column(&tx, "submit_events", "session_id", "TEXT")?;
        Self::ensure_column(&tx, "submit_events", "job_id", "TEXT")?;
        Self::ensure_column(&tx, "submit_events", "round_id", "INTEGER")?;
        Self::ensure_column(&tx, "submit_events", "template_epoch", "INTEGER")?;
        Self::ensure_column(&tx, "rounds", "status", "TEXT NOT NULL DEFAULT 'open'")?;
        Self::ensure_column(&tx, "rounds", "close_reason", "TEXT")?;
        Self::ensure_column(&tx, "rounds", "closed_at", "TEXT")?;

        // Add indexes for found_blocks
        tx.execute_batch(
            "CREATE INDEX IF NOT EXISTS idx_found_blocks_status_height 
                ON found_blocks(status, height);
             CREATE INDEX IF NOT EXISTS idx_found_blocks_height_hash 
                ON found_blocks(height, block_hash);",
        )?;
        // Index for PPLNS window query (true PPLNS across round boundaries)
        tx.execute_batch(
            "CREATE INDEX IF NOT EXISTS idx_shares_created_at 
                ON shares(created_at DESC);",
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
    /// Returns Some(share_id) when inserted, or None if this dedupe_key already existed.
    pub fn insert_share_idempotent(
        &self,
        worker_id: i64,
        template_id: u64,
        difficulty: f64,
        accepted: bool,
        stale: bool,
        dedupe_key: &str,
    ) -> Result<Option<i64>> {
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
        if rows > 0 {
            return Ok(Some(conn.last_insert_rowid()));
        }
        Ok(None)
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
        block_height: i32,
        worker_id: i64,
        worker_name: &str,
        payout_address: &str,
        persist_source: &str,
    ) -> Result<()> {
        let now = Utc::now().to_rfc3339();
        let round_id = self.resolve_round_for_template(template_id)?;
        let conn = self.conn.lock().map_err(|_| anyhow!("db mutex poisoned"))?;

        conn.execute(
            "INSERT INTO found_blocks(round_id, block_hash, height, template_id, worker_id, worker_name, payout_address, persist_source, coinbase_maturity_blocks, status, created_at)
             VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, 100, 'confirmed', ?9)
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
            "INSERT OR IGNORE INTO submit_events(block_hash, template_id, worker_id, worker_name, payout_address, node_result, round_id, created_at)
             VALUES(?1, ?2, ?3, ?4, ?5, 'accepted', ?6, ?7)",
            params![block_hash, template_id as i64, worker_id, worker_name, payout_address, round_id, now],
        )?;
        drop(conn);
        self.close_round(
            round_id,
            Some(template_id),
            "round_closed_found_block",
            Some(block_hash),
        )?;
        self.record_accounting_event(
            "found_block_observed",
            Some("accepted"),
            None,
            Some(worker_id),
            Some(worker_name),
            Some(payout_address),
            Some(round_id),
            Some(template_id),
            None,
            None,
            Some(block_hash),
            None,
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

    /// List weighted shares for PPLNS window, looking back from block find time.
    /// 
    /// Implements true PPLNS by querying across round boundaries. The window extends
    /// back in time until target_work_units is reached, regardless of round boundaries.
    /// This enforces early-leaver penalty and prevents late-joiner advantage.
    /// 
    /// # Arguments
    /// * `found_block_id` - The found block to get the cutoff time from
    /// * `target_work_units` - Target difficulty-weighted work units (N multiplier)
    /// * `hard_limit` - Maximum number of shares to retrieve (safety limit)
    pub fn list_weighted_shares_for_pplns_window(
        &self,
        found_block_id: i64,
        target_work_units: f64,
        hard_limit: u32,
    ) -> Result<Vec<PplnsWindowShare>> {
        let conn = self.conn.lock().map_err(|_| anyhow!("db mutex poisoned"))?;
        // Get the block find time as the cutoff point for the PPLNS window
        let cutoff: String = conn.query_row(
            "SELECT created_at FROM found_blocks WHERE id=?1",
            params![found_block_id],
            |r| r.get(0),
        )?;

        // Query shares ordered by creation time (most recent first), without round_id filter.
        // This allows the PPLNS window to span multiple rounds, implementing true PPLNS.
        let mut stmt = conn.prepare(
            "SELECT s.id, w.payout_address, s.difficulty, s.created_at
             FROM shares s
             JOIN workers w ON w.id = s.worker_id
             WHERE s.accepted=1 AND s.stale=0 AND s.created_at <= ?1
             ORDER BY s.created_at DESC
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
                "SELECT id, block_hash FROM found_blocks WHERE status='matured' ORDER BY id ASC LIMIT 1",
                [],
                |r| Ok((r.get::<_, i64>(0)?, r.get::<_, String>(1)?)),
            )
            .optional()?;
        Ok(row)
    }

    /// Find latest found_block by height (for reconciliation)
    pub fn find_latest_found_block(&self) -> Result<Option<FoundBlock>> {
        let conn = self.conn.lock().map_err(|_| anyhow!("db mutex poisoned"))?;
        let row = conn
            .query_row(
                "SELECT id, round_id, block_hash, height, status, 
                        template_id, worker_id, worker_name, 
                        payout_address, persist_source, 
                        disconnected_at, orphan_reason, matured_at, created_at
                 FROM found_blocks 
                 ORDER BY height DESC 
                 LIMIT 1",
                [],
                |r| Ok(FoundBlock {
                    id: r.get(0)?,
                    round_id: r.get(1)?,
                    block_hash: r.get(2)?,
                    height: r.get(3)?,
                    status: r.get(4)?,
                    template_id: r.get(5)?,
                    worker_id: r.get(6)?,
                    worker_name: r.get(7)?,
                    payout_address: r.get(8)?,
                    persist_source: r.get(9)?,
                    disconnected_at: r.get::<_, Option<String>>(10)?.map(|s| 
                        DateTime::parse_from_rfc3339(&s).map(|d| d.with_timezone(&Utc)).ok()
                    ).flatten(),
                    orphan_reason: r.get(11)?,
                    matured_at: r.get::<_, Option<String>>(12)?.map(|s| 
                        DateTime::parse_from_rfc3339(&s).map(|d| d.with_timezone(&Utc)).ok()
                    ).flatten(),
                    created_at: r.get(13)?,
                }),
            )
            .optional()?;
        Ok(row)
    }

    /// Find found_block by exact height
    pub fn find_found_block_by_height(&self, height: i64) -> Result<Option<FoundBlock>> {
        let conn = self.conn.lock().map_err(|_| anyhow!("db mutex poisoned"))?;
        let row = conn
            .query_row(
                "SELECT id, round_id, block_hash, height, status, 
                        template_id, worker_id, worker_name, 
                        payout_address, persist_source, 
                        disconnected_at, orphan_reason, matured_at, created_at
                 FROM found_blocks 
                 WHERE height = ?1",
                params![height],
                |r| Ok(FoundBlock {
                    id: r.get(0)?,
                    round_id: r.get(1)?,
                    block_hash: r.get(2)?,
                    height: r.get(3)?,
                    status: r.get(4)?,
                    template_id: r.get(5)?,
                    worker_id: r.get(6)?,
                    worker_name: r.get(7)?,
                    payout_address: r.get(8)?,
                    persist_source: r.get(9)?,
                    disconnected_at: r.get::<_, Option<String>>(10)?.map(|s| 
                        DateTime::parse_from_rfc3339(&s).map(|d| d.with_timezone(&Utc)).ok()
                    ).flatten(),
                    orphan_reason: r.get(11)?,
                    matured_at: r.get::<_, Option<String>>(12)?.map(|s| 
                        DateTime::parse_from_rfc3339(&s).map(|d| d.with_timezone(&Utc)).ok()
                    ).flatten(),
                    created_at: r.get(13)?,
                }),
            )
            .optional()?;
        Ok(row)
    }

    /// Find found_block by height AND hash (for precise orphan detection)
    pub fn find_found_block_by_height_and_hash(
        &self,
        height: i64,
        hash: &str,
    ) -> Result<Option<FoundBlock>> {
        let conn = self.conn.lock().map_err(|_| anyhow!("db mutex poisoned"))?;
        let row = conn
            .query_row(
                "SELECT id, round_id, block_hash, height, status, 
                        template_id, worker_id, worker_name, 
                        payout_address, persist_source, 
                        disconnected_at, orphan_reason, matured_at, created_at
                 FROM found_blocks 
                 WHERE height = ?1 AND block_hash = ?2",
                params![height, hash],
                |r| Ok(FoundBlock {
                    id: r.get(0)?,
                    round_id: r.get(1)?,
                    block_hash: r.get(2)?,
                    height: r.get(3)?,
                    status: r.get(4)?,
                    template_id: r.get(5)?,
                    worker_id: r.get(6)?,
                    worker_name: r.get(7)?,
                    payout_address: r.get(8)?,
                    persist_source: r.get(9)?,
                    disconnected_at: r.get::<_, Option<String>>(10)?.map(|s| 
                        DateTime::parse_from_rfc3339(&s).map(|d| d.with_timezone(&Utc)).ok()
                    ).flatten(),
                    orphan_reason: r.get(11)?,
                    matured_at: r.get::<_, Option<String>>(12)?.map(|s| 
                        DateTime::parse_from_rfc3339(&s).map(|d| d.with_timezone(&Utc)).ok()
                    ).flatten(),
                    created_at: r.get(13)?,
                }),
            )
            .optional()?;
        Ok(row)
    }

    /// Mark found_block as confirmed (status only; confirmations computed on-demand)
    pub fn mark_found_block_confirmed(
        &self,
        block_hash: &str,
    ) -> Result<()> {
        let conn = self.conn.lock().map_err(|_| anyhow!("db mutex poisoned"))?;
        conn.execute(
            "UPDATE found_blocks 
             SET status = 'confirmed' 
             WHERE block_hash = ?1 AND status = 'pending'",
            params![block_hash],
        )?;
        Ok(())
    }

    /// Mark found_block as orphaned with reason
    pub fn mark_found_block_orphaned(
        &self,
        block_hash: &str,
        reason: &str,
    ) -> Result<()> {
        let now = Utc::now().to_rfc3339();
        let conn = self.conn.lock().map_err(|_| anyhow!("db mutex poisoned"))?;
        conn.execute(
            "UPDATE found_blocks 
             SET status = 'orphaned', 
                 disconnected_at = ?1, 
                 orphan_reason = ?2 
             WHERE block_hash = ?3",
            params![now, reason, block_hash],
        )?;
        Ok(())
    }

    /// Mark matured blocks based on tip height and coinbase maturity.
    /// Formula: confirmations = tip_height - height + 1
    /// DEPRECATED: Confirmations are now computed on-demand. Use mark_blocks_matured instead.
    #[deprecated(since = "0.3.0", note = "Use mark_blocks_matured which uses computed confirmations")]
    pub fn sync_found_block_confirmations(
        &self,
        _tip_height: i64,
        _min_confirmations: u32,
    ) -> Result<()> {
        // No-op: confirmations are computed on-demand via FoundBlock::confirmations()
        Ok(())
    }

    /// Mark blocks as matured based on tip height.
    /// Uses computed confirmations: tip_height - height + 1
    pub fn mark_blocks_matured(
        &self,
        tip_height: i64,
        coinbase_maturity: i64,
    ) -> Result<()> {
        let now = Utc::now().to_rfc3339();
        let conn = self.conn.lock().map_err(|_| anyhow!("db mutex poisoned"))?;
        // Blocks where (tip_height - height + 1) >= coinbase_maturity
        // → height <= tip_height - coinbase_maturity + 1
        let max_height = tip_height - coinbase_maturity + 1;
        conn.execute(
            "UPDATE found_blocks
             SET status = 'matured', 
                 matured_at = ?1
             WHERE status = 'confirmed' 
               AND height <= ?2",
            params![now, max_height],
        )?;
        Ok(())
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
        dust: &[(String, i64)],
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
                "INSERT INTO payout_share_snapshots(payout_batch_id, share_id, payout_address, work_units, share_created_at, ordering_criterion, truncation_reason, created_at)
                 VALUES(?1, ?2, ?3, ?4, ?5, 'round_id+share_outcome_id_desc', NULL, ?6)",
                params![batch_id, share.share_id, share.payout_address, share.work_units, share.created_at, now],
            )?;
        }
        for (addr, sat) in dust {
            tx.execute(
                "INSERT INTO payout_dust_ledger(payout_batch_id, found_block_id, address, amount_sat, policy, created_at)
                 VALUES(?1, ?2, ?3, ?4, 'carry_forward', ?5)",
                params![batch_id, found_block_id, addr, sat, now],
            )?;
        }
        tx.commit()?;
        Ok(batch_id)
    }

    /// Get accumulated dust amount for a payout address.
    /// 
    /// Returns the total un-paid dust amount for the given address from the dust ledger.
    /// This is used for dust carry-forward in PPLNS payouts.
    pub fn get_accumulated_dust(&self, address: &str) -> Result<i64> {
        let conn = self.conn.lock().map_err(|_| anyhow!("db mutex poisoned"))?;
        let total: Option<i64> = conn.query_row(
            "SELECT COALESCE(SUM(amount_sat), 0) FROM payout_dust_ledger WHERE address=?1",
            params![address],
            |r| r.get(0),
        )?;
        Ok(total.unwrap_or(0))
    }

    /// Reduce dust ledger entries for an address after paying out accumulated dust.
    /// 
    /// This is called after a payout that includes previously accumulated dust.
    /// Uses FIFO ordering (oldest entries first) to reduce dust ledger entries.
    /// 
    /// # Arguments
    /// * `address` - The payout address to reduce dust for
    /// * `amount_sat` - The amount of dust that was paid out
    pub fn reduce_dust_ledger(&self, address: &str, amount_sat: i64) -> Result<()> {
        if amount_sat <= 0 {
            return Ok(());
        }
        let mut conn = self.conn.lock().map_err(|_| anyhow!("db mutex poisoned"))?;
        let tx = conn.transaction()?;
        
        // Get dust entries in FIFO order (oldest first)
        // Use a block scope to ensure stmt is dropped before tx.commit()
        let dust_entries: Vec<(i64, i64)> = {
            let mut stmt = tx.prepare(
                "SELECT id, amount_sat FROM payout_dust_ledger 
                 WHERE address=?1 AND amount_sat > 0 
                 ORDER BY created_at ASC",
            )?;
            let rows = stmt.query_map(params![address], |r| {
                Ok((r.get::<_, i64>(0)?, r.get::<_, i64>(1)?))
            })?;
            // Collect rows into a Vec to release the borrow on tx
            rows.filter_map(|r| r.ok()).collect()
        };
        
        let mut remaining_to_reduce = amount_sat;
        for (entry_id, entry_amount) in dust_entries {
            if remaining_to_reduce <= 0 {
                break;
            }
            
            if entry_amount <= remaining_to_reduce {
                // Consume entire entry
                tx.execute(
                    "UPDATE payout_dust_ledger SET amount_sat=0 WHERE id=?1",
                    params![entry_id],
                )?;
                remaining_to_reduce -= entry_amount;
            } else {
                // Partial consumption
                let new_amount = entry_amount - remaining_to_reduce;
                tx.execute(
                    "UPDATE payout_dust_ledger SET amount_sat=?1 WHERE id=?2",
                    params![new_amount, entry_id],
                )?;
                remaining_to_reduce = 0;
            }
        }
        
        tx.commit()?;
        Ok(())
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

    pub fn found_block_state_summary(&self) -> Result<FoundBlockStateSummary> {
        let conn = self.conn.lock().map_err(|_| anyhow!("db mutex poisoned"))?;
        let confirmed: i64 = conn.query_row(
            "SELECT COUNT(*) FROM found_blocks WHERE status='confirmed'",
            [],
            |r| r.get(0),
        )?;
        let matured: i64 = conn.query_row(
            "SELECT COUNT(*) FROM found_blocks WHERE status='matured'",
            [],
            |r| r.get(0),
        )?;
        let orphaned: i64 = conn.query_row(
            "SELECT COUNT(*) FROM found_blocks WHERE status='orphaned'",
            [],
            |r| r.get(0),
        )?;
        let paid: i64 = conn.query_row(
            "SELECT COUNT(*) FROM found_blocks WHERE status='paid'",
            [],
            |r| r.get(0),
        )?;
        Ok(FoundBlockStateSummary {
            confirmed: confirmed as u64,
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

    pub fn worker_accounting_summary(&self, limit: u32) -> Result<Vec<WorkerAccountingSummary>> {
        let conn = self.conn.lock().map_err(|_| anyhow!("db mutex poisoned"))?;
        let mut stmt = conn.prepare(
            "SELECT w.id, w.payout_address, w.worker_suffix,
                    SUM(CASE WHEN so.status='accepted' THEN 1 ELSE 0 END) AS accepted,
                    SUM(CASE WHEN so.status='rejected' THEN 1 ELSE 0 END) AS rejected,
                    SUM(CASE WHEN so.status='stale' THEN 1 ELSE 0 END) AS stale
             FROM workers w
             LEFT JOIN share_outcomes so ON so.worker_id=w.id
             GROUP BY w.id, w.payout_address, w.worker_suffix
             ORDER BY w.id DESC LIMIT ?1",
        )?;
        let rows = stmt.query_map(params![limit as i64], |r| {
            Ok(WorkerAccountingSummary {
                worker_id: r.get(0)?,
                payout_address: r.get(1)?,
                worker_suffix: r.get(2)?,
                accepted: r.get::<_, Option<i64>>(3)?.unwrap_or(0) as u64,
                rejected: r.get::<_, Option<i64>>(4)?.unwrap_or(0) as u64,
                stale: r.get::<_, Option<i64>>(5)?.unwrap_or(0) as u64,
            })
        })?;
        let mut out = Vec::new();
        for row in rows {
            out.push(row?);
        }
        Ok(out)
    }

    pub fn rejected_share_reasons(&self, limit: u32) -> Result<Vec<RejectedReasonSummary>> {
        let conn = self.conn.lock().map_err(|_| anyhow!("db mutex poisoned"))?;
        let mut stmt = conn.prepare(
            "SELECT COALESCE(reject_reason, 'unknown') AS reason, COUNT(*)
             FROM share_outcomes
             WHERE status='rejected'
             GROUP BY reason
             ORDER BY COUNT(*) DESC
             LIMIT ?1",
        )?;
        let rows = stmt.query_map(params![limit as i64], |r| {
            Ok(RejectedReasonSummary {
                reason: r.get(0)?,
                count: r.get::<_, i64>(1)? as u64,
            })
        })?;
        let mut out = Vec::new();
        for row in rows {
            out.push(row?);
        }
        Ok(out)
    }

    pub fn scheduler_health_summary(&self) -> Result<SchedulerHealthSummary> {
        let conn = self.conn.lock().map_err(|_| anyhow!("db mutex poisoned"))?;
        let matured_found_blocks_ready: i64 = conn.query_row(
            "SELECT COUNT(*) FROM found_blocks WHERE status='matured'",
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

    pub fn record_authorization_event(
        &self,
        session_id: &str,
        worker_name: &str,
        payout_address: Option<&str>,
        worker_suffix: Option<&str>,
        authorized: bool,
        reason: Option<&str>,
    ) -> Result<()> {
        let now = Utc::now().to_rfc3339();
        let conn = self.conn.lock().map_err(|_| anyhow!("db mutex poisoned"))?;
        conn.execute(
            "INSERT INTO authorization_events(session_id, worker_name, payout_address, worker_suffix, authorized, reason, created_at)
             VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![
                session_id,
                worker_name,
                payout_address,
                worker_suffix,
                if authorized { 1 } else { 0 },
                reason,
                now
            ],
        )?;
        Ok(())
    }

    pub fn record_accounting_event(
        &self,
        event_type: &str,
        status: Option<&str>,
        session_id: Option<&str>,
        worker_id: Option<i64>,
        worker_name: Option<&str>,
        payout_address: Option<&str>,
        round_id: Option<i64>,
        template_id: Option<u64>,
        template_epoch: Option<u64>,
        job_id: Option<&str>,
        block_hash: Option<&str>,
        payload_json: Option<&str>,
    ) -> Result<()> {
        let now = Utc::now().to_rfc3339();
        let conn = self.conn.lock().map_err(|_| anyhow!("db mutex poisoned"))?;
        conn.execute(
            "INSERT INTO accounting_events(event_type, status, session_id, worker_id, worker_name, payout_address, round_id, template_id, template_epoch, job_id, block_hash, payload_json, created_at)
             VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)",
            params![
                event_type,
                status,
                session_id,
                worker_id,
                worker_name,
                payout_address,
                round_id,
                template_id.map(|v| v as i64),
                template_epoch.map(|v| v as i64),
                job_id,
                block_hash,
                payload_json,
                now
            ],
        )?;
        Ok(())
    }

    pub fn resolve_round_for_template(&self, template_id: u64) -> Result<i64> {
        let now = Utc::now().to_rfc3339();
        let conn = self.conn.lock().map_err(|_| anyhow!("db mutex poisoned"))?;
        let open_round: Option<i64> = conn
            .query_row(
                "SELECT id FROM rounds WHERE status='open' ORDER BY id DESC LIMIT 1",
                [],
                |r| r.get(0),
            )
            .optional()?;
        if let Some(id) = open_round {
            return Ok(id);
        }
        conn.execute(
            "INSERT INTO rounds(start_template_id, status, created_at) VALUES(?1, 'open', ?2)",
            params![template_id as i64, now],
        )?;
        let id = conn.last_insert_rowid();
        conn.execute(
            "INSERT INTO round_events(round_id, event_type, template_id, created_at) VALUES(?1, 'round_opened', ?2, ?3)",
            params![id, template_id as i64, Utc::now().to_rfc3339()],
        )?;
        Ok(id)
    }

    pub fn close_round(
        &self,
        round_id: i64,
        end_template_id: Option<u64>,
        reason: &str,
        block_hash: Option<&str>,
    ) -> Result<()> {
        let now = Utc::now().to_rfc3339();
        let conn = self.conn.lock().map_err(|_| anyhow!("db mutex poisoned"))?;
        conn.execute(
            "UPDATE rounds
             SET status='closed', close_reason=?2, closed_at=?3, end_template_id=COALESCE(?4, end_template_id), found_block_hash=COALESCE(?5, found_block_hash)
             WHERE id=?1 AND status='open'",
            params![round_id, reason, now, end_template_id.map(|v| v as i64), block_hash],
        )?;
        conn.execute(
            "INSERT INTO round_events(round_id, event_type, reason, block_hash, template_id, created_at)
             VALUES(?1, ?2, ?3, ?4, ?5, ?6)",
            params![round_id, reason, reason, block_hash, end_template_id.map(|v| v as i64), Utc::now().to_rfc3339()],
        )?;
        Ok(())
    }

    pub fn record_share_outcome(&self, outcome: ShareOutcomeInsert<'_>) -> Result<()> {
        let now = Utc::now().to_rfc3339();
        let conn = self.conn.lock().map_err(|_| anyhow!("db mutex poisoned"))?;
        conn.execute(
            "INSERT OR IGNORE INTO share_outcomes(session_id, worker_id, worker_name, payout_address, template_id, template_epoch, job_id, round_id, dedupe_key, status, reject_reason, node_result, low_diff_ok, network_target_ok, block_hash, share_id, created_at)
             VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17)",
            params![
                outcome.session_id,
                outcome.worker_id,
                outcome.worker_name,
                outcome.payout_address,
                outcome.template_id as i64,
                outcome.template_epoch as i64,
                outcome.job_id,
                outcome.round_id,
                outcome.dedupe_key,
                outcome.status,
                outcome.reject_reason,
                outcome.node_result,
                outcome.low_diff_ok.map(|v| if v { 1 } else { 0 }),
                outcome.network_target_ok.map(|v| if v { 1 } else { 0 }),
                outcome.block_hash,
                outcome.share_id,
                now
            ],
        )?;
        Ok(())
    }

    pub fn list_submitted_batches_pending_confirmation(
        &self,
        limit: u32,
    ) -> Result<Vec<(i64, i64, String)>> {
        let conn = self.conn.lock().map_err(|_| anyhow!("db mutex poisoned"))?;
        let mut stmt = conn.prepare(
            "SELECT id, found_block_id, submitted_txid
             FROM payout_batches
             WHERE status='submitted' AND submitted_txid IS NOT NULL
             ORDER BY id ASC LIMIT ?1",
        )?;
        let rows = stmt.query_map(params![limit as i64], |r| {
            Ok((r.get(0)?, r.get(1)?, r.get(2)?))
        })?;
        let mut out = Vec::new();
        for row in rows {
            out.push(row?);
        }
        Ok(out)
    }

    pub fn schedule_batch_retry(&self, batch_id: i64, err: &str, retry_at: &str) -> Result<()> {
        let conn = self.conn.lock().map_err(|_| anyhow!("db mutex poisoned"))?;
        conn.execute(
            "UPDATE payout_batches SET status='failed', last_error=?2, next_retry_at=?3, attempt_count=attempt_count+1 WHERE id=?1",
            params![batch_id, err, retry_at],
        )?;
        Ok(())
    }

    pub fn list_repairable_missing_found_blocks(&self) -> Result<Vec<MissingFoundBlock>> {
        self.reconcile_missing_found_blocks_detail()
    }

    pub fn repair_missing_found_blocks_from_submit_events(&self) -> Result<u64> {
        let missing = self.reconcile_missing_found_blocks_detail()?;
        let mut repaired = 0u64;
        for row in missing {
            if let (Some(template_id), Some(worker_id), Some(worker_name), Some(payout_address)) = (
                row.template_id,
                row.worker_id,
                row.worker_name.as_deref().map(|s| s.to_string()),
                row.payout_address.as_deref().map(|s| s.to_string()),
            ) {
                let _ = self.record_found_block(
                    &row.block_hash,
                    template_id as u64,
                    0,
                    worker_id,
                    &worker_name,
                    &payout_address,
                    "reconcile_repair",
                );
                repaired += 1;
            }
        }
        Ok(repaired)
    }

    /// Acquire a scheduler lease for the given instance.
    ///
    /// Returns `Ok(true)` if lease was acquired successfully.
    /// Returns `Ok(false)` if another instance currently holds the lease.
    pub fn acquire_scheduler_lease(&self, instance_id: &str, duration_minutes: i64) -> Result<bool> {
        let now = Utc::now();
        let expires = now + Duration::minutes(duration_minutes);
        
        let conn = self.conn.lock().map_err(|_| anyhow!("db mutex poisoned"))?;
        
        let rows = conn.execute(
            "INSERT INTO payout_scheduler_lease(id, owner, expires_at) 
             VALUES(1, ?1, ?2)
             ON CONFLICT(id) DO UPDATE SET 
                 owner=excluded.owner,
                 expires_at=excluded.expires_at
             WHERE expires_at < ?3",
            params![instance_id, expires.to_rfc3339(), now.to_rfc3339()],
        )?;
        
        Ok(rows > 0)
    }

    /// Renew an existing scheduler lease.
    ///
    /// Returns `Ok(true)` if lease was renewed successfully.
    /// Returns `Ok(false)` if this instance doesn't hold the lease.
    pub fn renew_scheduler_lease(&self, instance_id: &str, duration_minutes: i64) -> Result<bool> {
        let now = Utc::now();
        let expires = now + Duration::minutes(duration_minutes);
        
        let conn = self.conn.lock().map_err(|_| anyhow!("db mutex poisoned"))?;
        
        let rows = conn.execute(
            "UPDATE payout_scheduler_lease 
             SET expires_at = ?1 
             WHERE id = 1 AND owner = ?2",
            params![expires.to_rfc3339(), instance_id],
        )?;
        
        Ok(rows > 0)
    }

    /// Release a scheduler lease voluntarily.
    ///
    /// Returns `Ok(true)` if lease was released.
    /// Returns `Ok(false)` if this instance didn't hold the lease.
    pub fn release_scheduler_lease(&self, instance_id: &str) -> Result<bool> {
        let conn = self.conn.lock().map_err(|_| anyhow!("db mutex poisoned"))?;
        
        let rows = conn.execute(
            "DELETE FROM payout_scheduler_lease WHERE id = 1 AND owner = ?1",
            params![instance_id],
        )?;
        
        Ok(rows > 0)
    }

    /// Check if a lease is currently held and by whom.
    ///
    /// Returns `Ok(Some(owner))` if lease is held (not expired).
    /// Returns `Ok(None)` if no lease exists or lease has expired.
    pub fn check_lease_status(&self) -> Result<Option<String>> {
        let now = Utc::now();
        let conn = self.conn.lock().map_err(|_| anyhow!("db mutex poisoned"))?;
        
        let owner: Option<String> = conn.query_row(
            "SELECT owner FROM payout_scheduler_lease 
             WHERE id = 1 AND expires_at > ?1",
            params![now.to_rfc3339()],
            |r| r.get(0),
        ).optional()?;
        
        Ok(owner)
    }

    #[cfg(test)]
    pub fn expire_lease_for_test(&self) -> Result<()> {
        let now = Utc::now();
        let past = (now - Duration::minutes(5)).to_rfc3339();
        let conn = self.conn.lock().map_err(|_| anyhow!("db mutex poisoned"))?;
        conn.execute(
            "UPDATE payout_scheduler_lease SET expires_at = ?1 WHERE id = 1",
            params![past],
        )?;
        Ok(())
    }

    /// Get detailed lease information including expiration time.
    pub fn get_lease_info(&self) -> Result<Option<LeaseInfo>> {
        let now = Utc::now();
        let conn = self.conn.lock().map_err(|_| anyhow!("db mutex poisoned"))?;
        
        let row: Option<(String, String)> = conn.query_row(
            "SELECT owner, expires_at FROM payout_scheduler_lease WHERE id = 1",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        ).optional()?;
        
        match row {
            Some((owner, expires_str)) => {
                let expires_at_dt = DateTime::parse_from_rfc3339(&expires_str)
                    .map(|dt| dt.with_timezone(&Utc))
                    .unwrap_or_else(|_| Utc::now());
                
                let is_expired = expires_at_dt < now;
                let is_valid = !is_expired;
                
                Ok(Some(LeaseInfo {
                    owner,
                    expires_at: expires_str,
                    is_valid,
                    is_expired,
                }))
            }
            None => Ok(None),
        }
    }
}

pub struct ShareOutcomeInsert<'a> {
    pub session_id: &'a str,
    pub worker_id: i64,
    pub worker_name: &'a str,
    pub payout_address: &'a str,
    pub template_id: u64,
    pub template_epoch: u64,
    pub job_id: &'a str,
    pub round_id: i64,
    pub dedupe_key: &'a str,
    pub status: &'a str,
    pub reject_reason: Option<&'a str>,
    pub node_result: Option<&'a str>,
    pub low_diff_ok: Option<bool>,
    pub network_target_ok: Option<bool>,
    pub block_hash: Option<&'a str>,
    pub share_id: Option<i64>,
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
            .unwrap()
            .is_some());
        assert!(db
            .insert_share_idempotent(worker.id, 1, 1.0, true, false, "k1")
            .unwrap()
            .is_none());
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
        assert_eq!(status, "confirmed");
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
        db.mark_found_block_orphaned("bh2", "test_orphan").unwrap();
        
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
        // Mark as matured (tip=220, height=120 => confirmations=101 >= 100)
        db.mark_blocks_matured(220, 100).unwrap();
        
        let conn = db.conn.lock().unwrap();
        let status: String = conn
            .query_row(
                "SELECT status FROM found_blocks WHERE block_hash='bh3'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(status, "matured");
        
        // Verify orphaned block returns -1 confirmations
        let orphaned = db.find_found_block_by_height(10).unwrap().unwrap();
        assert_eq!(orphaned.confirmations(220), -1);
        
        // Verify matured block has correct confirmations
        let matured = db.find_found_block_by_height(11).unwrap().unwrap();
        assert_eq!(matured.confirmations(220), 101);
    }
}
