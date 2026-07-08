use anyhow::Result;
use parking_lot::Mutex;
use rusqlite::Connection;
use rusqlite::ToSql;
use std::sync::Arc;

/// Raw share submission record (per UBQ §Share).
/// Captures the fact that a miner submitted work. Immutable once persisted.
#[derive(Debug, Clone)]
pub struct Share {
    pub id: i64,
    pub worker_id: i64,
    pub session_id: String,
    pub job_id: String,
    pub template_id: u64,
    pub template_epoch: u64,
    pub extranonce1: String,
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
        template_id: u64,
        template_epoch: u64,
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
              extranonce1, extranonce2, ntime_hex_6b, nonce_hex_8b, difficulty, dedupe_key)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
        )?;

        let rows_affected = stmt.execute(rusqlite::params![
            share.worker_id,
            share.session_id,
            share.job_id,
            share.template_id,
            share.template_epoch,
            share.extranonce1,
            share.extranonce2,
            share.ntime_hex_6b,
            share.nonce_hex_8b,
            share.difficulty,
            share.dedupe_key,
        ])?;

        if rows_affected == 0 {
            // Duplicate — get the existing ID
            let mut query = conn.prepare("SELECT id FROM shares WHERE dedupe_key = ?1")?;
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
            let mut query = conn.prepare("SELECT id FROM share_outcomes WHERE dedupe_key = ?1")?;
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
        let mut stmt = conn.prepare("SELECT COUNT(*) FROM share_outcomes WHERE status = ?1")?;
        let count: i64 = stmt.query_row([status], |row| row.get(0))?;
        Ok(count)
    }

    /// Sum accepted share difficulty submitted since the given timestamp.
    /// Used to compute pool hashrate: sum_difficulty × 2^32 / window_seconds.
    pub fn sum_difficulty_since(&self, since: &str) -> Result<f64> {
        let conn = self.conn.lock();
        let sql = "
            SELECT COALESCE(SUM(s.difficulty), 0.0)
            FROM shares s
            JOIN share_outcomes so ON s.id = so.share_id
            WHERE so.status = 'accepted'
              AND so.created_at >= ?1
        ";
        conn.query_row(sql, rusqlite::params![since], |row| row.get(0))
            .map_err(Into::into)
    }

    /// Update the node_result field on a share_outcome after block submission to lotusd.
    /// Returns the number of rows updated (should be 0 or 1).
    pub fn update_outcome_node_result(&self, dedupe_key: &str, node_result: &str) -> Result<usize> {
        let conn = self.conn.lock();
        let mut stmt =
            conn.prepare("UPDATE share_outcomes SET node_result = ?1 WHERE dedupe_key = ?2")?;
        let rows = stmt.execute(rusqlite::params![node_result, dedupe_key])?;
        Ok(rows)
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
        let mut stmt = conn.prepare("SELECT COUNT(*) FROM shares WHERE worker_id = ?1")?;
        let count: i64 = stmt.query_row([worker_id], |row| row.get(0))?;
        Ok(count)
    }

    /// Insert both raw share and share outcome atomically in a single transaction.
    ///
    /// This ensures that a share and its outcome are always persisted together.
    /// If either insert fails (e.g., constraint violation), both are rolled back.
    /// Returns (share_id, outcome_id, is_new) where is_new=false means duplicate.
    pub fn insert_share_and_outcome_atomic(
        &self,
        share: &Share,
        outcome: &ShareOutcome,
    ) -> Result<(Option<i64>, Option<i64>, bool)> {
        let conn = self.conn.lock();

        // Use SQLite transaction for atomicity
        conn.execute_batch("BEGIN TRANSACTION")?;

        let result = (|| -> Result<(Option<i64>, Option<i64>, bool)> {
            // Insert share
            let (share_id, share_is_new) = {
                let mut stmt = conn.prepare(
                    "INSERT OR IGNORE INTO shares
                     (worker_id, session_id, job_id, template_id, template_epoch,
                      extranonce1, extranonce2, ntime_hex_6b, nonce_hex_8b, difficulty, dedupe_key)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
                )?;

                let rows = stmt.execute(rusqlite::params![
                    share.worker_id,
                    share.session_id,
                    share.job_id,
                    share.template_id,
                    share.template_epoch,
                    share.extranonce1,
                    share.extranonce2,
                    share.ntime_hex_6b,
                    share.nonce_hex_8b,
                    share.difficulty,
                    share.dedupe_key,
                ])?;

                let is_new = rows > 0;
                let id = if is_new {
                    conn.last_insert_rowid()
                } else {
                    // Duplicate — get the existing ID
                    let mut query = conn.prepare("SELECT id FROM shares WHERE dedupe_key = ?1")?;
                    query.query_row([&share.dedupe_key], |row| row.get(0))?
                };
                (id, is_new)
            };

            // Insert share outcome
            let (outcome_id, outcome_is_new) = {
                let mut stmt = conn.prepare(
                    "INSERT OR IGNORE INTO share_outcomes
                     (share_id, session_id, worker_id, job_id, round_id, dedupe_key,
                      status, reject_reason, node_result, low_diff_ok, network_target_ok, block_hash)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
                )?;

                let rows = stmt.execute([
                    share_id.to_sql()?,
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

                let is_new = rows > 0;
                let id = if is_new {
                    conn.last_insert_rowid()
                } else {
                    // Duplicate — get the existing ID
                    let mut query =
                        conn.prepare("SELECT id FROM share_outcomes WHERE dedupe_key = ?1")?;
                    query.query_row([&outcome.dedupe_key], |row| row.get(0))?
                };
                (id, is_new)
            };

            Ok((
                Some(share_id),
                Some(outcome_id),
                share_is_new || outcome_is_new,
            ))
        })();

        match result {
            Ok(val) => {
                conn.execute_batch("COMMIT")?;
                Ok(val)
            }
            Err(e) => {
                conn.execute_batch("ROLLBACK")?;
                Err(e)
            }
        }
    }

    /// Count rejected share_outcomes grouped by reject_reason.
    pub fn count_rejected_by_reason(&self) -> Result<std::collections::HashMap<String, i64>> {
        let conn = self.conn.lock();
        let mut stmt = conn.prepare(
            "SELECT reject_reason, COUNT(*) FROM share_outcomes WHERE status = 'rejected' GROUP BY reject_reason"
        )?;
        let rows = stmt.query_map([], |row| {
            let reason: Option<String> = row.get(0)?;
            let count: i64 = row.get(1)?;
            Ok((reason.unwrap_or_else(|| "unknown".to_string()), count))
        })?;
        let mut map = std::collections::HashMap::new();
        for row in rows {
            let (reason, count) = row?;
            *map.entry(reason).or_insert(0) += count;
        }
        Ok(map)
    }

    /// Count share outcomes by round, grouped by worker, with worker details.
    /// Returns tuples of (worker_id, payout_address, worker_suffix,
    /// total_shares, accepted_shares, rejected_shares).
    pub fn count_outcomes_by_round_with_workers(
        &self,
        round_id: i64,
    ) -> Result<Vec<(i64, String, Option<String>, i64, i64, i64)>> {
        let conn = self.conn.lock();
        let mut stmt = conn.prepare(
            "SELECT so.worker_id, w.payout_address, w.worker_suffix,
                    COUNT(*) as total,
                    SUM(CASE WHEN so.status = 'accepted' THEN 1 ELSE 0 END) as accepted,
                    SUM(CASE WHEN so.status = 'rejected' THEN 1 ELSE 0 END) as rejected
             FROM share_outcomes so
             JOIN workers w ON w.id = so.worker_id
             WHERE so.round_id = ?1
             GROUP BY so.worker_id
             ORDER BY so.worker_id",
        )?;

        let rows = stmt.query_map([round_id], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, Option<String>>(2)?,
                row.get::<_, i64>(3)?,
                row.get::<_, i64>(4)?,
                row.get::<_, i64>(5)?,
            ))
        })?;

        let mut breakdown = Vec::new();
        for row in rows {
            breakdown.push(row?);
        }
        Ok(breakdown)
    }

    /// Total number of raw share records.
    pub fn total_count(&self) -> Result<i64> {
        let conn = self.conn.lock();
        let mut stmt = conn.prepare("SELECT COUNT(*) FROM shares")?;
        let count: i64 = stmt.query_row([], |row| row.get(0))?;
        Ok(count)
    }

    /// List shares with optional filters.
    ///
    /// - `worker_id`: filter by worker
    /// - `status`: filter by share_outcomes.status (JOIN)
    /// - `from` / `to`: ISO date range filter on shares.created_at
    /// - `limit` / `offset`: pagination
    pub fn list_shares(
        &self,
        worker_id: Option<i64>,
        status: Option<&str>,
        from: Option<&str>,
        to: Option<&str>,
        limit: Option<i64>,
        offset: Option<i64>,
    ) -> Result<Vec<Share>> {
        let conn = self.conn.lock();
        let mut conditions: Vec<String> = Vec::new();
        let mut params: Vec<Box<dyn rusqlite::types::ToSql>> = Vec::new();

        if let Some(wid) = worker_id {
            conditions.push(format!("s.worker_id = ?{}", params.len() + 1));
            params.push(Box::new(wid));
        }

        if let Some(st) = status {
            conditions.push(format!("so.status = ?{}", params.len() + 1));
            params.push(Box::new(st.to_string()));
        }

        if let Some(f) = from {
            conditions.push(format!("s.created_at >= ?{}", params.len() + 1));
            params.push(Box::new(f.to_string()));
        }

        if let Some(t) = to {
            conditions.push(format!("s.created_at <= ?{}", params.len() + 1));
            params.push(Box::new(t.to_string()));
        }

        let where_clause = if conditions.is_empty() {
            String::new()
        } else {
            format!("WHERE {}", conditions.join(" AND "))
        };

        // JOIN share_outcomes only when status filter is present
        let join_clause = if status.is_some() {
            "LEFT JOIN share_outcomes so ON so.dedupe_key = s.dedupe_key"
        } else {
            ""
        };

        let sql = format!(
            "SELECT s.id, s.worker_id, s.session_id, s.job_id, s.template_id,
                    s.template_epoch, s.extranonce1, s.extranonce2,
                    s.ntime_hex_6b, s.nonce_hex_8b, s.difficulty, s.dedupe_key
             FROM shares s
             {join_clause}
             {where_clause}
             ORDER BY s.id DESC"
        );

        let mut stmt = conn.prepare(&sql)?;
        let param_refs: Vec<&dyn rusqlite::types::ToSql> =
            params.iter().map(|p| p.as_ref()).collect();

        // Apply limit/offset after query
        let rows = stmt.query_map(param_refs.as_slice(), |row| {
            Ok(Share {
                id: row.get(0)?,
                worker_id: row.get(1)?,
                session_id: row.get(2)?,
                job_id: row.get(3)?,
                template_id: row.get(4)?,
                template_epoch: row.get(5)?,
                extranonce1: row.get(6)?,
                extranonce2: row.get(7)?,
                ntime_hex_6b: row.get(8)?,
                nonce_hex_8b: row.get(9)?,
                difficulty: row.get(10)?,
                dedupe_key: row.get(11)?,
            })
        })?;

        let mut shares: Vec<Share> = Vec::new();
        for row in rows {
            shares.push(row?);
        }

        // Apply limit/offset in-memory for now
        // (optimize to SQL-level pagination if needed)
        if let Some(off) = offset {
            if off as usize <= shares.len() {
                shares = shares.split_off(off as usize);
            } else {
                shares.clear();
            }
        }
        if let Some(lim) = limit {
            shares.truncate(lim as usize);
        }

        Ok(shares)
    }

    /// List share outcomes with optional filters.
    pub fn list_outcomes(
        &self,
        worker_id: Option<i64>,
        status: Option<&str>,
        from: Option<&str>,
        to: Option<&str>,
        limit: Option<i64>,
        offset: Option<i64>,
    ) -> Result<Vec<ShareOutcome>> {
        let conn = self.conn.lock();
        let mut conditions: Vec<String> = Vec::new();
        let mut params: Vec<Box<dyn rusqlite::types::ToSql>> = Vec::new();

        if let Some(wid) = worker_id {
            conditions.push(format!("so.worker_id = ?{}", params.len() + 1));
            params.push(Box::new(wid));
        }

        if let Some(st) = status {
            conditions.push(format!("so.status = ?{}", params.len() + 1));
            params.push(Box::new(st.to_string()));
        }

        if let Some(f) = from {
            conditions.push(format!("so.created_at >= ?{}", params.len() + 1));
            params.push(Box::new(f.to_string()));
        }

        if let Some(t) = to {
            conditions.push(format!("so.created_at <= ?{}", params.len() + 1));
            params.push(Box::new(t.to_string()));
        }

        let where_clause = if conditions.is_empty() {
            String::new()
        } else {
            format!("WHERE {}", conditions.join(" AND "))
        };

        let sql = format!(
            "SELECT so.id, so.share_id, so.session_id, so.worker_id, so.job_id,
                    so.round_id, so.dedupe_key, so.status, so.reject_reason,
                    so.node_result, so.low_diff_ok, so.network_target_ok, so.block_hash
             FROM share_outcomes so
             {where_clause}
             ORDER BY so.id DESC"
        );

        let mut stmt = conn.prepare(&sql)?;
        let param_refs: Vec<&dyn rusqlite::types::ToSql> =
            params.iter().map(|p| p.as_ref()).collect();

        let rows = stmt.query_map(param_refs.as_slice(), |row| {
            Ok(ShareOutcome {
                id: row.get(0)?,
                share_id: row.get(1)?,
                session_id: row.get(2)?,
                worker_id: row.get(3)?,
                job_id: row.get(4)?,
                round_id: row.get(5)?,
                dedupe_key: row.get(6)?,
                status: row.get(7)?,
                reject_reason: row.get(8)?,
                node_result: row.get(9)?,
                low_diff_ok: row.get(10)?,
                network_target_ok: row.get(11)?,
                block_hash: row.get(12)?,
            })
        })?;

        let mut outcomes: Vec<ShareOutcome> = Vec::new();
        for row in rows {
            outcomes.push(row?);
        }

        // Apply limit/offset in-memory
        if let Some(off) = offset {
            if off as usize <= outcomes.len() {
                outcomes = outcomes.split_off(off as usize);
            } else {
                outcomes.clear();
            }
        }
        if let Some(lim) = limit {
            outcomes.truncate(lim as usize);
        }

        Ok(outcomes)
    }

    /// Count shares with matching filters (JOINs share_outcomes for status filter).
    pub fn count_shares(
        &self,
        worker_id: Option<i64>,
        status: Option<&str>,
        from: Option<&str>,
        to: Option<&str>,
    ) -> Result<i64> {
        let conn = self.conn.lock();
        let mut conditions: Vec<String> = Vec::new();
        let mut params: Vec<Box<dyn rusqlite::types::ToSql>> = Vec::new();

        if let Some(wid) = worker_id {
            conditions.push(format!("s.worker_id = ?{}", params.len() + 1));
            params.push(Box::new(wid));
        }

        if let Some(f) = from {
            conditions.push(format!("s.created_at >= ?{}", params.len() + 1));
            params.push(Box::new(f.to_string()));
        }

        if let Some(t) = to {
            conditions.push(format!("s.created_at <= ?{}", params.len() + 1));
            params.push(Box::new(t.to_string()));
        }

        let join_clause = if let Some(st) = status {
            conditions.push(format!("so.status = ?{}", params.len() + 1));
            params.push(Box::new(st.to_string()));
            "LEFT JOIN share_outcomes so ON so.dedupe_key = s.dedupe_key"
        } else {
            ""
        };

        let where_clause = if conditions.is_empty() {
            String::new()
        } else {
            format!("WHERE {}", conditions.join(" AND "))
        };

        let sql = format!("SELECT COUNT(*) FROM shares s {join_clause} {where_clause}");

        let mut stmt = conn.prepare(&sql)?;
        let param_refs: Vec<&dyn rusqlite::types::ToSql> =
            params.iter().map(|p| p.as_ref()).collect();
        let count: i64 = stmt.query_row(param_refs.as_slice(), |row| row.get(0))?;
        Ok(count)
    }

    /// Count share outcomes with matching filters.
    pub fn count_outcomes(
        &self,
        worker_id: Option<i64>,
        status: Option<&str>,
        from: Option<&str>,
        to: Option<&str>,
    ) -> Result<i64> {
        let conn = self.conn.lock();
        let mut conditions: Vec<String> = Vec::new();
        let mut params: Vec<Box<dyn rusqlite::types::ToSql>> = Vec::new();

        if let Some(wid) = worker_id {
            conditions.push(format!("so.worker_id = ?{}", params.len() + 1));
            params.push(Box::new(wid));
        }

        if let Some(st) = status {
            conditions.push(format!("so.status = ?{}", params.len() + 1));
            params.push(Box::new(st.to_string()));
        }

        if let Some(f) = from {
            conditions.push(format!("so.created_at >= ?{}", params.len() + 1));
            params.push(Box::new(f.to_string()));
        }

        if let Some(t) = to {
            conditions.push(format!("so.created_at <= ?{}", params.len() + 1));
            params.push(Box::new(t.to_string()));
        }

        let where_clause = if conditions.is_empty() {
            String::new()
        } else {
            format!("WHERE {}", conditions.join(" AND "))
        };

        let sql = format!("SELECT COUNT(*) FROM share_outcomes so {where_clause}");

        let mut stmt = conn.prepare(&sql)?;
        let param_refs: Vec<&dyn rusqlite::types::ToSql> =
            params.iter().map(|p| p.as_ref()).collect();
        let count: i64 = stmt.query_row(param_refs.as_slice(), |row| row.get(0))?;
        Ok(count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::accounting::schema::init_schema;
    use tempfile::NamedTempFile;

    fn create_test_share(
        worker_id: i64,
        template_id: u64,
        template_epoch: u64,
        dedupe_key: &str,
    ) -> Share {
        Share {
            id: 0,
            worker_id,
            session_id: "sess-1".to_string(),
            job_id: format!("job-{}-{}", template_id, template_epoch),
            template_id,
            template_epoch,
            extranonce1: "00000001".to_string(),
            extranonce2: "00112233".to_string(),
            ntime_hex_6b: "001122334455".to_string(),
            nonce_hex_8b: "0011223344556677".to_string(),
            difficulty: 1.0,
            dedupe_key: dedupe_key.to_string(),
        }
    }

    fn create_test_outcome(
        share_id: i64,
        worker_id: i64,
        dedupe_key: &str,
        status: &str,
    ) -> ShareOutcome {
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
        let share = create_test_share(
            1,
            42,
            12345,
            "1:42:12345:00112233:001122334455:0011223344556677",
        );

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
        let share = create_test_share(
            1,
            42,
            12345,
            "1:42:12345:00112233:001122334455:0011223344556677",
        );
        let share_id = repo.insert_share(&share).unwrap().unwrap();

        let outcome = create_test_outcome(
            share_id,
            1,
            "1:42:12345:00112233:001122334455:0011223344556677",
            "accepted",
        );
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
        repo.insert_share(&create_test_share(1, 1, 100, "dk1"))
            .unwrap();
        repo.insert_share(&create_test_share(1, 1, 101, "dk2"))
            .unwrap();
        repo.insert_share(&create_test_share(2, 1, 102, "dk3"))
            .unwrap();

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
        repo.insert_share_outcome(&create_test_outcome(sid1, 1, "dk1", "accepted"))
            .unwrap();

        let s2 = create_test_share(1, 1, 101, "dk2");
        let sid2 = repo.insert_share(&s2).unwrap().unwrap();
        repo.insert_share_outcome(&create_test_outcome(sid2, 1, "dk2", "accepted"))
            .unwrap();

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

        repo.insert_share(&create_test_share(1, 1, 100, "dk1"))
            .unwrap();
        repo.insert_share(&create_test_share(1, 1, 101, "dk2"))
            .unwrap();

        assert_eq!(repo.total_count().unwrap(), 2);
    }

    #[test]
    fn test_count_rejected_by_reason() {
        let f = NamedTempFile::new().unwrap();
        let conn = Connection::open(f.path()).unwrap();
        init_schema(&conn).unwrap();
        setup_worker(&conn, 1, "test_address", None);

        let repo = ShareRepository::new(Arc::new(Mutex::new(conn)));

        // Insert shares and outcomes with various rejection reasons
        let s1 = create_test_share(1, 1, 100, "dk1");
        let sid1 = repo.insert_share(&s1).unwrap().unwrap();
        let mut o1 = create_test_outcome(sid1, 1, "dk1", "rejected");
        o1.reject_reason = Some("stale-job".to_string());
        repo.insert_share_outcome(&o1).unwrap();

        let s2 = create_test_share(1, 1, 101, "dk2");
        let sid2 = repo.insert_share(&s2).unwrap().unwrap();
        let mut o2 = create_test_outcome(sid2, 1, "dk2", "rejected");
        o2.reject_reason = Some("low-difficulty-share".to_string());
        repo.insert_share_outcome(&o2).unwrap();

        let s3 = create_test_share(1, 1, 102, "dk3");
        let sid3 = repo.insert_share(&s3).unwrap().unwrap();
        let mut o3 = create_test_outcome(sid3, 1, "dk3", "rejected");
        o3.reject_reason = Some("low-difficulty-share".to_string());
        repo.insert_share_outcome(&o3).unwrap();

        let s4 = create_test_share(1, 1, 103, "dk4");
        let sid4 = repo.insert_share(&s4).unwrap().unwrap();
        repo.insert_share_outcome(&create_test_outcome(sid4, 1, "dk4", "accepted"))
            .unwrap();

        let reasons = repo.count_rejected_by_reason().unwrap();

        assert_eq!(reasons.get("stale-job"), Some(&1));
        assert_eq!(reasons.get("low-difficulty-share"), Some(&2));
        assert_eq!(reasons.len(), 2);
        assert!(reasons.get("accepted").is_none());
    }

    #[test]
    fn test_build_dedupe_key() {
        let key = ShareRepository::build_dedupe_key(
            1,
            42,
            12345,
            "00112233",
            "001122334455",
            "0011223344556677",
        );
        assert_eq!(key, "1:42:12345:00112233:001122334455:0011223344556677");
    }

    #[test]
    fn test_list_shares_empty_when_no_shares() {
        let f = NamedTempFile::new().unwrap();
        let conn = Connection::open(f.path()).unwrap();
        init_schema(&conn).unwrap();

        let repo = ShareRepository::new(Arc::new(Mutex::new(conn)));
        let shares = repo
            .list_shares(None, None, None, None, None, None)
            .unwrap();
        assert!(shares.is_empty(), "expected no shares in empty database");
    }

    #[test]
    fn test_list_shares_returns_all_shares() {
        let f = NamedTempFile::new().unwrap();
        let conn = Connection::open(f.path()).unwrap();
        init_schema(&conn).unwrap();
        setup_worker(&conn, 1, "addr1", None);
        setup_worker(&conn, 2, "addr2", None);

        let repo = ShareRepository::new(Arc::new(Mutex::new(conn)));
        let s1 = create_test_share(1, 10, 100, "key1");
        let s2 = create_test_share(2, 20, 200, "key2");
        repo.insert_share(&s1).unwrap();
        repo.insert_share(&s2).unwrap();

        let shares = repo
            .list_shares(None, None, None, None, None, None)
            .unwrap();
        assert_eq!(shares.len(), 2, "should return all inserted shares");
    }

    #[test]
    fn test_list_shares_filter_by_worker_id() {
        let f = NamedTempFile::new().unwrap();
        let conn = Connection::open(f.path()).unwrap();
        init_schema(&conn).unwrap();
        setup_worker(&conn, 1, "addr1", None);
        setup_worker(&conn, 2, "addr2", None);

        let repo = ShareRepository::new(Arc::new(Mutex::new(conn)));
        repo.insert_share(&create_test_share(1, 10, 100, "key1"))
            .unwrap();
        repo.insert_share(&create_test_share(2, 20, 200, "key2"))
            .unwrap();
        repo.insert_share(&create_test_share(1, 30, 300, "key3"))
            .unwrap();

        let shares = repo
            .list_shares(Some(1), None, None, None, None, None)
            .unwrap();
        assert_eq!(shares.len(), 2, "worker 1 should have 2 shares");
        assert!(shares.iter().all(|s| s.worker_id == 1));
    }

    #[test]
    fn test_list_shares_with_limit_and_offset() {
        let f = NamedTempFile::new().unwrap();
        let conn = Connection::open(f.path()).unwrap();
        init_schema(&conn).unwrap();
        setup_worker(&conn, 1, "addr1", None);

        let repo = ShareRepository::new(Arc::new(Mutex::new(conn)));
        for i in 0..5 {
            let s = create_test_share(1, 10 + i, 100 + i, &format!("key{i}"));
            repo.insert_share(&s).unwrap();
        }

        let limited = repo
            .list_shares(None, None, None, None, Some(2), None)
            .unwrap();
        assert_eq!(limited.len(), 2, "limit=2 should return 2 shares");

        let offset = repo
            .list_shares(None, None, None, None, Some(2), Some(2))
            .unwrap();
        assert_eq!(offset.len(), 2, "offset=2 limit=2 should return 2 shares");
        // With offset=2, we skip the first 2, so results differ
        assert_ne!(
            limited[0].id, offset[0].id,
            "offset should return different results"
        );
    }

    #[test]
    fn test_list_shares_filter_by_status() {
        let f = NamedTempFile::new().unwrap();
        let conn = Connection::open(f.path()).unwrap();
        init_schema(&conn).unwrap();
        setup_worker(&conn, 1, "addr1", None);

        let repo = ShareRepository::new(Arc::new(Mutex::new(conn)));
        repo.insert_share(&create_test_share(1, 10, 100, "key_accept"))
            .unwrap();
        repo.insert_share(&create_test_share(1, 20, 200, "key_reject"))
            .unwrap();

        // Insert matching share_outcomes (share IDs are 1 and 2 after inserts)
        repo.insert_share_outcome(&create_test_outcome(1, 1, "key_accept", "accepted"))
            .unwrap();
        repo.insert_share_outcome(&create_test_outcome(2, 1, "key_reject", "rejected"))
            .unwrap();

        let accepted = repo
            .list_shares(None, Some("accepted"), None, None, None, None)
            .unwrap();
        assert_eq!(accepted.len(), 1, "should find 1 accepted share");
        assert_eq!(accepted[0].dedupe_key, "key_accept");
    }

    #[test]
    fn test_list_shares_filter_by_date_range() {
        let f = NamedTempFile::new().unwrap();
        let conn = Connection::open(f.path()).unwrap();
        init_schema(&conn).unwrap();
        setup_worker(&conn, 1, "addr1", None);

        let repo = ShareRepository::new(Arc::new(Mutex::new(conn)));
        repo.insert_share(&create_test_share(1, 10, 100, "key1"))
            .unwrap();

        // Use a far-past and far-future date to ensure all shares are included
        let shares = repo
            .list_shares(
                None,
                None,
                Some("2020-01-01"),
                Some("2030-01-01"),
                None,
                None,
            )
            .unwrap();
        assert!(!shares.is_empty(), "should find shares in wide date range");
    }

    #[test]
    fn test_list_outcomes_empty_when_no_outcomes() {
        let f = NamedTempFile::new().unwrap();
        let conn = Connection::open(f.path()).unwrap();
        init_schema(&conn).unwrap();

        let repo = ShareRepository::new(Arc::new(Mutex::new(conn)));
        let outcomes = repo
            .list_outcomes(None, None, None, None, None, None)
            .unwrap();
        assert!(
            outcomes.is_empty(),
            "expected no outcomes in empty database"
        );
    }

    #[test]
    fn test_list_outcomes_returns_all_outcomes() {
        let f = NamedTempFile::new().unwrap();
        let conn = Connection::open(f.path()).unwrap();
        init_schema(&conn).unwrap();
        setup_worker(&conn, 1, "addr1", None);

        let repo = ShareRepository::new(Arc::new(Mutex::new(conn)));
        repo.insert_share(&create_test_share(1, 10, 100, "k1"))
            .unwrap();
        repo.insert_share(&create_test_share(1, 20, 200, "k2"))
            .unwrap();
        repo.insert_share_outcome(&create_test_outcome(1, 1, "k1", "accepted"))
            .unwrap();
        repo.insert_share_outcome(&create_test_outcome(2, 1, "k2", "rejected"))
            .unwrap();

        let outcomes = repo
            .list_outcomes(None, None, None, None, None, None)
            .unwrap();
        assert_eq!(outcomes.len(), 2, "should return all outcomes");
    }

    #[test]
    fn test_list_outcomes_filter_by_worker_id() {
        let f = NamedTempFile::new().unwrap();
        let conn = Connection::open(f.path()).unwrap();
        init_schema(&conn).unwrap();
        setup_worker(&conn, 1, "addr1", None);
        setup_worker(&conn, 2, "addr2", None);

        let repo = ShareRepository::new(Arc::new(Mutex::new(conn)));
        repo.insert_share(&create_test_share(1, 10, 100, "k1"))
            .unwrap();
        repo.insert_share(&create_test_share(1, 20, 200, "k2"))
            .unwrap();
        repo.insert_share(&create_test_share(2, 30, 300, "k3"))
            .unwrap();
        repo.insert_share_outcome(&create_test_outcome(1, 1, "k1", "accepted"))
            .unwrap();
        repo.insert_share_outcome(&create_test_outcome(2, 1, "k2", "rejected"))
            .unwrap();
        repo.insert_share_outcome(&create_test_outcome(3, 2, "k3", "accepted"))
            .unwrap();

        let outcomes = repo
            .list_outcomes(Some(1), None, None, None, None, None)
            .unwrap();
        assert_eq!(outcomes.len(), 2, "worker 1 should have 2 outcomes");
    }

    #[test]
    fn test_list_outcomes_filter_by_status() {
        let f = NamedTempFile::new().unwrap();
        let conn = Connection::open(f.path()).unwrap();
        init_schema(&conn).unwrap();
        setup_worker(&conn, 1, "addr1", None);

        let repo = ShareRepository::new(Arc::new(Mutex::new(conn)));
        repo.insert_share(&create_test_share(1, 10, 100, "k1"))
            .unwrap();
        repo.insert_share(&create_test_share(1, 20, 200, "k2"))
            .unwrap();
        repo.insert_share_outcome(&create_test_outcome(1, 1, "k1", "accepted"))
            .unwrap();
        repo.insert_share_outcome(&create_test_outcome(2, 1, "k2", "rejected"))
            .unwrap();

        let outcomes = repo
            .list_outcomes(None, Some("accepted"), None, None, None, None)
            .unwrap();
        assert_eq!(outcomes.len(), 1);
        assert_eq!(outcomes[0].status, "accepted");
    }

    #[test]
    fn test_sum_difficulty_since() {
        let f = NamedTempFile::new().unwrap();
        let conn = Connection::open(f.path()).unwrap();
        init_schema(&conn).unwrap();
        setup_worker(&conn, 1, "test_addr", None);

        // Insert shares with explicit timestamps
        // 3 accepted shares at 2.0 difficulty within window (1 min ago)
        // 1 rejected share at 2.0 difficulty within window
        // 1 accepted share at 1.0 difficulty outside window (10 min ago)
        conn.execute_batch("
            INSERT INTO shares (id, worker_id, session_id, job_id, template_id, template_epoch, extranonce1, extranonce2, ntime_hex_6b, nonce_hex_8b, difficulty, dedupe_key, created_at)
            VALUES (1, 1, 'sess-1', 'job-1', 1, 100, '00000001', '11000001', '110000000001', '1100000000000001', 2.0, 'dk1', datetime('now', '-1 minutes'));
            INSERT INTO shares (id, worker_id, session_id, job_id, template_id, template_epoch, extranonce1, extranonce2, ntime_hex_6b, nonce_hex_8b, difficulty, dedupe_key, created_at)
            VALUES (2, 1, 'sess-1', 'job-1', 1, 100, '00000001', '11000002', '110000000002', '1100000000000002', 2.0, 'dk2', datetime('now', '-1 minutes'));
            INSERT INTO shares (id, worker_id, session_id, job_id, template_id, template_epoch, extranonce1, extranonce2, ntime_hex_6b, nonce_hex_8b, difficulty, dedupe_key, created_at)
            VALUES (3, 1, 'sess-1', 'job-1', 1, 100, '00000001', '11000003', '110000000003', '1100000000000003', 2.0, 'dk3', datetime('now', '-1 minutes'));
            INSERT INTO shares (id, worker_id, session_id, job_id, template_id, template_epoch, extranonce1, extranonce2, ntime_hex_6b, nonce_hex_8b, difficulty, dedupe_key, created_at)
            VALUES (4, 1, 'sess-1', 'job-1', 1, 100, '00000001', '11000004', '110000000004', '1100000000000004', 2.0, 'dk4', datetime('now', '-1 minutes'));
            INSERT INTO shares (id, worker_id, session_id, job_id, template_id, template_epoch, extranonce1, extranonce2, ntime_hex_6b, nonce_hex_8b, difficulty, dedupe_key, created_at)
            VALUES (5, 1, 'sess-1', 'job-1', 1, 100, '00000001', '11000005', '110000000005', '1100000000000005', 1.0, 'dk5', datetime('now', '-10 minutes'));
        ").unwrap();

        // Insert share outcomes: dk1/dk2/dk5 = accepted, dk3 = stale, dk4 = rejected
        conn.execute_batch("
            INSERT INTO share_outcomes (share_id, session_id, worker_id, job_id, dedupe_key, status, created_at)
            VALUES (1, 'sess-1', 1, 'job-1', 'dk1', 'accepted', datetime('now', '-1 minutes'));
            INSERT INTO share_outcomes (share_id, session_id, worker_id, job_id, dedupe_key, status, created_at)
            VALUES (2, 'sess-1', 1, 'job-1', 'dk2', 'accepted', datetime('now', '-1 minutes'));
            INSERT INTO share_outcomes (share_id, session_id, worker_id, job_id, dedupe_key, status, created_at)
            VALUES (3, 'sess-1', 1, 'job-1', 'dk3', 'stale', datetime('now', '-1 minutes'));
            INSERT INTO share_outcomes (share_id, session_id, worker_id, job_id, dedupe_key, status, created_at)
            VALUES (4, 'sess-1', 1, 'job-1', 'dk4', 'rejected', datetime('now', '-1 minutes'));
            INSERT INTO share_outcomes (share_id, session_id, worker_id, job_id, dedupe_key, status, created_at)
            VALUES (5, 'sess-1', 1, 'job-1', 'dk5', 'accepted', datetime('now', '-10 minutes'));
        ").unwrap();

        let repo = ShareRepository::new(Arc::new(Mutex::new(conn)));

        // Sum all accepted shares regardless of timestamp = dk1 (2.0) + dk2 (2.0) + dk5 (1.0) = 5.0
        let sum = repo.sum_difficulty_since("1970-01-01 00:00:00").unwrap();
        let expected_within = 2.0 + 2.0 + 1.0; // only accepted: dk1 + dk2 + dk5
        assert!(
            (sum - expected_within).abs() < f64::EPSILON,
            "expected {expected_within}, got {sum}"
        );

        // A future timestamp should return 0.0
        let sum_future = repo.sum_difficulty_since("2099-01-01 00:00:00").unwrap();
        assert!(
            (sum_future - 0.0).abs() < f64::EPSILON,
            "expected 0.0 for future timestamp, got {sum_future}"
        );
    }
}
