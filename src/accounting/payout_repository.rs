use anyhow::Result;
use parking_lot::Mutex;
use rusqlite::{params, Connection};
use std::sync::Arc;

/// A payout batch created when a found block matures and PPLNS payout is calculated.
#[derive(Debug, Clone)]
pub struct PayoutBatch {
    pub id: i64,
    pub round_id: i64,
    pub status: String,
    pub total_amount: i64,
    pub pool_fee_amount: i64,
    pub pool_fee_address: Option<String>,
    pub miner_count: i64,
    pub retry_key: Option<String>,
    pub last_error: Option<String>,
    pub next_retry_at: Option<String>,
    pub attempt_count: i64,
    pub signed_payload_ref: Option<String>,
    pub submitted_txid: Option<String>,
}

/// An individual miner payout within a payout batch.
#[derive(Debug, Clone)]
pub struct Payout {
    pub id: i64,
    pub batch_id: i64,
    pub worker_id: i64,
    pub payout_address: String,
    pub amount: i64,
    pub dust_carried_forward: i64,
}

/// A share snapshot recorded when a payout batch is created, for auditability.
#[derive(Debug, Clone)]
pub struct PayoutShareSnapshot {
    pub id: i64,
    pub batch_id: i64,
    pub share_id: i64,
    pub share_outcome_id: i64,
    pub payout_address: String,
    pub work_units: f64,
    pub share_created_at: String,
}

#[derive(Clone)]
pub struct PayoutRepository {
    conn: Arc<Mutex<Connection>>,
}

impl PayoutRepository {
    pub fn new(conn: Arc<Mutex<Connection>>) -> Self {
        Self { conn }
    }

    /// Create a new payout batch with 'pending' status.
    /// Returns the created PayoutBatch.
    pub fn create_payout_batch(
        &self,
        round_id: i64,
        total_amount: i64,
        pool_fee_amount: i64,
        pool_fee_address: Option<&str>,
        miner_count: i64,
        retry_key: &str,
    ) -> Result<PayoutBatch> {
        let conn = self.conn.lock();
        let mut stmt = conn.prepare(
            "INSERT INTO payout_batches
             (round_id, total_amount, pool_fee_amount, pool_fee_address, miner_count, retry_key, status)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'pending')"
        )?;
        stmt.execute(params![
            round_id,
            total_amount,
            pool_fee_amount,
            pool_fee_address,
            miner_count,
            retry_key,
        ])?;
        let id = conn.last_insert_rowid();
        Ok(PayoutBatch {
            id,
            round_id,
            status: "pending".to_string(),
            total_amount,
            pool_fee_amount,
            pool_fee_address: pool_fee_address.map(|s| s.to_string()),
            miner_count,
            retry_key: Some(retry_key.to_string()),
            last_error: None,
            next_retry_at: None,
            attempt_count: 0,
            signed_payload_ref: None,
            submitted_txid: None,
        })
    }

    /// Get a payout batch by its ID.
    pub fn get_batch_by_id(&self, id: i64) -> Result<Option<PayoutBatch>> {
        let conn = self.conn.lock();
        let mut stmt = conn.prepare(
            "SELECT id, round_id, status, total_amount, pool_fee_amount, pool_fee_address,
                    miner_count, retry_key, last_error, next_retry_at, attempt_count,
                    signed_payload_ref, submitted_txid
             FROM payout_batches WHERE id = ?1",
        )?;
        let batch = stmt.query_row(params![id], |row| {
            Ok(PayoutBatch {
                id: row.get(0)?,
                round_id: row.get(1)?,
                status: row.get(2)?,
                total_amount: row.get(3)?,
                pool_fee_amount: row.get(4)?,
                pool_fee_address: row.get(5)?,
                miner_count: row.get(6)?,
                retry_key: row.get(7)?,
                last_error: row.get(8)?,
                next_retry_at: row.get(9)?,
                attempt_count: row.get(10)?,
                signed_payload_ref: row.get(11)?,
                submitted_txid: row.get(12)?,
            })
        });
        match batch {
            Ok(b) => Ok(Some(b)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// List payout batches, optionally filtered by status.
    pub fn list_batches(&self, status_filter: Option<&str>) -> Result<Vec<PayoutBatch>> {
        let conn = self.conn.lock();
        let (sql, params_vec): (String, Vec<Box<dyn rusqlite::types::ToSql>>) =
            if let Some(status) = status_filter {
                (
                    "SELECT id, round_id, status, total_amount, pool_fee_amount, pool_fee_address,
                            miner_count, retry_key, last_error, next_retry_at, attempt_count,
                            signed_payload_ref, submitted_txid
                     FROM payout_batches WHERE status = ?1 ORDER BY id DESC"
                        .to_string(),
                    vec![Box::new(status.to_string())],
                )
            } else {
                (
                    "SELECT id, round_id, status, total_amount, pool_fee_amount, pool_fee_address,
                            miner_count, retry_key, last_error, next_retry_at, attempt_count,
                            signed_payload_ref, submitted_txid
                     FROM payout_batches ORDER BY id DESC"
                        .to_string(),
                    vec![],
                )
            };

        let mut stmt = conn.prepare(&sql)?;
        let param_refs: Vec<&dyn rusqlite::types::ToSql> =
            params_vec.iter().map(|p| p.as_ref()).collect();
        let rows = stmt.query_map(param_refs.as_slice(), |row| {
            Ok(PayoutBatch {
                id: row.get(0)?,
                round_id: row.get(1)?,
                status: row.get(2)?,
                total_amount: row.get(3)?,
                pool_fee_amount: row.get(4)?,
                pool_fee_address: row.get(5)?,
                miner_count: row.get(6)?,
                retry_key: row.get(7)?,
                last_error: row.get(8)?,
                next_retry_at: row.get(9)?,
                attempt_count: row.get(10)?,
                signed_payload_ref: row.get(11)?,
                submitted_txid: row.get(12)?,
            })
        })?;

        let mut batches = Vec::new();
        for row in rows {
            batches.push(row?);
        }
        Ok(batches)
    }

    /// Update a payout batch's status.
    pub fn update_batch_status(&self, id: i64, status: &str) -> Result<()> {
        let conn = self.conn.lock();
        let mut stmt = conn.prepare("UPDATE payout_batches SET status = ?1 WHERE id = ?2")?;
        stmt.execute(params![status, id])?;
        Ok(())
    }

    /// Mark a batch as submitted with the given txid.
    pub fn mark_batch_submitted(&self, id: i64, txid: &str) -> Result<()> {
        let conn = self.conn.lock();
        let mut stmt = conn.prepare(
            "UPDATE payout_batches SET status = 'submitted', submitted_txid = ?1 WHERE id = ?2",
        )?;
        stmt.execute(params![txid, id])?;
        Ok(())
    }

    /// Record an individual miner payout within a batch.
    pub fn record_payout(
        &self,
        batch_id: i64,
        worker_id: i64,
        payout_address: &str,
        amount: i64,
        dust_carried_forward: i64,
    ) -> Result<Payout> {
        let conn = self.conn.lock();
        let mut stmt = conn.prepare(
            "INSERT INTO payouts (batch_id, worker_id, payout_address, amount, dust_carried_forward)
             VALUES (?1, ?2, ?3, ?4, ?5)"
        )?;
        stmt.execute(params![
            batch_id,
            worker_id,
            payout_address,
            amount,
            dust_carried_forward,
        ])?;
        let id = conn.last_insert_rowid();
        Ok(Payout {
            id,
            batch_id,
            worker_id,
            payout_address: payout_address.to_string(),
            amount,
            dust_carried_forward,
        })
    }

    /// List payouts for a batch.
    pub fn get_payouts_by_batch(&self, batch_id: i64) -> Result<Vec<Payout>> {
        let conn = self.conn.lock();
        let mut stmt = conn.prepare(
            "SELECT id, batch_id, worker_id, payout_address, amount, dust_carried_forward
             FROM payouts WHERE batch_id = ?1 ORDER BY id ASC",
        )?;
        let rows = stmt.query_map(params![batch_id], |row| {
            Ok(Payout {
                id: row.get(0)?,
                batch_id: row.get(1)?,
                worker_id: row.get(2)?,
                payout_address: row.get(3)?,
                amount: row.get(4)?,
                dust_carried_forward: row.get(5)?,
            })
        })?;
        let mut payouts = Vec::new();
        for row in rows {
            payouts.push(row?);
        }
        Ok(payouts)
    }

    /// Get or create a dust balance entry for an address.
    /// Returns (current_balance, is_new).
    pub fn get_or_create_dust_balance(&self, payout_address: &str) -> Result<(i64, bool)> {
        let conn = self.conn.lock();
        let existing = conn.query_row(
            "SELECT balance FROM dust_balances WHERE payout_address = ?1",
            params![payout_address],
            |row| row.get::<_, i64>(0),
        );
        match existing {
            Ok(balance) => Ok((balance, false)),
            Err(rusqlite::Error::QueryReturnedNoRows) => {
                let mut stmt = conn.prepare(
                    "INSERT INTO dust_balances (payout_address, balance) VALUES (?1, 0)",
                )?;
                stmt.execute(params![payout_address])?;
                Ok((0, true))
            }
            Err(e) => Err(e.into()),
        }
    }

    /// Update a dust balance for an address.
    pub fn update_dust_balance(&self, payout_address: &str, balance: i64) -> Result<()> {
        let conn = self.conn.lock();
        let mut stmt = conn.prepare(
            "INSERT INTO dust_balances (payout_address, balance)
             VALUES (?1, ?2)
             ON CONFLICT(payout_address) DO UPDATE SET balance = ?2, updated_at = CURRENT_TIMESTAMP"
        )?;
        stmt.execute(params![payout_address, balance])?;
        Ok(())
    }

    /// Bulk insert payout share snapshots for a batch.
    pub fn snapshot_shares(&self, snapshots: &[PayoutShareSnapshot]) -> Result<usize> {
        if snapshots.is_empty() {
            return Ok(0);
        }
        let conn = self.conn.lock();
        let mut count = 0usize;
        for snap in snapshots {
            let mut stmt = conn.prepare(
                "INSERT INTO payout_share_snapshots
                 (batch_id, share_id, share_outcome_id, payout_address, work_units, share_created_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)"
            )?;
            stmt.execute(params![
                snap.batch_id,
                snap.share_id,
                snap.share_outcome_id,
                snap.payout_address,
                snap.work_units,
                snap.share_created_at,
            ])?;
            count += 1;
        }
        Ok(count)
    }

    /// Get share snapshots for a payout batch.
    pub fn get_snapshots_by_batch(&self, batch_id: i64) -> Result<Vec<PayoutShareSnapshot>> {
        let conn = self.conn.lock();
        let mut stmt = conn.prepare(
            "SELECT id, batch_id, share_id, share_outcome_id, payout_address, work_units, share_created_at
             FROM payout_share_snapshots WHERE batch_id = ?1 ORDER BY id ASC"
        )?;
        let rows = stmt.query_map(params![batch_id], |row| {
            Ok(PayoutShareSnapshot {
                id: row.get(0)?,
                batch_id: row.get(1)?,
                share_id: row.get(2)?,
                share_outcome_id: row.get(3)?,
                payout_address: row.get(4)?,
                work_units: row.get(5)?,
                share_created_at: row.get(6)?,
            })
        })?;
        let mut snapshots = Vec::new();
        for row in rows {
            snapshots.push(row?);
        }
        Ok(snapshots)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::accounting::schema::init_schema;
    use tempfile::NamedTempFile;

    fn create_round(conn: &Connection, id: i64, start: i64) {
        conn.execute(
            "INSERT INTO rounds (id, start_template_id, status) VALUES (?1, ?2, 'open')",
            rusqlite::params![id, start],
        )
        .unwrap();
    }

    fn create_worker(conn: &Connection, id: i64, addr: &str) {
        conn.execute(
            "INSERT INTO workers (id, payout_address) VALUES (?1, ?2)",
            rusqlite::params![id, addr],
        )
        .unwrap();
    }

    #[test]
    fn test_create_and_get_batch() {
        let f = NamedTempFile::new().unwrap();
        let conn = Connection::open(f.path()).unwrap();
        init_schema(&conn).unwrap();
        create_round(&conn, 1, 42);
        let repo = PayoutRepository::new(Arc::new(Mutex::new(conn)));

        let batch = repo
            .create_payout_batch(1, 100000, 1000, None, 3, "hash:3")
            .unwrap();
        assert_eq!(batch.status, "pending");
        assert_eq!(batch.total_amount, 100000);
        assert_eq!(batch.pool_fee_amount, 1000);
        assert_eq!(batch.miner_count, 3);
        assert!(batch.retry_key.is_some());

        let fetched = repo.get_batch_by_id(batch.id).unwrap().unwrap();
        assert_eq!(fetched.id, batch.id);
        assert_eq!(fetched.total_amount, 100000);
    }

    #[test]
    fn test_get_batch_not_found() {
        let f = NamedTempFile::new().unwrap();
        let conn = Connection::open(f.path()).unwrap();
        init_schema(&conn).unwrap();
        let repo = PayoutRepository::new(Arc::new(Mutex::new(conn)));

        let result = repo.get_batch_by_id(999).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_list_batches() {
        let f = NamedTempFile::new().unwrap();
        let conn = Connection::open(f.path()).unwrap();
        init_schema(&conn).unwrap();
        create_round(&conn, 1, 42);
        let repo = PayoutRepository::new(Arc::new(Mutex::new(conn)));

        assert!(repo.list_batches(None).unwrap().is_empty());
        repo.create_payout_batch(1, 50000, 500, None, 2, "h1:2")
            .unwrap();
        repo.create_payout_batch(1, 60000, 600, None, 1, "h2:1")
            .unwrap();
        assert_eq!(repo.list_batches(None).unwrap().len(), 2);
    }

    #[test]
    fn test_list_batches_by_status() {
        let f = NamedTempFile::new().unwrap();
        let conn = Connection::open(f.path()).unwrap();
        init_schema(&conn).unwrap();
        create_round(&conn, 1, 42);
        let repo = PayoutRepository::new(Arc::new(Mutex::new(conn)));

        let b = repo
            .create_payout_batch(1, 50000, 500, None, 1, "h1:1")
            .unwrap();
        repo.update_batch_status(b.id, "submitted").unwrap();

        let pending = repo.list_batches(Some("pending")).unwrap();
        assert_eq!(pending.len(), 0);
        let submitted = repo.list_batches(Some("submitted")).unwrap();
        assert_eq!(submitted.len(), 1);
    }

    #[test]
    fn test_update_batch_status() {
        let f = NamedTempFile::new().unwrap();
        let conn = Connection::open(f.path()).unwrap();
        init_schema(&conn).unwrap();
        create_round(&conn, 1, 42);
        let repo = PayoutRepository::new(Arc::new(Mutex::new(conn)));

        let b = repo
            .create_payout_batch(1, 50000, 500, None, 1, "h1:1")
            .unwrap();
        repo.update_batch_status(b.id, "failed").unwrap();

        let fetched = repo.get_batch_by_id(b.id).unwrap().unwrap();
        assert_eq!(fetched.status, "failed");
    }

    #[test]
    fn test_record_and_list_payouts() {
        let f = NamedTempFile::new().unwrap();
        let conn = Connection::open(f.path()).unwrap();
        init_schema(&conn).unwrap();
        create_round(&conn, 1, 42);
        create_worker(&conn, 10, "addr1");
        create_worker(&conn, 20, "addr2");
        let repo = PayoutRepository::new(Arc::new(Mutex::new(conn)));

        let batch = repo
            .create_payout_batch(1, 100000, 1000, None, 2, "hash:2")
            .unwrap();

        repo.record_payout(batch.id, 10, "addr1", 60000, 0).unwrap();
        repo.record_payout(batch.id, 20, "addr2", 39000, 0).unwrap();

        let payouts = repo.get_payouts_by_batch(batch.id).unwrap();
        assert_eq!(payouts.len(), 2);
        assert_eq!(payouts[0].amount, 60000);
        assert_eq!(payouts[1].amount, 39000);
    }

    #[test]
    fn test_dust_balance_create_and_update() {
        let f = NamedTempFile::new().unwrap();
        let conn = Connection::open(f.path()).unwrap();
        init_schema(&conn).unwrap();
        let repo = PayoutRepository::new(Arc::new(Mutex::new(conn)));

        let (bal, is_new) = repo.get_or_create_dust_balance("addr1").unwrap();
        assert_eq!(bal, 0);
        assert!(is_new);

        repo.update_dust_balance("addr1", 500).unwrap();
        let (bal2, is_new2) = repo.get_or_create_dust_balance("addr1").unwrap();
        assert_eq!(bal2, 500);
        assert!(!is_new2);
    }
}
