use anyhow::Result;
use parking_lot::Mutex;
use rusqlite::{params, Connection};
use std::sync::Arc;

/// A block found by the pool, submitted to lotusd and tracked through its lifecycle.
#[derive(Debug, Clone)]
pub struct FoundBlock {
    pub id: i64,
    pub round_id: i64,
    pub block_hash: String,
    pub height: i32,
    pub status: String,
    pub worker_id: Option<i64>,
    pub template_id: Option<u64>,
    pub persist_source: Option<String>,
    pub orphan_reason: Option<String>,
    pub matured_at: Option<String>,
    /// Total coinbase output value in satoshis (subsidy + tx fees).
    /// Set from MiningJob.coinbase_value when the block is found.
    pub coinbase_value: u64,
    /// Transaction ID of the coinbase transaction, set after block is confirmed.
    /// Used by the payout signer to build the spending transaction.
    pub coinbase_txid: Option<String>,
    /// Network target hex string (e.g. "0000000009d01000...") at the time
    /// the block was found. Used to compute network difficulty for PPLNS window.
    pub network_target_hex: String,
}

#[derive(Clone)]
pub struct FoundBlockRepository {
    conn: Arc<Mutex<Connection>>,
}

impl FoundBlockRepository {
    pub fn new(conn: Arc<Mutex<Connection>>) -> Self {
        Self { conn }
    }

    /// Record a new found block.
    /// Returns the newly created FoundBlock.
    pub fn record_found_block(
        &self,
        round_id: i64,
        block_hash: &str,
        height: i32,
        worker_id: Option<i64>,
        template_id: Option<u64>,
        persist_source: Option<&str>,
        coinbase_value: u64,
        network_target_hex: &str,
    ) -> Result<FoundBlock> {
        let conn = self.conn.lock();
        let mut stmt = conn.prepare(
            "INSERT INTO found_blocks
             (round_id, block_hash, height, worker_id, template_id, persist_source, status,
              coinbase_value, network_target_hex)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'immature', ?7, ?8)",
        )?;
        stmt.execute(params![
            round_id,
            block_hash,
            height,
            worker_id,
            template_id,
            persist_source,
            coinbase_value,
            network_target_hex,
        ])?;
        let id = conn.last_insert_rowid();
        Ok(FoundBlock {
            id,
            round_id,
            block_hash: block_hash.to_string(),
            height,
            status: "immature".to_string(),
            worker_id,
            template_id,
            persist_source: persist_source.map(|s| s.to_string()),
            orphan_reason: None,
            matured_at: None,
            coinbase_value,
            coinbase_txid: None,
            network_target_hex: network_target_hex.to_string(),
        })
    }

    /// Mark a found block as orphaned with the given reason.
    pub fn mark_orphaned(&self, block_hash: &str, reason: &str) -> Result<()> {
        let conn = self.conn.lock();
        let mut stmt = conn.prepare(
            "UPDATE found_blocks SET status = 'orphaned', orphan_reason = ?1 WHERE block_hash = ?2",
        )?;
        stmt.execute(params![reason, block_hash])?;
        Ok(())
    }

    /// Restore an orphaned block to 'immature' status and clear the orphan reason.
    /// Only affects blocks whose status is 'orphaned'.
    pub fn un_orphan(&self, block_hash: &str) -> Result<()> {
        let conn = self.conn.lock();
        conn.execute(
            "UPDATE found_blocks SET status = 'immature', orphan_reason = NULL WHERE block_hash = ?1 AND status = 'orphaned'",
            params![block_hash],
        )?;
        Ok(())
    }

    /// Get a found block by its block hash.
    pub fn get_by_hash(&self, block_hash: &str) -> Result<Option<FoundBlock>> {
        let conn = self.conn.lock();
        let mut stmt = conn.prepare(
            "SELECT id, round_id, block_hash, height, status, worker_id, template_id,
                    persist_source, orphan_reason, matured_at,
                    coinbase_value, coinbase_txid, network_target_hex
             FROM found_blocks WHERE block_hash = ?1",
        )?;
        let block = stmt.query_row(params![block_hash], |row| {
            Ok(FoundBlock {
                id: row.get(0)?,
                round_id: row.get(1)?,
                block_hash: row.get(2)?,
                height: row.get(3)?,
                status: row.get(4)?,
                worker_id: row.get(5)?,
                template_id: {
                    let tid: Option<i64> = row.get(6)?;
                    debug_assert!(
                        tid.map_or(true, |v| v >= 0),
                        "template_id must be non-negative"
                    );
                    tid.map(|v| v as u64)
                },
                persist_source: row.get(7)?,
                orphan_reason: row.get(8)?,
                matured_at: row.get(9)?,
                coinbase_value: {
                    let cv: i64 = row.get(10)?;
                    debug_assert!(cv >= 0, "coinbase_value must be non-negative");
                    cv as u64
                },
                coinbase_txid: row.get(11)?,
                network_target_hex: row.get(12)?,
            })
        });
        match block {
            Ok(b) => Ok(Some(b)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// Find a found block by round_id.
    /// Returns the most recent block for that round (if multiple exist).
    pub fn get_by_round_id(&self, round_id: i64) -> Result<Option<FoundBlock>> {
        let conn = self.conn.lock();
        let mut stmt = conn.prepare(
            "SELECT id, round_id, block_hash, height, status, worker_id, template_id,
                    persist_source, orphan_reason, matured_at,
                    coinbase_value, coinbase_txid, network_target_hex
             FROM found_blocks WHERE round_id = ?1 ORDER BY id DESC LIMIT 1",
        )?;
        let block = stmt.query_row(params![round_id], |row| {
            Ok(FoundBlock {
                id: row.get(0)?,
                round_id: row.get(1)?,
                block_hash: row.get(2)?,
                height: row.get(3)?,
                status: row.get(4)?,
                worker_id: row.get(5)?,
                template_id: {
                    let tid: Option<i64> = row.get(6)?;
                    assert!(
                        tid.map_or(true, |v| v >= 0),
                        "template_id must be non-negative"
                    );
                    tid.map(|v| v as u64)
                },
                persist_source: row.get(7)?,
                orphan_reason: row.get(8)?,
                matured_at: row.get(9)?,
                coinbase_value: {
                    let cv: i64 = row.get(10)?;
                    assert!(cv >= 0, "coinbase_value must be non-negative");
                    cv as u64
                },
                coinbase_txid: row.get(11)?,
                network_target_hex: row.get(12)?,
            })
        });
        match block {
            Ok(b) => Ok(Some(b)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// Update the status of a found block.
    pub fn update_status(&self, id: i64, status: &str) -> Result<()> {
        let conn = self.conn.lock();
        let mut stmt = conn.prepare("UPDATE found_blocks SET status = ?1 WHERE id = ?2")?;
        stmt.execute(params![status, id])?;
        Ok(())
    }

    /// Set the coinbase transaction ID for a found block.
    pub fn update_coinbase_txid(&self, id: i64, coinbase_txid: &str) -> Result<()> {
        let conn = self.conn.lock();
        let mut stmt = conn.prepare("UPDATE found_blocks SET coinbase_txid = ?1 WHERE id = ?2")?;
        stmt.execute(params![coinbase_txid, id])?;
        Ok(())
    }

    /// Mark a found block as matured.
    ///
    /// Sets status to 'matured' and records the maturation timestamp.
    /// Called when the block reaches `min_confirmations` confirmations.
    pub fn mark_matured(&self, id: i64) -> Result<()> {
        let conn = self.conn.lock();
        conn.execute(
            "UPDATE found_blocks SET status = 'matured', matured_at = CURRENT_TIMESTAMP WHERE id = ?1",
            params![id],
        )?;
        Ok(())
    }

    /// List found blocks, optionally filtered by status.
    pub fn list(&self, status_filter: Option<&str>) -> Result<Vec<FoundBlock>> {
        let conn = self.conn.lock();
        let (sql, params_vec): (String, Vec<Box<dyn rusqlite::types::ToSql>>) =
            if let Some(status) = status_filter {
                (
                    "SELECT id, round_id, block_hash, height, status, worker_id, template_id,
                            persist_source, orphan_reason, matured_at,
                            coinbase_value, coinbase_txid, network_target_hex
                     FROM found_blocks WHERE status = ?1 ORDER BY id DESC"
                        .to_string(),
                    vec![Box::new(status.to_string())],
                )
            } else {
                (
                    "SELECT id, round_id, block_hash, height, status, worker_id, template_id,
                            persist_source, orphan_reason, matured_at,
                            coinbase_value, coinbase_txid, network_target_hex
                     FROM found_blocks ORDER BY id DESC"
                        .to_string(),
                    vec![],
                )
            };

        let mut stmt = conn.prepare(&sql)?;
        let param_refs: Vec<&dyn rusqlite::types::ToSql> =
            params_vec.iter().map(|p| p.as_ref()).collect();
        let rows = stmt.query_map(param_refs.as_slice(), |row| {
            Ok(FoundBlock {
                id: row.get(0)?,
                round_id: row.get(1)?,
                block_hash: row.get(2)?,
                height: row.get(3)?,
                status: row.get(4)?,
                worker_id: row.get(5)?,
                template_id: {
                    let tid: Option<i64> = row.get(6)?;
                    assert!(
                        tid.map_or(true, |v| v >= 0),
                        "template_id must be non-negative"
                    );
                    tid.map(|v| v as u64)
                },
                persist_source: row.get(7)?,
                orphan_reason: row.get(8)?,
                matured_at: row.get(9)?,
                coinbase_value: {
                    let cv: i64 = row.get(10)?;
                    assert!(cv >= 0, "coinbase_value must be non-negative");
                    cv as u64
                },
                coinbase_txid: row.get(11)?,
                network_target_hex: row.get(12)?,
            })
        })?;

        let mut blocks = Vec::new();
        for row in rows {
            blocks.push(row?);
        }
        Ok(blocks)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::accounting::schema::init_schema;
    use tempfile::NamedTempFile;

    /// Create a round with the given id so foreign key constraints pass.
    fn create_round(conn: &Connection, id: i64, start_template_id: i64) {
        conn.execute(
            "INSERT INTO rounds (id, start_template_id, status) VALUES (?1, ?2, 'open')",
            rusqlite::params![id, start_template_id],
        )
        .unwrap();
    }

    fn create_worker(conn: &Connection, id: i64, address: &str) {
        conn.execute(
            "INSERT INTO workers (id, payout_address) VALUES (?1, ?2)",
            rusqlite::params![id, address],
        )
        .unwrap();
    }

    #[test]
    fn test_record_and_get_by_hash() {
        let f = NamedTempFile::new().unwrap();
        let conn = Connection::open(f.path()).unwrap();
        init_schema(&conn).unwrap();
        create_round(&conn, 1, 42);
        create_worker(&conn, 42, "lotus_address");
        let repo = FoundBlockRepository::new(Arc::new(Mutex::new(conn)));

        let block = repo
            .record_found_block(
                1,
                "0000abc",
                1292529,
                Some(42),
                Some(100),
                Some("json-rpc"),
                5000000000,
                "0000000009d01000000000000000000000000000000000000000000000000000",
            )
            .unwrap();

        assert_eq!(block.block_hash, "0000abc");
        assert_eq!(block.round_id, 1);
        assert_eq!(block.height, 1292529);
        assert_eq!(block.status, "immature");
        assert_eq!(block.persist_source, Some("json-rpc".to_string()));

        let fetched = repo.get_by_hash("0000abc").unwrap().unwrap();
        assert_eq!(fetched.id, block.id);
        assert_eq!(fetched.block_hash, "0000abc");
    }

    #[test]
    fn test_get_by_hash_returns_none_for_missing() {
        let f = NamedTempFile::new().unwrap();
        let conn = Connection::open(f.path()).unwrap();
        init_schema(&conn).unwrap();
        let repo = FoundBlockRepository::new(Arc::new(Mutex::new(conn)));

        let result = repo.get_by_hash("nonexistent").unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_mark_orphaned() {
        let f = NamedTempFile::new().unwrap();
        let conn = Connection::open(f.path()).unwrap();
        init_schema(&conn).unwrap();
        create_round(&conn, 1, 42);
        let repo = FoundBlockRepository::new(Arc::new(Mutex::new(conn)));

        repo.record_found_block(1, "0000abc", 1292529, None, None, None, 0, "")
            .unwrap();
        repo.mark_orphaned("0000abc", "reorg_detected").unwrap();

        let block = repo.get_by_hash("0000abc").unwrap().unwrap();
        assert_eq!(block.status, "orphaned");
        assert_eq!(block.orphan_reason, Some("reorg_detected".to_string()));
    }

    #[test]
    fn test_un_orphan_restores_status_and_clears_reason() {
        let f = NamedTempFile::new().unwrap();
        let conn = Connection::open(f.path()).unwrap();
        init_schema(&conn).unwrap();
        create_round(&conn, 1, 42);
        let repo = FoundBlockRepository::new(Arc::new(Mutex::new(conn)));

        repo.record_found_block(1, "block1", 100, None, None, None, 0, "")
            .unwrap();
        repo.mark_orphaned("block1", "reorg").unwrap();

        let block = repo.get_by_hash("block1").unwrap().unwrap();
        assert_eq!(block.status, "orphaned");
        assert_eq!(block.orphan_reason, Some("reorg".to_string()));

        repo.un_orphan("block1").unwrap();

        let block = repo.get_by_hash("block1").unwrap().unwrap();
        assert_eq!(block.status, "immature");
        assert_eq!(block.orphan_reason, None);
    }

    #[test]
    fn test_un_orphan_noop_for_non_orphaned() {
        let f = NamedTempFile::new().unwrap();
        let conn = Connection::open(f.path()).unwrap();
        init_schema(&conn).unwrap();
        create_round(&conn, 1, 42);
        let repo = FoundBlockRepository::new(Arc::new(Mutex::new(conn)));

        // Block starts as 'immature' — un_orphan should leave it unchanged
        repo.record_found_block(1, "block1", 100, None, None, None, 0, "")
            .unwrap();
        repo.un_orphan("block1").unwrap();

        let block = repo.get_by_hash("block1").unwrap().unwrap();
        assert_eq!(block.status, "immature");
        assert_eq!(block.orphan_reason, None);
    }

    #[test]
    fn test_un_orphan_noop_for_nonexistent() {
        let f = NamedTempFile::new().unwrap();
        let conn = Connection::open(f.path()).unwrap();
        init_schema(&conn).unwrap();
        let repo = FoundBlockRepository::new(Arc::new(Mutex::new(conn)));

        // Calling un_orphan on a non-existent hash should not error
        repo.un_orphan("nonexistent").unwrap();
    }

    #[test]
    fn test_list_all() {
        let f = NamedTempFile::new().unwrap();
        let conn = Connection::open(f.path()).unwrap();
        init_schema(&conn).unwrap();
        create_round(&conn, 1, 42);
        let repo = FoundBlockRepository::new(Arc::new(Mutex::new(conn)));

        assert!(repo.list(None).unwrap().is_empty());

        repo.record_found_block(1, "block1", 100, None, None, None, 0, "")
            .unwrap();
        repo.record_found_block(1, "block2", 101, None, None, None, 0, "")
            .unwrap();

        assert_eq!(repo.list(None).unwrap().len(), 2);
    }

    #[test]
    fn test_list_by_status() {
        let f = NamedTempFile::new().unwrap();
        let conn = Connection::open(f.path()).unwrap();
        init_schema(&conn).unwrap();
        create_round(&conn, 1, 42);
        let repo = FoundBlockRepository::new(Arc::new(Mutex::new(conn)));

        repo.record_found_block(1, "block1", 100, None, None, None, 0, "")
            .unwrap();
        repo.record_found_block(1, "block2", 101, None, None, None, 0, "")
            .unwrap();
        repo.mark_orphaned("block1", "reorg").unwrap();

        let immature = repo.list(Some("immature")).unwrap();
        assert_eq!(immature.len(), 1);
        assert_eq!(immature[0].block_hash, "block2");

        let orphaned = repo.list(Some("orphaned")).unwrap();
        assert_eq!(orphaned.len(), 1);
        assert_eq!(orphaned[0].block_hash, "block1");
    }

    #[test]
    fn test_duplicate_block_hash_rejected() {
        let f = NamedTempFile::new().unwrap();
        let conn = Connection::open(f.path()).unwrap();
        init_schema(&conn).unwrap();
        create_round(&conn, 1, 42);
        let repo = FoundBlockRepository::new(Arc::new(Mutex::new(conn)));

        repo.record_found_block(1, "samehash", 100, None, None, None, 0, "")
            .unwrap();

        let err = repo
            .record_found_block(1, "samehash", 101, None, None, None, 0, "")
            .unwrap_err();
        assert!(
            err.to_string().contains("UNIQUE"),
            "duplicate block_hash should fail with UNIQUE constraint, got: {}",
            err
        );
    }

    #[test]
    fn test_update_status_transitions() {
        let f = NamedTempFile::new().unwrap();
        let conn = Connection::open(f.path()).unwrap();
        init_schema(&conn).unwrap();
        create_round(&conn, 1, 100);

        let repo = FoundBlockRepository::new(Arc::new(Mutex::new(conn)));
        let block = repo
            .record_found_block(1, "hash1", 100, None, None, None, 50000, "")
            .unwrap();

        assert_eq!(block.status, "immature");

        repo.update_status(block.id, "paid").unwrap();
        let updated = repo.get_by_hash("hash1").unwrap().unwrap();
        assert_eq!(updated.status, "paid");
    }

    #[test]
    fn test_update_coinbase_txid() {
        let f = NamedTempFile::new().unwrap();
        let conn = Connection::open(f.path()).unwrap();
        init_schema(&conn).unwrap();
        create_round(&conn, 1, 100);

        let repo = FoundBlockRepository::new(Arc::new(Mutex::new(conn)));
        let block = repo
            .record_found_block(1, "hash1", 100, None, None, None, 50000, "")
            .unwrap();

        // coinbase_txid should start as None
        assert!(block.coinbase_txid.is_none());

        let expected_txid = "a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0c1d2e3f4a5b6c7d8e9f0a1b2";
        repo.update_coinbase_txid(block.id, expected_txid).unwrap();

        let updated = repo.get_by_hash("hash1").unwrap().unwrap();
        assert_eq!(updated.coinbase_txid.as_deref(), Some(expected_txid));
    }

    #[test]
    fn test_new_block_coinbase_txid_defaults_to_null() {
        let f = NamedTempFile::new().unwrap();
        let conn = Connection::open(f.path()).unwrap();
        init_schema(&conn).unwrap();
        create_round(&conn, 1, 100);

        let repo = FoundBlockRepository::new(Arc::new(Mutex::new(conn)));
        let block = repo
            .record_found_block(1, "hash1", 100, None, None, None, 50000, "")
            .unwrap();

        // New blocks should have coinbase_txid = None (NULL in DB)
        assert!(
            block.coinbase_txid.is_none(),
            "coinbase_txid should be None for newly recorded blocks"
        );
    }
}
