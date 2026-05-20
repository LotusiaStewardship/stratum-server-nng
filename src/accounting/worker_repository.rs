use anyhow::Result;
use rusqlite::Connection;
use rusqlite::params;
use std::sync::Arc;
use parking_lot::Mutex;

#[derive(Debug, Clone)]
pub struct Worker {
    pub id: i64,
    pub payout_address: String,
    pub worker_suffix: Option<String>,
}

#[derive(Clone)]
pub struct WorkerRepository {
    conn: Arc<Mutex<Connection>>,
}

impl WorkerRepository {
    pub fn new(conn: Arc<Mutex<Connection>>) -> Self {
        Self { conn }
    }

    pub fn upsert(&self, payout_address: &str, worker_suffix: Option<&str>) -> Result<Worker> {
        // Convert empty string to NULL for storage
        let suffix_value = worker_suffix.filter(|s| !s.is_empty());
        
        let conn = self.conn.lock();
        
        // Insert or get existing
        let mut stmt = conn.prepare(
            "INSERT INTO workers (payout_address, worker_suffix) 
             VALUES (?1, ?2) 
             ON CONFLICT(payout_address, worker_suffix) DO UPDATE SET 
             payout_address = excluded.payout_address
             RETURNING id, payout_address, worker_suffix",
        )?;

        let worker = stmt.query_row(
            params![payout_address, suffix_value],
            |row| {
                Ok(Worker {
                    id: row.get(0)?,
                    payout_address: row.get(1)?,
                    worker_suffix: row.get(2)?,
                })
            },
        )?;

        Ok(worker)
    }

    pub fn get_by_id(&self, id: i64) -> Result<Option<Worker>> {
        let conn = self.conn.lock();
        let mut stmt = conn
            .prepare("SELECT id, payout_address, worker_suffix FROM workers WHERE id = ?1")?;

        let worker = stmt.query_row([id], |row| {
            Ok(Worker {
                id: row.get(0)?,
                payout_address: row.get(1)?,
                worker_suffix: row.get(2)?,
            })
        });

        match worker {
            Ok(w) => Ok(Some(w)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }
    /// List all workers ordered by ID ascending.
    pub fn list_all(&self) -> Result<Vec<Worker>> {
        let conn = self.conn.lock();
        let mut stmt = conn.prepare(
            "SELECT id, payout_address, worker_suffix FROM workers ORDER BY id ASC"
        )?;
        let rows = stmt.query_map([], |row| {
            Ok(Worker {
                id: row.get(0)?,
                payout_address: row.get(1)?,
                worker_suffix: row.get(2)?,
            })
        })?;
        let mut workers = Vec::new();
        for row in rows {
            workers.push(row?);
        }
        Ok(workers)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::accounting::schema::init_schema;
    use tempfile::NamedTempFile;

    #[test]
    fn test_upsert_new_worker() {
        let f = NamedTempFile::new().unwrap();
        let conn = Connection::open(f.path()).unwrap();
        init_schema(&conn).unwrap();

        let repo = WorkerRepository::new(Arc::new(Mutex::new(conn)));
        let worker = repo
            .upsert("lotus_16PSJNf1EDEfGvaYzaXJCJZrXH4pgiTo7kyW61iGi", Some("rig1"))
            .unwrap();

        assert_eq!(
            worker.payout_address,
            "lotus_16PSJNf1EDEfGvaYzaXJCJZrXH4pgiTo7kyW61iGi"
        );
        assert_eq!(worker.worker_suffix, Some("rig1".to_string()));
        assert_eq!(worker.id, 1);
    }

    #[test]
    fn test_upsert_existing_worker() {
        let f = NamedTempFile::new().unwrap();
        let conn = Connection::open(f.path()).unwrap();
        init_schema(&conn).unwrap();

        let repo = WorkerRepository::new(Arc::new(Mutex::new(conn)));
        let worker1 = repo
            .upsert("lotus_16PSJNf1EDEfGvaYzaXJCJZrXH4pgiTo7kyW61iGi", Some("rig1"))
            .unwrap();
        let worker2 = repo
            .upsert("lotus_16PSJNf1EDEfGvaYzaXJCJZrXH4pgiTo7kyW61iGi", Some("rig1"))
            .unwrap();

        // Same ID, not a duplicate
        assert_eq!(worker1.id, worker2.id);
    }

    #[test]
    fn test_upsert_worker_no_suffix() {
        let f = NamedTempFile::new().unwrap();
        let conn = Connection::open(f.path()).unwrap();
        init_schema(&conn).unwrap();

        let repo = WorkerRepository::new(Arc::new(Mutex::new(conn)));
        let worker = repo
            .upsert("lotus_16PSJNf1EDEfGvaYzaXJCJZrXH4pgiTo7kyW61iGi", None)
            .unwrap();

        assert_eq!(worker.worker_suffix, None);
    }

    #[test]
    fn test_get_by_id() {
        let f = NamedTempFile::new().unwrap();
        let conn = Connection::open(f.path()).unwrap();
        init_schema(&conn).unwrap();

        let repo = WorkerRepository::new(Arc::new(Mutex::new(conn)));
        let worker = repo
            .upsert("lotus_16PSJNf1EDEfGvaYzaXJCJZrXH4pgiTo7kyW61iGi", Some("rig1"))
            .unwrap();

        let fetched = repo.get_by_id(worker.id).unwrap().unwrap();
        assert_eq!(fetched.id, worker.id);
        assert_eq!(fetched.payout_address, worker.payout_address);

        let not_found = repo.get_by_id(999).unwrap();
        assert!(not_found.is_none());
    }
}
