use anyhow::Result;
use rusqlite::{Connection, ToSql};

#[derive(Debug, Clone)]
pub struct Share {
    pub id: i64,
    pub worker_id: i64,
    pub session_id: String,
    pub job_id: String,
    pub extranonce2: String,
    pub ntime_hex_6b: String,
    pub nonce_hex_8b: String,
    pub difficulty: f64,
    pub status: String,
    pub reject_reason: Option<String>,
}

pub struct ShareRepository<'a> {
    conn: &'a Connection,
}

impl<'a> ShareRepository<'a> {
    pub fn new(conn: &'a Connection) -> Self {
        Self { conn }
    }

    pub fn insert(&self, share: &Share) -> Result<i64> {
        let mut stmt = self.conn.prepare(
            "INSERT INTO shares (worker_id, session_id, job_id, extranonce2, ntime_hex_6b, nonce_hex_8b, difficulty, status, reject_reason)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
        )?;

        let id = stmt.insert([
            share.worker_id.to_sql()?,
            share.session_id.to_sql()?,
            share.job_id.to_sql()?,
            share.extranonce2.to_sql()?,
            share.ntime_hex_6b.to_sql()?,
            share.nonce_hex_8b.to_sql()?,
            share.difficulty.to_sql()?,
            share.status.to_sql()?,
            share.reject_reason.to_sql()?,
        ])?;

        Ok(id as i64)
    }

    pub fn count_by_worker(&self, worker_id: i64) -> Result<i64> {
        let mut stmt = self
            .conn
            .prepare("SELECT COUNT(*) FROM shares WHERE worker_id = ?1")?;

        let count: i64 = stmt.query_row([worker_id], |row| row.get(0))?;
        Ok(count)
    }

    pub fn count_by_status(&self, status: &str) -> Result<i64> {
        let mut stmt = self
            .conn
            .prepare("SELECT COUNT(*) FROM shares WHERE status = ?1")?;

        let count: i64 = stmt.query_row([status], |row| row.get(0))?;
        Ok(count)
    }

    pub fn total_count(&self) -> Result<i64> {
        let mut stmt = self.conn.prepare("SELECT COUNT(*) FROM shares")?;
        let count: i64 = stmt.query_row([], |row| row.get(0))?;
        Ok(count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::accounting::schema::init_schema;
    use tempfile::NamedTempFile;

    fn create_test_share(worker_id: i64, status: &str) -> Share {
        Share {
            id: 0,
            worker_id,
            session_id: "sess-1".to_string(),
            job_id: "job-1".to_string(),
            extranonce2: "00112233".to_string(),
            ntime_hex_6b: "001122334455".to_string(),
            nonce_hex_8b: "0011223344556677".to_string(),
            difficulty: 1.0,
            status: status.to_string(),
            reject_reason: None,
        }
    }

    fn setup_worker(conn: &Connection, id: i64) {
        conn.execute(
            "INSERT INTO workers (id, payout_address, worker_suffix) VALUES (?1, ?2, ?3)",
            rusqlite::params![id as i64, "test_address", Option::<String>::None],
        )
        .unwrap();
    }

    #[test]
    fn test_insert_accepted_share() {
        let f = NamedTempFile::new().unwrap();
        let conn = Connection::open(f.path()).unwrap();
        init_schema(&conn).unwrap();
        setup_worker(&conn, 1);

        let share_repo = ShareRepository::new(&conn);
        let share = create_test_share(1, "accepted");

        let id = share_repo.insert(&share).unwrap();
        assert_eq!(id, 1);
    }

    #[test]
    fn test_insert_rejected_share() {
        let f = NamedTempFile::new().unwrap();
        let conn = Connection::open(f.path()).unwrap();
        init_schema(&conn).unwrap();
        setup_worker(&conn, 1);

        let share_repo = ShareRepository::new(&conn);
        let mut share = create_test_share(1, "rejected");
        share.reject_reason = Some("low-difficulty-share".to_string());

        let id = share_repo.insert(&share).unwrap();
        assert_eq!(id, 1);
    }

    #[test]
    fn test_count_by_worker() {
        let f = NamedTempFile::new().unwrap();
        let conn = Connection::open(f.path()).unwrap();
        init_schema(&conn).unwrap();
        setup_worker(&conn, 1);
        setup_worker(&conn, 2);

        let share_repo = ShareRepository::new(&conn);
        share_repo.insert(&create_test_share(1, "accepted")).unwrap();
        share_repo.insert(&create_test_share(1, "accepted")).unwrap();
        share_repo.insert(&create_test_share(2, "accepted")).unwrap();

        assert_eq!(share_repo.count_by_worker(1).unwrap(), 2);
        assert_eq!(share_repo.count_by_worker(2).unwrap(), 1);
    }

    #[test]
    fn test_count_by_status() {
        let f = NamedTempFile::new().unwrap();
        let conn = Connection::open(f.path()).unwrap();
        init_schema(&conn).unwrap();
        setup_worker(&conn, 1);

        let share_repo = ShareRepository::new(&conn);
        share_repo.insert(&create_test_share(1, "accepted")).unwrap();
        share_repo.insert(&create_test_share(1, "accepted")).unwrap();
        share_repo.insert(&create_test_share(1, "rejected")).unwrap();

        assert_eq!(share_repo.count_by_status("accepted").unwrap(), 2);
        assert_eq!(share_repo.count_by_status("rejected").unwrap(), 1);
    }

    #[test]
    fn test_total_count() {
        let f = NamedTempFile::new().unwrap();
        let conn = Connection::open(f.path()).unwrap();
        init_schema(&conn).unwrap();
        setup_worker(&conn, 1);

        let share_repo = ShareRepository::new(&conn);
        assert_eq!(share_repo.total_count().unwrap(), 0);

        share_repo.insert(&create_test_share(1, "accepted")).unwrap();
        share_repo.insert(&create_test_share(1, "accepted")).unwrap();

        assert_eq!(share_repo.total_count().unwrap(), 2);
    }
}
