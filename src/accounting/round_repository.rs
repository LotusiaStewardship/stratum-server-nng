use anyhow::Result;
use rusqlite::{Connection, params};
use std::sync::Arc;
use parking_lot::Mutex;

/// A payout round during which shares accumulate toward the next pool-found block.
#[derive(Debug, Clone)]
pub struct Round {
    pub id: i64,
    pub start_template_id: i64,
    pub end_template_id: Option<i64>,
    pub status: String,
    pub found_block_hash: Option<String>,
}

#[derive(Clone)]
pub struct RoundRepository {
    conn: Arc<Mutex<Connection>>,
}

impl RoundRepository {
    pub fn new(conn: Arc<Mutex<Connection>>) -> Self {
        Self { conn }
    }

    /// Ensure one open round exists. If no open round is found, creates one with the
    /// given `start_template_id`. Returns the open round and a bool indicating
    /// whether a new round was created (true) or an existing one was returned (false).
    pub fn get_or_create_current_round(&self, start_template_id: i64) -> Result<(Round, bool)> {
        let conn = self.conn.lock();

        // Try to find an existing open round
        let existing = conn.query_row(
            "SELECT id, start_template_id, end_template_id, status, found_block_hash 
             FROM rounds WHERE status = 'open' LIMIT 1",
            [],
            |row| {
                Ok(Round {
                    id: row.get(0)?,
                    start_template_id: row.get(1)?,
                    end_template_id: row.get(2)?,
                    status: row.get(3)?,
                    found_block_hash: row.get(4)?,
                })
            },
        );

        match existing {
            Ok(round) => Ok((round, false)),
            Err(rusqlite::Error::QueryReturnedNoRows) => {
                // No open round exists — create one
                let mut stmt = conn.prepare(
                    "INSERT INTO rounds (start_template_id, status) VALUES (?1, 'open')"
                )?;
                stmt.execute([start_template_id])?;
                let id = conn.last_insert_rowid();
                Ok((Round {
                    id,
                    start_template_id,
                    end_template_id: None,
                    status: "open".to_string(),
                    found_block_hash: None,
                }, true))
            }
            Err(e) => Err(e.into()),
        }
    }

    /// Close a round by setting its end_template_id and new status.
    /// After closing, the round is no longer returned by `get_or_create_current_round`.
    pub fn close_round(&self, id: i64, end_template_id: i64, status: &str) -> Result<()> {
        let conn = self.conn.lock();
        let mut stmt = conn.prepare(
            "UPDATE rounds SET end_template_id = ?1, status = ?2 WHERE id = ?3"
        )?;
        stmt.execute(params![end_template_id, status, id])?;
        Ok(())
    }

    /// Resolve which round a template belongs to.
    ///
    /// If the template's ID falls within an existing round's range (between start_template_id
    /// and end_template_id, inclusive), returns that round. If no round covers this template,
    /// creates a new open round with this template_id as the start.
    pub fn resolve_round_for_template(&self, template_id: i64) -> Result<(Round, bool)> {
        let conn = self.conn.lock();

        // First try: template matches an open round's range (start <= template_id <= end or open)
        let matching = conn.query_row(
            "SELECT id, start_template_id, end_template_id, status, found_block_hash 
             FROM rounds 
             WHERE (status = 'open' AND start_template_id <= ?1)
                OR (status != 'open' AND start_template_id <= ?1 
                    AND (end_template_id IS NULL OR end_template_id >= ?1))
             ORDER BY start_template_id DESC LIMIT 1",
            [template_id],
            |row| {
                Ok(Round {
                    id: row.get(0)?,
                    start_template_id: row.get(1)?,
                    end_template_id: row.get(2)?,
                    status: row.get(3)?,
                    found_block_hash: row.get(4)?,
                })
            },
        );

        match matching {
            Ok(round) => Ok((round, false)),
            Err(rusqlite::Error::QueryReturnedNoRows) => {
                // No round covers this template — create a new open round
                // Release lock before recursive-like call
                std::mem::drop(conn);
                self.get_or_create_current_round(template_id)
            }
            Err(e) => Err(e.into()),
        }
    }

    /// Get a round by its ID.
    pub fn get_by_id(&self, id: i64) -> Result<Option<Round>> {
        let conn = self.conn.lock();
        let mut stmt = conn.prepare(
            "SELECT id, start_template_id, end_template_id, status, found_block_hash 
             FROM rounds WHERE id = ?1"
        )?;
        let round = stmt.query_row([id], |row| {
            Ok(Round {
                id: row.get(0)?,
                start_template_id: row.get(1)?,
                end_template_id: row.get(2)?,
                status: row.get(3)?,
                found_block_hash: row.get(4)?,
            })
        });
        match round {
            Ok(r) => Ok(Some(r)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// List rounds, optionally filtered by status.
    pub fn list(&self, status_filter: Option<&str>) -> Result<Vec<Round>> {
        let conn = self.conn.lock();
        let (sql, params): (String, Vec<Box<dyn rusqlite::types::ToSql>>) = if let Some(status) = status_filter {
            (
                "SELECT id, start_template_id, end_template_id, status, found_block_hash 
                 FROM rounds WHERE status = ?1 ORDER BY id DESC".to_string(),
                vec![Box::new(status.to_string())],
            )
        } else {
            (
                "SELECT id, start_template_id, end_template_id, status, found_block_hash 
                 FROM rounds ORDER BY id DESC".to_string(),
                vec![],
            )
        };

        let mut stmt = conn.prepare(&sql)?;
        let param_refs: Vec<&dyn rusqlite::types::ToSql> = params.iter().map(|p| p.as_ref()).collect();
        let rows = stmt.query_map(param_refs.as_slice(), |row| {
            Ok(Round {
                id: row.get(0)?,
                start_template_id: row.get(1)?,
                end_template_id: row.get(2)?,
                status: row.get(3)?,
                found_block_hash: row.get(4)?,
            })
        })?;

        let mut rounds = Vec::new();
        for row in rows {
            rounds.push(row?);
        }
        Ok(rounds)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::accounting::schema::init_schema;
    use tempfile::NamedTempFile;

    #[test]
    fn test_get_or_create_current_round_creates_new_round_when_none_exists() {
        let f = NamedTempFile::new().unwrap();
        let conn = Connection::open(f.path()).unwrap();
        init_schema(&conn).unwrap();
        let repo = RoundRepository::new(Arc::new(Mutex::new(conn)));

        let (round, is_new) = repo.get_or_create_current_round(42).unwrap();

        assert!(is_new, "a new round should be created");
        assert_eq!(round.status, "open");
        assert_eq!(round.start_template_id, 42);
        assert_eq!(round.id, 1);
        assert!(round.end_template_id.is_none());
        assert!(round.found_block_hash.is_none());
    }

    #[test]
    fn test_get_or_create_current_round_returns_existing_open_round() {
        let f = NamedTempFile::new().unwrap();
        let conn = Connection::open(f.path()).unwrap();
        init_schema(&conn).unwrap();
        let repo = RoundRepository::new(Arc::new(Mutex::new(conn)));

        let (round1, _) = repo.get_or_create_current_round(42).unwrap();
        let (round2, is_new) = repo.get_or_create_current_round(99).unwrap();

        assert!(!is_new, "second call should return existing round");
        // Same round returned, start_template_id from the first call
        assert_eq!(round1.id, round2.id);
        assert_eq!(round2.start_template_id, 42);
        assert_eq!(round2.status, "open");
    }

    #[test]
    fn test_close_round_marks_round_as_closed() {
        let f = NamedTempFile::new().unwrap();
        let conn = Connection::open(f.path()).unwrap();
        init_schema(&conn).unwrap();
        let repo = RoundRepository::new(Arc::new(Mutex::new(conn)));

        let (round, _) = repo.get_or_create_current_round(42).unwrap();
        repo.close_round(round.id, 99, "found").unwrap();

        let fetched = repo.get_by_id(round.id).unwrap().unwrap();
        assert_eq!(fetched.status, "found");
        assert_eq!(fetched.end_template_id, Some(99));

        // A new open round is created when none exists
        let (new_round, is_new) = repo.get_or_create_current_round(100).unwrap();
        assert!(is_new, "a new round should be created when none exists");
        assert_eq!(new_round.start_template_id, 100);
        assert_ne!(new_round.id, round.id);
    }

    #[test]
    fn test_resolve_round_for_template_returns_open_round() {
        let f = NamedTempFile::new().unwrap();
        let conn = Connection::open(f.path()).unwrap();
        init_schema(&conn).unwrap();
        let repo = RoundRepository::new(Arc::new(Mutex::new(conn)));

        // Create an open round for template 42
        let _ = repo.get_or_create_current_round(42).unwrap();

        // Resolving for any template >= 42 should return the open round
        let (resolved, _is_new) = repo.resolve_round_for_template(55).unwrap();
        assert_eq!(resolved.status, "open");
    }

    #[test]
    fn test_resolve_round_for_template_creates_new_round() {
        let f = NamedTempFile::new().unwrap();
        let conn = Connection::open(f.path()).unwrap();
        init_schema(&conn).unwrap();
        let repo = RoundRepository::new(Arc::new(Mutex::new(conn)));

        // No rounds exist — resolving should create one
        let (resolved, is_new) = repo.resolve_round_for_template(42).unwrap();
        assert!(is_new, "a new round should be created");
        assert_eq!(resolved.start_template_id, 42);
        assert_eq!(resolved.status, "open");
    }

    #[test]
    fn test_get_by_id_returns_none_for_missing() {
        let f = NamedTempFile::new().unwrap();
        let conn = Connection::open(f.path()).unwrap();
        init_schema(&conn).unwrap();
        let repo = RoundRepository::new(Arc::new(Mutex::new(conn)));

        let result = repo.get_by_id(999).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_list_rounds() {
        let f = NamedTempFile::new().unwrap();
        let conn = Connection::open(f.path()).unwrap();
        init_schema(&conn).unwrap();
        let repo = RoundRepository::new(Arc::new(Mutex::new(conn)));

        // No rounds yet
        assert!(repo.list(None).unwrap().is_empty());

        // Create one round
        let _ = repo.get_or_create_current_round(1).unwrap();
        assert_eq!(repo.list(None).unwrap().len(), 1);

        // Create another after closing the first
        let (first, _) = repo.get_or_create_current_round(1).unwrap();
        repo.close_round(first.id, 10, "found").unwrap();
        let _ = repo.get_or_create_current_round(11).unwrap();
        assert_eq!(repo.list(None).unwrap().len(), 2);
    }

    #[test]
    fn test_list_rounds_by_status() {
        let f = NamedTempFile::new().unwrap();
        let conn = Connection::open(f.path()).unwrap();
        init_schema(&conn).unwrap();
        let repo = RoundRepository::new(Arc::new(Mutex::new(conn)));

        let (r1, _) = repo.get_or_create_current_round(1).unwrap();
        repo.close_round(r1.id, 10, "found").unwrap();

        let (r2, _) = repo.get_or_create_current_round(11).unwrap();

        let open = repo.list(Some("open")).unwrap();
        assert_eq!(open.len(), 1);
        assert_eq!(open[0].id, r2.id);

        let found = repo.list(Some("found")).unwrap();
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].id, r1.id);
    }
}
