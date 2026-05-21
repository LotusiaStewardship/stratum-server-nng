use anyhow::Result;
use rusqlite::{Connection, params};
use std::sync::Arc;
use parking_lot::Mutex;

/// A share entry within the PPLNS payout window.
/// Returned by `calculate_pplns_window` and used by `plan::build_payout_plan`.
#[derive(Debug, Clone)]
pub struct PplnsShareEntry {
    pub share_id: i64,
    pub share_outcome_id: i64,
    pub worker_id: i64,
    pub payout_address: String,
    pub difficulty: f64,
    pub created_at: String,
}

/// Calculate the PPLNS window for a found block.
///
/// Window ends at `found_at` (share_outcomes.created_at for the block-finding share).
/// Extends backward until cumulative work_units >= n_multiplier × network_difficulty.
///
/// Returns shares in the window, ordered by created_at DESC.
/// If the window is shorter than the threshold (not enough shares), all available
/// shares are returned.
///
/// Excludes shares from orphaned rounds per UBQ invariant.
pub fn calculate_pplns_window(
    conn: &Connection,
    found_at: &str,
    n_multiplier: f64,
    network_difficulty: f64,
) -> Result<Vec<PplnsShareEntry>> {
    let sql = "
        SELECT so.share_id, so.id AS share_outcome_id, so.worker_id,
               w.payout_address, s.difficulty, so.created_at
        FROM share_outcomes so
        JOIN shares s ON s.id = so.share_id
        JOIN workers w ON w.id = so.worker_id
        JOIN rounds r ON r.id = so.round_id
        WHERE so.status = 'accepted'
          AND r.status != 'orphaned'
          AND so.created_at <= ?1
        ORDER BY so.created_at DESC
    ";
    let mut stmt = conn.prepare(sql)?;
    let rows = stmt.query_map(params![found_at], |row| {
        Ok(PplnsShareEntry {
            share_id: row.get(0)?,
            share_outcome_id: row.get(1)?,
            worker_id: row.get(2)?,
            payout_address: row.get(3)?,
            difficulty: row.get(4)?,
            created_at: row.get(5)?,
        })
    })?;

    let all_shares: Vec<PplnsShareEntry> = rows.collect::<std::result::Result<_, _>>()?;

    let threshold = n_multiplier * network_difficulty as f64;
    let mut cumulative = 0.0f64;

    // Find the cutoff: accumulate difficulty until we hit the threshold
    let cutoff_index = all_shares.iter().position(|entry| {
        cumulative += entry.difficulty;
        cumulative >= threshold
    });

    match cutoff_index {
        Some(idx) => Ok(all_shares[..=idx].to_vec()),
        None => Ok(all_shares), // Not enough shares to fill window — return all
    }
}

/// Calculate the PPLNS window for a found block, using an Arc<Mutex<Connection>>.
/// Convenience wrapper for use in AccountingService.
pub fn calculate_pplns_window_arc(
    conn: &Arc<Mutex<Connection>>,
    found_at: &str,
    n_multiplier: f64,
    network_difficulty: f64,
) -> Result<Vec<PplnsShareEntry>> {
    let conn = conn.lock();
    calculate_pplns_window(&conn, found_at, n_multiplier, network_difficulty)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::accounting::schema::init_schema;
    use tempfile::NamedTempFile;

    fn setup_shares(conn: &Connection) {
        // Create workers and rounds first
        conn.execute_batch(
            "INSERT INTO workers (id, payout_address) VALUES (1, 'addr1');
             INSERT INTO workers (id, payout_address) VALUES (2, 'addr2');
             INSERT INTO rounds (id, start_template_id, status) VALUES (1, 100, 'open');
             INSERT INTO rounds (id, start_template_id, status) VALUES (2, 200, 'orphaned');"
        ).unwrap();

        // Shares and outcomes with explicit created_at for deterministic order
        // Share 3 (latest, diff 4.0)
        conn.execute(
            "INSERT INTO shares (id, worker_id, session_id, job_id, template_id, template_epoch,
                                 extranonce1, extranonce2, ntime_hex_6b, nonce_hex_8b, difficulty, dedupe_key)
             VALUES (3, 2, 's2', 'j3', 100, 3, 'e1', 'en2', 'ntime', 'nonce', 4.0, 'dk3')",
            [],
        ).unwrap();
        conn.execute(
            "INSERT INTO share_outcomes (id, share_id, session_id, worker_id, job_id, round_id, dedupe_key,
                                         status, low_diff_ok, network_target_ok, created_at)
             VALUES (3, 3, 's2', 2, 'j3', 1, 'dk3', 'accepted', 1, 0, '2026-05-20T12:00:03')",
            [],
        ).unwrap();

        // Share 2 (middle, diff 2.0)
        conn.execute(
            "INSERT INTO shares (id, worker_id, session_id, job_id, template_id, template_epoch,
                                 extranonce1, extranonce2, ntime_hex_6b, nonce_hex_8b, difficulty, dedupe_key)
             VALUES (2, 1, 's1', 'j2', 100, 2, 'e1', 'en2', 'ntime', 'nonce', 2.0, 'dk2')",
            [],
        ).unwrap();
        conn.execute(
            "INSERT INTO share_outcomes (id, share_id, session_id, worker_id, job_id, round_id, dedupe_key,
                                         status, low_diff_ok, network_target_ok, created_at)
             VALUES (2, 2, 's1', 1, 'j2', 1, 'dk2', 'accepted', 1, 0, '2026-05-20T12:00:02')",
            [],
        ).unwrap();

        // Share 1 (oldest, diff 1.5)
        conn.execute(
            "INSERT INTO shares (id, worker_id, session_id, job_id, template_id, template_epoch,
                                 extranonce1, extranonce2, ntime_hex_6b, nonce_hex_8b, difficulty, dedupe_key)
             VALUES (1, 1, 's1', 'j1', 100, 1, 'e1', 'en2', 'ntime', 'nonce', 1.5, 'dk1')",
            [],
        ).unwrap();
        conn.execute(
            "INSERT INTO share_outcomes (id, share_id, session_id, worker_id, job_id, round_id, dedupe_key,
                                         status, low_diff_ok, network_target_ok, created_at)
             VALUES (1, 1, 's1', 1, 'j1', 1, 'dk1', 'accepted', 1, 0, '2026-05-20T12:00:01')",
            [],
        ).unwrap();

        // Orphaned round share (should be excluded)
        conn.execute(
            "INSERT INTO shares (id, worker_id, session_id, job_id, template_id, template_epoch,
                                 extranonce1, extranonce2, ntime_hex_6b, nonce_hex_8b, difficulty, dedupe_key)
             VALUES (4, 1, 's3', 'j4', 200, 4, 'e1', 'en2', 'ntime', 'nonce', 999.0, 'dk4')",
            [],
        ).unwrap();
        conn.execute(
            "INSERT INTO share_outcomes (id, share_id, session_id, worker_id, job_id, round_id, dedupe_key,
                                         status, low_diff_ok, network_target_ok, created_at)
             VALUES (4, 4, 's3', 1, 'j4', 2, 'dk4', 'accepted', 1, 0, '2026-05-20T12:00:04')",
            [],
        ).unwrap();
    }

    #[test]
    fn test_window_selects_shares_up_to_threshold() {
        let f = NamedTempFile::new().unwrap();
        let conn = Connection::open(f.path()).unwrap();
        init_schema(&conn).unwrap();
        setup_shares(&conn);

        // Shares: [4.0 (w2), 2.0 (w1), 1.5 (w1)] in DESC order
        // Threshold = 3.0 * 2.0 = 6.0
        // Cumulative: 4.0 + 2.0 = 6.0 >= 6.0 => cutoff after share 2 (index 0 and 1)
        let result = calculate_pplns_window(&conn, "9999-12-31", 3.0, 2.0).unwrap();
        assert_eq!(result.len(), 2, "should include exactly 2 shares to meet threshold");
        assert!(result[0].difficulty >= 2.0, "first share should be highest difficulty");
    }

    #[test]
    fn test_window_returns_all_shares_when_insufficient() {
        let f = NamedTempFile::new().unwrap();
        let conn = Connection::open(f.path()).unwrap();
        init_schema(&conn).unwrap();
        setup_shares(&conn);

        // Very high threshold that no amount of shares can meet
        let result = calculate_pplns_window(&conn, "9999-12-31", 9999.0, 1.0).unwrap();
        assert_eq!(result.len(), 3, "should return all 3 non-orphaned shares");
    }

    #[test]
    fn test_window_excludes_orphaned_round_shares() {
        let f = NamedTempFile::new().unwrap();
        let conn = Connection::open(f.path()).unwrap();
        init_schema(&conn).unwrap();
        setup_shares(&conn);

        // Threshold = 1.0 (small) — should pick up only non-orphaned shares
        let result = calculate_pplns_window(&conn, "9999-12-31", 0.5, 1.0).unwrap();
        // Only the first few shares up to threshold, none from orphaned round
        for entry in &result {
            // All returned shares should have reasonable difficulties (not 999.0)
            assert!(entry.difficulty < 100.0, "orphaned round share should be excluded");
        }
    }

    #[test]
    fn test_window_respects_found_at_timestamp() {
        let f = NamedTempFile::new().unwrap();
        let conn = Connection::open(f.path()).unwrap();
        init_schema(&conn).unwrap();
        setup_shares(&conn);

        // found_at before any shares -> empty result
        let result = calculate_pplns_window(&conn, "2000-01-01", 1.0, 1.0).unwrap();
        assert!(result.is_empty(), "no shares before 2000");
    }

    #[test]
    fn test_window_aggregates_difficulty_correctly() {
        let f = NamedTempFile::new().unwrap();
        let conn = Connection::open(f.path()).unwrap();
        init_schema(&conn).unwrap();
        setup_shares(&conn);

        // Threshold = 5.5 -> cumulative: 4.0 + 2.0 = 6.0 >= 5.5 => first 2 shares
        let result = calculate_pplns_window(&conn, "9999-12-31", 2.75, 2.0).unwrap();
        assert_eq!(result.len(), 2, "threshold=5.5 should stop after share with diff=2.0");
        let cum: f64 = result.iter().map(|e| e.difficulty).sum();
        assert!(cum >= 5.5, "cumulative difficulty should meet threshold");
    }
}
