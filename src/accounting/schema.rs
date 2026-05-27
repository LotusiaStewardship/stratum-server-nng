use anyhow::Result;
use rusqlite::Connection;

pub fn init_schema(conn: &Connection) -> Result<()> {
    // Enable foreign keys and WAL journal mode
    conn.execute_batch("PRAGMA foreign_keys = ON;")?;
    conn.execute_batch("PRAGMA journal_mode = WAL;")?;

    conn.execute_batch(
        "
        -- workers table
        CREATE TABLE IF NOT EXISTS workers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            payout_address TEXT NOT NULL,
            worker_suffix TEXT,
            created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(payout_address, worker_suffix)
        );

        -- authorization_events table (immutable audit log per UBQ)
        -- Every mining.authorize attempt produces one record, regardless of outcome.
        CREATE TABLE IF NOT EXISTS authorization_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id TEXT NOT NULL,
            worker_name TEXT NOT NULL,
            payout_address TEXT NOT NULL,
            worker_suffix TEXT,
            authorized INTEGER NOT NULL,  -- 1=true, 0=false
            reason TEXT,
            created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
        );

        CREATE INDEX IF NOT EXISTS idx_auth_events_session ON authorization_events(session_id);
        CREATE INDEX IF NOT EXISTS idx_auth_events_address ON authorization_events(payout_address);

        -- shares table (raw submission record, immutable per UBQ)
        -- Captures the fact that a miner submitted work.
        -- Status and validation details live in share_outcomes.
        CREATE TABLE IF NOT EXISTS shares (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            worker_id INTEGER NOT NULL,
            session_id TEXT NOT NULL,
            job_id TEXT NOT NULL,
            template_id INTEGER NOT NULL,
            template_epoch INTEGER NOT NULL,
            extranonce1 TEXT NOT NULL,
            extranonce2 TEXT NOT NULL,
            ntime_hex_6b TEXT NOT NULL,
            nonce_hex_8b TEXT NOT NULL,
            difficulty REAL NOT NULL,      -- P_diff at assignment time (immutable)
            dedupe_key TEXT NOT NULL UNIQUE,
            created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (worker_id) REFERENCES workers(id)
        );

        CREATE INDEX IF NOT EXISTS idx_shares_worker_id ON shares(worker_id);
        CREATE INDEX IF NOT EXISTS idx_shares_created_at ON shares(created_at);
        CREATE INDEX IF NOT EXISTS idx_shares_dedupe_key ON shares(dedupe_key);

        -- share_outcomes table (validation pipeline result per UBQ)
        -- Captures the full validation result linked to the raw share.
        CREATE TABLE IF NOT EXISTS share_outcomes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            share_id INTEGER NOT NULL,
            session_id TEXT NOT NULL,
            worker_id INTEGER NOT NULL,
            job_id TEXT NOT NULL,
            round_id INTEGER,          -- resolved at insert time (NULL until Slice 5)
            dedupe_key TEXT NOT NULL UNIQUE,
            status TEXT NOT NULL,       -- 'accepted', 'rejected', 'stale'
            reject_reason TEXT,         -- NULL when accepted; one of 'stale-job',
                                        -- 'invalid-submit-shape', 'low-difficulty-share',
                                        -- 'ntime-mismatch', 'unauthorized-worker'
            node_result TEXT,           -- lotusd submission result (NULL for rejected shares)
            low_diff_ok INTEGER,       -- 1 if hash met P_diff target, 0 otherwise
            network_target_ok INTEGER, -- 1 if hash met N_diff target, 0 otherwise (high-hash share)
            block_hash TEXT,            -- non-NULL if share found a block candidate
            created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (share_id) REFERENCES shares(id),
            FOREIGN KEY (worker_id) REFERENCES workers(id)
        );

        CREATE INDEX IF NOT EXISTS idx_share_outcomes_share_id ON share_outcomes(share_id);
        CREATE INDEX IF NOT EXISTS idx_share_outcomes_worker_id ON share_outcomes(worker_id);
        CREATE INDEX IF NOT EXISTS idx_share_outcomes_dedupe_key ON share_outcomes(dedupe_key);
        CREATE INDEX IF NOT EXISTS idx_share_outcomes_status ON share_outcomes(status);
        CREATE INDEX IF NOT EXISTS idx_share_outcomes_round_id ON share_outcomes(round_id);

        -- rounds table (payout round tracking)
        CREATE TABLE IF NOT EXISTS rounds (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            start_template_id INTEGER NOT NULL,
            end_template_id INTEGER,
            status TEXT NOT NULL DEFAULT 'open',  -- 'open', 'found', 'closed', 'paid', 'orphaned'
            found_block_hash TEXT,
            created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
        );

        -- accounting_events table (append-only audit log per UBQ)
        CREATE TABLE IF NOT EXISTS accounting_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_type TEXT NOT NULL,
            status TEXT NOT NULL,
            session_id TEXT,
            worker_id INTEGER,
            worker_name TEXT,
            payout_address TEXT,
            round_id INTEGER,
            template_id INTEGER,
            template_epoch INTEGER,
            job_id TEXT,
            block_hash TEXT,
            height INTEGER,
            payload_json TEXT,
            created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
        );

        CREATE INDEX IF NOT EXISTS idx_accounting_events_type ON accounting_events(event_type);
        CREATE INDEX IF NOT EXISTS idx_accounting_events_created ON accounting_events(created_at);

        -- found_blocks table
        CREATE TABLE IF NOT EXISTS found_blocks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            round_id INTEGER NOT NULL,
            block_hash TEXT NOT NULL UNIQUE,
            height INTEGER NOT NULL,
            status TEXT NOT NULL DEFAULT 'immature',
            worker_id INTEGER,
            template_id INTEGER,
            extranonce1 TEXT,
            persist_source TEXT,
            orphan_reason TEXT,
            matured_at DATETIME,
            coinbase_value INTEGER NOT NULL DEFAULT 0,
            coinbase_txid TEXT,
            network_target_hex TEXT NOT NULL DEFAULT '',
            created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (round_id) REFERENCES rounds(id),
            FOREIGN KEY (worker_id) REFERENCES workers(id)
        );

        CREATE INDEX IF NOT EXISTS idx_found_blocks_status ON found_blocks(status);
        CREATE INDEX IF NOT EXISTS idx_found_blocks_hash ON found_blocks(block_hash);

        -- payout_batches table (per UBQ §Payout Batch)
        CREATE TABLE IF NOT EXISTS payout_batches (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            round_id INTEGER NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending',  -- 'pending', 'submitted', 'confirmed', 'failed'
            total_amount INTEGER NOT NULL,
            pool_fee_amount INTEGER NOT NULL,
            pool_fee_address TEXT,
            miner_count INTEGER NOT NULL,
            retry_key TEXT UNIQUE,
            last_error TEXT,
            next_retry_at DATETIME,
            attempt_count INTEGER DEFAULT 0,
            signed_payload_ref TEXT,
            submitted_txid TEXT,
            created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (round_id) REFERENCES rounds(id)
        );

        CREATE INDEX IF NOT EXISTS idx_payout_batches_round ON payout_batches(round_id);
        CREATE INDEX IF NOT EXISTS idx_payout_batches_status ON payout_batches(status);

        -- payouts table (individual miner payment within a batch)
        CREATE TABLE IF NOT EXISTS payouts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            batch_id INTEGER NOT NULL,
            worker_id INTEGER NOT NULL,
            payout_address TEXT NOT NULL,
            amount INTEGER NOT NULL,
            dust_carried_forward INTEGER DEFAULT 0,
            created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (batch_id) REFERENCES payout_batches(id),
            FOREIGN KEY (worker_id) REFERENCES workers(id)
        );

        CREATE INDEX IF NOT EXISTS idx_payouts_batch ON payouts(batch_id);
        CREATE INDEX IF NOT EXISTS idx_payouts_address ON payouts(payout_address);

        -- payout_share_snapshots table (auditable record of which shares were paid)
        CREATE TABLE IF NOT EXISTS payout_share_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            batch_id INTEGER NOT NULL,
            share_id INTEGER NOT NULL,
            share_outcome_id INTEGER NOT NULL,
            payout_address TEXT NOT NULL,
            work_units REAL NOT NULL,
            share_created_at DATETIME NOT NULL,
            created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (batch_id) REFERENCES payout_batches(id),
            FOREIGN KEY (share_id) REFERENCES shares(id),
            FOREIGN KEY (share_outcome_id) REFERENCES share_outcomes(id)
        );

        CREATE INDEX IF NOT EXISTS idx_snapshots_batch ON payout_share_snapshots(batch_id);

        -- dust_balances table (per-address dust carry-forward tracking)
        CREATE TABLE IF NOT EXISTS dust_balances (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            payout_address TEXT NOT NULL UNIQUE,
            balance INTEGER NOT NULL DEFAULT 0,
            updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        ",
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    /// Schema initialization must be safe to call multiple times on the same database.
    /// This contract exists because the server may be restarted against an existing DB file.
    #[test]
    fn test_init_schema_idempotent() {
        let f = NamedTempFile::new().unwrap();
        let conn = Connection::open(f.path()).unwrap();
        init_schema(&conn).unwrap();
        init_schema(&conn).unwrap();
    }
}
