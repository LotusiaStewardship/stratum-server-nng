use anyhow::Result;
use rusqlite::Connection;

pub fn init_schema(conn: &Connection) -> Result<()> {
    // Enable foreign keys
    conn.execute_batch("PRAGMA foreign_keys = ON;")?;
    
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

        -- shares table
        CREATE TABLE IF NOT EXISTS shares (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            worker_id INTEGER NOT NULL,
            session_id TEXT NOT NULL,
            job_id TEXT NOT NULL,
            extranonce2 TEXT NOT NULL,
            ntime_hex_6b TEXT NOT NULL,
            nonce_hex_8b TEXT NOT NULL,
            difficulty REAL NOT NULL,
            status TEXT NOT NULL,
            reject_reason TEXT,
            created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (worker_id) REFERENCES workers(id)
        );

        CREATE INDEX IF NOT EXISTS idx_shares_worker_id ON shares(worker_id);
        CREATE INDEX IF NOT EXISTS idx_shares_created_at ON shares(created_at);
        ",
    )?;
    Ok(())
}
