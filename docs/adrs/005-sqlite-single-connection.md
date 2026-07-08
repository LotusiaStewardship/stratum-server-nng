# ADR 005: SQLite Single Connection with Serialized Access

**Context:** Accounting  
**Date:** 2026-05-22  
**Status:** Accepted

## Decision

All database access uses a single `rusqlite::Connection` wrapped in `Arc<Mutex<Connection>>`. No connection pool (r2d2, deadpool). No read replicas or separate write connection.

## Rationale

SQLite's performance sweet spot is single-writer with WAL mode. Multiple concurrent writers (via connection pool) degrade to serialized writes anyway because SQLite uses a database-level write lock. Adding a connection pool introduces complexity (pool configuration, connection health checks, checkout timeouts) without throughput benefit for this workload — share submissions and API queries both access the same small set of tables. The single-connection model is simple, predictable, and eliminates an entire class of concurrency bugs (stale reads across connections, write serialization surprises).

## Considered Options

1. **Single connection, serialized via Mutex (chosen):** `Arc<Mutex<Connection>>`. Simple, predictable, no pool overhead.
2. **Connection pool (r2d2):** Multiple connections for concurrent reads. Would improve API query latency under heavy write load, but adds pool management complexity.
3. **Separate read/write connections:** Write connection for share persistence, read connection for API queries. Mitigates write-contention on reads but adds consistency concerns (read connection sees stale state between WAL checkpoints).

## Consequences

- Share persistence (write-heavy) blocks API queries (read-heavy) while the write lock is held. In practice share persistence is sub-millisecond for a single INSERT, so contention is negligible.
- WAL mode allows concurrent reads during writes at the SQLite level, but the `Mutex` serializes all access in the application layer. If profiling shows contention, the Mutex can be replaced with a read-write lock (`parking_lot::RwLock`) without changing the connection model.
- The `ShutdownCoordinator` performs a WAL checkpoint (`PRAGMA wal_checkpoint(TRUNCATE)`) during graceful shutdown to prevent WAL file growth across restarts.
