# Modular Architecture Refactor - Vertical Slices

**Source:** [modular-architecture-refactor.md](./modular-architecture-refactor.md)  
**Status:** Ready for implementation  
**Last updated:** 2026-05-18

---

## Slice Overview

| # | Title | Type | Blocked By | Estimated Effort | Status |
|---|-------|------|------------|------------------|--------|
| 1 | Minimal Stratum Server (Tracer Bullet) | AFK | None | 1 day | ✅ Done |
| 2 | NNG Template Integration | AFK | #1 | 1 day | ✅ Done |
| 3 | Share Validation Pipeline | AFK | #2 | 1 day |
| 4 | Per-Session VarDiff | AFK | #3 | 1 day |
| 5 | Worker and Round Accounting | AFK | #3 | 1 day |
| 6 | Found Blocks and Reorg Handling | AFK | #5 | 1 day |
| 7 | PPLNS Payout Calculation | AFK | #6 | 2 days |
| 8 | Complete HTTP API | AFK | #5 | 1 day |
| 9 | Payout Signer Abstraction | HITL (needs key config) | #7 | 0.5 days |

---

## Slice 1: Minimal Stratum Server (Tracer Bullet)

### What to build

A minimal end-to-end Stratum V1 server that accepts TCP connections, handles subscribe/authorize, distributes a **static** mining job (hardcoded template), records share submissions, and exposes a health check via HTTP API. Includes **graceful shutdown** handling for SIGINT/SIGTERM.

This slice validates the core architecture: TCP server → protocol parsing → session management → share persistence → HTTP API → shutdown lifecycle.

### Acceptance criteria

- [ ] TCP server listens on configured bind address (default `0.0.0.0:3334`)
- [ ] Miner can connect and send `mining.subscribe`, receives proper response with session extranonce1
- [ ] Miner can send `mining.authorize` with valid Lotus address, receives `true` response
- [ ] After authorize, miner receives `mining.notify` with static job (hardcoded prevhash, coinbase, etc.)
- [ ] Miner can submit share via `mining.submit`, server responds with `true` (accepted) or `false` (rejected)
- [ ] Share is persisted to SQLite `shares` table with status (accepted/rejected)
- [ ] HTTP server listens on configured bind address (default `127.0.0.1:18080`)
- [ ] `GET /api/v1/health` returns server status (uptime, connected miner count)
- [ ] `GET /api/v1/stats` returns basic stats (total shares, accepted/rejected counts)
- [ ] **Graceful shutdown on SIGINT/SIGTERM:**
  - [ ] Server stops accepting new connections within 1s of signal
  - [ ] Active sessions notified and closed gracefully
  - [ ] In-flight share validations complete (max 5s timeout)
  - [ ] All pending shares flushed to database
  - [ ] SQLite connection closed cleanly (WAL checkpoint)
  - [ ] Process exits within 30s total
- [ ] **Emergency shutdown on SIGQUIT:**
  - [ ] Immediate exit (no flush, for corrupted state recovery)
- [ ] Unit tests for protocol parsing (valid/invalid requests)
- [ ] Integration test: full subscribe → authorize → submit flow
- [ ] Integration test: graceful shutdown with in-flight shares (verify persistence)

### Testing scope

**Test:**
- Protocol parsing (valid JSON, invalid JSON, unknown methods)
- Session state machine (subscribe required before authorize)
- Share persistence (insert, query by worker)
- HTTP API endpoints (health, stats)

**Don't test:**
- TCP connection handling (too trivial)
- Static job data (will be replaced by NNG in next slice)

### Module structure

```
src/
├── main.rs                 # Entry point, signal handling, shutdown orchestration
├── config.rs               # Configuration loading
├── shutdown.rs             # Graceful shutdown coordinator (broadcast channel, timeouts)
├── stratum_protocol/
│   ├── mod.rs
│   ├── protocol.rs         # StratumRequest, StratumResponse, Method enum
│   ├── session.rs          # SessionState (extranonce1, authorized_workers, active_jobs)
│   └── server.rs           # TCP listener, handle_conn, shutdown-aware
├── share_processing/
│   ├── mod.rs
│   └── persistence.rs      # Share insert (calls accounting facade)
├── accounting/
│   ├── mod.rs              # AccountingService facade
│   ├── share_repository.rs # Share insert/query
│   └── schema.rs           # CREATE TABLE shares, workers
└── http_api/
    ├── mod.rs
    ├── server.rs           # Axum HTTP server, shutdown-aware
    └── routes/
        ├── health.rs       # GET /api/v1/health
        └── stats.rs        # GET /api/v1/stats
```

### Shutdown coordinator

```rust
// shutdown.rs
pub struct ShutdownCoordinator {
    shutdown_tx: broadcast::Sender<()>,
    shutdown_timeout_secs: u64,  // default 30
    flush_timeout_secs: u64,     // default 5
}

impl ShutdownCoordinator {
    pub fn new() -> Self;
    pub fn signal(&self) -> ShutdownSignal;  // Cloneable handle for tasks
    pub fn initiate_shutdown(&self) -> Result<()>;  // Called from main on SIGINT/SIGTERM
    pub fn wait_for_completion(&self) -> Result<()>;  // Wait for all tasks to exit
}

pub struct ShutdownSignal {
    pub fn recv(&self) -> impl Future<Output = ()>;  // Wait for shutdown signal
    pub fn is_shutdown(&self) -> bool;  // Check if shutdown initiated
}
```

**Usage pattern:**

```rust
// In main.rs
let shutdown = ShutdownCoordinator::new();
let shutdown_signal = shutdown.signal();

// Spawn tasks with shutdown signal
tokio::spawn(async move {
    tokio::select! {
        _ = stratum_server.run() => {},
        _ = shutdown_signal.recv() => {
            // Cleanup: stop accepting, close connections
        }
    }
});

// On SIGINT/SIGTERM
shutdown.initiate_shutdown();
shutdown.wait_for_completion()?;  // Max 30s timeout
```

### Database schema

```sql
-- workers table
CREATE TABLE workers (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    payout_address TEXT NOT NULL,
    worker_suffix TEXT,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(payout_address, worker_suffix)
);

-- shares table
CREATE TABLE shares (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    worker_id INTEGER NOT NULL,
    session_id TEXT NOT NULL,
    job_id TEXT NOT NULL,
    extranonce2 TEXT NOT NULL,
    ntime_hex_6b TEXT NOT NULL,
    nonce_hex_8b TEXT NOT NULL,
    difficulty REAL NOT NULL,
    status TEXT NOT NULL,  -- 'accepted', 'rejected', 'stale', 'low-difficulty'
    reject_reason TEXT,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (worker_id) REFERENCES workers(id)
);

CREATE INDEX idx_shares_worker_id ON shares(worker_id);
CREATE INDEX idx_shares_created_at ON shares(created_at);
```

### Notes

- **Static job:** Hardcode a minimal job with fake prevhash, coinbase, etc. This is temporary — Slice 2 replaces with NNG template.
- **No validation yet:** Accept all shares as valid (status='accepted'). Slice 3 adds validation.
- **No difficulty tracking yet:** Use fixed difficulty=1.0 for all shares. Slice 4 adds VarDiff.
- **No rounds yet:** Shares don't have round_id. Slice 5 adds round accounting.
- **Graceful shutdown:** Implemented in Slice 1 as foundational infrastructure. All subsequent slices must respect shutdown signals (use `shutdown_signal.recv()` in long-running tasks).

---

## Slice 2: NNG Template Integration

**Status:** ✅ Completed 2026-05-18

### What to build

Connect to lotusd via NNG, fetch real mining templates on startup, and distribute them to miners via `mining.notify`. Replace static job with dynamic template from node.

### Acceptance criteria

- [x] NNG RPC client connects to configured `nng_rpc_url`
- [x] On startup, fetch `MiningTemplate` via NNG RPC `get_mining_template`
- [x] Template is converted to `MiningJob` and cached in memory
- [x] After authorize, miner receives `mining.notify` with real template data
- [x] Job cache supports multiple jobs (LRU eviction, max 512 jobs)
- [x] `GET /api/v1/stats` includes network difficulty from template
- [x] Unit tests for template → job conversion
- [x] Integration test: fetch template from lotusd (mocked or real node)

### Testing scope

**Test:**
- NNG RPC client (template fetch)
- Template → job conversion (coinbase handling, merkle branches)
- Job cache (insert, query, LRU eviction)

**Don't test:**
- NNG pub/sub events (Slice 6 adds event-driven refresh)
- Template validation (assume lotusd provides valid templates)

### Module structure

```
src/
├── node_integration/
│   ├── mod.rs
│   ├── nng/
│   │   ├── mod.rs
│   │   └── rpc_client.rs   # NNG RPC calls (get_mining_template)
│   └── template.rs         # MiningJob conversion from MiningTemplate
└── stratum_protocol/
    └── job.rs              # MiningJob struct, notify_params()
```

### Notes

- **No event-driven refresh yet:** Template fetched only on startup. Slice 6 adds `miningwrkchg` pub/sub.
- **Job cache:** In-memory VecDeque with LRU eviction (max 512 jobs).
- **Template epoch:** Start with epoch=1, increment on each fetch.
- **Shutdown handling:** NNG RPC client must close connections on shutdown signal (register for shutdown in Slice 2).

---

## Slice 3: Share Validation Pipeline

### What to build

Validate shares before accepting: check difficulty target, verify header hash, ensure job is active. Reject invalid shares with appropriate error codes.

### Acceptance criteria

- [ ] Share validator checks:
  - Job exists and is active (not stale)
  - extranonce2 format (correct length, hex)
  - ntime format (6 bytes, hex)
  - nonce format (8 bytes, hex)
  - Share meets session difficulty target (header hash ≤ target)
- [ ] Rejected shares recorded with `reject_reason`:
  - `stale-job` — job not in session's active set
  - `invalid-submit-shape` — malformed extranonce2/ntime/nonce
  - `low-difficulty-share` — hash doesn't meet difficulty target
- [ ] Share validation is synchronous (blocks response until complete)
- [ ] `GET /api/v1/stats` includes rejection breakdown (by reason)
- [ ] Unit tests for each validation rule
- [ ] Integration test: submit valid/invalid shares, verify responses

### Testing scope

**Test:**
- Difficulty validation (header hash vs target)
- Format validation (extranonce2, ntime, nonce)
- Job staleness detection
- Rejection reason assignment

**Don't test:**
- Cryptographic primitives (use bitcoinsuite)
- TCP error handling (already tested in Slice 1)

### Module structure

```
src/
├── share_processing/
│   ├── mod.rs
│   ├── validator.rs        # validate_share(Share, Job, Session) -> ValidationResult
│   └── difficulty.rs       # difficulty_to_target, header_meets_difficulty (or use bitcoinsuite)
└── stratum_protocol/
    └── server.rs           # Call validator before recording share
```

### Notes

- **Synchronous validation:** Required by Stratum V1 protocol (no "pending" state).
- **Use bitcoinsuite:** Leverage `bitcoinsuite-bitcoind-stratum::build_stratum_header` and `header_meets_difficulty`.
- **Accounting facade:** Validator calls `accounting.record_share(share, outcome)` — Option C from domain session.

---

## Slice 4: Per-Session VarDiff

### What to build

Implement per-session VarDiff controller. Each TCP connection has independent difficulty that retargets based on share rate. Target: 1 share per 20 seconds, retarget every 60 seconds.

### Acceptance criteria

- [ ] Each session has independent `VarDiff` instance
- [ ] VarDiff configuration from `config.toml`:
  - `vardiff_min_floor` — absolute minimum (default 0.001)
  - `vardiff_initial_pct` — initial as % of network diff (default 0.01)
  - `vardiff_target_secs` — target time between shares (default 20s)
  - `vardiff_retarget_secs` — retarget interval (default 60s)
- [ ] On session start, difficulty = `network_diff * vardiff_initial_pct`
- [ ] On each accepted share, record timestamp for rate calculation
- [ ] Every `vardiff_retarget_secs`, compute new difficulty:
  - If share rate too fast → increase difficulty (max 1.5× per retarget)
  - If share rate too slow → decrease difficulty (min 0.67× per retarget)
  - Clamp to `[vardiff_min_floor, network_diff]`
- [ ] Send `mining.set_difficulty` to session when difficulty changes
- [ ] Share difficulty recorded at submission time (immutable)
- [ ] Unit tests for VarDiff retarget logic
- [ ] Integration test: session with fast/slow shares, verify retargeting

### Testing scope

**Test:**
- Retarget calculation (ratio, clamping)
- Difficulty bounds (floor, ceiling)
- Share rate computation (timestamp window)

**Don't test:**
- TCP message formatting (already tested)
- Per-worker difficulty (VarDiff is per-session, not per-worker)

### Module structure

```
src/
├── share_processing/
│   └── difficulty.rs       # VarDiff struct, record_share(), maybe_retarget()
└── stratum_protocol/
    └── session.rs          # Session includes VarDiff instance
```

### Notes

- **Per-session, not per-worker:** VarDiff operates on TCP connection level (domain decision).
- **Network diff ceiling:** VarDiff max = current network difficulty from template.
- **Share rate target:** Default 20s between shares (industry standard).

---

## Slice 5: Worker and Round Accounting

### What to build

Implement worker persistence (across sessions) and round accounting. Workers are identified by `(payout_address, worker_suffix)`. Rounds track share accumulation periods.

### Acceptance criteria

- [ ] Worker repository:
  - `upsert_worker(payout_address, worker_suffix) -> Worker` — create or fetch
  - Worker has persistent ID across sessions
- [ ] Round repository:
  - `get_or_create_current_round() -> Round` — ensure one open round
  - Round has `start_template_id`, `status` (open/found/closed)
- [ ] Shares include `worker_id` and `round_id` foreign keys
- [ ] On share submission:
  - Worker upserted (if new)
  - Round fetched (current open round)
  - Share recorded with both IDs
- [ ] `GET /api/v1/workers` — list workers with share counts
- [ ] `GET /api/v1/workers/{id}` — worker details (shares, hashrate)
- [ ] `GET /api/v1/rounds` — list rounds with status
- [ ] Unit tests for worker/round repositories
- [ ] Integration test: multiple sessions, same worker, verify share aggregation

### Testing scope

**Test:**
- Worker upsert (create new, fetch existing)
- Round lifecycle (open → found → closed)
- Share attribution (worker_id, round_id)
- HTTP API queries (workers, rounds)

**Don't test:**
- PPLNS window calculation (Slice 7)
- Round closure on block found (Slice 6)

### Module structure

```
src/
├── accounting/
│   ├── mod.rs              # AccountingService facade
│   ├── worker_repository.rs # Worker upsert/query
│   ├── round_repository.rs  # Round get/create/close
│   ├── share_repository.rs  # Share insert with worker_id, round_id
│   └── schema.rs           # CREATE TABLE workers, rounds
└── http_api/
    └── routes/
        ├── workers.rs      # GET /api/v1/workers, /workers/{id}
        └── rounds.rs       # GET /api/v1/rounds
```

### Database schema additions

```sql
-- rounds table
CREATE TABLE rounds (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    start_template_id INTEGER NOT NULL,
    end_template_id INTEGER,
    status TEXT NOT NULL DEFAULT 'open',  -- 'open', 'found', 'closed', 'paid', 'orphaned'
    found_block_hash TEXT,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Add round_id to shares
ALTER TABLE shares ADD COLUMN round_id INTEGER;
ALTER TABLE shares ADD FOREIGN KEY (round_id) REFERENCES rounds(id);

CREATE INDEX idx_shares_round_id ON shares(round_id);
```

### Notes

- **Round assignment:** Shares assigned to current round at insert time (not backfilled).
- **Worker persistence:** Worker exists across sessions (domain invariant).
- **No found_block yet:** Rounds can be open/found/closed, but no found_block table yet (Slice 6).

---

## Slice 6: Found Blocks and Reorg Handling

### What to build

Track found blocks submitted to lotusd. Handle blockchain reorgs via NNG pub/sub events (`blkconnected`, `blkdisconctd`, `miningwrkchg`). Mark orphaned blocks and adjust accounting.

### Acceptance criteria

- [ ] Found block repository:
  - `record_found_block(round_id, block_hash, height, worker_id) -> FoundBlock`
  - `mark_orphaned(block_hash, reason) -> ()`
  - `get_by_hash(block_hash) -> FoundBlock`
- [ ] NNG pub/sub client subscribes to:
  - `miningwrkchg` — template refresh (with coalescing)
  - `blkconnected` — block connected (for tip tracking)
  - `blkdisconctd` — block disconnected (for orphan detection)
- [ ] On `blkdisconctd` event:
  - Check if disconnected block is in `found_blocks`
  - If yes, mark as orphaned with reason="reorg_detected"
  - Mark round as orphaned
  - Mark shares in round as orphaned (status='orphaned')
- [ ] On `miningwrkchg` event:
  - Fetch new template via NNG RPC
  - Broadcast `mining.notify` with `clean_jobs=true` to all sessions
  - Create new job in cache
- [ ] Event coalescing: 100ms debounce for `miningwrkchg` events
- [ ] `GET /api/v1/blocks` — list found blocks with status (confirmed/orphaned)
- [ ] Unit tests for orphan detection logic
- [ ] Integration test: simulate reorg, verify orphan handling

### Testing scope

**Test:**
- Found block persistence (insert, query, orphan)
- NNG pub/sub event parsing
- Event coalescing (debounce logic)
- Orphan cascade (block → round → shares)

**Don't test:**
- Template refresh logic (already tested in Slice 2)
- Payout reversal (Slice 7 handles payouts)

### Module structure

```
src/
├── node_integration/
│   └── nng/
│       ├── pub_sub.rs      # NNG pub/sub subscription, event parsing
│       └── events.rs       # NodeEvent enum (MiningWorkChanged, BlockConnected, BlockDisconnected)
├── accounting/
│   ├── found_block_repository.rs # Found block CRUD
│   └── schema.rs           # CREATE TABLE found_blocks
└── stratum_protocol/
    └── server.rs           # Broadcast mining.notify on template refresh
```

### Database schema additions

```sql
-- found_blocks table
CREATE TABLE found_blocks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    round_id INTEGER NOT NULL,
    block_hash TEXT NOT NULL UNIQUE,
    height INTEGER NOT NULL,
    status TEXT NOT NULL DEFAULT 'confirmed',  -- 'confirmed', 'matured', 'paid', 'orphaned'
    worker_id INTEGER,
    template_id INTEGER,
    persist_source TEXT,  -- 'submitblock' or other
    orphan_reason TEXT,
    matured_at DATETIME,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (round_id) REFERENCES rounds(id),
    FOREIGN KEY (worker_id) REFERENCES workers(id)
);

-- Add status to rounds
ALTER TABLE rounds ADD COLUMN status TEXT NOT NULL DEFAULT 'open';
-- Add orphaned status to shares
ALTER TABLE shares ADD COLUMN status TEXT;  -- Modify existing to include 'orphaned'
```

### Notes

- **Event coalescing:** 100ms debounce for `miningwrkchg` to reduce template refresh frequency during high mempool variance.
- **Orphan cascade:** Block orphaned → round orphaned → shares orphaned (but shares remain in PPLNS window per domain decision).
- **No payout reversal:** Orphaned shares stay in PPLNS window; orphan cost absorbed by pool fees.
- **Shutdown handling:** NNG pub/sub loop must unsubscribe and close on shutdown signal (use `tokio::select!` with shutdown receiver).

---

## Slice 7: PPLNS Payout Calculation

### What to build

Implement PPLNS payout calculation. When a block matures, calculate miner payouts based on difficulty-weighted shares in trailing window. Support dust carry-forward.

### Acceptance criteria

- [ ] PPLNS window calculation:
  - Window ends at found block's template ID
  - Extends backward until cumulative work = `n_multiplier × network_difficulty`
  - Work units = share difficulty (difficulty-weighted)
  - Aggregates shares by payout address
- [ ] Payout plan construction:
  - Gross reward = block subsidy (from config or node)
  - Fee = `gross_reward * fee_bps / 10000`
  - Net reward = gross - fee
  - Distribute net reward proportionally by work units
  - Handle remainder satoshis (distribute to largest fractional parts)
  - Dust = amounts < `min_payout_sat` (carried forward)
- [ ] Dust tracking:
  - Dust accumulated per address across rounds
  - Added to next payout calculation
- [ ] Payout batch repository:
  - `create_payout_batch(round_id, plan) -> PayoutBatch`
  - `record_payout(batch_id, address, amount) -> ()`
- [ ] `GET /api/v1/payouts` — list payout batches
- [ ] `GET /api/v1/payouts/{id}` — batch details with miner payouts
- [ ] Unit tests for PPLNS calculation (deterministic, remainder distribution)
- [ ] Integration test: full payout flow (found block → plan → batch)

### Testing scope

**Test:**
- PPLNS window aggregation (work units, by address)
- Payout plan construction (fee, net, remainder, dust)
- Dust carry-forward (accumulation, inclusion in next payout)
- Deterministic payout (same inputs → same outputs)

**Don't test:**
- Transaction signing (Slice 9)
- Payout scheduling automation (out of scope)

### Module structure

```
src/
├── payout/
│   ├── mod.rs
│   ├── scheme/
│   │   ├── mod.rs          # PayoutScheme trait
│   │   └── pplns.rs        # PPLNS implementation
│   ├── plan.rs             # PayoutPlan struct (outputs, dust, fee)
│   ├── window.rs           # PPLNS window calculation
│   └── transaction.rs      # Coinbase tx construction (no signing yet)
├── accounting/
│   ├── payout_repository.rs # Payout batch CRUD
│   └── schema.rs           # CREATE TABLE payout_batches, payouts
└── http_api/
    └── routes/
        └── payouts.rs      # GET /api/v1/payouts, /payouts/{id}
```

### Database schema additions

```sql
-- payout_batches table
CREATE TABLE payout_batches (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    round_id INTEGER NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',  -- 'pending', 'submitted', 'confirmed'
    total_amount INTEGER NOT NULL,
    fee_amount INTEGER NOT NULL,
    miner_count INTEGER NOT NULL,
    submitted_txid TEXT,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (round_id) REFERENCES rounds(id)
);

-- payouts table
CREATE TABLE payouts (
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

-- dust tracking (per address)
CREATE TABLE dust_balances (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    payout_address TEXT NOT NULL UNIQUE,
    balance INTEGER NOT NULL DEFAULT 0,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);
```

### Notes

- **PayoutScheme trait:** Scaffold for future PPS/PROP implementations.
- **No signing yet:** Payout plan constructed but not signed/submitted (Slice 9).
- **Dust tracking:** Per-address dust accumulation for carry-forward.
- **Shutdown handling:** Payout calculation must complete or rollback on shutdown (no partial payouts).

---

## Slice 8: Complete HTTP API

### What to build

Complete HTTP API with all endpoints for workers, shares, rounds, blocks, payouts. Add pagination, filtering, authentication.

### Acceptance criteria

- [ ] Authentication middleware:
  - Bearer token via `Authorization: Bearer <token>` header
  - Token from `api_token` config
- [ ] Pagination:
  - Query params: `limit`, `offset` (or `page`, `per_page`)
  - Response includes: `total`, `has_more`
- [ ] Endpoints:
  - `GET /api/v1/workers` — list with pagination, filter by payout_address
  - `GET /api/v1/workers/{id}` — details with hashrate (5-min rolling window)
  - `GET /api/v1/shares` — list with filters (worker_id, status, from, to)
  - `GET /api/v1/rounds` — list with status filter
  - `GET /api/v1/rounds/{id}` — details with share breakdown by worker
  - `GET /api/v1/blocks` — list with status filter (confirmed/orphaned)
  - `GET /api/v1/blocks/{hash}` — block details with miner attribution
  - `GET /api/v1/payouts` — list with status filter
  - `GET /api/v1/payouts/{id}` — batch details with miner payouts
  - `GET /api/v1/health` — health check (uptime, miners, last template)
  - `GET /api/v1/stats` — aggregated stats (shares, blocks, hashrate, difficulty)
- [ ] Error handling:
  - 401 Unauthorized (missing/invalid token)
  - 404 Not Found (resource doesn't exist)
  - 500 Internal Server Error (with correlation ID)
- [ ] Unit tests for API routes
- [ ] Integration test: authenticated requests, pagination, filtering

### Testing scope

**Test:**
- Authentication (valid/invalid tokens)
- Pagination (limit, offset, has_more)
- Filtering (by status, date range, worker)
- Error responses (401, 404, 500)

**Don't test:**
- Dashboard UI (out of scope)
- WebSocket live updates (out of scope)

### Module structure

```
src/
└── http_api/
    ├── mod.rs
    ├── server.rs           # Axum server, auth middleware
    ├── models.rs           # API DTOs (request/response)
    ├── pagination.rs       # Pagination logic
    └── routes/
        ├── mod.rs
        ├── health.rs
        ├── stats.rs
        ├── workers.rs
        ├── shares.rs
        ├── rounds.rs
        ├── blocks.rs
        └── payouts.rs
```

### Notes

- **API versioning:** URL versioning (`/api/v1/`) per domain decision.
- **DTOs separate from domain models:** API models in `http_api/models.rs`, domain models in respective contexts.
- **No dashboard:** API only, no HTML rendering or WebSocket.

---

## Slice 9: Payout Signer Abstraction

### What to build

Implement payout signer abstraction to support both in-process signing (internal key) and external signer (HTTP webhook). Configuration determines mode.

### Acceptance criteria

- [ ] Signer trait:
  - `trait Signer: Send + Sync { fn sign_and_submit(&self, plan: &PayoutPlan) -> Result<String>; }`
- [ ] Internal signer:
  - Uses private key from `pool.signing.private_key` config
  - Signs payout transaction, submits via JSON-RPC `sendrawtransaction`
  - Returns txid
- [ ] External signer (scaffold):
  - POST payout plan to configured webhook URL
  - Poll for txid submission (or receive callback)
  - Returns txid
- [ ] Configuration:
  - `pool.signing.mode` — "internal" or "external"
  - `pool.signing.private_key` — required for internal mode
  - `pool.signing.webhook_url` — required for external mode
- [ ] Payout scheduler (manual trigger for now):
  - CLI command or API endpoint to trigger payout for matured blocks
  - Calls signer, records txid in payout_batch
- [ ] Unit tests for internal signer (mock JSON-RPC)
- [ ] Integration test: full payout flow (plan → sign → submit)

### Testing scope

**Test:**
- Internal signer (transaction construction, signing)
- JSON-RPC submission (mocked)
- Configuration validation (mode, key presence)

**Don't test:**
- External signer webhook (scaffold only)
- Payout scheduling automation (manual trigger only)

### Module structure

```
src/
└── payout/
    └── signer/
        ├── mod.rs          # Signer trait
        ├── internal.rs     # Internal key signer
        └── external.rs     # External webhook signer (scaffold)
```

### Notes

- **HITL:** Requires human to configure private key or webhook URL.
- **Manual trigger:** No automatic scheduling — operator triggers payout manually.
- **External signer scaffold:** Basic structure, can be fleshed out later.

---

## Dependency Graph

```
#1 (Minimal Server + Shutdown)
    │
    ▼
#2 (NNG Template)
    │
    ▼
#3 (Share Validation)
    │
    ├──────► #4 (VarDiff)
    │
    ├──────► #5 (Worker/Round Accounting)
    │              │
    │              ├──────► #6 (Found Blocks/Reorg)
    │              │              │
    │              │              ▼
    │              │         #7 (PPLNS Payout)
    │              │              │
    │              │              ▼
    │              │         #9 (Payout Signer)
    │              │
    │              ▼
    │         #8 (Complete HTTP API)
    │
    └──────► #8 (Complete HTTP API)
```

**Implementation order:**
1. #1 → #2 → #3 (core Stratum flow)
2. #4 (VarDiff, depends on #3)
3. #5 (Accounting, depends on #3)
4. #6 (Reorg handling, depends on #5)
5. #7 (Payouts, depends on #6)
6. #8 (HTTP API, can parallelize after #5)
7. #9 (Signer, depends on #7)

**Cross-cutting: Graceful Shutdown**
Implemented in #1, all subsequent slices must:
- Register for shutdown signal (`shutdown_signal.recv()`)
- Complete cleanup in order (stop accepting → flush → close)
- Respect timeouts (5s flush, 30s total)

---

## Questions for User

1. **Granularity:** Do these slices feel right, or should any be split/merged?
2. **Dependencies:** Are the dependency relationships correct?
3. **AFK vs HITL:** Is Slice 9 correctly marked as HITL (needs key config)?
4. **Testing scope:** Does the testing scope align with your expectations (pragmatic TDD)?
5. **Priority:** Should any slices be reprioritized (e.g., HTTP API earlier for dashboard integration)?
