# Modular Architecture Refactor - Vertical Slices

**Source:** [modular-architecture-refactor.md](./modular-architecture-refactor.md)  
**Status:** Ready for implementation  
**Last updated:** 2026-05-19  
**UBQ alignment:** [docs/UBIQUITOUS_LANGUAGE.md](../../../UBIQUITOUS_LANGUAGE.md)

---

## Slice Overview

| # | Title | Type | Blocked By | Estimated Effort | Status |
|---|-------|------|------------|------------------|--------|
| 1 | Minimal Stratum Server (Tracer Bullet) | AFK | None | 1 day | ✅ Done (UBQ-aligned) |
| 2 | NNG Template Integration | AFK | #1 | 1 day | ✅ Done (UBQ-aligned) |
| 3 | Share Validation Pipeline | AFK | #2 | 1 day | ✅ Done (UBQ-aligned) |
| 4 | Per-Session VarDiff | AFK | #3 | 1 day | ✅ Done (UBQ-aligned) |
| 5 | Worker and Round Accounting | AFK | #3 | 1 day | ✅ Done (UBQ-aligned) |
| 6 | Found Blocks and Reorg Handling (includes JSON-RPC submitblock) | AFK | #5 | 2 days |
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
- [ ] Authorization events recorded to `authorization_events` table for every authorize attempt (success or failure)
- [ ] After authorize, miner receives `mining.notify` with static job (hardcoded prevhash, coinbase, etc.)
- [ ] Session maintains `assigned_jobs` map tracking `(job_id, P_diff, ntime)` for each dispatched `mining.notify`, capped at `MAX_ASSIGNED_JOBS_PER_SESSION` (default 128)
- [ ] Miner can submit share via `mining.submit`, server responds with `true` (accepted) or `false` (rejected)
- [ ] Share is persisted to SQLite `shares` table with `dedupe_key` for idempotent insertion
- [ ] Share outcome is persisted to `share_outcomes` table with validation result
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
- [ ] Unit tests for session state machine (subscribe before authorize, assigned_jobs tracking)
- [ ] Unit tests for authorization event recording
- [ ] Integration test: full subscribe → authorize → submit flow
- [ ] Integration test: deduplicate identical shares (verify dedupe_key enforcement)
- [ ] Integration test: graceful shutdown with in-flight shares (verify persistence)

### Testing scope

**Test:**
- Protocol parsing (valid JSON, invalid JSON, unknown methods)
- Session state machine (subscribe required before authorize, assigned_jobs cap)
- Authorization event recording (success, failure, worker name parsing)
- Share persistence (insert, dedupe_key uniqueness)
- Share outcome recording (status, reject_reason)
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
│   ├── session.rs          # SessionState (extranonce1, authorized_workers, assigned_jobs map)
│   └── server.rs           # TCP listener, handle_conn, shutdown-aware
├── share_processing/
│   ├── mod.rs
│   └── persistence.rs      # Share + ShareOutcome insert (calls accounting facade)
├── accounting/
│   ├── mod.rs              # AccountingService facade
│   ├── share_repository.rs # Share + ShareOutcome insert/query
│   ├── worker_repository.rs# Worker upsert/query
│   └── schema.rs           # CREATE TABLE workers, shares, share_outcomes, authorization_events
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

-- authorization_events table (immutable audit log per UBQ)
CREATE TABLE authorization_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id TEXT NOT NULL,
    worker_name TEXT NOT NULL,
    payout_address TEXT NOT NULL,
    worker_suffix TEXT,
    authorized INTEGER NOT NULL,  -- 1=true, 0=false
    reason TEXT,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_auth_events_session ON authorization_events(session_id);
CREATE INDEX idx_auth_events_address ON authorization_events(payout_address);

-- shares table (raw submission record, immutable per UBQ)
-- Captures the fact that a miner submitted work.
CREATE TABLE shares (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    worker_id INTEGER NOT NULL,
    session_id TEXT NOT NULL,
    job_id TEXT NOT NULL,
    template_id INTEGER NOT NULL,
    template_epoch INTEGER NOT NULL,
    extranonce2 TEXT NOT NULL,
    ntime_hex_6b TEXT NOT NULL,
    nonce_hex_8b TEXT NOT NULL,
    difficulty REAL NOT NULL,      -- P_diff at assignment time (immutable)
    dedupe_key TEXT NOT NULL UNIQUE,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (worker_id) REFERENCES workers(id)
);

CREATE INDEX idx_shares_worker_id ON shares(worker_id);
CREATE INDEX idx_shares_created_at ON shares(created_at);
CREATE INDEX idx_shares_dedupe_key ON shares(dedupe_key);

-- share_outcomes table (validation pipeline result per UBQ)
-- Captures the full validation result linked to the raw share.
CREATE TABLE share_outcomes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    share_id INTEGER NOT NULL,
    session_id TEXT NOT NULL,
    worker_id INTEGER NOT NULL,
    job_id TEXT NOT NULL,
    round_id INTEGER,          -- resolved at insert time (NULL until Slice 5)
    dedupe_key TEXT NOT NULL UNIQUE,
    status TEXT NOT NULL,       -- 'accepted', 'rejected', 'stale'
    reject_reason TEXT,         -- NULL when accepted; one of 'stale-job', 'invalid-submit-shape',
                                -- 'low-difficulty-share', 'ntime-mismatch', 'unauthorized-worker'
    node_result TEXT,           -- lotusd submission result (NULL for rejected shares)
    low_diff_ok INTEGER,       -- 1 if hash met P_diff target, 0 otherwise
    network_target_ok INTEGER, -- 1 if hash met N_diff target, 0 otherwise (high-hash share)
    block_hash TEXT,            -- non-NULL if share found a block candidate
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (share_id) REFERENCES shares(id),
    FOREIGN KEY (worker_id) REFERENCES workers(id)
);

CREATE INDEX idx_share_outcomes_share_id ON share_outcomes(share_id);
CREATE INDEX idx_share_outcomes_worker_id ON share_outcomes(worker_id);
CREATE INDEX idx_share_outcomes_dedupe_key ON share_outcomes(dedupe_key);
CREATE INDEX idx_share_outcomes_status ON share_outcomes(status);
```

### Assigned Jobs Bookkeeping

Session state must track every dispatched `mining.notify` in an `assigned_jobs` map:

```rust
pub struct SessionState {
    pub session_id: String,
    pub extranonce1: String,
    pub extranonce2_size: u8,
    pub is_subscribed: bool,
    pub is_authorized: bool,
    pub authorized_workers: HashSet<String>,
    pub assigned_jobs: VecDeque<AssignedJob>,  // (job_id, P_diff, ntime) pairs
}

pub struct AssignedJob {
    pub job_id: String,
    pub p_diff: f64,        // P_diff at assignment time
    pub ntime: String,      // ntime frozen at assignment time
}
```

**Key invariants (per UBQ):**
- When a miner submits against a job, the submitted ntime must match the frozen ntime from assignment. If different → `ntime-mismatch` rejection.
- Share difficulty = P_diff at assignment time, NOT at submission time.
- Capped at `MAX_ASSIGNED_JOBS_PER_SESSION` (default 128) to bound memory.
- When `clean_jobs=true` job arrives, all previous jobs become stale (removed from assigned_jobs).

### Notes

- **Static job:** Hardcode a minimal job with fake prevhash, coinbase, etc. This is temporary — Slice 2 replaces with NNG template.
- **No validation yet:** Accept all shares as valid (status='accepted' in share_outcomes). Slice 3 adds real validation.
- **No difficulty tracking yet:** Use fixed difficulty=1.0 for all shares. Slice 4 adds VarDiff.
- **No rounds yet:** `round_id` in share_outcomes is NULL. Slice 5 adds round resolution.
- **Dedupe key:** Format per UBQ: `worker_id:template_id:template_epoch:extranonce2:ntime:nonce`. Both `shares` and `share_outcomes` enforce `UNIQUE(dedupe_key)`.
- **Authorization events:** Every `mining.authorize` attempt produces one immutable audit event, regardless of success/failure.
- **Graceful shutdown:** Implemented in Slice 1 as foundational infrastructure. All subsequent slices must respect shutdown signals (use `shutdown_signal.recv()` in long-running tasks).

---

## Slice 2: NNG Template Integration

**Status:** ✅ Completed 2026-05-19 (UBQ-aligned)

### What to build

Connect to lotusd via NNG, fetch real mining templates on startup, and distribute them to miners via `mining.notify`. Replace static job with dynamic template from node.

### Acceptance criteria

- [x] NNG RPC client connects to configured `nng_rpc_url`
- [x] On startup, fetch `MiningTemplate` via NNG RPC `get_mining_template`
- [x] Template is converted to `MiningJob` and cached in memory
- [x] Job ID format follows UBQ: `job-{template_id}-{epoch}` (epoch from miningwrkchg events)
- [x] After authorize, miner receives `mining.notify` with real template data
- [x] Job cache supports multiple jobs (LRU eviction, max 512 jobs)
- [x] `GET /api/v1/stats` includes network difficulty from template
- [x] Unit tests for template → job conversion
- [x] Integration test: fetch template from lotusd (mocked or real node)

### Testing scope

**Test:**
- NNG RPC client (template fetch)
- Template → job conversion (coinbase handling, merkle branches, job_id format)
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
- **Template epoch:** The epoch is a monotonically increasing counter from lotusd's `miningwrkchg` events. On startup, epoch starts at the template's `curtime` field (which is an opaque monotonic counter / timestamp from the node). On subsequent `miningwrkchg` events (Slice 6), the epoch is incremented.
- **Job ID format:** Per UBQ: `job-{template_id}-{epoch}`. Example: `job-42-1234567890`.
- **MiningJob struct** includes `template_id`, `template_epoch`, `network_target_hex` for share validation and round accounting.
- **Shutdown handling:** NNG RPC client must close connections on shutdown signal (register for shutdown in Slice 2).

---

## Slice 3: Share Validation Pipeline

### What to build

Validate shares before accepting: check difficulty target, verify header hash, ensure job is active. Reject invalid shares with appropriate error codes. Record outcomes in `share_outcomes` table.

### Acceptance criteria

- [ ] Share validator checks:
  - Worker is authorized for this session
  - Job exists and is active in session's `assigned_jobs` map (not stale)
  - Submitted ntime matches frozen ntime from assigned job (ntime-mismatch check)
  - extranonce2 format (correct length, hex)
  - ntime format (6 bytes, hex)
  - nonce format (8 bytes, hex)
  - Share meets session P_diff target (header hash ≤ P_diff target)
  - Share meets N_diff target? (header hash ≤ N_diff target) — recorded as `network_target_ok` flag
- [ ] Rejected shares recorded in `share_outcomes` with `reject_reason`:
  - `unauthorized-worker` — worker not in session's authorized set
  - `stale-job` — job not in session's assigned_jobs (stale or unknown)
  - `ntime-mismatch` — submitted ntime != frozen ntime from assigned job
  - `invalid-submit-shape` — malformed extranonce2/ntime/nonce
  - `low-difficulty-share` — header hash doesn't meet P_diff target
- [ ] Accepted shares recorded with:
  - `status='accepted'`, `reject_reason=NULL`
  - `low_diff_ok=1` (always true for accepted)
  - `network_target_ok=0/1` (1 if hash meets N_diff — potential block found)
  - `block_hash` populated if `network_target_ok=1`
- [ ] Share validation is synchronous (blocks response until complete)
- [ ] Raw share (`shares` table) and outcome (`share_outcomes` table) are inserted atomically
- [ ] Dedupe key prevents duplicate insertions (INSERT OR IGNORE)
- [ ] `GET /api/v1/stats` includes rejection breakdown (by reason)
- [ ] Unit tests for each validation rule
- [ ] Integration test: submit valid/invalid shares, verify responses and persistence

### Testing scope

**Test:**
- Difficulty validation (header hash vs P_diff target)
- N_diff / network_target_ok detection
- ntime-mismatch detection
- Format validation (extranonce2, ntime, nonce)
- Job staleness detection (assigned_jobs lookup)
- Rejection reason assignment
- Dedupe key enforcement (idempotent insert)
- Atomic share + outcome insertion

**Don't test:**
- Cryptographic primitives (use bitcoinsuite)
- TCP error handling (already tested in Slice 1)

### Validation flow (pseudocode)

```
fn validate_share(req, session, job_cache) -> ValidationResult {
    // 1. Check worker authorization
    if worker not in session.authorized_workers → reject(unauthorized-worker)

    // 2. Parse submit params
    let (job_id, extranonce2, ntime, nonce) = extract_params(req)
    if any param invalid format → reject(invalid-submit-shape)

    // 3. Look up assigned job in session
    let assigned = session.assigned_jobs.get(job_id)
    if assigned is None → reject(stale-job)

    // 4. Check ntime matches frozen ntime from assignment
    if ntime != assigned.ntime → reject(ntime-mismatch)

    // 5. Build stratum header and check hash
    let header = build_stratum_header(job, extranonce2, ntime, nonce, session.extranonce1)
    let hash = double_sha256(header)

    // 6. Check P_diff target
    if !header_meets_difficulty(&hash, assigned.p_diff) → reject(low-difficulty-share)

    // 7. Check N_diff target
    let meets_network = header_meets_difficulty(&hash, job.network_target_hex)

    // 8. Accepted! Build outcome
    ValidationResult {
        accepted: true,
        low_diff_ok: true,
        network_target_ok: meets_network,
        block_hash: if meets_network { Some(hash) } else { None },
    }
}
```

### Module structure

```
src/
├── share_processing/
│   ├── mod.rs
│   ├── validator.rs        # validate_share(Share, AssignedJob, Job) -> ValidationResult
│   └── difficulty.rs       # difficulty_to_target, header_meets_difficulty (or use bitcoinsuite)
└── stratum_protocol/
    └── server.rs           # Call validator before recording share + outcome
```

### Notes

- **Synchronous validation:** Required by Stratum V1 protocol (no "pending" state). The validator runs in the hot path before responding to the miner.
- **Use bitcoinsuite:** Leverage `bitcoinsuite-bitcoind-stratum::build_stratum_header` and `header_meets_difficulty`.
- **Share + Outcome atomicity:** Both tables are inserted in a single SQLite transaction. If the outcome insert fails (e.g., dedupe violation), the raw share insert is also rolled back.
- **High-hash shares:** A share whose hash meets P_diff but NOT N_diff is accepted with `network_target_ok=false`. These count toward PPLNS work units per UBQ invariant.
- **Blocks found:** When `network_target_ok=true`, the share is a valid block candidate. The outcome includes the block_hash. Slice 6 handles submission to lotusd.

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
- [ ] On session start, difficulty = `N_diff * vardiff_initial_pct`
- [ ] P_diff is clamped to `[vardiff_min_floor, N_diff]` at all times
- [ ] On each accepted share, record timestamp for rate calculation
- [ ] Every `vardiff_retarget_secs`, compute new P_diff:
  - If share rate too fast → increase difficulty (max 1.5× per retarget)
  - If share rate too slow → decrease difficulty (min 0.67× per retarget)
  - Clamp to `[vardiff_min_floor, N_diff]`
- [ ] Send `mining.set_difficulty` to session when P_diff changes
- [ ] P_diff is captured in `assigned_jobs` at notify dispatch time (immutable on shares)
- [ ] When N_diff changes (new template), VarDiff's ceiling (`N_diff`) updates immediately via `update_max()`
- [ ] Unit tests for VarDiff retarget logic
- [ ] Integration test: session with fast/slow shares, verify retargeting

### Testing scope

**Test:**
- Retarget calculation (ratio, clamping)
- Difficulty bounds (floor `vardiff_min_floor`, ceiling `N_diff`)
- `update_max()` — immediate ceiling adjustment when N_diff changes
- Share rate computation (timestamp window)
- P_diff captured in assigned_jobs at dispatch time

**Don't test:**
- TCP message formatting (already tested)
- Per-worker difficulty (VarDiff is per-session, not per-worker)

### Module structure

```
src/
├── share_processing/
│   └── difficulty.rs       # VarDiff struct, record_share(), maybe_retarget(), update_max()
└── stratum_protocol/
    └── session.rs          # Session includes VarDiff instance
```

### Notes

- **Per-session, not per-worker:** VarDiff operates on TCP connection level (domain decision per UBQ).
- **N_diff ceiling:** VarDiff max = current network difficulty from template. When N_diff changes (new template from lotusd), `update_max()` is called on each session's VarDiff.
- **P_diff at assignment:** When `mining.notify` is dispatched, the current P_diff is captured in the `AssignedJob` record. This value becomes the share's immutable difficulty.
- **Share rate target:** Default 20s between shares (industry standard).
- **UBQ invariant:** P_diff ∈ [vardiff_min_floor, N_diff].

---

## Slice 5: Worker and Round Accounting

**Status:** ✅ Completed 2026-05-19 (UBQ-aligned)

### What to build

Implement worker persistence (across sessions) and round accounting. Workers are identified by `(payout_address, worker_suffix)`. Rounds track share accumulation periods. Add `accounting_events` audit log.

### Acceptance criteria

- [x] Worker repository:
  - `upsert_worker(payout_address, worker_suffix) -> Worker` — create or fetch
  - Worker has persistent ID across sessions
- [x] Round repository:
  - `get_or_create_current_round(start_template_id) -> Round` — ensure one open round
  - Round has `start_template_id`, `status` (open/found/closed/paid/orphaned)
  - `close_round(id, end_template_id, status)` marks round as closed
  - `resolve_round_for_template(template_id)` returns correct round for template
  - `list(status_filter)` lists rounds with optional status filter
- [x] Share outcomes include `round_id` foreign key (resolved at insert time via `resolve_round_for_template(template_id)`)
- [x] On share submission:
  - Worker upserted (if new)
  - Round resolved via `resolve_round_for_template(template_id)`
  - Share outcome recorded with round_id
- [x] **Accounting events table** (append-only audit log):
  - Record `share_outcome` events with status (accepted/rejected)
  - Record `round_opened`, `round_closed` events
  - Queryable by `event_type` with pagination
- [x] `GET /api/v1/workers` — list workers with share counts
- [x] `GET /api/v1/workers/{id}` — worker details (shares)
- [x] `GET /api/v1/rounds` — list rounds with status filter
- [x] Unit tests for worker/round repositories (8 round_repo tests)
- [x] Unit tests for accounting event recording (4 event_repo tests)
- [x] Unit tests for AccountingService facade (3 service tests)
- [x] Integration test: round resolution in share recording path
- [x] HTTP API tests for workers (3 tests) and rounds (3 tests)

### Testing scope

**Test:**
- Worker upsert (create new, fetch existing)
- Round lifecycle (open → found → closed → paid → orphaned)
- `resolve_round_for_template` — correct round assignment
- Share attribution (worker_id, round_id)
- Accounting event recording (type, status, context)
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
│   ├── share_repository.rs  # Share + ShareOutcome insert with worker_id, round_id
│   ├── accounting_event_repository.rs # Append-only audit events
│   └── schema.rs           # CREATE TABLE workers, rounds, accounting_events
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

-- accounting_events table (append-only audit log per UBQ)
CREATE TABLE accounting_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_type TEXT NOT NULL,    -- 'share_outcome', 'round_opened', 'round_closed',
                                 -- 'found_block_observed', 'found_block_orphaned'
    status TEXT NOT NULL,        -- 'accepted', 'rejected', 'stale', 'orphaned'
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
    payload_json TEXT,         -- Arbitrary JSON payload for extensibility
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (worker_id) REFERENCES workers(id),
    FOREIGN KEY (round_id) REFERENCES rounds(id)
);

CREATE INDEX idx_accounting_events_type ON accounting_events(event_type);
CREATE INDEX idx_accounting_events_created ON accounting_events(created_at);

-- Add round_id to share_outcomes
ALTER TABLE share_outcomes ADD COLUMN round_id INTEGER;
ALTER TABLE share_outcomes ADD FOREIGN KEY (round_id) REFERENCES rounds(id);

CREATE INDEX idx_share_outcomes_round_id ON share_outcomes(round_id);
```

### Notes

- **Round assignment:** Shares assigned to current round via `resolve_round_for_template(template_id)` at insert time, not backfilled.
- **Worker persistence:** Worker exists across sessions (domain invariant per UBQ).
- **Accounting events:** Append-only. `event_type` is the primary access path. No event types are ever removed, only deprecated.
- **No found_block yet:** Rounds can be open/found/closed/paid/orphaned, but no found_block table yet (Slice 6).
- **UBQ invariant:** A share whose header hash meets P_diff but NOT N_diff (high-hash share) is still recorded as accepted with `network_target_ok=false`. These count toward PPLNS work units.

---

## Slice 6: Found Blocks and Reorg Handling

### What to build

Track found blocks submitted to lotusd. Handle blockchain reorgs via NNG pub/sub events (`blkconnected`, `blkdisconctd`, `miningwrkchg`). Mark orphaned blocks and adjust accounting. Build a JSON-RPC HTTP client for block submission and chain queries.

### Acceptance criteria

#### JSON-RPC Client

- [ ] JSON-RPC client connects to configured `bitcoind_rpc.url` with authentication
  - [ ] Uses `rpc_user` / `rpc_pass` from config for HTTP Basic Auth
  - [ ] Configurable via `[bitcoind_rpc]` section in config.toml or env var overrides
- [ ] JSON-RPC request formatting:
  - [ ] Builds valid JSON-RPC 2.0 request objects: `{"jsonrpc":"2.0","id":<n>,"method":"<method>","params":<params>}`
  - [ ] Auto-incrementing request IDs for correlation
- [ ] JSON-RPC response parsing:
  - [ ] Parses successful responses: `{"result":<value>,"error":null,"id":<n>}`
  - [ ] Parses error responses: `{"result":null,"error":{"code":<n>,"message":<s>},"id":<n>}` and returns structured error
  - [ ] Handles HTTP transport errors (connection refused, timeout, DNS failure)
  - [ ] Handles malformed JSON responses
- [ ] `submitblock(mined_block_hex: &str) -> Result<SubmitBlockResult>`:
  - [ ] Calls `submitblock` JSON-RPC method with raw hex-encoded block
  - [ ] Returns structured result: `SubmitBlockResult { accepted: bool, block_hash: String, error: Option<String> }`
  - [ ] Gracefully handles rejection (duplicate, invalid, orphaned)
- [ ] Placeholder `getblockcount()` for future use (scaffold, returns `Result<i32>`)
- [ ] JSON-RPC client is `Send + Sync`, shares one `reqwest::Client` across requests
- [ ] Unit tests for:
  - [ ] Request formatting (correct JSON-RPC 2.0 structure, auth header)
  - [ ] Response parsing (success, error, malformed)
  - [ ] `submitblock` result parsing (accepted vs rejected)
  - [ ] HTTP error propagation (timeout, connection refused)

#### Block Submission Pipeline

- [ ] After share validation produces `network_target_ok=true`:
  - [ ] Build the full block from template + miner submission (coinbase + extranonce1 + extranonce2 + header fields)
  - [ ] Serialize to raw hex bytes
  - [ ] Submit to lotusd via `submitblock` JSON-RPC call
  - [ ] If accepted: record share_outcome with `node_result="accepted"`, create `FoundBlock` record with `persist_source="json-rpc"`
  - [ ] If rejected: record share_outcome with `node_result="rejected: <reason>"`, skip found_block creation
  - [ ] If lotusd unavailable (connection error): share is still accepted as high-hash share, block submission retried on best-effort basis
- [ ] Duplicate detection: if the same block is submitted twice (second miner sends same block), second submission returns `"duplicate"` — handle gracefully (don't create duplicate found_block)

#### Found Block Repository

- [ ] Found block repository:
  - `record_found_block(round_id, block_hash, height, worker_id) -> FoundBlock`
  - `mark_orphaned(block_hash, reason) -> ()`
  - `get_by_hash(block_hash) -> Option<FoundBlock>`
  - `list(status_filter: Option<&str>) -> Vec<FoundBlock>`

#### NNG Pub/Sub Event Handling

- [ ] NNG pub/sub client subscribes to:
  - `miningwrkchg` — template refresh (with 100ms coalescing)
  - `blkconnected` — block connected (for tip tracking)
  - `blkdisconctd` — block disconnected (for orphan detection)
- [ ] On `blkdisconctd` event:
  - Check if disconnected block is in `found_blocks`
  - If yes, mark as orphaned with reason="reorg_detected"
  - Mark round as orphaned
  - Record `accounting_event` with type `found_block_orphaned`
  - **Do NOT orphan shares** (per UBQ: shares remain valid in PPLNS window)
- [ ] On `miningwrkchg` event:
  - Fetch new template via NNG RPC
  - Broadcast `mining.notify` with `clean_jobs=true` to all sessions
  - All previous jobs become stale immediately (UBQ invariant)
  - Clear session's `assigned_jobs` map
  - Create new job in cache
- [ ] Event coalescing: 100ms debounce for `miningwrkchg` events

#### HTTP API

- [ ] `GET /api/v1/blocks` — list found blocks with status (confirmed/orphaned), optional `?status=` filter
- [ ] `GET /api/v1/blocks/{hash}` — block detail with miner attribution and round info

#### Configuration

- [ ] Wire `nng_pub_url` from config.toml into `Config` struct
- [ ] Wire `[bitcoind_rpc]` section into `Config` struct (`url`, `rpc_user`, `rpc_pass`)
- [ ] Environment variable overrides: `NNG_PUB_URL`, `BITCOIND_RPC_URL`, `BITCOIND_RPC_USER`, `BITCOIND_RPC_PASS`

### Testing scope

**Test:**
- JSON-RPC client request formatting and response parsing
- `submitblock` result parsing (accepted, duplicate, rejected, error)
- HTTP error handling (connection refused, timeout, malformed response)
- Found block persistence (insert, query, orphan, list with filter)
- Block submission pipeline (full block build → submit → record/reject)
- Duplicate block detection (second submission doesn't create duplicate found_block)
- NNG pub/sub event parsing
- Event coalescing (debounce logic)
- Orphan cascade (block → round, but NOT shares)
- `clean_jobs=true` → assigned_jobs cleared
- Accounting event recording (found_block_orphaned, found_block_observed)

**Don't test:**
- Template refresh logic (already tested in Slice 2)
- Payout reversal (Slice 7 handles payouts)
- Actual lotusd network calls (mock JSON-RPC responses)

### Module structure

```
src/
├── node_integration/
│   ├── nng/
│   │   ├── pub_sub.rs      # NNG pub/sub subscription, event parsing
│   │   └── events.rs       # NodeEvent enum (MiningWorkChanged, BlockConnected, BlockDisconnected)
│   └── json_rpc/
│       ├── mod.rs          # Re-exports
│       ├── client.rs       # JSON-RPC HTTP client with auth, request/response handling
│       └── methods.rs      # RPC method implementations (submitblock, getblockcount)
├── accounting/
│   ├── found_block_repository.rs # Found block CRUD
│   └── schema.rs           # CREATE TABLE found_blocks
└── stratum_protocol/
    ├── block_builder.rs    # Build full block from template + miner submission
    └── server.rs           # Wire block submission in share validation hot path,
                            # broadcast mining.notify on template refresh
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
    persist_source TEXT,  -- 'json-rpc' or other
    orphan_reason TEXT,
    matured_at DATETIME,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (round_id) REFERENCES rounds(id),
    FOREIGN KEY (worker_id) REFERENCES workers(id)
);
```

### Notes

- **JSON-RPC client shares `reqwest::Client`** with a single connection pool, not per-call connections.
- **Block building:** The stratum server has all the pieces to reconstruct the full block: template header + coinbase1 + extranonce1 + extranonce2 + coinbase2 + merkle branches. `block_builder.rs` assembles these into raw block bytes for `submitblock`.
- **Event coalescing:** 100ms debounce for `miningwrkchg` to reduce template refresh frequency during high mempool variance.
- **Orphan cascade per UBQ:** Block orphaned → round orphaned → shares remain valid in PPLNS window. The `round_id` is historical accounting only — PPLNS window calculation is share-ID-based, not round-based. Orphan cost is absorbed by pool fees over time.
- **UBQ invariant:** When a round is orphaned, its shares remain valid and stay in the PPLNS window.
- **UBQ invariant:** When `clean_jobs=true`, ALL previous jobs become stale immediately. This differs from Bitcoin where only merkle_root changes and miners could theoretically continue working.
- **UBQ invariant:** The reason code for `miningwrkchg` (NewTip/Reorg/MempoolRefresh/ManualInvalidation) is for logging and observability only — all events trigger `clean_jobs=true`.
- **Shutdown handling:** NNG pub/sub loop and JSON-RPC client must respect shutdown signal (use `tokio::select!` with shutdown receiver).

---

## Slice 7: PPLNS Payout Calculation

### What to build

Implement PPLNS payout calculation. When a block matures, calculate miner payouts based on difficulty-weighted shares in trailing window. Support dust carry-forward.

### Acceptance criteria

- [ ] PPLNS window calculation:
  - Window ends at found block's submission time (via share_outcomes.created_at)
  - Extends backward until cumulative work units = `n_multiplier × N_diff`
  - Work units = share.difficulty (P_diff at assignment time, immutable)
  - Aggregates by payout_address
  - Includes shares where `network_target_ok=false` (high-hash shares count as work)
  - Excludes orphaned shares (shares from orphaned rounds)
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
- [ ] Payout share snapshot:
  - Captures which shares were in the window when payout was calculated
  - Stored in `payout_share_snapshots` table
  - Created atomically with the payout batch
- [ ] `GET /api/v1/payouts` — list payout batches
- [ ] `GET /api/v1/payouts/{id}` — batch details with miner payouts
- [ ] Unit tests for PPLNS calculation (deterministic, remainder distribution)
- [ ] Integration test: full payout flow (found block → plan → batch)

### Testing scope

**Test:**
- PPLNS window aggregation (work units, by address)
- Payout plan construction (fee, net, remainder, dust)
- Dust carry-forward (accumulation, inclusion in next payout)
- Payout share snapshot (complete record of paid shares)
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
│   ├── payout_snapshot_repository.rs # Payout share snapshot
│   └── schema.rs           # CREATE TABLE payout_batches, payouts, payout_share_snapshots, dust_balances
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
    retry_key TEXT UNIQUE,       -- 'block_hash:num_outputs' for idempotent retry
    last_error TEXT,
    next_retry_at DATETIME,
    attempt_count INTEGER DEFAULT 0,
    signed_payload_ref TEXT,     -- 'rawtx:<txid>' for manual recovery
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

-- payout_share_snapshots table (auditable record per UBQ)
CREATE TABLE payout_share_snapshots (
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

CREATE INDEX idx_snapshots_batch ON payout_share_snapshots(batch_id);

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
- **Payout batch retry:** `retry_key` prevents duplicate batches if scheduler retries.
- **Payout share snapshot:** Created atomically with the payout batch. Enables post-hoc audit of which shares contributed.
- **UBQ invariant:** Shares from orphaned rounds remain in PPLNS window (not excluded). The window is share-ID-based, not round-based.
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
  - `GET /api/v1/share-outcomes` — list share outcomes with validation details
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
- **Share outcomes endpoint:** Separate from raw shares. Clients can query validation results independently.

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

## UBQ Cross-Reference

Key UBQ invariants that span multiple slices:

| UBQ Concept | Defined In | Implemented In |
|---|---|---|
| Share is immutable once persisted | UBQ §Share | Slice 1 (shares table) |
| Share Outcome captures full validation | UBQ §Share Outcome | Slice 1 (share_outcomes table), Slice 3 |
| Dedupe key: `worker_id:template_id:template_epoch:extranonce2:ntime:nonce` | UBQ §Dedupe Key | Slice 1 (UNIQUE constraint) |
| Session assigned_jobs with (job_id, P_diff, ntime) | UBQ §Assigned Job | Slice 1 (SessionState), Slice 3 (ntime check) |
| ntime-mismatch rejection | UBQ §Assigned Job | Slice 3 |
| P_diff at assignment time (immutable) | UBQ §Share Difficulty | Slice 1 (difficulty field), Slice 3, Slice 4 |
| VarDiff is per-session, not per-worker | UBQ §Session | Slice 4 |
| P_diff ∈ [vardiff_min_floor, N_diff] | UBQ §Pool Difficulty | Slice 4 |
| High-hash share: accepted with network_target_ok=false | UBQ §Accepted Share | Slice 3 |
| Authorization events are immutable | UBQ §Authorization Event | Slice 1 (authorization_events table) |
| Round membership resolved at insert time | UBQ §Share | Slice 5 |
| Orphaned shares remain in PPLNS window | UBQ §Round | Slice 6, Slice 7 |
| clean_jobs=true → ALL previous jobs stale | UBQ §Job | Slice 2, Slice 6 |
| Accounting events are append-only | UBQ §Accounting Event | Slice 5 |
| Payout share snapshot for audit | UBQ §Payout Share Snapshot | Slice 7 |
| Template epoch: monotonically increasing counter | UBQ §Template Epoch | Slice 2, Slice 6 |
| Job ID format: `job-{template_id}-{epoch}` | UBQ §Job | Slice 2 |

---

## Stratum V1 Protocol Compatibility

Notes on Stratum V1 extension methods and their current status between this server and the canonical GPU miner (`lotus-gpu-miner`).

### Verified Compatibility (canonical GPU miner)

The miner at [`lotus-gpu-miner`](../../../../lotus-gpu-miner/) sends only these client-to-server methods:
- `mining.subscribe` — supported ✅
- `mining.authorize` — supported ✅
- `mining.submit` — supported ✅
- `mining.ping` — supported ✅ (returns `true` with no error)

The miner handles these server-to-client notifications:
- `mining.set_difficulty` — supported ✅
- `mining.set_extranonce` — supported ✅ (but server does not yet send this; see below)
- `mining.notify` — supported ✅

All core Stratum V1 flows work. The miner connects, subscribes, authorizes, receives jobs and difficulty updates, and submits shares without issues.

### Discrepancies

#### `mining.extranonce.subscribe` (client-to-server)
**Status:** 🟡 Ignored (returns "unknown method" error)
**GPU miner:** Does NOT send this method. No compatibility impact for canonical miner.
**Third-party impact:** Some Stratum V1 proxy software (e.g., btcproxy, cgminer forks) send this during initialization. An error response may cause warnings or disconnection.
**Fix priority:** Low — implement as no-op success response for third-party compatibility.

#### `mining.suggest_difficulty` (client-to-server)
**Status:** 🟡 Ignored (returns "unknown method" error)
**GPU miner:** Does NOT send this method. No compatibility impact for canonical miner.
**Third-party impact:** Some Stratum miners send this as a courtesy to suggest a preferred difficulty. Returning an error is spec-compliant.
**Fix priority:** Low — return `true` as a no-op for third-party compatibility.

#### `mining.set_extranonce` (server-to-client)
**Status:** 🔴 Server does not send this notification
**GPU miner:** Handles this correctly (updates extranonce1 and extranonce2_size).
**Current behavior:** The server assigns a unique extranonce1 at session creation and never changes it. This method is not needed for the current design, but may be required if extranonce rotation is ever implemented (e.g., for privacy or session merging).
**Fix priority:** None — not needed unless extranonce rotation is added.

#### `mining.set_difficulty` (server-to-client — P_diff clamp on N_diff drop)
**Status:** 🔴 Gap — See Gap 4 in architecture review
**Details:** When N_diff drops and a session's VarDiff ceiling contracts, P_diff is silently clamped down. The miner is NOT notified via `mining.set_difficulty`. See the architecture review for the fix.

### Future Work

When implementing support for third-party miners or proxies:
1. Return `true` for `mining.extranonce.subscribe` (no-op, no actual extranonce subscription)
2. Return `true` for `mining.suggest_difficulty` (no-op, ignore suggested difficulty)
3. These changes are trivial and can be done as part of any maintenance cycle.
