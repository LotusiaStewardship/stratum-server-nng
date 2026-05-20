# Stratum Core Context

**Last updated:** 2026-05-19  
**Related spec:** [Modular Architecture Refactor](./specs/modular-architecture-refactor-slices.md)  
**Ubiquitous Language:** [UBIQUITOUS_LANGUAGE.md](../../UBIQUITOUS_LANGUAGE.md)

---

## Bounded Context

The **Stratum Core** context owns the Stratum V1 mining protocol implementation, share validation pipeline, session management with per-session VarDiff, and HTTP API for pool operators.

### Boundary

- **Inside:** TCP server, protocol parsing, session state machine, share validation, VarDiff, share persistence, round accounting, HTTP API
- **Outside:** Node integration (NNG RPC/pub-sub), payout calculation and signing, external monitoring

### Dependencies

| Module | Depends On | Purpose |
|--------|-----------|---------|
| `stratum_protocol` | `share_processing`, `accounting`, `node_integration` | TCP server, session management, protocol parsing |
| `share_processing` | None (standalone) | VarDiff, share validation |
| `accounting` | None (standalone) | DB schema, repositories, accounting facade |
| `http_api` | `accounting` | REST API for pool operators |
| `node_integration` | `stratum_protocol::job` | NNG RPC client, template conversion |
| `shutdown` | None (standalone) | Graceful shutdown coordinator |

---

## Module Responsibilities

### `stratum_protocol`
- TCP listener accepts miner connections
- Protocol parsing (JSON-RPC, Stratum methods)
- Session state machine (subscribe → authorize → submit)
- Job assignment and tracking (`assigned_jobs` map per session)
- `mining.set_difficulty` sent on session start and after VarDiff retarget
- `mining.notify` sent after authorization

### `share_processing`
- `VarDiff` — per-session variable difficulty controller
- `validator` — share validation pipeline (format, authorization, ntime, difficulty)
- `network_target_hex_to_difficulty` — shared helper for N_diff computation

### `accounting`
- `WorkerRepository` — upsert/query workers by `(payout_address, worker_suffix)`
- `ShareRepository` — insert shares and outcomes atomically, dedupe key enforcement, queries
- `RoundRepository` — round lifecycle (open → found → closed → paid → orphaned)
- `AccountingEventRepository` — append-only audit log
- `AccountingService` — facade orchestrating multi-repo operations

### `http_api`
- Axum-based REST API
- Bearer token authentication (except `/health`)
- Endpoints: health, stats, workers, rounds

### `node_integration`
- NNG RPC client for lotusd communication
- Template → MiningJob conversion
- Job cache (LRU eviction)

---

## Key Invariants

### Session
- A session must subscribe before it can authorize
- A session must authorize before receiving `mining.notify`
- Extranonce1 is unique per session and never reused
- `assigned_jobs` is capped at `MAX_ASSIGNED_JOBS_PER_SESSION` (128)
- `clean_jobs=true` clears all assigned jobs immediately

### VarDiff
- P_diff ∈ [vardiff_min_floor, N_diff] at all times
- P_diff is clamped down when N_diff decreases (via `update_max`)
- Share difficulty = P_diff at assignment time (immutable)
- VarDiff is per-session, not per-worker
- `mining.set_difficulty` is sent on session start and after each retarget

### Share Validation
- Worker must be in session's authorized set
- Job must be in session's assigned_jobs (not stale)
- Submitted ntime must match frozen ntime from assigned job
- Header hash must meet P_diff target
- N_diff check is recorded as `network_target_ok` flag (not a rejection)
- All shares (accepted and rejected) are persisted
- Share + outcome insertion is atomic (transaction)

### Accounting
- Shares are immutable once persisted (never deleted)
- Dedupe key prevents duplicate insertions: `worker_id:template_id:template_epoch:extranonce2:ntime:nonce`
- Round membership is resolved at insert time via `resolve_round_for_template`
- Accounting events are append-only (never updated or deleted)
- Workers persist across sessions

### HTTP API
- Health endpoint is public (no auth)
- All other endpoints require `Authorization: Bearer <token>`
- Token is configured via `api_token` in config or `STRATUM_API_TOKEN` env var

---

## Cross-Cutting Concerns

### Graceful Shutdown
- All long-running tasks register with `ShutdownCoordinator`
- Shutdown signal is a broadcast channel
- Tasks stop accepting work, flush, then exit
- WAL checkpoint on shutdown if database connection is available
- Total shutdown timeout: 30s; flush timeout: 5s

### Template Lifecycle
- Template fetched once on startup via NNG RPC
- Converted to MiningJob and cached in JobCache (LRU, max 512)
- N_diff broadcast to all sessions via `StratumServer::notify_new_job()`
- Slice 6 will add event-driven refresh via NNG pub/sub

---

## Future Considerations (Post-Slice 5)

- Slice 6: NNG pub/sub (miningwrchg, blkconnected, blkdisconctd), found blocks, reorg handling
- Slice 7: PPLNS payout calculation
- Slice 8: Complete HTTP API (shares, blocks, payouts, pagination)
- Slice 9: Payout signer abstraction
