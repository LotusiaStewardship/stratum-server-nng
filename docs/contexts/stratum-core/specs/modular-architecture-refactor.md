# Modular Architecture Refactor

**Status:** Superseded by slices
**Context(s):** Stratum Core, Accounting, Payout, Node Integration  
**Date:** 2026-05-18  
**Updated:** 2026-05-22  

> This document is the original architecture proposal. The implementation followed the
> [vertical slices spec](./modular-architecture-refactor-slices.md) derived from this document.
> Implementation divergences are noted below.

## Problem

The existing stratum-server-nng implementation has become difficult to maintain due to monolithic domain modules that conflate multiple responsibilities:

1. **Accounting module** contains all table creation, inserts, and queries in a single file, making it hard to:
   - Understand data ownership boundaries
   - Add new tables or modify existing schemas
   - Test database operations in isolation
   - Track which operations belong to which aggregate

2. **Stratum server runtime** mixes connection handling, share validation, job distribution, and difficulty management in a single `handle_conn` function that spans 1000+ lines

3. **Share validation** occurs in the hot path, blocking response to miners while cryptographic operations complete

4. **Difficulty tracking** is global when it should be per-session (different miners require different difficulties to achieve target share rates)

5. **Payout logic** is tightly coupled to PPLNS-specific implementation, making it difficult to add alternative payout schemes (PPS, PROP)

6. **HTTP API** includes dashboard-specific endpoints that belong in a separate consumer application

These architectural issues make the codebase harder to test, extend, and maintain as the pool scales.

## Solution

Refactor the codebase into properly separated modules with clear boundaries, deep interfaces, and asynchronous processing where appropriate. The refactored system will:

- Maintain identical Stratum V1 protocol behavior and miner compatibility
- Preserve NNG event-driven template refresh with coalescing
- Keep PPLNS payout scheme as default but make it pluggable
- Separate accounting operations by aggregate root
- Move share validation to async processing pipeline
- Implement per-session difficulty tracking
- Expose HTTP API for external dashboard consumption (dashboard UI out of scope)

## User Stories

1. **As a pool operator**, I want to add a new accounting table (e.g., worker statistics) without modifying unrelated database code, so that I can extend tracking without risking regressions

2. **As a developer**, I want share validation to occur asynchronously so that miner response latency is minimized and validation can be scaled independently

3. **As a pool operator**, I want each miner session to have independent difficulty tracking so that miners with different hashrates all achieve the target share rate (default: 1 share per 20 seconds)

4. **As a developer**, I want to add a new payout scheme (e.g., PPS) without modifying existing PPLNS code, so that payout strategies can be tested and deployed independently

5. **As a dashboard developer**, I want a clean HTTP API that exposes pool state (workers, shares, rounds, found blocks) without dashboard-specific rendering logic, so that I can build UI in a separate repository

6. **As a pool operator**, I want the payout mechanism to be hookable to external scheduling software, so that I can choose between built-in automation or third-party payout management

7. **As a developer**, I want to understand module boundaries clearly so that I can navigate the codebase and make changes without unintended side effects

## Implementation Decisions

### Module Architecture

The system will be organized into the following bounded contexts:

#### 1. Stratum Protocol Context (`stratum-protocol`)
**Responsibility:** Stratum V1 wire protocol, session management, message parsing

**Modules:**
- `protocol` — Stratum V1 message types (request/response), JSON parsing, method dispatch
- `session` — Per-session state (extranonce1, subscription status, authorized workers, active jobs)
- `server` — TCP listener, connection acceptance, session lifecycle
- `job` — Mining job representation, notify parameter construction

**Interfaces:**
- Input: Raw TCP lines from miners
- Output: Stratum responses, mining.notify broadcasts
- Dependencies: None (pure protocol layer)

**Deep Module Opportunity:** `protocol` module exposes only `parse_line(&str) -> Result<StratumRequest>` and `format_response(&StratumResponse) -> String`, hiding all JSON and method parsing complexity.

#### 2. Share Processing Context (`share-processing`)
**Responsibility:** Share validation, difficulty tracking, share persistence

**Modules:**
- `validator` — Synchronous share validation (difficulty check, header hash computation). Must complete before Stratum response sent.
- `difficulty` — Per-session VarDiff controller with retarget logic
- `persistence` — Share persistence interface (abstracted from SQLite). Can be async/queued after response sent.
- `repository` — Share query interface for HTTP API and payout calculations

**Interfaces:**
- Input: Validated `mining.submit` requests with session context
- Output: Share acceptance/rejection results, persisted share records
- Dependencies: Stratum Protocol (for job lookup), Accounting (for persistence)

**Key Decision:** Share validation is architecturally separated into the `share-processing` context but executes **synchronously** in the response path. Stratum V1 protocol requires immediate accept/reject response — there is no "pending" state. The validation logic is extracted into a dedicated module with clean interfaces, enabling independent testing and future optimization, but blocks the response until complete.

**Per-Session Difficulty:** Each session maintains independent `VarDiff` instance with:
- `current_difficulty`: Current share difficulty for this miner
- `target_share_secs`: Target time between shares (default 20s)
- `retarget_interval_secs`: How often to adjust (default 60s)
- `share_history`: Timestamps of recent accepted shares for rate calculation

**Invalid Share Handling (Scaffold):** The share validator will track rejection statistics per-session but will NOT disconnect miners on initial release. A `MinerPolicy` trait will be scaffolded for future use:
```rust
trait MinerPolicy: Send + Sync {
    fn record_share(&mut self, accepted: bool);
    fn should_disconnect(&self) -> bool;
    fn disconnect_reason(&self) -> Option<&'static str>;
}
```
Initial implementation: `LoggingPolicy` — logs all rejections, never disconnects. Future implementations can add threshold-based banning once pool stability is proven.

#### 3. Accounting Context (`accounting`)
**Responsibility:** Persistent storage of pool state

**Modules (organized by aggregate root):**
- `worker` — Worker registration, payout address mapping, worker suffix tracking
  - `worker_repository.rs` — CRUD operations for workers table
  - `worker_schema.rs` — Table DDL, indexes
- `share` — Share records (accepted, rejected, stale)
  - `share_repository.rs` — Insert, query by worker/time/window
  - `share_schema.rs` — Table DDL, indexes
- `round` — Mining round lifecycle (open, found, closed, paid)
  - `round_repository.rs` — Round creation, closure, status queries
  - `round_schema.rs` — Table DDL
- `found_block` — Found block tracking (confirmed, matured, orphaned, paid)
  - `found_block_repository.rs` — Block persistence, status updates, orphan handling
  - `found_block_schema.rs` — Table DDL
- `payout` — Payout batches, dust tracking, payout history
  - `payout_repository.rs` — Batch creation, payout records
  - `payout_schema.rs` — Table DDL
- `migrations` — Schema versioning, migration execution
  - `migrations.rs` — Migration runner, version tracking

**Interfaces:**
- Input: Domain events (share submitted, block found, round closed)
- Output: Query results for HTTP API, payout calculations
- Dependencies: None (pure persistence layer)

**Key Decision:** Each module owns its schema DDL and repository operations. No central "database module" — instead, each aggregate root is self-contained.

**Blockchain Reorg Handling:** All repository operations must support updates and cascading changes. When a block is orphaned:
1. `found_block_repository::mark_orphaned()` — Update block status
2. `share_repository::invalidate_by_round()` — Mark shares in affected round as orphaned
3. `round_repository::close_orphaned()` — Close round with orphaned status
4. `payout_repository::reverse_by_block()` — Reverse any payouts for orphaned block

This is critical for live blockchain operation — orphans and reorgs are not edge cases, they are expected events.

#### 4. Payout Context (`payout`)
**Responsibility:** Reward distribution calculation and transaction construction

**Modules:**
- `scheme` — Payout scheme trait and implementations
  - `payout_scheme.rs` — Trait: `calculate_distribution(&FoundBlock, &PplnsWindow) -> PayoutPlan`
  - `pplns.rs` — PPLNS implementation (share-weighted window, dust carry-forward)
  - `pps.rs` — PPS scaffold (future implementation)
- `plan` — Payout plan representation (outputs, dust, fees)
- `window` — PPLNS window computation (share aggregation, work unit calculation)
- `transaction` — Coinbase transaction construction, output assembly
- `signer` — Transaction signing abstraction (internal key, external HSM, multisig)
  - `signer.rs` — Trait: `sign_and_submit(&PayoutPlan) -> Result<Txid>`
  - `internal_signer.rs` — Internal private key signing
  - `external_signer.rs` — External signer integration (future)

**Interfaces:**
- Input: Found block event, PPLNS window shares
- Output: PayoutPlan with outputs and dust, submitted txid
- Dependencies: Accounting (for share queries), Node Integration (for submission)

**Extensibility:** Payout schemes implement the `PayoutScheme` trait. Adding PPS requires only implementing the trait and registering in configuration.

**Signer Abstraction:** Two integration modes supported:
- **In-process trait**: `Signer::sign_and_submit(&PayoutPlan) -> Result<Txid>` for internal keys, HSM libraries
- **HTTP webhook**: POST payout plan to external service, poll for txid submission
Configuration determines mode; both produce same `PayoutPlan` output.

#### 5. Node Integration Context (`node-integration`)
**Responsibility:** Communication with lotusd node

**Modules:**
- `nng` — NNG RPC and pub/sub adapters
  - `rpc_client.rs` — NNG RPC calls (get_mining_template, get_block)
  - `pub_sub.rs` — Event subscription, event coalescing, debouncing
- `json_rpc` — JSON-RPC HTTP client for block submission
  - `submitblock.rs` — Block submission via JSON-RPC
  - `getblockcount.rs` — Chain tip queries
- `template` — Mining template management, refresh logic
- `events` — Normalized node events (MiningWorkChanged, BlockConnected, BlockDisconnected)

**Interfaces:**
- Input: NNG pub/sub messages, template refresh requests
- Output: MiningTemplate, block submission results, node events
- Dependencies: None (infrastructure layer)

**Event Coalescing:** Preserve 100ms debounce window for `MiningWorkChanged` events to reduce template refresh frequency during high mempool variance. Accounting events (`BlockConnected`, `BlockDisconnected`) processed immediately.

#### 6. HTTP API Context (`http-api`)
**Responsibility:** Operator API for monitoring and external dashboard integration

**Modules:**
- `server` — HTTP server setup, authentication middleware
- `routes` — API endpoints (organized by resource)
  - `workers.rs` — Worker list, individual worker details, hashrate
  - `shares.rs` — Share queries (by worker, time range, status)
  - `rounds.rs` — Round history, current round status
  - `blocks.rs` — Found blocks, confirmation status, payout status
  - `payouts.rs` — Payout batches, individual payouts
  - `health.rs` — Health checks, server stats
- `models` — API request/response DTOs (separate from domain models)
- `pagination` — Common pagination logic

**Interfaces:**
- Input: HTTP requests with bearer token auth
- Output: JSON API responses
- Dependencies: Accounting (for queries), Stratum Protocol (for live stats)

**Out of Scope:** Dashboard HTML rendering, WebSocket live updates, static asset serving. These belong in separate dashboard repository.

### Cross-Cutting Concerns

#### Configuration
- Single `config.rs` module with structured configuration types
- Network auto-detection from RPC port preserved
- Validation on startup (scripts, addresses, VarDiff parameters)

#### Graceful Shutdown
The server MUST handle shutdown signals gracefully to prevent:
- Lost in-flight share submissions
- Database corruption (unfinished transactions)
- Hanging TCP connections
- Leaked NNG connections

**Shutdown sequence:**
1. Stop accepting new TCP connections (Stratum + HTTP)
2. Signal all active sessions to disconnect (send notification, close sockets)
3. Wait for in-flight share validations to complete (max 5s timeout)
4. Flush pending share records to database
5. Close NNG pub/sub and RPC connections
6. Close SQLite connection (ensure WAL checkpoint)
7. Exit process

**Signal handling:**
- SIGINT (Ctrl+C) — graceful shutdown
- SIGTERM — graceful shutdown
- SIGQUIT — immediate shutdown (no flush, for emergencies)

**Implementation:**
- Use `tokio::select!` with shutdown channel in all long-running tasks
- Each task registers for shutdown and completes cleanup in order
- Main function waits for all tasks to exit before process termination
- Max shutdown timeout: 30s (force exit after)

**Testing:**
- Integration test: start server, submit shares, send SIGINT, verify shares persisted
- Verify no database corruption after shutdown during active mining

#### Database Access
- **Single SQLite connection** with serialized access (no connection pool)
- Rationale: SQLite is optimized for single-writer pattern; connection pooling adds complexity without benefit for this workload
- Write operations: serialized through single connection
- Read operations (HTTP API): can use separate read-only connection if profiling shows contention
- All repositories accept `&SqliteConnection` reference, not owned connection

#### Logging and Observability
- Structured logging with tracing crate
- Metrics endpoints for Prometheus (future)
- Session-level tracing IDs for request correlation

#### Error Handling
- Domain-specific error types per context
- Error conversion traits for cross-context boundaries
- User-facing error messages separated from internal diagnostics

### Data Flow

```
┌──────────────────────────────────────────────────────────────────────────┐
│                           Stratum Miner                                  │
└────────────────────────────────┬─────────────────────────────────────────┘
                                 │ Stratum V1 TCP
                                 ▼
┌──────────────────────────────────────────────────────────────────────────┐
│                        Stratum Protocol Context                          │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐     │
│  │   server    │──│  protocol   │──│   session   │──│    job      │     │
│  └─────────────┘  └─────────────┘  └─────────────┘  └─────────────┘     │
└────────────────────────────────┬─────────────────────────────────────────┘
                                 │
              ┌──────────────────┼──────────────────┐
              │ (synchronous)    │                  │
              ▼                  ▼                  ▼
    ┌─────────────────┐ ┌─────────────────┐ ┌─────────────────┐
    │ Share Processing│ │  Node Integration│ │   HTTP API      │
    │   Context       │ │    Context      │ │    Context      │
    │                 │ │                 │ │                 │
    │ ┌─────────────┐ │ │ ┌─────────────┐ │ │ ┌─────────────┐ │
    │ │  validator  │ │ │ │    nng      │ │ │ │   routes    │ │
    │ └─────────────┘ │ │ └─────────────┘ │ │ └─────────────┘ │
    │ ┌─────────────┐ │ │ ┌─────────────┐ │ │ ┌─────────────┐ │
    │ │  difficulty │ │ │ │  json_rpc   │ │ │ │   models    │ │
    │ └─────────────┘ │ │ └─────────────┘ │ │ └─────────────┘ │
    │ ┌─────────────┐ │ │ ┌─────────────┐ │ │                 │
    │ │ persistence │ │ │ │  template   │ │ │                 │
    │ └─────────────┘ │ │ └─────────────┘ │ │                 │
    └────────┬────────┘ └────────┬────────┘ └─────────────────┘
             │                   │
             ▼                   ▼
    ┌─────────────────────────────────────────┐
    │         Accounting Context              │
    │                                         │
    │  ┌─────────┐ ┌─────────┐ ┌───────────┐ │
    │  │ worker  │ │  share  │ │   round   │ │
    │  └─────────┘ └─────────┘ └───────────┘ │
    │  ┌─────────────┐ ┌─────────┐          │
    │  │ found_block │ │ payout  │          │
    │  └─────────────┘ └─────────┘          │
    └─────────────────────────────────────────┘
             │
             ▼
    ┌─────────────────────────────────────────┐
    │           Payout Context                │
    │                                         │
    │  ┌─────────────┐ ┌───────────────────┐  │
    │  │   scheme    │ │    transaction    │  │
    │  │  (pplns)    │ │      signer       │  │
    │  └─────────────┘ └───────────────────┘  │
    └─────────────────────────────────────────┘
             │
             ▼
    ┌─────────────────────────────────────────┐
    │              lotusd                     │
    │    (NNG + JSON-RPC)                     │
    └─────────────────────────────────────────┘
```

### Schema Changes

No schema changes required — existing tables preserved:
- `workers` — Worker registration
- `shares` / `share_outcomes` — Share records
- `rounds` — Mining rounds
- `found_blocks` — Found block tracking
- `payout_batches` / `payouts` — Payout history
- `schema_migrations` — Version tracking

Schema DDL moved from central location to per-module `*_schema.rs` files.

### API Contracts

#### HTTP API Endpoints (New)

```
GET /api/v1/workers
  - List all workers with hashrate, share counts
  - Query params: pagination, payout_address filter

GET /api/v1/workers/{worker_id}
  - Individual worker details
  - Includes: shares accepted/rejected, hashrate, last share time

GET /api/v1/shares
  - Query shares by worker, time range, status
  - Query params: worker_id, from, to, status, pagination

GET /api/v1/rounds
  - List rounds with status, found block hash, total work
  - Query params: status filter, pagination

GET /api/v1/rounds/{round_id}
  - Round details with share breakdown by worker

GET /api/v1/blocks
  - Found blocks with confirmation status, payout status
  - Query params: status filter, height range, pagination

GET /api/v1/payouts
  - Payout batches with txid, total amount, miner count
  - Query params: status filter, pagination

GET /api/v1/health
  - Server health check
  - Returns: uptime, connected miners, last template refresh

GET /api/v1/stats
  - Aggregated statistics
  - Returns: total shares, total blocks, pool hashrate, network difficulty
```

**Authentication:** Bearer token via `Authorization: Bearer <token>` header

### Testing Strategy

**Unit Tests:**
- Protocol parsing (valid/invalid Stratum messages)
- VarDiff retarget logic (share rate calculations, clamping)
- PPLNS window computation (work unit aggregation, dust carry-forward)
- Payout plan construction (remainder distribution, fee calculation)
- Share validation (difficulty checks, header hash computation)

**Integration Tests:**
- Full stratum session (subscribe, authorize, submit share)
- Template refresh on NNG event
- Block submission via JSON-RPC
- Payout batch creation and signing

**Test Boundaries:**
- Mock NNG pub/sub events for template refresh tests
- Mock JSON-RPC responses for block submission tests
- In-memory SQLite for repository tests
- Deterministic time for VarDiff and PPLNS tests

### Out of Scope

The following are explicitly **not** part of this refactor:

1. **Dashboard UI** — HTML rendering, WebSocket live updates, static assets (belongs in separate `stratum-dashboard` repository)

2. **Payout Scheduler Automation** — Automatic payout triggering on schedule (the payout mechanism will be hookable, but scheduling logic is out of scope)

3. **Stratum V2 Support** — This refactor maintains Stratum V1 protocol compatibility only

4. **Multi-Pool Failover** — Single pool instance operation only

5. **Alternative Payout Schemes** — PPS and PROP are scaffolded but not implemented (PPLNS is the only active scheme)

6. **Prometheus Metrics** — Metrics endpoints are future work

## Resolved Decisions

The following open questions have been resolved:

1. **Invalid share handling**: No banning on initial release. Rejected shares are logged for operator review only. The architecture will include scaffolding for pluggable miner connection handling policies (e.g., reject-rate-based disconnection) that can be enabled once the pool is proven stable with consistent miners.

2. **Payout signer extensibility**: Support both approaches:
   - **Trait-based plugin system** for in-process signers (internal key, HSM integration)
   - **HTTP callbacks** for external signer services (webhook-style notifications)
   This allows flexibility for both self-hosted and third-party payout management.

3. **HTTP API versioning**: URL versioning (`/api/v1/`, `/api/v2/`) for simplicity and explicitness.

4. **Database connection pooling**: Single connection with serialized access for initial implementation. SQLite is optimized for this pattern and it eliminates connection pool complexity. Can revisit pooling if profiling shows contention.

5. **CRUD for blockchain events**: Direct CRUD with full update/delete support is required. The Lotus blockchain can reorg, orphan blocks, and double-spend. The accounting system must support:
   - Marking found blocks as orphaned when `blkdisconctd` events arrive
   - Reversing share outcomes associated with orphaned blocks
   - Closing rounds that were thought to be won but actually orphaned
   - Adjusting payout records when blocks are orphaned
   
   This is not optional — it is a core requirement for operating on a live blockchain.
