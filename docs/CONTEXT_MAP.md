# Context Map

**Last updated:** 2026-05-27

Bounded contexts within the `stratum-server-nng` binary. All contexts live in a single process but have clear module boundaries and ownership.

## Context Overview

```
                    ┌─────────────────┐
                    │   lotusd (node) │  (external — not in this repo)
                    └────────┬────────┘
                             │ NNG pub/sub + RPC
                             ▼
┌─────────────────────────────────────────────────┐
│              Node Integration                   │
│  (template fetch, event consumer, job cache,    │
│   chain state, payout maturation triggers)      │
└───────┬──────────────────────┬──────────────────┘
        │ MiningJob            │ matured block hashes
        ▼                      ▼
┌──────────────────┐  ┌──────────────────┐
│   Stratum Core   │  │   HTTP API       │
│ (TCP server, protocol│  │ (operator routes) │
│  sessions, VarDiff,  │  └────────┬─────────┘
│  share validation)    │           │
└───────────┬──────────┘           │
            │ shares, outcomes     │ queries
            ▼                      │
┌──────────────────────┐           │
│     Accounting       │◄──────────┘
│ (SQLite repositories,│
│  schema, facade)     │
└───────────┬──────────┘
            │ shares, rounds, blocks
            ▼
┌──────────────────────┐
│       Payout         │
│ (PPLNS, plan, dust)  │
└──────────────────────┘
```

## Bounded Contexts

Each context has a dedicated CONTEXT.md with full boundary and invariant documentation:
- [Accounting](contexts/accounting/CONTEXT.md)
- [HTTP API](contexts/http-api/CONTEXT.md)
- [Node Integration](contexts/node-integration/CONTEXT.md)
- [Payout](contexts/payout/CONTEXT.md)
- [Stratum Core](contexts/stratum-core/CONTEXT.md)

### Stratum Core

- **Owns:** TCP listener, Stratum V1 protocol parsing (subscribe/authorize/submit), per-session state machine, VarDiff controller, share validation pipeline
- **Module:** `src/stratum_protocol/`, `src/share_processing/`
- **Depends on:** `Node Integration` (for live `MiningJob` data and job broadcasts), `Accounting` (for persisting shares/outcomes/events)
- **Key contracts:** `MiningJob` received via `broadcast::Receiver` from JobCache; `ValidationResult` produced per share; `SessionState` tracks per-connection state

### Node Integration

- **Owns:** NNG RPC client (template fetching), NNG pub/sub event consumer (template changes, block disconnects), JSON-RPC HTTP client (block submission, chain queries), block building/submitblock, job caching
- **Module:** `src/node_integration/`
- **Depends on:** `Accounting` (for found_block tracking via NNG event consumer), `Stratum Core` (for job broadcasting)
- **Key contracts:** `template_to_job()` converts `MiningTemplate` → `MiningJob`; `JobCache` stores jobs for staleness checks; `build_submit_block()` reconstructs full block for submission

### Accounting

- **Owns:** SQLite database schema and initialization, all repository CRUD (workers, shares, share_outcomes, rounds, found_blocks, accounting_events, payout_batches, payouts, dust_balances, payout_share_snapshots), `AccountingService` facade for multi-step operations
- **Module:** `src/accounting/`
- **Depends on:** Nothing internal (pure data access layer)
- **Key contracts:** `AccountingService` provides unified interface for share recording, round management, found block tracking, payout creation; all writes go through SQLite with WAL mode

### Payout

- **Owns:** PPLNS window calculation, payout plan construction, dust carry-forward logic
- **Module:** `src/payout/`
- **Depends on:** `Accounting` (reads share data via `ShareRepository/ShareOutcomeRepository` pattern, writes payout batches via `PayoutRepository`)
- **Key contracts:** `calculate_pplns_window()` returns windowed shares; `build_payout_plan()` creates `PayoutPlan` with outputs; dust balance updated atomically with payout batch

### HTTP API

- **Owns:** Axum HTTP server, all route handlers (health, stats, workers, rounds, blocks, payouts), auth middleware, `AppState` wiring
- **Module:** `src/http_api/`
- **Depends on:** `Accounting` (for data queries), `Stratum Core` (for connected_miners count)
- **Key contracts:** `AppState` holds optional repository references; routes return JSON with consistent error handling

## Cross-Context Concerns

| Concern | Primary Owner | Secondary |
|---------|--------------|-----------|
| Share lifecycle (submit → validate → persist) | Stratum Core | Accounting |
| Found block lifecycle (detect → track → payout) | Node Integration → Accounting → Payout | — |
| Miner session lifecycle (connect → auth → submit → disconnect) | Stratum Core | — |
| Template lifecycle (fetch → cache → broadcast → stale) | Node Integration | Stratum Core |
| Database schema migrations | Accounting | — |

## Shared Kernel

- **`MiningJob`** struct — produced by Node Integration `template_to_job()`, consumed by Stratum Core for notify and validation, cached in `JobCache`
- **`Share` / `ShareOutcome`** — produced by Stratum Core validation, consumed by Accounting for persistence, consumed by Payout for PPLNS window
- **`SessionState`** — owned by Stratum Core, drives authorization and assigned_job staleness checks
- **`VarDiffConfig`** — config-derived, used by Stratum Core for per-session difficulty control
