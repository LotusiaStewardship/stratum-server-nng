# HTTP API Context

**Last updated:** 2026-05-22  
**Related spec:** [Modular Architecture Refactor](../stratum-core/specs/modular-architecture-refactor-slices.md)  
**Ubiquitous Language:** [UBIQUITOUS_LANGUAGE.md](../../UBIQUITOUS_LANGUAGE.md)

---

## Bounded Context

The **HTTP API** context owns the Axum-based REST API for pool operators. It provides health monitoring, data queries (workers, rounds, blocks, payouts), and administrative actions (payout triggering).

### Boundary

- **Inside:** Axum HTTP server, route handlers, authentication middleware, `AppState` wiring, request/response DTOs
- **Outside:** Business logic (share validation, payout calculation), database access (delegated to Accounting), Stratum protocol state

### Dependencies

| Module | Depends On | Purpose |
|--------|-----------|---------|
| `server` | None (standalone) | Axum Router construction, auth middleware, AppState |
| `routes/health` | `AppState` | GET /api/v1/health |
| `routes/stats` | `AppState`, `ShareRepository` | GET /api/v1/stats |
| `routes/workers` | `AppState`, `WorkerRepository`, `ShareRepository` | GET /api/v1/workers, /workers/{id} |
| `routes/rounds` | `AppState`, `RoundRepository`, `ShareRepository` | GET /api/v1/rounds, /rounds/{id} |
| `routes/blocks` | `AppState`, `FoundBlockRepository` | GET /api/v1/blocks, /blocks/{hash} |
| `routes/payouts` | `AppState`, `PayoutRepository`, `AccountingService` | GET /api/v1/payouts, /payouts/{id}, POST /admin/payouts/trigger/{block_hash} |

### Module Structure

```
src/http_api/
├── mod.rs            # Re-exports
├── server.rs         # AppState, create_router(), PayoutConfig, auth middleware
└── routes/
    ├── mod.rs        # Re-exports all route handlers
    ├── health.rs     # GET /api/v1/health
    ├── stats.rs      # GET /api/v1/stats
    ├── workers.rs    # GET /api/v1/workers, /workers/{id}
    ├── rounds.rs     # GET /api/v1/rounds, /rounds/{id}
    ├── blocks.rs     # GET /api/v1/blocks, /blocks/{hash}
    └── payouts.rs    # GET /api/v1/payouts, /payouts/{id}, POST /admin/payouts/trigger/{block_hash}
```

### Endpoints

| Method | Path | Auth | Description |
|--------|------|------|-------------|
| GET | `/api/v1/health` | No | Server status, uptime, connected miners |
| GET | `/api/v1/stats` | Yes | Share counts, acceptance rate, rejection breakdown |
| GET | `/api/v1/workers` | Yes | List workers with share counts |
| GET | `/api/v1/workers/{id}` | Yes | Worker details |
| GET | `/api/v1/rounds` | Yes | List rounds with optional status filter |
| GET | `/api/v1/rounds/{id}` | Yes | Round details with per-worker share breakdown |
| GET | `/api/v1/blocks` | Yes | List found blocks with optional status filter |
| GET | `/api/v1/blocks/{hash}` | Yes | Block details |
| GET | `/api/v1/payouts` | Yes | List payout batches with optional status filter |
| GET | `/api/v1/payouts/{id}` | Yes | Batch details with per-miner payout breakdown |
| POST | `/api/v1/admin/payouts/trigger/{block_hash}` | Yes | Manually trigger payout calculation for a confirmed block |

### Auth

- Bearer token via `Authorization: Bearer <token>` header.
- Token configured via `api_token` in config or `STRATUM_API_TOKEN` env var.
- Health endpoint is unauthenticated; all others return 401 on missing/mismatched token.

### AppState

```rust
pub struct AppState {
    pub stats: Arc<RwLock<ServerStats>>,
    pub share_repo: Option<ShareRepository>,
    pub worker_repo: Option<WorkerRepository>,
    pub round_repo: Option<RoundRepository>,
    pub found_block_repo: Option<FoundBlockRepository>,
    pub payout_repo: Option<PayoutRepository>,
    pub accounting_service: Option<AccountingService>,
    pub payout_config: Option<PayoutConfig>,
    pub api_token: String,
}
```

All repository fields are `Option` — individual route handlers return `AppError::DbNotConfigured` if a required repository is missing. In normal operation (started via `main.rs`), all repositories are populated.

### Known Limitations (Slice 8 scope)

- No pagination (`limit`/`offset`) on list endpoints
- No `GET /api/v1/shares` endpoint
- No `GET /api/v1/rounds/{id}/shares` endpoint
- No hashrate endpoint (5-min rolling window)
- No error correlation IDs on 4xx/5xx responses
