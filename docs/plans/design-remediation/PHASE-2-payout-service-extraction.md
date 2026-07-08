# Phase 2: PayoutService Extraction + Repository Move

**Status:** Proposed  
**Bullets:** 1 (PayoutService), 2 (Repository move)  
**Theme:** Structural — moves code between modules  
**Risk:** High — touches wiring in main.rs, PayoutHandler, AppState  
**Estimated effort:** 2 sessions  

---

## Overview

Phase 2 is the structural core of the remediation. It resolves the cyclic dependency between `accounting` and `payout` by:

1. **Bullet 1:** Creating `PayoutService` in the payout context and moving 3 payout orchestration methods out of `AccountingService`.
2. **Bullet 2:** Moving `PayoutRepository` and payout table DDL from the accounting module to the payout module.

These are the only structural moves in the entire plan — everything else is additive or surface-level.

---

## Bullet 1: Extract PayoutService

### Domain Position
- **Context:** Payout (moves from Accounting)
- **Role:** Core logic — orchestrates PPLNS window → payout plan → batch creation → signing
- **Concepts:** `PayoutService`, payout plan, payout batch, dust tracking

### Current AccountingService Public API

Verified in `src/accounting/service.rs`:

| Method | Line | Stays or Moves |
|--------|------|----------------|
| `new()` | 32 | Stays (repos only, no payout_repo or conn) |
| `record_share()` | 52 | Stays |
| `get_or_create_current_round()` | 159 | Stays |
| `resolve_round_for_template()` | 187 | Stays |
| `close_round()` | 212 | Stays |
| `update_share_outcome_node_result()` | 235 | Stays |
| `record_found_block()` | 245 | Stays |
| `record_event()` | 288 | Stays |
| **`create_payout_for_found_block()`** | **302** | **→ PayoutService** |
| **`rebuild_payout_plan_for_batch()`** | **538** | **→ PayoutService** |
| **`process_pending_payouts()`** | **740** | **→ PayoutService** |
| `reconcile_found_blocks()` | 930 | Stays |
| `check_maturation()` | 1013 | Stays |

### Data Flow

```
Current:                           Intended:
PayoutHandler → AccountingService     PayoutHandler → PayoutService
                   ↕ cyclic dep                                  ↓ one-way
              payout::plan, pplns              accounting::repositories (data only)
```

### Implementation Steps

1. **Create `src/payout/service.rs`**
   ```rust
   pub struct PayoutService {
       accounting: AccountingService,
       payout_repo: PayoutRepository,
       conn: Arc<Mutex<Connection>>,
       fee_bps: u32,
       fee_address: Option<String>,
       min_payout_sat: i64,
       n_multiplier: f64,
       scheme: Box<dyn PayoutScheme>,
       signer: Arc<dyn Signer>,
       rpc_client: Arc<JsonRpcClient>,
   }
   ```

   **Why both `conn` and `accounting`?** The three methods have different dependency patterns:
   - `create_payout_for_found_block` uses raw SQL via `self.conn.lock()` -- it does not call repo methods for inner operations
   - `rebuild_payout_plan_for_batch` also uses raw SQL via `self.conn.lock()` -- same pattern
   - `process_pending_payouts` uses `self.found_block_repo` (to query blocks by round) and `self.payout_repo` (for batch CRUD) -- the former comes from `self.accounting.found_block_repo`
   
   The `conn` field is the same `Arc<Mutex<Connection>>` that AccountingService's repos share (passed into both constructors from main.rs).

2. **Move methods** — copy `create_payout_for_found_block`, `rebuild_payout_plan_for_batch`, `process_pending_payouts` from `AccountingService` to `PayoutService`. Adapt:
   - `self.xxx_repo` → `self.accounting.xxx_repo` (for accounting-owned repos: worker, share, round, event, found_block)
   - `self.payout_repo` → `self.payout_repo` (now on PayoutService)
   - `self.conn.lock()` → `self.conn.lock()` (now on PayoutService)

3. **Update `src/accounting/service.rs`**
   - Remove `payout_repo` field
   - Remove `conn` field  
   - Remove `use crate::payout::*` imports
   - Remove `pub use` of payout repos from `accounting/mod.rs` (after Bullet 2)

4. **Update `src/payout/handler.rs`**
   - `PayoutHandler::new()` takes `PayoutService` instead of `AccountingService`
   - `process_pending_payouts_locked()` calls `self.payout_service.process_pending_payouts()` (no params -- fields already injected)

5. **Update `src/http_api/server.rs` -- AppState addition**
   
   Add new field so the trigger endpoint can find PayoutService:
   ```rust
   pub struct AppState {
       pub stats: Arc<RwLock<ServerStats>>,
       pub share_repo: Option<ShareRepository>,
       pub worker_repo: Option<WorkerRepository>,
       pub round_repo: Option<RoundRepository>,
       pub found_block_repo: Option<FoundBlockRepository>,
       pub payout_repo: Option<PayoutRepository>,
       pub api_token: String,
       pub accounting_service: Option<AccountingService>,
       pub payout_service: Option<PayoutService>,     // NEW
       pub payout_config: Option<PayoutConfig>,
   }
   ```
   The test AppState constructor (line 162) adds `payout_service: None`.

6. **Update `src/http_api/routes/payouts.rs`**
   - `trigger_payout` reads from `state.payout_service` instead of `state.accounting_service`:
   ```rust
   // BEFORE:
   let accounting = state.accounting_service.ok_or(AppError::DbNotConfigured)?;
   let batch_id = accounting.create_payout_for_found_block(&found_block, ...);
   // AFTER:
   let payout_svc = state.payout_service.ok_or(AppError::DbNotConfigured)?;
   let batch_id = payout_svc.create_payout_for_found_block(&found_block, ...);
   ```

7. **Update `src/main.rs`**
   ```rust
   let accounting_service = AccountingService::new(conn.clone());
   let payout_service = PayoutService::new(
       accounting_service.clone(),
       payout_repo.clone(),   // created separately (or from accounting before Bullet 2)
       conn.clone(),
       &config.pool.fee,
       &config.pool.pplns,
       signer,
       rpc_client,
   );
   ```

### Files Modified/Created
| File | Action |
|---|---|
| `src/payout/service.rs` | **Create** — PayoutService |
| `src/accounting/service.rs` | **Modify** — remove 3 methods, remove payout_repo, remove conn |
| `src/payout/handler.rs` | **Modify** -- take PayoutService |
| `src/http_api/server.rs` | **Modify** -- add `payout_service` field to AppState |
| `src/http_api/routes/payouts.rs` | **Modify** -- reference PayoutService |
| `src/main.rs` | **Modify** -- create and wire PayoutService |

### Test Impact
- Move the 3 methods' integration tests from `service.rs` test module to `payout/service.rs`
- Update test references: `accounting.create_payout_for_found_block(...)` → `payout_service.create_payout_for_found_block(...)`
- All existing test scenarios must pass unchanged

---

## Bullet 2: Move PayoutRepository to Payout Context

### Domain Position
- **Context:** Payout (moves from Accounting)
- **Role:** Data access — CRUD for payout_batches, payouts, payout_share_snapshots, dust_balances
- **Concepts:** `PayoutRepository`, payout tables

### Current Ownership

| Artifact | Current Location | Target Location |
|----------|-----------------|-----------------|
| `PayoutRepository` struct + impl | `src/accounting/payout_repository.rs` | `src/payout/repository.rs` |
| Payout table DDL | `src/accounting/schema.rs` | `src/payout/schema.rs` |
| `PayoutBatch`, `Payout`, etc. structs | `src/accounting/payout_repository.rs` | `src/payout/repository.rs` |

### Implementation Steps

1. **Create `src/payout/repository.rs`**
   - Copy `PayoutBatch`, `Payout`, `PayoutShareSnapshot` structs from `accounting/payout_repository.rs`
   - Copy `PayoutRepository` struct + all impl methods
   - Remove payout-related imports from accounting

2. **Create `src/payout/schema.rs`**
   - Extract payout table DDL from `accounting/schema.rs` (found_blocks is NOT a payout table — it stays in accounting)
   - Tables to move: `payout_batches`, `payouts`, `payout_share_snapshots`, `dust_balances`
   - `init_schema()` function that creates only these tables
   - Tables that stay in accounting: workers, shares, share_outcomes, rounds, found_blocks, accounting_events, authorization_events

3. **Update `src/main.rs`**
   - Call `payout::schema::init_schema(&conn)` in addition to `accounting::init_schema(&conn)`
   - Create payout_repo from `payout::PayoutRepository::new(conn.clone())`

4. **Update `src/http_api/server.rs`**
   - Change `use crate::accounting::PayoutRepository` → `use crate::payout::PayoutRepository`
   - `payout_repo: Option<PayoutRepository>` stays the same (type path changes)

5. **Update all `use crate::accounting::Payout*` references** to `use crate::payout::*`

6. **Delete `src/accounting/payout_repository.rs`**

7. **Remove payout DDL from `src/accounting/schema.rs`**

### Files Modified/Created
| File | Action |
|---|---|
| `src/payout/repository.rs` | **Create** — PayoutRepository |
| `src/payout/schema.rs` | **Create** — payout table DDL |
| `src/accounting/payout_repository.rs` | **Delete** |
| `src/accounting/schema.rs` | **Modify** — remove payout DDL |
| `src/main.rs` | **Modify** — call payout::init_schema, create payout::PayoutRepository |
| `src/http_api/server.rs` | **Modify** — update import path |
| `src/http_api/routes/payouts.rs` | **Modify** — update import path |
| `src/payout/service.rs` | **Modify** — use payout::PayoutRepository |
| `src/payout/mod.rs` | **Modify** — add `pub mod repository`, `pub mod schema` |

### Dependency Order
**Must be done AFTER Bullet 1** — `AccountingService` must no longer reference `self.payout_repo` before we remove the field and delete the file.

### Test Impact
- Payout repository tests move from `accounting/` test module to `payout/repository.rs`
- No test logic changes — same tests, same assertions, different import path

---

### Documentation Updates

| Document | Change |
|----------|--------|
| `docs/CONTEXT_MAP.md` | Update ownership boundaries — payout module now owns PayoutRepository, PayoutService, and payout tables. Remove cyclic dependency arrow between accounting and payout. Add `payout::service` to payout context. |
| `docs/contexts/payout/CONTEXT.md` | Add PayoutService description. Update module listing to include `repository`, `schema`, `service`. Document cross-context FK from `payout_batches` to accounting's `rounds` table. |
| `docs/contexts/accounting/CONTEXT.md` | Remove payout-related responsibilities (`PayoutRepository`, payout orchestration methods). Simplify boundary description. Remove `conn` field reference from AccountingService struct. |
| `docs/SCHEMA.md` | Move payout table documentation (`payout_batches`, `payouts`, `payout_share_snapshots`, `dust_balances`) to a section noting ownership in payout context. Update `found_blocks` table to add missing `extranonce1` and `created_at` columns. Correct `payout_batches` status lifecycle (remove `'signed'`). |
| `docs/UBIQUITOUS_LANGUAGE.md` | Update PayoutService entry if any term definitions changed. |

---

## Phase 2 Verification

```
cargo build      # must pass clean — no unresolved imports
cargo test       # must pass — all tests updated for new module paths
```

### Healing Check
After Phase 2:
- `grep -r 'use crate::accounting::Payout' src/` returns nothing
- `grep -r 'use crate::payout::' src/accounting/` returns nothing (no cyclic dep)
- `grep -rn 'payout_repo' src/accounting/` returns nothing

## Completion Checklist

- [ ] `PayoutService` struct created in `payout/service.rs` with 3 moved methods
- [ ] `AccountingService` no longer has payout_repo, conn, or payout imports
- [ ] `PayoutHandler` takes `PayoutService`
- [ ] `process_pending_payouts` signature simplified to `&self` only (no 6 injected params)
- [ ] `AppState` has new `payout_service: Option<PayoutService>` field
- [ ] `PayoutRepository` lives in `payout/repository.rs`
- [ ] `payout/schema.rs` owns payout table DDL
- [ ] `payout/mod.rs` registers `repository`, `schema`, `service`
- [ ] `accounting/mod.rs` removed `pub use payout_repository::*`
- [ ] `main.rs` wires both `accounting::init_schema()` and `payout::init_schema()`
- [ ] All import paths updated
- [ ] Documentation updated per table above
- [ ] `cargo test` passes
