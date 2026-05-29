# Accounting Context

**Last updated:** 2026-05-22  
**Related spec:** [Modular Architecture Refactor](../stratum-core/specs/modular-architecture-refactor-slices.md)  
**Ubiquitous Language:** [UBIQUITOUS_LANGUAGE.md](../../UBIQUITOUS_LANGUAGE.md)

---

## Bounded Context

The **Accounting** context owns all persistent data — schema initialization, SQLite repositories, and the `AccountingService` facade that orchestrates multi-step operations.

### Boundary

- **Inside:** Database schema (all CREATE TABLE statements), repository CRUD for every table, deduplication logic, atomic share+outcome insertion, round lifecycle management, audit event recording
- **Outside:** Stratum protocol handling, share validation, payout calculation logic, HTTP API routing

### Dependencies

| Module | Depends On | Purpose |
|--------|-----------|---------|
| `schema` | None | Table creation, index creation |
| `worker_repository` | `schema` | Worker upsert and query |
| `share_repository` | `schema`, `worker_repository` | Share + outcome insertion with dedupe, round resolution |
| `round_repository` | `schema` | Round lifecycle (open → found → closed → paid → orphaned) |
| `found_block_repository` | `schema` | Found block tracking (record, list, status transitions) |
| `payout_repository` | `schema` | Payout batch CRUD, individual payouts, dust balance, share snapshot |
| `accounting_event_repository` | `schema` | Append-only event log |
| `service` | All repositories | Multi-step operations (payout creation, round close, block reconciliation) |

### Module Structure

```
src/accounting/
├── mod.rs                        # Re-exports, AccountingService struct
├── schema.rs                     # init_schema(): PRAGMA config + all CREATE TABLE statements
├── service.rs                    # AccountingService facade (share recording, payout creation, reconciliation)
├── worker_repository.rs          # Worker upsert/query
├── share_repository.rs           # Share + ShareOutcome insert with dedupe, stats queries, sum_difficulty_since()
├── round_repository.rs           # Round get_or_create, close, resolve_for_template, list
├── found_block_repository.rs     # Found block record, list by status, update status
├── payout_repository.rs          # Payout batch CRUD, individual payouts, dust balance, share snapshot
└── accounting_event_repository.rs # Append-only event recording and query
```

### Key Invariants

- **Shares are immutable** once persisted (never deleted or updated). The `dedupe_key` UNIQUE constraint is the only write-level gate — identical submissions produce no-op INSERT OR IGNORE.
- **Share + outcome insertion is atomic** (single SQLite transaction). Both are committed or neither.
- **Dedupe key format:** `worker_id:template_id:template_epoch:extranonce2:ntime:nonce`
- **Round membership** is resolved at share insertion time via `resolve_round_for_template(template_id)`. If no open round exists for that template, a new round is created.
- **Accounting events are append-only.** Existing records are never updated or deleted. Provides a complete chronological audit trail.
- **Database access is single-connection** via `Arc<Mutex<Connection>>` (see ADR 005).
- **WAL journal mode** enables concurrent reads. The `ShutdownCoordinator` performs a WAL checkpoint on graceful shutdown.

### Tables

| Table | Purpose | Key Constraints |
|-------|---------|-----------------|
| `workers` | Miner identity across sessions | UNIQUE(payout_address, worker_suffix) |
| `authorization_events` | Immutable auth audit log | — |
| `shares` | Raw submission records | UNIQUE(dedupe_key), FK → workers |
| `share_outcomes` | Validation results | UNIQUE(dedupe_key), FK → shares, FK → workers, FK → rounds |
| `rounds` | Payout round lifecycle | — |
| `found_blocks` | Block tracking, coinbase_value | UNIQUE(block_hash) |
| `accounting_events` | Append-only operational audit | — |
| `payout_batches` | Payout batch lifecycle | UNIQUE(retry_key) |
| `payouts` | Individual miner payouts | FK → payout_batches |
| `payout_share_snapshots` | Share window audit record | FK → payout_batches |
| `dust_balances` | Per-address dust accumulation | UNIQUE(payout_address) |
