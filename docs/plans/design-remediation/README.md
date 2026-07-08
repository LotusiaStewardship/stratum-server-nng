# Design Remediation: `complete-refactor` Alignment Plan

**Status:** Proposed  
**Last updated:** 2026-05-28  
**Branch target:** `complete-refactor`  
**Total phases:** 4  
**Estimated effort:** ~6-8 implementation sessions

---

## Purpose

The `complete-refactor` branch implemented the intended modular architecture (Slices 1-9), but deviations from the original design accumulated across multiple contexts. This plan remediates **10 specific deviations** through 4 phased implementation waves.

The three most consequential deviations:

1. **Cyclic dependency accounting ↔ payout** — `AccountingService` orchestrates payout business logic (`create_payout_for_found_block`, `process_pending_payouts`, `rebuild_payout_plan_for_batch`), forcing a two-way dependency between separate contexts.
2. **Payout not confirmed on-chain** — `found_block.status` transitions to `'paid'` before the payout transaction confirms; no code ever transitions `payout_batches` to `'confirmed'`.
3. **Un-orphan gap** — Blocks disconnected by reorg and later reconnected via `BlockConnected` events are never restored; they stay `'orphaned'` permanently.

---

## Deviation Inventory

| # | Deviation | Severity | Bullet |
|---|-----------|----------|--------|
| 1 | AccountingService orchestrates payout business logic, creating cyclic dependency | Critical | 1 |
| 2 | PayoutRepository lives in accounting module despite Payout owning the logic | High | 2 |
| 3 | No PayoutScheme trait — cannot add second scheme without changing callers | Medium | 3 |
| 4 | Coinbase tx construction and signing are conflated in InternalSigner | Low | 4 |
| 5 | No un-orphan logic for reconnected blocks | High | 5 |
| 6 | No 'confirmed' transition for payout batches; premature 'paid' on found_block | High | 6 |
| 7 | Pagination missing on 3 of 6 list endpoints | Medium | 7 |
| 8 | Config structs undocumented | Low | 8 |
| 9 | No pool hashrate endpoint | Medium | 9 |
| 10 | Tracing domain comment misaligned with context map | Low | 10 |

---

## Phase Structure

| Phase | Bullets | Theme | Files Touched | Risk |
|-------|---------|-------|---------------|------|
| [PHASE-1](./PHASE-1-scheme-and-transaction.md) | 3, 4 | PayoutScheme trait + TransactionBuilder | ~7 files | Low — purely additive |
| [PHASE-2](./PHASE-2-payout-service-extraction.md) | 1, 2 | Extract PayoutService, move PayoutRepository | ~10 files | High — structural, moves code |
| [PHASE-3](./PHASE-3-domain-model-refinement.md) | 5, 6 | Un-orphan logic, payout confirmation via blkconnected | ~8 files | Medium — correctness changes |
| [PHASE-4](./PHASE-4-surface-and-docs.md) | 7, 8, 9, 10 | Pagination, hashrate, config docs, tracing alignment | ~9 files | Low — additive + docs |

---

## Dependency Graph

```
PHASE-1 (additive)          PHASE-2 (structural)        PHASE-3 (correctness)      PHASE-4 (surface)
─────────────────────       ────────────────────        ─────────────────────       ──────────────────
Bullet 3 (Scheme trait)    Bullet 1 (PayoutService)    Bullet 5 (Un-orphan)        Bullet 7 (Pagination)
Bullet 4 (TxBuilder)              │                    Bullet 6 (Confirm)          Bullet 9 (Hashrate)
                                  ▼                                                    │
                          Bullet 2 (Move Repo)                                          ▼
                                                                               Bullets 8, 10 (Docs)
```

PHASE-1 must complete before PHASE-2 (Bullet 1 depends on PayoutScheme trait).  
PHASE-2 must complete before PHASE-3 (Bullet 6 depends on PayoutService).  
PHASE-4 has no dependencies on prior phases beyond code stability.

---

## Ground Truth References

Each bullet's claims were verified against the actual codebase during a holistic audit on 2026-05-28:

| Reference | What was verified |
|-----------|-------------------|
| `src/accounting/service.rs` | 3 payout methods exist, cyclic import of `crate::payout::*` |
| `src/payout/handler.rs` | Imports `AccountingService` directly |
| `src/accounting/payout_repository.rs` | Lives in accounting, referenced from http_api and main.rs |
| `src/payout/signer/internal.rs` | `build_payout_tx` conflates construction + signing |
| `src/node_integration/nng/consumer.rs` | `handle_block_connected` has no orphan check; `BlockConnected` event carries full Block with txs |
| `src/accounting/found_block_repository.rs` | `get_by_hash` exists; no `un_orphan` method |
| `src/payout/mod.rs` | `PayoutEvent::BlockConnected` carries no data |
| `src/http_api/routes/*.rs` | Only workers and shares have pagination |
| `src/accounting/share_repository.rs` | No `sum_difficulty_since` method |
| `src/logging.rs` | Comment claims domains = bounded contexts, inaccurate |
| `../lotusd/src/nng_interface/nng_interface.cpp` | `BlockConnected`, `BlockDisconnected` carry full block |
| `../lotusd/src/rpc/mining.cpp` | `GetNetworkHashPS` = work_diff / time_diff |
| `../bitcoinsuite/bitcoinsuite-bitcoind-nng/src/structs.rs` | `Block.txs: Vec<BlockTx>` with `tx.txid: Sha256d` |

---

## Verification Policy

After each bullet is implemented:
1. `cargo build` must pass with no warnings
2. `cargo test` must pass (existing tests, possibly updated for new signatures)
3. Pre-existing test failures must be identified and excluded from blame

No bullet touches more than 5 source files. All are reversible with `git revert`.

---

## Related Documents

- [Phase 1: PayoutScheme + TransactionBuilder](./PHASE-1-scheme-and-transaction.md)
- [Phase 2: PayoutService Extraction + Repository Move](./PHASE-2-payout-service-extraction.md)
- [Phase 3: Un-Orphan + Payout Confirmation](./PHASE-3-domain-model-refinement.md)
- [Phase 4: Pagination, Hashrate, Config Docs, Tracing](./PHASE-4-surface-and-docs.md)
