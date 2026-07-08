# ADR 007: Empty-Output Guard and Auto-Rebuild for Payout Batches

**Context:** Payout  
**Date:** 2026-05-28  
**Status:** Accepted

## Decision

Two layers of defense against accidentally sending the entire block reward to the pool fee address:

1. **Empty-output guard in `InternalSigner::build_payout_tx`** — Refuse to sign any payout transaction where `PayoutPlan.outputs` is empty. Returns a descriptive error instead of constructing a `Leftover`-only transaction.

2. **Auto-rebuild in `AccountingService::process_pending_payouts`** — When loading a pending batch reveals an empty `payouts` table, automatically recalculate the PPLNS window via `rebuild_payout_plan_for_batch` and re-insert payouts + snapshots before signing.

## Rationale

### The bug

A pool operator manually deleted rows from the `payouts` table (to investigate a payout discrepancy). On the next retry cycle, `process_pending_payouts` loaded the batch, found zero payouts, reconstructed a `PayoutPlan` with `outputs: []`, and called `sign_and_submit`. The `TxBuilder` created a transaction with only a `Leftover` (pool fee) output, which absorbed the entire coinbase input value. The block reward was sent in full to the fee address.

### Why guard + rebuild instead of just guard

| Approach | Pros | Cons |
|----------|------|------|
| **Guard only** (bail on empty outputs) | Minimal code; easy to reason about | Batch stays stuck in `pending`; operator must manually re-create payouts |
| **Auto-rebuild** | Fully automated recovery; no operator intervention needed | Dust double-counting (see below); more code; changes payout amounts if fee config changed |
| **CLI rebuild command only** | No dust issue; operator controls when to rebuild | Requires external intervention; not triggered automatically |

The combined approach gives defense-in-depth: the guard prevents the worst case from reaching the network, and the auto-rebuild handles recovery transparently.

### Dust double-counting trade-off

When `rebuild_payout_plan_for_batch` runs, it reads the current `dust_balances` table. If the original plan produced dust (amounts below `min_payout_sat`), those amounts were added to `dust_balances` during the original `create_payout_for_found_block` transaction. The rebuild then reads these higher dust balances and gives the miner additional weight in the proportional allocation.

**Impact:** The miner receives a slightly larger payout than they would have gotten from the original plan. The excess is bounded by `dust_carried_forward` per address, which by definition is below `min_payout_sat` (typically 546 satoshis). This is an acceptable operator loss — it overpays miners by a negligible amount rather than underpaying them or requiring manual recovery.

### When rebuild is NOT attempted

- Batch status is not `pending` (assumes `submitted` or `confirmed` batches have an on-chain tx that cannot be reversed)
- No `share_outcome` found for the block hash (block-finding share missing from DB)
- PPLNS window returns zero shares (no shares to distribute)
- `build_payout_plan` produces empty outputs again (cannot make progress)
- Any of the above triggers an error, the batch is skipped with a warning, and the guard in `build_payout_tx` ensures no bad tx is broadcast

## Consequences

- Three new methods on `PayoutRepository`: `delete_payouts_by_batch`, `delete_snapshots_by_batch` (used by rebuild to clear stale data before re-insertion)
- One new method on `AccountingService`: `rebuild_payout_plan_for_batch`
- `process_pending_payouts` signature changed to accept `fee_bps`, `fee_address`, `min_payout_sat`, `n_multiplier` (needed for rebuild; previously these were only in the handler)
- Dust overpayment risk documented and accepted. If this becomes an issue in practice, a future improvement could store the original pre-rebuild dust snapshots and restore them during rebuild.
- No schema changes — rebuild reuses existing tables and constraints.
