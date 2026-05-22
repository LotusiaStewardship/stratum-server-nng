# Payout Context

**Last updated:** 2026-05-22  
**Related spec:** [Modular Architecture Refactor](../stratum-core/specs/modular-architecture-refactor-slices.md)  
**Ubiquitous Language:** [UBIQUITOUS_LANGUAGE.md](../../UBIQUITOUS_LANGUAGE.md)

---

## Bounded Context

The **Payout** context owns PPLNS window calculation and payout plan construction. It determines which shares qualify for a given found block's reward and how the reward is distributed among miners.

### Boundary

- **Inside:** PPLNS window computation (cumulative difficulty threshold), payout plan building (fee deduction, proportional distribution, remainder handling, dust carry-forward)
- **Outside:** Transaction signing (Slice 9), payout batch persistence (Accounting), HTTP API triggers

### Dependencies

| Module | Depends On | Purpose |
|--------|-----------|---------|
| `pplns` | SQLite (via conn) | PPLNS window query — cumulative difficulty from share_outcomes |
| `plan` | `pplns` | PayoutPlan construction, distribution math |

### Module Structure

```
src/payout/
├── mod.rs            # Re-exports
├── pplns.rs          # calculate_pplns_window() — cumulative difficulty threshold
└── plan.rs           # build_payout_plan() — PayoutPlan, PayoutOutput, distribution
```

### PPLNS Window Algorithm

1. Window ends at the found block's `share_outcomes.created_at` (the timestamp of the share that submitted the block).
2. Window extends backward until cumulative work units ≥ `n_multiplier × N_diff`.
3. Work units = `share.difficulty` (P_diff at assignment time, immutable).
4. Only accepted shares are included (status = 'accepted').
5. Shares are aggregated by `payout_address`.
6. Results are ordered by `created_at DESC` (newest first).
7. **Orphaned-round shares are not excluded** — orphan risk is socialized via window dilution.

### Payout Plan Construction

1. **Gross reward:** `found_blocks.coinbase_value` (populated from `MiningJob.coinbase_value` at block-find time).
2. **Fee:** `gross_reward × fee_bps / 10000`. Deducted before miner distribution.
3. **Net reward:** `gross_reward - pool_fee`.
4. **Distribution:** Net reward is split proportionally by work units per payout address.
5. **Remainder:** Leftover satoshis from integer division are distributed to the largest fractional parts.
6. **Dust:** Amounts below `min_payout_sat` are carried forward via `dust_balances` table (see ADR 002).
7. **Dust inclusion:** Dust is added as bonus work weight proportional to the current payout's `gross_reward`.

### Key Invariants

- PPLNS window is share-ID-based, not round-based (orphaned-round shares are included).
- Payout plan is deterministic — same inputs produce identical outputs (tested).
- Payout batch creation is atomic (runs in a SQLite transaction).
- `retry_key = "{block_hash}:{num_outputs}"` with UNIQUE constraint prevents duplicate batches.
- Dust is additive-only per address (balance never decreases except on payout).
- Payout share snapshots capture which shares were in the window at payout time for post-hoc audit.

### Known Limitations (deferred to Slice 9)

- No transaction signing — payout batches are created with `status='pending'`.
- No maturation check — schedules creates batches for any `confirmed` block regardless of confirmation count.
- No `found_block → paid` status transition — blocks remain `confirmed` after payout batch creation.
