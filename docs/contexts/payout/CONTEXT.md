# Payout Context

**Last updated:** 2026-05-28  
**Related specs:** [Modular Architecture Refactor](../stratum-core/specs/modular-architecture-refactor-slices.md), [Block Maturation Check](./specs/maturation-check.md), [ADR 007: Payout Empty-Output Guard](../adrs/007-payout-empty-output-guard.md)  
**Ubiquitous Language:** [UBIQUITOUS_LANGUAGE.md](../../UBIQUITOUS_LANGUAGE.md)

---

## Bounded Context

The **Payout** context owns PPLNS window calculation and payout plan construction. It determines which shares qualify for a given found block's reward and how the reward is distributed among miners.

### Boundary

- **Inside:** PPLNS window computation (cumulative difficulty threshold), payout plan building (fee deduction, proportional distribution, remainder handling, dust carry-forward), payout transaction signing and broadcast
- **Outside:** payout batch persistence (Accounting), HTTP API triggers

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
├── plan.rs           # build_payout_plan() — PayoutPlan, PayoutOutput, distribution
└── signer/
    ├── mod.rs        # Signer trait (async), SignedBatchData
    ├── internal.rs   # InternalSigner — builds tx with TxBuilder, signs with P2PKHSignatory, broadcasts via sendrawtransaction
    └── external.rs   # ExternalSigner — POSTs payout plan to webhook URL, returns txid from response
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
- **Empty-output guard:** `build_payout_tx` refuses to sign a transaction with zero miner outputs. See [ADR 007](../adrs/007-payout-empty-output-guard.md).
- **Auto-rebuild:** If `process_pending_payouts` loads a pending batch with an empty `payouts` table, it automatically recalculates the PPLNS window via `rebuild_payout_plan_for_batch` and re-inserts payouts + snapshots before signing. This recovers from accidentally deleted payout records.

### Known Limitations

- **Maturation check implemented:** Blocks transition `immature → matured → paid`. See the [maturation check spec](./specs/maturation-check.md).
- External signer is a scaffold — no retry/poll logic for async signing workflows.
- `pool.signing.webhook_url` config accepted but not yet exposed in all environments.
- Signing uses `process_pending_payouts` which can be called from the scheduler loop or any trigger point.

### Slice 9: Payout Automation (completed)

See the [Slice 9 spec](../stratum-core/specs/modular-architecture-refactor-slices.md#slice-9-payout-signer-abstraction) for details.

Payout automation is event-driven, replacing the old timer-based scheduler:

1. **Maturation check:** On each `MiningWorkChanged` event (fires on every new block), the NNG consumer updates the shared `ChainTip` and calls `AccountingService::check_maturation` to promote `immature` blocks to `matured` status
2. **Maturation event:** Newly matured block hashes are sent through an `mpsc::UnboundedSender` channel to the `PayoutHandler`
3. **PayoutHandler:** Receives block hashes, creates PPLNS payout batches via `create_payout_for_found_block`, then signs/submits pending batches via `process_pending_payouts`
4. **Signer:** Constructed from config (`pool.signing.mode`), either internal (in-process secp256k1) or external (HTTP webhook)
5. **Gating:** Controlled by `pplns.payout_enabled` — when disabled, blocks accumulate at `matured` status for manual processing

`process_pending_payouts` on `AccountingService`:
1. Queries `payout_batches` with `status='pending'`
2. Loads the associated `FoundBlock` by round_id
3. Resolves coinbase txid via `getblock` RPC (stores in `found_blocks.coinbase_txid` for reuse)
4. Fetches coinbase output details via `getrawtransaction`
5. Finds the first spendable (non-OP_RETURN) vout (Lotus: vout[0] is OP_RETURN metadata)
6. Reconstructs `SignedBatchData` from DB data
7. Calls the configured `Signer` impl
8. On success: marks batch `submitted` with txid, transitions `found_block.status` to `paid`
6. If the `payouts` table has no rows for this batch (e.g., accidentally deleted), calls `rebuild_payout_plan_for_batch` to recalculate the PPLNS window from scratch and re-insert payouts + snapshots + dust balances atomically. If rebuild fails (no share_outcome, empty PPLNS window, rebuild produces empty outputs), the batch is skipped with a warning.
7. Reconstructs `SignedBatchData` from DB data (or rebuilt outputs).
8. Calls the configured `Signer` impl.
9. The `InternalSigner::build_payout_tx` guards against empty `plan.outputs` — if zero miner outputs somehow reach this point, signing fails with a descriptive error.
10. On success: marks batch `submitted` with txid, transitions `found_block.status` to `paid`.
11. On failure: batch stays `pending` for retry (errors logged per-batch).
