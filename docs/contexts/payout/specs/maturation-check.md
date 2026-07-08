# Block Maturation Check for Payout Eligibility

**Status:** Final
**Context(s):** Payout, Accounting, Node Integration
**Date:** 2026-05-22

## Problem

The payout scheduler creates PPLNS payout batches for any `found_blocks` record with `status = 'confirmed'`. This status is set **immediately** when a block is found and submitted to lotusd — it reflects "submission accepted," not "coinbase output is spendable." Lotus requires 100 confirmations before a coinbase output can be spent (standard Bitcoin/Lotus consensus rule). Attempting to build and broadcast a coinbase-spending transaction before maturation will be rejected by the network.

The result: payout batches are created for immature blocks, and if `process_pending_payouts` runs against them, it will produce invalid transactions that fail on submission.

## Solution

Eliminate the `confirmed` status. Blocks are recorded directly as `immature` on successful submission. The payout scheduler promotes them to `matured` only after the configured confirmation threshold is reached. Payout batches are created only for `matured` blocks.

The chain tip height needed for confirmation computation is already flowing through the NNG pub/sub system — every `BlockConnected`, `BlockDisconnected`, and `miningwrkchg` event carries the relevant height. No `getblockcount` RPC call needed.

## User Stories

1. As a **pool operator**, I want blocks to only become payout-eligible after 100 confirmations, so that my payout transactions are valid and accepted by the network.
2. As a **pool operator**, I want to see which blocks are confirmed-but-immature, so that I can monitor payout progress.
3. As a **maintainer**, I want the `min_confirmations` threshold to be configurable, so that I can adjust it for regtest/testnet (typically 1) vs mainnet (typically 100).

## Implementation Decisions

### Block lifecycle

```
found → immature (recorded on successful lotusd submission)
      → matured  (confirmations >= min_confirmations)
      → paid     (payout batch signed and submitted)
      → orphaned (reorg detected, from any status)
```

`confirmed` is removed entirely. No code writes it, no code reads it.

### Confirmation computation

Confirmations = `chain_tip_height - block_height + 1`. The chain tip height is tracked at runtime via an `Arc<AtomicU64>` shared between the NNG consumer and the payout scheduler.

The consumer updates the shared height from two sources, both carrying `LotusHeader.height`:

1. **`BlockConnected`**: `latest_height = max(latest_height, event.block.header.height)` — the tip can only advance on connect.
2. **`BlockDisconnected`**: if `event.block.header.height == latest_height`, decrement `latest_height` by 1 — the tip block was removed from the chain. Disconnections below the tip don't affect it.

`miningwrkchg` is NOT used for height tracking — it fires on mempool changes too (header size, merkle branches), which have nothing to do with the chain tip. Only `BlockConnected` and `BlockDisconnected` are authoritative for tip height.

This keeps the height accurate at all times, even during rapid reorgs where multiple disconnect/connect events interleave before a `miningwrkchg` fires.

### Event-driven flow (implemented)

Payout automation is now event-driven, eliminating the timer-based scheduler:

1. On each `MiningWorkChanged` event (fires for every new block, reason `NewTip`):
   - Update the shared `ChainTip` with the event's `height` field
   - Call `AccountingService::check_maturation(tip_height, min_confirmations)`
2. `check_maturation`:
   - Queries `found_blocks` with `status = 'immature'`
   - For each, computes `confirmations = latest_height - block.height + 1`
   - If `confirmations >= min_confirmations`, calls `mark_matured(id)`
   - Returns the list of newly matured blocks
3. For each newly matured block, send its hash through the maturation event channel
4. The `PayoutHandler` task (gated by `pplns.payout_enabled`):
   - Receives the block hash
   - Creates a payout batch via `create_payout_for_found_block`
   - Calls `process_pending_payouts` to sign and submit pending batches
5. Startup reconciliation: after `getblockcount`, runs `check_maturation` for blocks that matured while offline

When `payout_enabled = false`, blocks accumulate at `matured` status for manual processing.

See also:
- [`ChainTip`](../../../UBIQUITOUS_LANGUAGE.md#chaintip)
- [`PayoutHandler`](../../../UBIQUITOUS_LANGUAGE.md#payouthandler)
- [`check_maturation` on AccountingService](../../../../src/accounting/service.rs)

### Schema changes

No new columns. The existing `status` field carries `immature` and `matured` as new values. The existing `matured_at` column (already in schema, never populated) is set by `mark_matured`.

### Repository changes

- `record_found_block`: initial status changes from `'confirmed'` to `'immature'`
- New method `mark_matured(id)`: sets `status = 'matured'`, `matured_at = CURRENT_TIMESTAMP`

### API exposure

No changes needed. `GET /api/v1/blocks` already returns the `status` field — operators see `immature` / `matured` / `paid` / `orphaned` directly.

### Cross-context impact

- **Payout context**: Gains maturation logic as the gate before batch creation.
- **Accounting context**: `record_found_block` writes `immature` instead of `confirmed`. `mark_matured` added to `FoundBlockRepository`.
- **Node Integration**: NNG consumer updates the shared `AtomicU64` from `BlockConnected`, `BlockDisconnected`, and `on_mining_work_changed`. The consumer already processes all three event types — this is a small additive change.
- **Stratum protocol / main.rs**: A shared `Arc<AtomicU64>` is created in `main.rs` and passed to both the NNG consumer and the payout scheduler.
- **HTTP API**: No changes.

### Shared state plumbing

A lightweight `ChainTip` or raw `Arc<AtomicU64>`:

```rust
let chain_tip = Arc::new(AtomicU64::new(0));
// Clone for NNG consumer, clone for payout scheduler
```

Consumer (in `BlockConnected` handler):
```rust
let header_height = event.block.header.height as u64;
let prev = chain_tip.load(Ordering::Relaxed);
if header_height > prev {
    chain_tip.store(header_height, Ordering::Relaxed);
}
```

Consumer (in `BlockDisconnected` handler):
```rust
let header_height = event.block.header.height as u64;
let prev = chain_tip.load(Ordering::Relaxed);
if header_height == prev {
    chain_tip.store(prev - 1, Ordering::Relaxed);
}
```

Scheduler:
```rust
let tip_height = chain_tip.load(Ordering::Relaxed) as i64;
```

Initial value of 0 is harmless — before the first template refresh, no found blocks exist, so no immature blocks are queried.

## Testing Strategy

- **Core logic**: Block with `confirmations < threshold` stays `immature`. Block with `confirmations >= threshold` becomes `matured`. Boundary at `threshold - 1` and `threshold`.
- **Repository**: `test_mark_matured_updates_status_and_matured_at`.
- **record_found_block**: Verify new blocks start as `immature` instead of `confirmed`.
- **ChainTip tracking**: Unit test the three update paths — `BlockConnected` advances, `BlockDisconnected` at tip decrements, `BlockDisconnected` below tip is a no-op.
- **Scheduler integration**: Set `latest_height` directly (no mock RPC), verify tick flow promotes blocks correctly.
- **Prior art**: Existing `test_update_status_transitions` in `found_block_repository.rs`.
- **Mock boundary**: No new RPC mocks. The `latest_height` is controlled directly in tests.

## Out of Scope

- **Reorg handling for matured blocks**: Already handled by existing `mark_orphaned`. No new logic needed.
- **Notifications when blocks mature**: Not needed for MVP. The scheduler processes them on its next tick.
- **Monitoring / alerting**: A future operator dashboard could surface immature block counts, but the API already exposes the data.

## Open Questions

None.
