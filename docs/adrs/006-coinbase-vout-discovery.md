# ADR 006: Dynamic Coinbase Vout Discovery for Payout Signing

**Context:** Payout  
**Date:** 2026-05-22  
**Status:** Accepted

## Decision

When the payout signer needs to build a coinbase-spending transaction, it resolves the spendable coinbase output vout index dynamically by scanning the `getrawtransaction` response for the first non-OP_RETURN, non-zero-value vout. The vout index is NOT stored in the database — only the coinbase txid is cached (in `found_blocks.coinbase_txid`).

## Rationale

Lotus constructs coinbase transactions with an OP_RETURN metadata output at `vout[0]`. This output carries pool identity data and has zero value — it cannot be spent. The actual spendable reward outputs follow at `vout[1]` and potentially beyond (if the coinbase has multiple payouts).

Hardcoding `vout[0]` would always skip the real reward. Hardcoding `vout[1]` assumes a fixed coinbase layout that may vary across lotusd versions or pool configurations (e.g., additional metadata outputs, extra payout destinations from mining template changes). Dynamic scanning by spendability is resilient to structural changes in the coinbase — any future template change that shifts output positions is handled automatically.

Alternative approaches considered and rejected:

1. **Store `coinbase_vout` alongside `coinbase_txid`** — More schema state to maintain, requires a separate RPC call to populate at block-find time (when we don't yet need it). The storage savings over runtime scanning are negligible (~4 bytes per block).

2. **Match by value against `found_block.coinbase_value`** — The total coinbase reward may be split across multiple outputs (metadata output = 0, reward outputs = partial amounts). Exact value matching against the gross reward would fail.

3. **Assume `vout[1]` for Lotus** — Brittle; breaks if lotusd adds metadata outputs or changes the reward output order. The dynamic scan is a few lines of code and eliminates an entire class of future-compatibility bugs.

## Consequences

- Each signing run for a fresh block (not yet cached) makes two additional RPC calls: `getblock` to resolve coinbase txid, `getrawtransaction` to get vout details. These are local RPC calls to the lotusd node, typically sub-millisecond on Unix sockets.
- Once `coinbase_txid` is cached in `found_blocks`, only `getrawtransaction` is needed on subsequent runs.
- The scanning logic adds ~10 lines per signing run, dominated by the RPC latency anyway.
- If Lotus changes its coinbase structure to use a different sentinel (e.g., `OP_RETURN` with non-zero value), the scan predicate would need updating.
