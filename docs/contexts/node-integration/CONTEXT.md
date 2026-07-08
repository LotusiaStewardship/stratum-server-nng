# Node Integration Context

**Last updated:** 2026-05-27  
**Related spec:** [Modular Architecture Refactor](../stratum-core/specs/modular-architecture-refactor-slices.md)  
**Ubiquitous Language:** [UBIQUITOUS_LANGUAGE.md](../../UBIQUITOUS_LANGUAGE.md)

---

## Bounded Context

The **Node Integration** context owns all communication with the lotusd node — NNG RPC for template fetching, NNG pub/sub for live events (template changes, block disconnects), JSON-RPC HTTP for block submission and chain queries, and block building for submitblock.

### Boundary

- **Inside:** NNG RPC client (template fetch, connect/disconnect lifecycle), NNG pub/sub consumer (miningwrkchg, blkconnected, blkdisconctd), JSON-RPC HTTP client (submitblock, getblockcount, getblockhash), template → MiningJob conversion, block building from template bytes, job caching
- **Outside:** Stratum protocol, share validation, HTTP API

### Dependencies

| Module | Depends On | Purpose |
|--------|-----------|---------|
| `nng/rpc_client` | `bitcoinsuite-bitcoind-nng` | NNG IPC connection, get_mining_template |
| `nng/consumer` | `nng/rpc_client`, Accounting | Pub/sub event handling, template change broadcasts, chain state tracking, payout maturation triggers |
| `json_rpc/client` | `reqwest` | HTTP JSON-RPC for submitblock and chain queries |
| `template` | `bitcoinsuite-bitcoind-nng`, `stratum_protocol::job` | MiningTemplate → MiningJob conversion |
| `block_builder` | `bitcoinsuite`, `stratum_protocol::job` | Full block reconstruction for submitblock |
| `job_cache` | None | In-memory LRU job storage |

### Module Structure

```
src/node_integration/
├── mod.rs                        # Re-exports
├── nng/
│   ├── mod.rs
│   ├── rpc_client.rs             # NNG RPC: connect, disconnect, get_mining_template
│   └── consumer.rs               # NNG pub/sub: event loop, mining work changed, block disconnected
├── json_rpc/
│   ├── mod.rs
│   └── client.rs                 # HTTP JSON-RPC: submitblock, getblockcount, getblockhash
├── template.rs                   # template_to_job(), verify_coinbase_outputs()
├── job_cache.rs                  # JobCache: Moka-based LRU with latest-job tracking
└── block_builder.rs              # build_submit_block(), coinbase reconstruction
```

### Key Contracts

- **`template_to_job(template, clean_jobs) -> MiningJob`** — Converts a `MiningTemplate` from lotusd into a `MiningJob` ready for Stratum dispatch. Handles epoch assignment, coinbase extraction, merkle branch parsing, and block byte capture.
- **`build_submit_block(job, extranonce1, extranonce2, ntime, nonce) -> Vec<u8>`** — Reconstructs the full serialized block for `submitblock` RPC. Replaces the template coinbase with the miner's reconstructed coinbase, builds the header via `build_stratum_header`, and updates the merkle root and size to match the validator's computation.
- **`verify_coinbase_outputs(template)`** — Checks that the template coinbase has at least one non-OP_RETURN output. Warns at startup if all outputs are OP_RETURN (would burn block rewards).
- **`NngEventConsumer`** — Long-running task spawned in `main.rs`. Consumes `miningwrkchg` (debounced at 100ms), `blkconnected` (immediate), and `blkdisconctd` (immediate) events.
  - `miningwrkchg` → template fetch, job conversion, broadcast via `broadcast::Sender` to all sessions. Injects the reason code into `MiningJob.reason` for miner-facing explanations. **No chain state or payout logic.**
  - `blkconnected` → decodes block height from Lotus header bytes via `LotusHeader::deser()`, updates `ChainTip`. Before maturation, checks if the arriving block hash belongs to an orphaned pool block and un-orphans it if safe (no submitted payout). Then calls `check_maturation()` and sends matured block hashes + transaction IDs through the payout maturation channel. **Sole driver of runtime chain-state tracking, reorg recovery, and payout triggers.**
  - `blkdisconctd` → records orphaned found_blocks via AccountingService on matched block hash.

### Reorg Detection

Two complementary paths:

1. **Runtime (NNG pub/sub):** `BlockDisconnected` events from lotusd trigger immediate orphan status updates on matched found_blocks via `found_block_repo.mark_orphaned()`.
2. **Startup (JSON-RPC):** On server start, `getblockcount` + `getblockhash` are used to reconcile found_blocks against current chain tip. Missing blocks are marked orphaned.

### Reorg Recovery (Un-Orphan)

When a `blkconnected` event matches an orphaned pool block, `handle_block_connected` runs an orphan check before the maturation check:

- **No payout batch:** un-orphans to `immature` → maturation check promotes immediately if depth ≥ min_confirmations.
- **Pending payout batch:** un-orphans to `matured` (block was previously matured before the reorg).
- **Submitted payout batch:** stays orphaned with a warning — payout was already sent, cannot reclaim.

### Shutdown

- NNG RPC client `disconnect()` is called after task completion during graceful shutdown.
- The NNG event consumer receives the broadcast shutdown signal and exits its event loop.
