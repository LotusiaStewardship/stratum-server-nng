# stratum-server-nng

Production-grade Stratum V1 pool server for the Lotus blockchain, using NNG (Nanomsg Next Generation) endpoints for efficient node communication.

---

## Overview

`stratum-server-nng` is a high-performance mining pool server written in Rust. It accepts miner connections over the Stratum V1 protocol, distributes mining jobs fetched from a `lotusd` node, validates submitted shares, and handles the full payout lifecycle — from PPLNS reward calculation through coinbase-spending transaction signing and broadcast.

The server is a single-process binary with clear module boundaries. It communicates with lotusd via NNG IPC (template requests, pub/sub events) and JSON-RPC (block submission, raw transaction broadcast).

## Features

- **Stratum V1 protocol** — full implementation: `mining.subscribe`, `mining.authorize`, `mining.notify`, `mining.submit`, `mining.set_difficulty`, `mining.ping`, `mining.extranonce.subscribe`, `mining.suggest_difficulty`
- **Dynamic per-session variable difficulty (VarDiff)** — auto-tracks network difficulty from lotusd templates, retargets each session independently using configurable time windows
- **PPLNS payout scheme** — difficulty-weighted Pay Per Last N Shares with configurable window multiplier, fee deduction, and dust balance carry-forward
- **Dual signing modes** — internal (private key in config) or external (delegate signing to a remote webhook service)
- **SQLite accounting** — shares, rounds, found blocks, payouts, workers, and accounting events in a single database with WAL mode and idempotent schema initialization
- **NNG node integration** — IPC-based mining template requests and pub/sub event consumption (block-connected, block-disconnected, mining-work-change)
- **Operator REST API** — authenticated endpoints for pool monitoring: workers, rounds, blocks, shares, payouts, and stats; pagination supported
- **Optional public HTTP dashboard** — health endpoint plus configurable read-only dashboard
- **Graceful shutdown** — coordinated task drain with SQLite WAL checkpoint on exit
- **Protocol hardening** — per-connection rate limiting, idle timeout, max request line size
- **Domain-tagged structured logging** — per-module log targets (`stratum`, `accounting`, `payout`, `validator`, `node`, etc.) with dynamic `EnvFilter` control

## Architecture

The project is organized into nine modules, each with a clear responsibility. All contexts live in a single process but maintain module-level boundaries.

```
                    ┌─────────────────┐
                    │   lotusd (node)  │  (external)
                    └────────┬────────┘
                             │ NNG pub/sub + RPC
                             ▼
┌─────────────────────────────────────────────┐
│           Node Integration                  │
│  (template fetch, event consumer, job cache)│
└───────────┬──────────────────────┬──────────┘
            │ MiningJob            │ events
            ▼                      ▼
┌──────────────────────┐  ┌──────────────────┐
│    Stratum Core      │  │   HTTP API       │
│  (server, protocol,  │  │ (operator +       │
│   session, params)   │  │  public dashboard)│
└──────┬───────────────┘  └──────────────────┘
       │ shares
       ▼
┌──────────────────────┐
│  Share Processing    │
│  (validator, VarDiff)│
└──────┬───────────────┘
       │ accepted shares
       ▼
┌──────────────────────┐  ┌──────────────────┐
│     Accounting       │  │    Payout        │
│  (repositories,      │  │ (PPLNS, plan,    │
│   events, schema)    │  │  signer)         │
└──────────────────────┘  └──────────────────┘
```

| Module | Responsibility |
|---|---|
| `stratum_protocol` | TCP server, Stratum message parsing, per-session state |
| `node_integration` | lotusd NNG RPC/client, pub/sub consumer, template→job conversion, block building, `JobCache` |
| `share_processing` | Share validation pipeline, per-session `VarDiff` with network-aware difficulty |
| `accounting` | SQLite repositories (shares, workers, rounds, blocks, payouts, events), schema init |
| `payout` | PPLNS window calculation, payout plan building (fee, dust), internal/external signing |
| `http_api` | Axum-based REST API with Bearer auth, pagination, optional dashboard |
| `config` | TOML configuration deserialization (`Config`, `VarDiffSettings`, `PoolSettings`, etc.) |
| `shutdown` | Graceful shutdown coordinator with task registry and WAL checkpoint |
| `logging` | Domain-tagged `tracing` macros per module |

See [`docs/CONTEXT_MAP.md`](docs/CONTEXT_MAP.md) for the full context map with data flow details, and [`docs/UBIQUITOUS_LANGUAGE.md`](docs/UBIQUITOUS_LANGUAGE.md) for canonical terminology.

## Quickstart

### Prerequisites

- **Rust** 1.81 or later (edition 2021)
- A **lotusd** node running with NNG endpoints enabled (see lotusd documentation)
- The `bitcoinsuite` crate family in a sibling directory (repo-relative path `../bitcoinsuite/`)

### Build

```bash
cargo build --release
```

### Configure

Copy the example configuration and edit for your environment:

```bash
cp config.example.toml config.toml
```

At minimum you will need to set:

- `nng_rpc_url` — lotusd NNG RPC endpoint (e.g. `ipc:///tmp/lotusd.rpc`)
- `nng_pub_url` — lotusd NNG pub/sub endpoint (e.g. `ipc:///tmp/lotusd.pub`)
- `pool.mining_identity.payout_address` — a Lotus address for block reward collection
- `bitcoind_rpc` — lotusd JSON-RPC credentials and URL

### Run

```bash
cargo run --release
```

The network (mainnet / testnet / regtest) is **auto-detected** from the `bitcoind_rpc.url` port:

| Port | Network |
|---|---|
| 10604 | mainnet |
| 11604 | testnet |
| 12604 | regtest |

The database path is auto-derived from the network but can be overridden with `sqlite_path`.

## Configuration Highlights

Key configuration groups (see [`config.example.toml`](config.example.toml) for the full reference with documentation):

| Section | Key settings |
|---|---|
| `[vardiff]` | `min_floor`, `initial_pct`, `target_secs`, `retarget_secs` |
| `[pool.mining_identity]` | `payout_address` or `payout_script_hex` |
| `[pool.fee]` | `enabled`, `fee_bps`, `fee_address` |
| `[pool.pplns]` | `n_multiplier`, `min_payout_sat`, `min_confirmations` |
| `[pool.signing]` | `mode` (internal/external), `private_key` or `webhook_url` |
| `[bitcoind_rpc]` | `rpc_user`, `rpc_pass`, `url` |

## HTTP API

Base path: `/api/v1`

| Method | Path | Auth | Description |
|---|---|---|---|
| GET | `/health` | No | Liveness check (server uptime, connected miners, network difficulty) |
| GET | `/stats` | Yes | Pool statistics summary |
| GET | `/workers` | Yes | List workers (paginated) |
| GET | `/workers/{id}` | Yes | Worker detail with share statistics |
| GET | `/rounds` | Yes | List mining rounds (paginated, filterable by status) |
| GET | `/rounds/{id}` | Yes | Round detail with worker share breakdown |
| GET | `/blocks` | Yes | List found blocks (paginated, filterable by status) |
| GET | `/blocks/{hash}` | Yes | Block detail |
| GET | `/payouts` | Yes | List payout batches (paginated) |
| GET | `/payouts/{id}` | Yes | Payout batch detail with individual payouts |
| POST | `/admin/payouts/trigger/{block_hash}` | Yes | Manually trigger payout for a matured block |
| GET | `/shares` | Yes | List raw share submissions (paginated) |
| GET | `/share-outcomes` | Yes | List validated share outcomes (paginated) |

Authenticated endpoints require a `Bearer` token in the `Authorization` header. The token is configured via `api_token` in the config file or the `STRATUM_API_TOKEN` environment variable.

When `http_enabled = true`, the optional public HTTP dashboard is served on the `http_bind` address.

## Documentation

| Document | Description |
|---|---|
| [`docs/CONSTITUTION.md`](docs/CONSTITUTION.md) | Development rules, standards, and workflow |
| [`docs/CONTEXT_MAP.md`](docs/CONTEXT_MAP.md) | Bounded contexts, data flow, and integration contracts |
| [`docs/UBIQUITOUS_LANGUAGE.md`](docs/UBIQUITOUS_LANGUAGE.md) | Canonical terminology across the codebase |
| [`docs/SCHEMA.md`](docs/SCHEMA.md) | SQLite database schema reference |
| [`docs/adrs/`](docs/adrs/) | Architecture Decision Records |

### Key ADRs

| ADR | Decision |
|---|---|
| [001](docs/adrs/001-per-session-vardiff.md) | VarDiff operates per TCP session, not per worker identity |
| [002](docs/adrs/002-dust-balance-based.md) | Dust tracked as balance, not FIFO ledger |
| [003](docs/adrs/003-extranonce-from-counter.md) | Extranonce1 derived from monotonically increasing session counter |
| [004](docs/adrs/004-pplns-only-payout.md) | PPLNS is the only payout scheme (schema is scheme-agnostic) |
| [005](docs/adrs/005-sqlite-single-connection.md) | Single SQLite connection with `Mutex` serialization (no pool) |
| [006](docs/adrs/006-canonical-type-alignment.md) | Domain types match lotusd FlatBuffers schema; conversions only at boundaries |
| [006](docs/adrs/006-coinbase-vout-discovery.md) | Coinbase vout index discovered dynamically by scanning spendable outputs |

## Development

### Build

```bash
cargo build
```

### Test

```bash
cargo test
```

All tests must pass before marking work complete. The test suite includes unit tests for every module (validation pipeline, VarDiff, PPLNS window calculation, payout plan building, session state machine, API handlers, job cache, block building, template conversion) plus integration tests for the full payout flow.

### Standards

Development is governed by [`docs/CONSTITUTION.md`](docs/CONSTITUTION.md):

- Every implementation plan must include a "Documentation Updates" section
- Tests are first-class deliverables
- No regressions: pre-existing tests must not be modified to pass
- ADRs are created for decisions that are hard to reverse, surprising, or the result of a real trade-off

## Related Repositories

- [lotusd](https://github.com/lotusia/lotusd) — Lotus core node (consensus, networking, wallet)
- [bitcoinsuite](https://github.com/lotusia/bitcoinsuite) — Bitcoin/Lotus protocol primitives (used as a local dependency)

## License

MIT © 2026 The Lotusia Stewardship. See [`LICENSE`](LICENSE).
