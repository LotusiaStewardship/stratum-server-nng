# Frequently Asked Questions

Common questions about mining with a Lotus pool running `stratum-server-nng`.

---

## General

### What is stratum-server-nng?

It is the mining pool server software that connects Stratum V1 miners to the Lotus network. It communicates with a `lotusd` node via NNG (Nanomsg Next Generation) to fetch mining templates, receive blockchain events, and submit blocks. It handles share validation, difficulty adjustment, accounting, and PPLNS-based payouts in a single process.

### What is Lotus?

Lotus is a UTXO-based blockchain (a Bitcoin fork) designed for high-throughput applications. It uses a proof-of-work consensus mechanism compatible with Bitcoin-style ASIC and GPU mining hardware.

### Do I need to run a Lotus node to mine?

You do not need a node yourself — you connect your miner to the pool's stratum address. The pool operator runs one or more `lotusd` nodes that the server communicates with.

---

## Mining & Protocol

### What is Stratum V1?

Stratum V1 is the most widely supported mining protocol. It works in a push model: the pool sends `mining.notify` messages with new block templates, and miners respond with `mining.submit` messages containing their share solutions. The pool never asks for work — it tells miners what to work on.

### How does a miner connect to the pool?

Miners connect via TCP to the pool's stratum port (default `0.0.0.0:3334` for mainnet). The flow is:

1. **Connect** — open TCP connection to the stratum address
2. **`mining.subscribe`** — miner subscribes, receives `extranonce1` and `extranonce2_size`
3. **`mining.authorize`** — miner authenticates with `address[.suffix]` as worker name and an arbitrary password
4. **`mining.set_difficulty`** — pool sends initial per-session difficulty
5. **`mining.notify`** — pool pushes new mining jobs as they arrive from lotusd
6. **`mining.submit`** — miner submits solved shares

### What is extranonce?

Extranonce is a Stratum V1 mechanism that lets the pool assign part of the nonce space to each connected miner, preventing duplicate work. This server uses a 4-byte `extranonce1` (assigned per-session, derived from a counter) and a 4-byte `extranonce2` (chosen by the miner per-share).

| Field | Size | Who assigns | Purpose |
|---|---|---|---|
| `extranonce1` | 4 bytes (8 hex) | Pool | Unique per connection; ensures each miner searches a distinct nonce space |
| `extranonce2` | 4 bytes (8 hex) | Miner | Incremented per share; combined with nonce and ntime for the full header |

### How does difficulty work?

Difficulty is managed at two levels:

**Network difficulty (N_diff)** — derived from the `network_target_hex` in each `MiningTemplate` published by lotusd. This reflects the current network-wide mining difficulty. It is the ceiling for per-session difficulty.

**Per-session variable difficulty (VarDiff)** — each TCP connection gets an independent `VarDiff` instance that starts at a configurable fraction of network difficulty (default 1%) and adjusts dynamically based on the miner's share submission rate. The goal is to keep each miner submitting shares at a steady cadence (default target: one share every 15 seconds). The difficulty is clamped between a configurable floor (`min_floor`, default 0.001) and the current network difficulty.

### What does a mining.notify message contain?

Each `mining.notify` message includes:

- **`job_id`** — unique identifier for this job
- **`prevhash`** — previous block hash (little-endian hex)
- **`coinbase1`** — first part of coinbase transaction (before extranonce)
- **`coinbase2`** — second part of coinbase transaction (after extranonce)
- **`merkle_branch`** — merkle tree branches for the header
- **`version`** — block version
- **`nbits`** — network target (compact bits)
- **`ntime`** — current timestamp
- **`clean_jobs`** — if true, all previous jobs are stale

### What happens when I submit a share?

The server validates your submission through a pipeline:

1. **Format check** — `extranonce2`, `ntime`, and `nonce` must be valid hex strings of the correct length
2. **Session check** — the worker must be subscribed and authorized
3. **Job staleness check** — the referenced `job_id` must be in the session's active job list
4. **ntime range check** — `ntime` must be within the job's valid window (job time ± 20 minutes)
5. **Duplicate check** — identical submissions (by dedupe key) are rejected
6. **Proof-of-work check** — the block header hash must meet the session's current difficulty target
7. **Low-difficulty check** — if the hash meets the session target but not network target, it is accepted as a share (no block found)

Accepted shares are persisted to the accounting database and contribute to the PPLNS payout window.

---

## Payouts

### How does PPLNS work?

PPLNS (Pay Per Last N Shares) rewards miners based on their contribution to the most recent shares, weighted by difficulty. When a block is found, the payout algorithm:

1. Determines the PPLNS window: `N = n_multiplier × network_difficulty` (in cumulative difficulty units)
2. Selects all shares within that window, ordered by submission time
3. Computes each miner's weight as their share difficulty divided by total window difficulty
4. Deducts the pool fee (configurable in basis points)
5. Distributes the net block reward proportionally to each miner
6. Addresses below `min_payout_sat` (default 546) are held as dust balances instead of creating zero-value outputs

### When are payouts processed?

Payouts are processed automatically when a found block reaches maturity (default 100 confirmations). The server monitors the chain tip — when a `BlockConnected` event shows a previously immature block now meets the maturity threshold, a payout batch is created, signed, and broadcast.

Payouts can also be triggered manually via the operator API:

```bash
curl -X POST /api/v1/admin/payouts/trigger/{block_hash} \
  -H "Authorization: Bearer <token>"
```

### What is the maturation period?

A found block must reach 100 confirmations before its coinbase outputs are spendable (this is a Bitcoin-derived consensus rule). The pool's `min_confirmations` setting defaults to 100 — you can raise it for additional safety, but never lower it below 100.

### How are fees calculated?

Pool fees are configured in basis points (100 bps = 1%). The fee is deducted from the gross block reward before miner payouts. When a `fee_address` is configured, the fee output is sent to that address separately. When no fee address is set, the fee is included in the payout distribution as additional reward.

### What happens to dust?

Dust (payout amounts below `min_payout_sat`) is tracked per-address as a running balance in a `dust_balances` table. Each payout round, the dust balance is included in the miner's weight calculation, so accumulated dust is paid out once it crosses the minimum threshold in combination with new rewards.

---

## Security & Signing

### What signing modes are available?

Two modes, configured under `[pool.signing]`:

**Internal signing** (`mode = "internal"`) — the pool process signs coinbase-spending transactions directly using the private key provided in the configuration file. This is simpler to set up but keeps the signing key inside the stratum process.

**External signing** (`mode = "external"`) — the pool sends the payout plan as JSON to a remote webhook service. The external service is responsible for building, signing, and broadcasting the transaction. The webhook must return `{"txid": "..."}` on success.

### Which signing mode should I use?

External signing is recommended for production deployments because it keeps private keys out of the stratum process entirely. The signing service can run in a separate, locked-down environment with hardware security module (HSM) support, dedicated audit logging, and independent access controls.

### How is the operator API secured?

All operator API endpoints (except `/health`) require a Bearer token in the `Authorization` header. The token is configured via the `api_token` field in `config.toml` or the `STRATUM_API_TOKEN` environment variable. Use a strong, random token and rotate it periodically.

---

## Operator API Reference

Base path: `/api/v1`

| Method | Path | Description |
|---|---|---|
| GET | `/health` | Liveness check — no auth required |
| GET | `/stats` | Pool statistics (uptime, connected miners, network difficulty) |
| GET | `/workers` | Paginated worker list |
| GET | `/workers/{id}` | Worker detail with statistics |
| GET | `/rounds` | Paginated round list, filterable by status |
| GET | `/rounds/{id}` | Round detail with per-worker share breakdown |
| GET | `/blocks` | Paginated found-block list, filterable by status |
| GET | `/blocks/{hash}` | Block detail |
| GET | `/payouts` | Paginated payout batch list |
| GET | `/payouts/{id}` | Payout batch detail with individual miner payouts |
| POST | `/admin/payouts/trigger/{block_hash}` | Manually trigger payout for a matured block |
| GET | `/shares` | Paginated raw share submissions |
| GET | `/share-outcomes` | Paginated validated share outcomes |

All authenticated endpoints use `Bearer <token>` in the `Authorization` header.

Pagination parameters (applied to list endpoints): `?limit=100&offset=0`. Default limit is 100; maximum is 1000.

---

## Troubleshooting

### Miners are connecting but all shares are rejected

Check the following:

1. Is the worker authorized? Ensure the miner is using the correct `address[.suffix]` format and sending `mining.authorize` before `mining.submit`.
2. Is the job still active? If `clean_jobs` was sent in a recent `mining.notify`, all previous jobs are stale.
3. Is the session difficulty reasonable? Very low difficulty can cause excessive share submissions that may be rate-limited. Check `vardiff_target_secs` in the configuration.
4. Are the `extranonce2`, `ntime`, and `nonce` values valid hex of the correct length?

### Miners are connecting but getting "unauthorized" errors

The worker name must follow the format `payout_address[.suffix]`. For example:
- `lotus_16PSJKdoxf1GgqytWwEop2rg7cNZHXCTxn2hhU3Zz` — bare address (one worker)
- `lotus_16PSJKdoxf1GgqytWwEop2rg7cNZHXCTxn2hhU3Zz.rig1` — address with rig suffix
- `lotus_16PSJKdoxf1GgqytWwEop2rg7cNZHXCTxn2hhU3Zz.rig2` — another rig, same address, different worker

Both use the same payout address but are tracked as separate workers for statistics.

### The server won't start

Check these common issues:

1. **NNG connection failure** — ensure lotusd is running and the `nng_rpc_url` / `nng_pub_url` in `config.toml` point to valid IPC endpoints
2. **Port conflict** — the stratum port, API port, or HTTP port may already be in use
3. **Database error** — if `sqlite_path` points to a non-writable location, the server will fail on schema initialization
4. **Missing configuration** — ensure `config.toml` exists and has all required fields filled in (start from `config.example.toml`)

### The database is growing large

The SQLite database in WAL mode will grow over time. The server performs a WAL checkpoint (`PRAGMA wal_checkpoint(TRUNCATE)`) during graceful shutdown to keep the file size in check. For ongoing maintenance, `VACUUM` can be run manually during a maintenance window.

### How do I trigger a payout manually?

Use the operator API:

```bash
curl -X POST "http://localhost:18080/api/v1/admin/payouts/trigger/<block-hash>" \
  -H "Authorization: Bearer <your-api-token>"
```

This is useful for testing or if automatic payout processing was interrupted.

---

*This FAQ is maintained alongside the `stratum-server-nng` codebase. If you have additional questions, please consult the source code or documentation in `docs/`.*
