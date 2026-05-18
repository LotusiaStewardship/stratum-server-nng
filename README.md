# stratum-server-nng

Production-grade Stratum V1 pool server for Lotus blockchain using NNG (Nanomsg Next Generation) endpoints.

## Overview

`stratum-server-nng` is a high-performance mining pool server that connects Lotus miners to the Lotus network. It provides:

- **Stratum V1 protocol** compatibility with existing mining hardware and software
- **Dynamic difficulty adjustment** that tracks network difficulty automatically
- **PPLNS (Pay Per Last N Shares)** payout scheme for fair reward distribution
- **SQLite-based accounting** for shares, rounds, found blocks, and payouts
- **NNG integration** for efficient communication with lotusd nodes
- **Operator API** for monitoring and management
- **Automatic payout scheduling** with internal transaction signing

## Architecture

### NNG Event-Driven Template Refresh

The Stratum server uses a **pub/sub notification + RPC fetch** pattern for mining template updates:

1. **NNG Pub/Sub** - Subscribes to lightweight event notifications from lotusd:
   - `miningwrkchg` - PRIMARY event: mining work changed (new block, reorg, mempool change, manual invalidation). Includes `template_epoch` for missed-event detection.
   - `blkconnected` - SECONDARY: block connected (for accounting: marking blocks matured)
   - `blkdisconctd` - SECONDARY: block disconnected (for accounting: orphaning found blocks)

**Note:** `mempooltxadd` / `mempooltxrem` are explicitly NOT subscribed — lotusd consolidates these into `miningwrkchg` to avoid excessive template refreshes.

2. **RPC Fetch** - On receiving any notification, the server fetches the full `MiningTemplate` via NNG RPC:
   - `prev_hash_stratum` - Previous block hash for miners
   - `coinbase1` / `coinbase2` - Coinbase transaction parts
   - `merkle_branches` - Transaction merkle path
   - `nbits_stratum`, `ntime_stratum` - Block header fields
   - `target` - Network difficulty target
   - `block` - Serialized block template
   - `height`, `template_id` - Block metadata

**Why this design?** The pub/sub channel transmits only event **topics** (strings), not heavy template payloads. This keeps the notification path lightweight while allowing on-demand template fetches with full control over parameters (payout script, coinbase identity).

```
┌─────────────────────────────────────────────────────────────────┐
│                        stratum-server-nng                       │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│       ┌──────────────┐  ┌──────────────┐  ┌──────────────┐      │
│       │   Stratum    │  │  Operator    │  │    Payout    │      │
│       │    Server    │  │     API      │  │   Scheduler  │      │
│       │  (TCP:3334)  │  │ (TCP:18080)  │  │   (Hourly)   │      │
│       └──────┬───────┘  └──────────────┘  └──────┬───────┘      │
│              │                                   │              │
│      ┌───────▼───────────────────────────────────▼───────┐      │
│      │              Accounting Database                  │      │
│      │         (SQLite: stratum-accounting)              │      │
│      │     - workers, shares, rounds, found_blocks       │      │
│      │     - payout_batches, schema_migrations           │      │
│      └──────┬────────────────────────────────────┬───────┘      │
│             │                                    │              │
│      ┌──────▼───────────┐              ┌─────────▼────────┐     │
│      │  NNG Adapter     │              │  Bitcoind RPC    │     │
│      │  - RPC (ipc/tcp) │              │  - sendrawtx     │     │
│      │  - Pub/Sub       │              │  - getrawtx      │     │
│      │                  │              │                  │     │
│      │  Pub/Sub events: │              │                  │     │
│      │  • miningwrkchg  │───fetches───►│  MiningTemplate  │     │
│      │  • blkconnected  │   template   │  (full payload)  │     │
│      │  • blkdisconctd  │              │                  │     │
│      └──────────────────┘              └──────────────────┘     │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
         │                                    │
         ▼                                    ▼
┌─────────────────┐                 ┌─────────────────┐
│    lotusd       │                 │    lotusd       │
│  NNG Endpoints  │                 │  JSON-RPC API   │
│  - nngrpc.pipe  │                 │  (port 10604)   │
│  - nngpub.pipe  │                 │                 │
└─────────────────┘                 └─────────────────┘
```

## Directory Structure

```
stratum-server-nng/
├── src/
│   ├── main.rs              # Entry point, task orchestration
│   ├── config.rs            # Configuration loading and validation
│   ├── lib.rs               # Library exports
│   ├── stratum/             # Stratum protocol implementation
│   │   ├── server.rs        # TCP server, connection handling
│   │   ├── engine.rs        # Request/response handling
│   │   ├── protocol.rs      # Stratum V1 protocol types
│   │   ├── job.rs           # Mining job management
│   │   ├── validation.rs    # Share and block validation
│   │   ├── vardiff.rs       # Variable difficulty algorithm
│   │   ├── network_diff.rs  # Network difficulty tracking
│   │   ├── diff_cache.rs    # Difficulty broadcast cache
│   │   ├── worker.rs      # Worker name parsing
│   │   └── mod.rs
│   ├── accounting/          # SQLite accounting layer
│   │   ├── sqlite.rs        # Database operations
│   │   ├── models.rs        # Data models
│   │   └── mod.rs
│   ├── api/                 # Operator HTTP API
│   │   ├── mod.rs           # Axum routes and handlers
│   │   └── ...
│   ├── payout/              # Payout scheduling and signing
│   │   ├── scheduler.rs     # Payout automation
│   │   ├── mod.rs           # PPLNS plan building
│   │   └── ...
│   └── nng/                 # NNG integration
│       ├── adapter.rs       # Bitcoind NNG adapter (RPC + Pub/Sub)
│       └── mod.rs
├── config.toml              # Runtime configuration
├── config.example.toml      # Configuration template
├── Cargo.toml               # Rust dependencies
├── docs/                    # Documentation
│   ├── plans/               # Implementation plans
│   └── ...
└── tests/                   # Integration tests
```

## Configuration

Copy `config.example.toml` to `config.toml` and adjust for your environment.

### Top-Level Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `debug` | bool | `false` | Enable debug-level logging |
| `stratum_bind` | string | `"0.0.0.0:3334"` | TCP bind address for miner connections |
| `api_bind` | string | `"127.0.0.1:18080"` | TCP bind address for operator API |
| `api_token` | string | required | Bearer token for API authentication |
| `sqlite_path` | string | `"./stratum-accounting.sqlite3"` | Path to SQLite database |
| `nng_rpc_url` | string | required | lotusd NNG RPC endpoint (e.g., `ipc://datadir/nngrpc.pipe`) |
| `nng_pub_url` | string | required | lotusd NNG Pub/Sub endpoint (e.g., `ipc://datadir/nngpub.pipe`) |
| `max_request_line_bytes` | int | `8192` | Maximum Stratum request line size |
| `per_conn_req_per_sec` | int | `128` | Rate limit per connection (requests/second) |
| `conn_idle_timeout_secs` | int | `180` | Disconnect idle connections after this duration |
| `max_jobs_cache` | int | `512` | Maximum mining jobs kept in memory |
| `job_refresh_secs` | int | `15` | Template refresh interval from lotusd |

### VarDiff Configuration (`[vardiff]`)

**Network-Aware Dynamic Difficulty** — Pool difficulty automatically tracks network difficulty from lotusd. Miners start at a percentage of network difficulty and VarDiff fine-tunes per-miner based on share rate.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `vardiff_min_floor` | float | `0.001` | Absolute minimum difficulty floor (safety only). Prevents crash to near-zero on edge cases |
| `vardiff_initial_pct` | float | `0.01` | Initial difficulty for new miners as fraction of network difficulty (1% = 0.01) |
| `vardiff_target_secs` | float | `15.0` | Target time between accepted shares. Lower = more shares, more precision |
| `vardiff_retarget_secs` | float | `90.0` | How often vardiff adjusts per miner. Should be ≥ 6× vardiff_target_secs |

**Rationale for defaults:**
- `vardiff_min_floor: 0.001` is a safety floor only — VarDiff operates within `[floor, network_diff]`
- `vardiff_initial_pct: 0.01` means miners start at 1% of network diff, then ramp up based on share rate
- `vardiff_target_secs: 15.0` provides a good balance between precision and server load
- `vardiff_retarget_secs: 90.0` allows ~6 samples for stable statistical adjustment

**Network-Aware Scaling Benefits:**
- No stale config: Pool difficulty tracks network difficulty automatically from lotusd templates
- Simpler mental model: Network difficulty = ground truth, no manual `max_difficulty` tuning
- Automatic adaptation: When network diff changes 10×, pool scales without config changes
- Per-miner optimization: VarDiff tunes each miner individually within the network-aware bounds

### Pool Configuration (`[pool]`)

#### Mining Identity (`[pool.mining_identity]`)

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `payout_address` | string | ✅ (or `payout_script_hex`) | Lotus address for block subsidies |
| `payout_script_hex` | string | ✅ (or `payout_address`) | Raw scriptPubKey hex for payouts |
| `coinbase_identity` | string | optional | UTF-8 tag embedded in coinbase scriptSig (e.g., `"Lotusia Pool"`) |

**Important:** The payout script must NOT be OP_RETURN/nulldata. The server validates this on startup.

#### Fee Configuration (`[pool.fee]`)

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `enabled` | bool | `true` | Enable pool fee collection |
| `fee_bps` | int | `100` | Fee in basis points (100 = 1.00%, max 10000) |
| `fee_address` | string | ✅ (if enabled) | Lotus address for fee collection |
| `fee_script_hex` | string | ✅ (if enabled) | Raw scriptPubKey hex for fees |

#### PPLNS Configuration (`[pool.pplns]`)

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `n_multiplier` | float | `1.0` | PPLNS window target work units. 1.0 ≈ one block's worth of cumulative work |
| `min_payout_sat` | int | `546` | Minimum payout per miner (dust threshold) |
| `payout_interval_secs` | int | `3600` | Scheduler run interval (seconds) |
| `min_confirmations` | int | `100` | Confirmations before payout eligibility (enforced minimum: 100) |

**PPLNS window behavior:**
- The PPLNS window looks back in time from the found block's creation time
- Shares are aggregated by payout address and weighted by their difficulty (work_units)
- The window ends when cumulative work reaches `n_multiplier` target, capped at 200K shares
- **True cross-round behavior**: The window spans multiple rounds (no round_id filter), preventing late-joiner advantage
- Larger `n_multiplier` values smooth variance but slow responsiveness to hashrate changes

#### Signing Configuration (`[pool.signing]`)

| Parameter | Type | Required | Description |
|-----------|------|----------|----------|
| `mode` | string | `"internal"` | Signing mode (currently only `internal` supported) |
| `private_key` | string | ✅ (for internal) | Private key for payout signing (32-byte hex or WIF format) |

**Security note:** The private key is used only for signing payout transactions. Never log this value. Prefer loading from a secure file or secret store.

### Bitcoind RPC Configuration (`[bitcoind_rpc]`)

Used for raw transaction broadcast (payout submission):

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `url` | string | required | JSON-RPC endpoint (e.g., `http://127.0.0.1:10604`) |
| `rpc_user` | string | required | RPC username |
| `rpc_pass` | string | required | RPC password |

## Operator API

The operator API provides monitoring and management endpoints. All endpoints except `/healthz` require Bearer token authentication.

### Authentication

Include the `Authorization` header with your configured token:

```bash
curl -H "Authorization: Bearer devtoken" http://127.0.0.1:18080/status
```

### Endpoints

| Method | Endpoint | Auth | Description |
|--------|----------|------|-------------|
| `GET` | `/healthz` | ❌ | Health check (always returns 200) |
| `GET` | `/readyz` | ❌ | Readiness check (checks DB availability) |
| `GET` | `/status` | ✅ | Full status snapshot with stats |
| `GET` | `/workers` | ✅ | List workers (last 100) |
| `GET` | `/rounds` | ✅ | List recent rounds (last 100) |
| `GET` | `/shares` | ✅ | List recent shares (last 100) |
| `GET` | `/payouts` | ✅ | List recent payout batches (last 100) |
| `GET` | `/workers/summary` | ✅ | Worker accounting summary with accepted/rejected/stale counts |
| `GET` | `/shares/rejected-reasons` | ✅ | Breakdown of rejected share reasons |
| `GET` | `/reconciliation/missing-found-blocks` | ✅ | Found blocks with missing persistence (reconciliation) |
| `GET` | `/health/payout-scheduler` | ✅ | Payout scheduler health (lease status, confirmed blocks, failed batches) |

### Example: Status Response

```json
{
  "status": "ok",
  "payout_method": "pplns",
  "idle_disconnects": 42,
  "rate_limit_disconnects": 3,
  "template_payout_mismatch_total": 0,
  "candidate_payout_mismatch_total": 0,
  "found_block_persist_ok_total": 5,
  "found_block_persist_error_total": 0,
  "found_block_observed_not_persisted_total": 0,
  "found_blocks": {
    "pending": 2,
    "matured": 1,
    "orphaned": 0,
    "paid": 2
  },
  "payout_batches": {
    "planned": 1,
    "signed": 0,
    "submitted": 0,
    "confirmed": 0,
    "invalidated_orphan": 0,
    "failed": 0
  },
  "latest_payout_batch_status": "confirmed",
  "latest_payout_batch_txid": "abc123...",
  "scheduler": {
    "matured_found_blocks_ready": 1,
    "retry_ready_batches": 0,
    "next_retry_at": null
  }
}
```

## Database Schema

The SQLite database tracks all accounting state. Key tables:

### `workers`
- `id` - Primary key
- `payout_address` - Lotus address
- `worker_suffix` - Optional worker identifier (e.g., `rig1`, `worker-abc`)
- `created_at` - Timestamp

### `shares`
- `id` - Primary key
- `worker_id` - Foreign key to workers
- `template_id` - Mining template ID
- `difficulty` - Share difficulty
- `accepted` - 1 if accepted, 0 if rejected
- `stale` - 1 if stale, 0 if not
- `dedupe_key` - Unique key for deduplication
- `created_at` - Timestamp

### `rounds`
- `id` - Primary key
- `start_template_id` - Round start template
- `end_template_id` - Round end template (when found)
- `found_block_hash` - Block hash if found
- `created_at` - Timestamp

### `found_blocks`
- `id` - Primary key
- `round_id` - Foreign key to rounds
- `block_hash` - Unique block hash
- `height` - Block height
- `status` - `pending`, `matured`, `orphaned`, `paid`
- `confirmations` - Number of confirmations
- `template_id` - Template that found the block
- `worker_id` - Worker that found the block
- `worker_name` - Worker name at find time
- `payout_address` - Payout address at find time
- `persist_source` - How the block was persisted
- `created_at` - Timestamp

### `payout_batches`
- `id` - Primary key
- `found_block_id` - Foreign key to found_blocks
- `retry_key` - Idempotency key for retries
- `gross_reward_sat` - Total reward in satoshis
- `fee_sat` - Pool fee in satoshis
- `net_reward_sat` - Net reward after fees
- `status` - `planned`, `signed`, `submitted`, `confirmed`, `failed`
- `submitted_txid` - Transaction ID if submitted
- `error` - Error message if failed
- `retry_at` - Scheduled retry time
- `created_at` - Timestamp

### `payout_dust_ledger`
Tracks un-paid dust amounts for carry-forward in PPLNS payouts:
- `id` - Primary key
- `payout_batch_id` - Foreign key to payout_batches (which batch created the dust)
- `found_block_id` - Foreign key to found_blocks
- `address` - Payout address (Lotus address string)
- `amount_sat` - Un-paid dust amount in satoshis (0 when fully paid out)
- `policy` - Payout policy (e.g., "pplns")
- `created_at` - Timestamp

When a miner's accumulated dust reaches `min_payout_sat`, it's included in their next payout. The ledger uses FIFO ordering to reduce entries after payout.

### `payout_entries`
Individual miner payouts within a batch:
- `id` - Primary key
- `payout_batch_id` - Foreign key to payout_batches
- `address` - Payout address
- `amount_sat` - Amount in satoshis
- `created_at` - Timestamp

### `payout_share_snapshots`
Shares aggregated for PPLNS window construction:
- `id` - Primary key
- `payout_batch_id` - Foreign key to payout_batches
- `share_id` - Foreign key to share_outcomes
- `payout_address` - Payout address
- `work_units` - Difficulty-weighted work units
- `share_created_at` - When the share was created
- `ordering_criterion` - For deterministic distribution
- `truncation_reason` - Why shares were truncated
- `created_at` - Timestamp

### `payout_scheduler_lease`
Scheduler lease management for distributed payout coordination:
- `id` - Primary key (always 1)
- `owner` - Lease owner identifier
- `expires_at` - Lease expiration time

### `accounting_events`
General accounting events for monitoring:
- `id` - Primary key
- `event_type` - Event type (e.g., "found_block_orphaned")
- `status`, `session_id`, `worker_id`, `worker_name`, `payout_address`, `share_id`, `round_id`, `template_id`, `template_epoch`, `job_id`, `block_hash`, `height`, `payload_json`, `created_at`

### `authorization_events`
Worker authorization events:
- `id` - Primary key
- `session_id` - Session identifier
- `worker_name` - Worker name
- `payout_address` - Payout address
- `worker_suffix` - Worker suffix
- `authorized` - Whether authorization succeeded
- `reason` - Rejection reason
- `created_at` - Timestamp

### `share_outcomes`
Share validation outcomes:
- `id` - Primary key
- `session_id`, `worker_id`, `worker_name`, `payout_address`, `template_id`, `template_epoch`, `job_id`, `round_id`, `dedupe_key`, `status`, `reject_reason`, `node_result`, `low_diff_ok`, `network_target_ok`, `block_hash`, `share_id`, `created_at`

### `round_events`
Round lifecycle events:
- `id` - Primary key
- `round_id` - Foreign key to rounds
- `event_type` - Event type
- `reason`, `block_hash`, `template_id`, `created_at`

### `submit_events`
Block submission results from node:
- `id` - Primary key
- `block_hash`, `template_id`, `worker_id`, `worker_name`, `payout_address`, `node_result`, `created_at`, `session_id`, `job_id`, `round_id`, `template_epoch`

### `meta` and `schema_migrations`
Metadata and schema version tracking tables.

## Stratum Protocol

Implements Stratum V1 protocol with the following methods:

### Client → Server

- `mining.subscribe` - Subscribe to mining notifications
- `mining.authorize` - Authenticate worker (format: `address.workerSuffix`)
- `mining.submit` - Submit share (params: `user`, `job_id`, `extraNonce2`, `nTime`, `nonce`)
- `mining.extranonce.subscribe` - Subscribe to extranonce changes
- `mining.ping` - Keep-alive ping
- `mining.suggest_difficulty` - Suggest difficulty (not used)

### Server → Client

- `mining.notify` - New mining job notification
- `mining.set_difficulty` - Adjust miner difficulty
- `mining.set_extranonce` - Update extranonce range

### Authentication Format

Workers authenticate with: `lotus_address.worker_suffix`

Examples:
- `lotus_qq987...xyz.main`
- `lotus_qq123...abc.rig1`
- `lotus_qq456...def` (no suffix)

The payout address is extracted from the authentication string. All shares are attributed to this address.

## Dynamic Difficulty

The server implements **network-aware dynamic difficulty** with per-miner VarDiff tuning:

1. **Network Difficulty** — Ground truth, tracked from lotusd mining templates via NNG pub/sub
2. **Miner Difficulty** — Starts at `network_diff × vardiff_initial_pct`, fine-tuned per-miner by VarDiff

### How It Works

1. lotusd publishes new mining templates via NNG pub/sub when chain state changes
2. Server fetches template, extracts target, converts to network difficulty
3. All miners receive `mining.set_difficulty` broadcast when network diff changes >10%
4. Each miner's VarDiff operates independently within `[vardiff_min_floor, network_diff]`
5. VarDiff retargets every `vardiff_retarget_secs` based on observed share rate

### VarDiff Algorithm

Per-miner difficulty adjusts based on:
- **Target interval** (`vardiff_target_secs`) — Desired time between accepted shares (default: 15s)
- **Actual interval** — Measured from timestamps of accepted shares in rolling window
- **Retarget interval** (`vardiff_retarget_secs`) — How often adjustment occurs (default: 90s)

**Adjustment formula:**
```
ratio = target_secs / avg_share_interval
ratio = clamp(ratio, 0.67, 1.5)  // ±50% max change per retarget
new_diff = current_diff × ratio
new_diff = clamp(new_diff, vardiff_min_floor, network_diff)
```

**Lifecycle:**
- **Initialization:** Miner connects → starts at `network_diff × 0.01` (1%)
- **Ramp-up:** Fast shares arrive → difficulty increases by up to 50% per retarget
- **Plateau:** Difficulty stabilizes when share rate matches target, or hits network diff ceiling
- **Network changes:** When network diff drops, miner difficulty is immediately clamped to new ceiling

## Payout System

### PPLNS Algorithm

The server uses PPLNS (Pay Per Last N Shares) with true cross-round window behavior:

**Window Construction:**
1. When a block is found, it enters `confirmed` status
2. After `min_confirmations` (min 100), it becomes `matured`
3. The scheduler runs every `payout_interval_secs` (default: 1 hour)
4. For each matured block:
   - Query shares accepted and non-stale from the found block's creation time backward
   - Stop when cumulative work reaches `n_multiplier` target (capped at 200K shares)
   - Aggregate by payout address, weighted by difficulty (work_units)
5. Build payout plan with fee deduction and dust carry-forward
6. Sign and broadcast payout transaction
7. Track confirmation via `getrawtransaction`

**Cross-Round Behavior:**
- The PPLNS window spans multiple rounds (no `round_id` filter)
- Prevents late-joiner advantage by including shares from previous rounds
- Implements early-leaver penalty: miners who leave before a block is found don't benefit from their earlier shares
- Dust carry-forward: un-paid amounts below `min_payout_sat` accumulate across rounds and are included in future payouts

**Deterministic Remainder Distribution:**
- Floor division remainder (fractional satoshis) distributed to miners with largest fractional parts
- Tie-breaking by address ascending for deterministic ordering
- Ensures reproducible, fair payouts regardless of input order

### Fee Calculation

```
fee_sat = gross_reward_sat * (fee_bps / 10000)
net_reward_sat = gross_reward_sat - fee_sat
```

Fees are deducted before distributing to miners. If no fee address is configured, fees return to the pool payout address.

### Transaction Signing

The internal signer:
1. Loads the coinbase transaction from the found block
2. Creates a new transaction spending the payout output (vout[1])
3. Signs with the configured private key (P2PKH only)
4. Broadcasts via JSON-RPC `sendrawtransaction`
5. Tracks confirmation via `getrawtransaction`

**Limitation:** Current implementation supports only P2PKH payout scripts.

## Deployment

### Prerequisites

- Rust 1.70+ (edition 2021)
- lotusd node with NNG endpoints enabled
- JSON-RPC access to lotusd for payout broadcast

### Build

```bash
cd stratum-server-nng
cargo build --release
```

### Configuration

1. Copy `config.example.toml` to `config.toml`
2. Set `payout_address` to your pool's receiving address
3. Configure `nng_rpc_url` and `nng_pub_url` for your lotusd instance
4. Set `api_token` to a secure random value
5. Configure `pool.signing.private_key` for payout signing
6. Adjust vardiff parameters for your network (testnet vs mainnet)

### Run

```bash
./target/release/stratum-server-nng --config config.toml
```

Or with debug logging:

```bash
./target/release/stratum-server-nng --config config.toml --debug
```

### Environment Variables

| Variable | Overrides | Description |
|----------|-----------|-------------|
| `STRATUM_API_TOKEN` | `api_token` | Bearer token for API authentication |

### Systemd Service Example

```ini
[Unit]
Description=Lotus Stratum Server
After=network.target lotusd.service

[Service]
Type=simple
User=lotus
WorkingDirectory=/opt/lotus/stratum-server-nng
ExecStart=/opt/lotus/stratum-server-nng/target/release/stratum-server-nng --config config.toml
Restart=on-failure
LimitNOFILE=65535

[Install]
WantedBy=multi-user.target
```

## Monitoring

### Metrics to Watch

- **`/status` endpoint** - Overall health and statistics
- **Idle disconnects** - High values may indicate network issues
- **Rate limit disconnects** - May indicate misconfigured miners or attacks
- **Found block persistence errors** - Should always be 0
- **Template/candidate payout mismatch** - Configuration drift indicator

### Logs

Key log messages:
- `runtime configuration loaded` - Startup config summary
- `nng endpoints configured` - NNG connection info
- `dynamic pool difficulty initialized` - VarDiff setup
- `payout batch signed and submitted` - Successful payout

Enable debug logging with `--debug` flag or `RUST_LOG` environment variable.

## Development

### Testing

```bash
# Run all tests
cargo test --lib

# Run specific module tests
cargo test --lib network_diff
cargo test --lib diff_cache
cargo test --lib accounting

# Check for compilation errors
cargo check

# Build with all features
cargo build --bin stratum-server-nng
```

### Code Structure Guidelines

- **stratum/** - Protocol and connection handling (stateless where possible)
- **accounting/** - Database operations (single source of truth)
- **api/** - HTTP handlers (thin layer over accounting)
- **payout/** - Payout logic (isolated from stratum)
- **nng/** - Node communication (adapter pattern)

### Adding Features

Before implementing:
1. Check nearest `AGENTS.md` for project guidelines
2. Verify no existing implementation in codebase
3. Consider if feature belongs in stratum-server or separate service
4. Ensure changes don't break existing accounting or payout logic

## Security Considerations

### Private Keys

- `pool.signing.private_key` is used only for payout signing
- Never log or expose this value
- Consider using a hardware wallet or external signer for production
- File permissions should be restricted (e.g., `chmod 600 config.toml`)

### API Token

- Use a strong random token (e.g., `openssl rand -hex 32`)
- Restrict API access to localhost or protected network
- Use reverse proxy with TLS for remote access

### Database

- SQLite WAL mode is enabled by default for durability
- Regular backups recommended for production
- Database contains sensitive payout information

### Network

- Stratum port (3334) should be publicly accessible for miners
- API port (18080) should be restricted to localhost/internal network
- NNG endpoints should use IPC when possible (not TCP)

## Troubleshooting

### Common Issues

**"invalid payout address"**
- Ensure address is valid Lotus address format
- Check network (testnet vs mainnet)

**"payout script cannot be OP_RETURN"**
- The payout address must be spendable (P2PKH, P2SH, etc.)
- OP_RETURN/nulldata scripts are rejected for safety

**"api_token required"**
- Set `api_token` in config or `STRATUM_API_TOKEN` env var
- Token cannot be empty

**"failed connecting to NNG endpoint"**
- Verify lotusd is running with NNG enabled
- Check IPC path permissions
- Ensure `nng_rpc_url` and `nng_pub_url` are correct

**"payout scheduler disabled"**
- Only `internal` signing mode is currently supported
- Check `pool.signing.mode` configuration

### Reconciliation

The server includes automatic reconciliation for missing found block persistence:

- Runs every 60 seconds
- Checks for `found_block` records without proper persistence
- Attempts repair from submit events
- Exposed via `/reconciliation/missing-found-blocks` API endpoint

## License

MIT License - see LICENSE file for details.

## Contributing

1. Read the root `AGENTS.md` for repository guidelines
2. Read nested `AGENTS.md` files for subproject-specific guidance
3. Keep changes minimal and focused
4. Add tests for new functionality
5. Update documentation as needed

## See Also

- [lotusd](../lotusd/) - Lotus core node
- [bitcoinsuite](../bitcoinsuite/) - Bitcoin/Lotus protocol primitives
- [lotus-web-wallet](../lotus-web-wallet/) - Web wallet that can connect to pool
- [chronik_nng](../chronik_nng/) - NNG indexer integration