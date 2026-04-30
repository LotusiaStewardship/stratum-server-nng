# stratum-server-nng

Rust Stratum V1 pooled mining server for Lotus.

This service uses **lotusd NNG RPC + NNG Pub/Sub** as its node-control plane, and stores pool accounting state in SQLite.

---

## 1) What this server does at runtime

At a high level, the process runs three concurrent loops:

1. **Stratum TCP loop** (`--stratum-bind`)
   - accepts miner sockets
   - handles `subscribe/authorize/submit/ping`
   - pushes `lotus.precomputed_work` and `mining.set_difficulty`

2. **Operator API loop** (`--api-bind`)
   - exposes health + admin read endpoints
   - token-auth protected (except `/healthz` and `/readyz`)

3. **Job refresh loop**
   - receives NNG pub events (`updateblktip`, `mempooltxadd`, `mempooltxrem`, `miningwrkchg`)
   - fetches consensus-derived templates via `GetMiningTemplateRequest`
   - builds per-session **precomputed Lotus work objects** and fans them out to connected miners
   - also performs periodic template refresh ticks

All accepted shares are written idempotently into SQLite (`shares.dedupe_key`).

---

## 2) Runtime model (important concepts)

### Job + template epoch

- Every generated job gets a monotonic `template_epoch`.
- `template_epoch` is used to reason about work freshness and staleness.
- Job IDs are derived as `job-<template_id>-<template_epoch>`.
- `template_id` comes from lotusd `GetMiningTemplateResponse`.

### Worker identity format

Workers are strictly parsed as:

```text
<lotus_address>[.<worker>]
```

Examples:
- `lotus_abc`
- `lotus_abc.rig01`

The left side is payout identity, optional suffix is a worker label.

### Share lifecycle

On `mining.submit`:
1. request shape checks (hex lengths etc.)
2. worker authorization + active job ownership checks
3. pool-side difficulty precheck
4. candidate block reconstruction from template + submit tuple
5. lotusd proposal validation (`ValidateMinedBlockProposalRequest`)
6. lotusd candidate submit (`SubmitMinedBlockRequest`)
7. response classification and idempotent share write
8. vardiff update and optional retarget (`mining.set_difficulty`)

---

## 3) Runtime configuration (config.toml)

Server configuration is file-based. Start with `--config` (default `./config.toml`).

Minimal required safety section:

```toml
[pool.mining_identity]
payout_address = "lotus_..." # or payout_script_hex
```

Startup now hard-fails if payout script is missing or resolves to `OP_RETURN`/nulldata.

CLI flags now only control bootstrap:
- `--config`
- `--debug`

### Logging and diagnostics

- `--debug` (default: `false`)
  - Enables verbose request/event debug logs.
  - Normal mode still emits operationally useful logs.

### Network/service binds

- `--stratum-bind` (default: `0.0.0.0:3334`)
- `--api-bind` (default: `127.0.0.1:18080`)

### Auth

- `api_token` is configured in `config.toml` (or env override `STRATUM_API_TOKEN`).

### Storage + lotusd connectivity

Configured in `config.toml`:
- `sqlite_path`
- `nng_rpc_url`
- `nng_pub_url`

Both `ipc://` and `tcp://` NNG URLs are supported.

### Difficulty / vardiff

Configured in `config.toml`:
- `initial_difficulty`, `min_difficulty`, `max_difficulty`
- `vardiff_target_secs`, `vardiff_retarget_secs`

### Protocol hardening

- `--max-request-line-bytes` (default: `8192`)
- `--per-conn-req-per-sec` (default: `128`)
- `--conn-idle-timeout-secs` (default: `180`)
- `--max-jobs-cache` (default: `512`)
- `--job-refresh-secs` (default: `15`)

---

## 4) Launch examples

### Local development / regtest

```bash
cp config.example.toml config.toml
cargo run -- --config ./config.toml
```

### Verbose debug run

```bash
cargo run -- --config ./config.toml --debug
```

### Production-style run (release)

```bash
cargo run --release -- \
  --api-token 'replace-with-long-random-token' \
  --stratum-bind 0.0.0.0:3334 \
  --api-bind 127.0.0.1:18080 \
  --sqlite-path /var/lib/stratum-server-nng/accounting.sqlite3 \
  --nng-rpc-url tcp://127.0.0.1:4555 \
  --nng-pub-url tcp://127.0.0.1:4556
```

---

## 5) Stratum protocol support

### Implemented methods

Client -> server:
- `mining.subscribe`
- `mining.authorize`
- `mining.submit`
- `mining.ping`

Server -> client:
- `mining.set_difficulty`
- `lotus.precomputed_work` (required Lotus mining work notification)

`lotus.precomputed_work` params are:
1. `job_id`
2. `header_160_hex`
3. `share_target_hex` (32-byte hex, big-endian)
4. `extranonce2_hex`
5. `ntime_hex_6b`
6. `clean_jobs`

Validation contract:
- `job_id` non-empty string
- `header_160_hex` length `320` hex chars
- `share_target_hex` length `64` hex chars
- `extranonce2_hex` length `8` hex chars
- `ntime_hex_6b` length `12` hex chars
- `clean_jobs` boolean

### Optional/scaffold behavior

- `mining.extranonce.subscribe`: accepted/acknowledged
- `mining.set_extranonce`: parsed but currently rejected as unsupported
- `mining.suggest_difficulty`: parsed but currently rejected as unsupported

---

## 6) Operator API

### Health endpoints

- `GET /healthz`
- `GET /readyz`

### Authenticated endpoints

Require:

```http
Authorization: Bearer <api-token>
```

Routes:
- `GET /status` (includes runtime idle/rate-limit disconnect counters)
- `GET /workers`
- `GET /rounds`
- `GET /shares`
- `GET /payouts`

Example:

```bash
curl -H 'Authorization: Bearer devtoken' http://127.0.0.1:18080/status
```

---

## 7) Log guide for operators

## Normal INFO logs you should expect

- startup and config load
- NNG adapter connection + subscriptions
- inbound NNG events (`updateblktip`, `mempool refresh`, `miningwrkchg`)
- job refresh tick and job publication
- connection accept/close
- accepted shares persisted
- vardiff retarget changes
- rate-limit or idle disconnect events

## DEBUG logs (`--debug`) include

- per-request method/id traces
- per-message NNG payload traces
- detailed precomputed_work/set_difficulty send traces
- cache depth and job publish diagnostics

## Example: healthy new-block flow

1. `NNG pub message received topic="updateblktip" ...` (debug)
2. `NNG event: updateblktip; refreshing job template_epoch=...` (info)
3. `published mining job job_id=... template_epoch=...` (debug)
4. `forwarded new mining job to miner ...` (info)

If step (2) appears but step (4) does not, there may be no active miner sessions.

---

## 8) Troubleshooting checklist

### No NNG events appear

- verify lotusd launched with matching `-nngpub` endpoint
- verify topic enablement includes `miningwrkchg` / `updateblktip`
- verify IPC path permissions (`ls -l` on pipe directory)
- run with `--debug` and check for `NNG pub subscriptions active` log

### Miners connect but do not receive new jobs

- verify `mining.subscribe` and `mining.authorize` success from miner logs
- check for `sent lotus.precomputed_work` lines
- check for session disconnects due to idle/rate limits

### Shares rejected or duplicated

- inspect `invalid-submit-shape`, `unauthorized-worker`, `stale-job`
- verify miner worker string format `<lotus_address>[.<worker>]`
- inspect duplicate share warnings in logs

---

## 9) Security baseline

- Operator API is bearer-token protected.
- Keep API bound to localhost or secured ingress.
- Prefer NNG IPC endpoints on same host where possible.
- Keep signing keys outside the core process unless explicitly enabling signer integrations.

---

## 10) Node integration expectations

This server is designed around lotusd NNG mining-capable interfaces and topics.

Expected mining-capable surfaces include:
- `GetMiningTemplateRequest`
- `SubmitMinedBlockRequest`
- `ValidateMinedBlockProposalRequest`
- `GetMiningStatusRequest`
- `miningwrkchg`

Current integration uses typed mining RPCs in `bitcoinsuite-bitcoind-nng` (flatbuffers v25 generation) and direct template-to-job mapping in server runtime.

---

## 11) Development quickstart

```bash
cargo test
cargo run -- --api-token devtoken --debug
```
