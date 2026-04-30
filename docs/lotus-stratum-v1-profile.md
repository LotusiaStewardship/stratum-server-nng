# Lotus Stratum V1 Profile (v1)

This profile defines the required behavior for `stratum-server-nng` clients.

## Handshake and sequencing

Required request sequence:

1. `mining.subscribe`
2. `mining.authorize`
3. `mining.submit` (only after successful authorize)

Server behavior:

- `mining.authorize` before subscribe -> error `[25, "not-subscribed", null]`
- `mining.submit` before subscribe -> error `[25, "not-subscribed", null]`
- unauthorized worker submit -> error `[24, "unauthorized-worker", null]`

The server sends `mining.set_difficulty` and `lotus.precomputed_work` only after a successful subscribe response.

## Work notification

`lotus.precomputed_work` is mandatory in this profile:

Params:
1. `job_id` (string, non-empty)
2. `header_160_hex` (320 hex chars)
3. `share_target_hex` (64 hex chars, big-endian)
4. `extranonce2_hex` (8 hex chars)
5. `ntime_hex_6b` (12 hex chars)
6. `clean_jobs` (bool)

## Error code matrix

- `20` invalid/unsupported request or shape
- `21` stale job
- `22` duplicate in-flight request id
- `23` low difficulty share
- `24` unauthorized worker
- `25` not subscribed

## Request ID policy

Duplicate checking is in-flight only. Historical ID reuse is allowed after prior request completion.

## Compatibility

Default mode is Lotus-native and requires `lotus.precomputed_work` support.

Compatibility matrix:

- `lotus-gpu-miner` (current): ✅ full support
- Generic Stratum V1 `mining.notify` client: ❌ by default
- Generic Stratum V1 `mining.notify` client with `emit_mining_notify_compat=true`: ⚠️ partial/experimental
