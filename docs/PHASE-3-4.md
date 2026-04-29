# Implementation notes (S1/S2/S3/S4/S6)

## S1 protocol engine
- `stratum/protocol.rs`
- `stratum/engine.rs`
- `stratum/server.rs`

Highlights:
- strict JSON-line parsing with max length guard
- request-id replay protection window
- per-connection rate limiting + idle timeout
- implemented methods: subscribe, authorize, submit, ping
- optional methods represented and handled safely

## S2 NNG adapter + job lifecycle
- `nng/adapter.rs`
- `stratum/job.rs`
- `stratum/server.rs`

Highlights:
- raw pub receive support via `bitcoinsuite-bitcoind-nng::PubInterface::recv_raw`
- topic normalization for template-affecting events
- bounded in-memory job cache with clean-job invalidation
- notify fanout via broadcast channel

## S3 share validation + vardiff
- `stratum/validation.rs`
- `stratum/vardiff.rs`
- `stratum/worker.rs`

Highlights:
- strict worker naming/auth format `<lotus_address>[.<worker>]`
- submit shape validation for nonce/time/extranonce fields
- stale-job checks against active job set
- independent vardiff model with clamp + retarget notifications

## S4 accounting core
- `accounting/sqlite.rs`
- `accounting/models.rs`

Schema includes:
- schema_migrations
- workers
- shares (idempotent dedupe key)
- rounds
- found_blocks
- payout_batches
- payout_entries
- meta (active payout method)

Current payout method activation:
- PPLNS active default
- PPS/PROP scaffolded

## S6 operator API
- `api/mod.rs`

Routes:
- `/status`
- `/workers`
- `/rounds`
- `/shares`
- `/payouts`
- `/healthz`
- `/readyz`
