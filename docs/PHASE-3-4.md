# Phase 3 + 4 implementation notes

## Phase S3 (share validation + vardiff)

Implemented modules:
- `stratum/validation.rs`
- `stratum/vardiff.rs`
- `stratum/worker.rs`

Highlights:
- strict native submit shape checks
- worker naming enforcement `<lotus_address>[.<worker>]`
- bounded vardiff timestamp buffer and clamped retarget

## Phase S4 (authoritative accounting)

Implemented module:
- `accounting/sqlite.rs`

Schema includes:
- workers
- shares (idempotent dedupe key)
- rounds (scaffold)
- payout_batches (scaffold)
- meta (active payout method)

Current payout method activation:
- PPLNS selected as active default
- PPS/PROP scaffolded as enum and reserved for future implementation
