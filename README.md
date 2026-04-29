# stratum-server-nng

Rust Stratum V1 pooled mining server for Lotus, using `lotusd` NNG endpoints as the sole node integration surface.

## Scope implemented in this iteration

- Foundation for production runtime (Tokio + structured logging)
- Native Stratum V1 protocol model and session state
- **Phase S3**: share pre-validation + vardiff model
- **Phase S4**: durable accounting core with SQLite3
- Token-authenticated operator API scaffold
- Optional payout signer integration scaffold

## Explicitly enforced

- Worker authorization format: `<lotus_address>[.<worker>]`
- PPLNS-ready accounting schema with strategy abstraction for future payout methods

## Node integration

Uses `bitcoinsuite-bitcoind-nng` and expects new mining RPC + pub message support:

- GetMiningTemplateRequest
- SubmitMinedBlockRequest
- ValidateMinedBlockProposalRequest
- GetMiningStatusRequest
- miningworkchg topic

## Future-scaffolded but intentionally not wired yet

- `mining.extranonce.subscribe`
- `mining.set_extranonce`
- `mining.suggest_difficulty`

These are represented in protocol enums/handlers as reserved TODOs and documented for later implementation review.

## Security baseline

- Operator API requires bearer token auth
- Key material is isolated behind optional signer trait (can remain disabled)
- Input validation for Stratum fields and worker naming rules

## Running (development)

```bash
cargo test
cargo run -- --help
```
