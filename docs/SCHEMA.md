# Database Schema Reference

**Last updated:** 2026-05-22  
**Source:** `src/accounting/schema.rs`

---

## Overview

SQLite database with WAL journal mode. Foreign keys enforced. All schema initialization is idempotent (safe to call on existing databases).

---

## Table: `workers`

Miner identity persisted across sessions.

| Column | Type | Constraints | Description |
|--------|------|-------------|-------------|
| id | INTEGER | PK AUTOINCREMENT | Surrogate key |
| payout_address | TEXT | NOT NULL | Lotus address for payouts |
| worker_suffix | TEXT | | Optional rig/worker name (NULL for bare address) |
| created_at | DATETIME | NOT NULL DEFAULT CURRENT_TIMESTAMP | First seen |

**Unique:** `(payout_address, worker_suffix)` — same address with different suffixes are distinct workers.

---

## Table: `authorization_events`

Immutable audit log of every `mining.authorize` attempt.

| Column | Type | Constraints | Description |
|--------|------|-------------|-------------|
| id | INTEGER | PK AUTOINCREMENT | |
| session_id | TEXT | NOT NULL | Session that sent authorize |
| worker_name | TEXT | NOT NULL | Raw worker name from submit |
| payout_address | TEXT | NOT NULL | Resolved from worker_name |
| worker_suffix | TEXT | | Extracted suffix |
| authorized | INTEGER | NOT NULL | 1=success, 0=failure |
| reason | TEXT | | Failure reason (NULL on success) |
| created_at | DATETIME | NOT NULL DEFAULT CURRENT_TIMESTAMP | |

**Indexes:** `session_id`, `payout_address`

---

## Table: `shares`

Raw share submission records. Immutable once persisted.

| Column | Type | Constraints | Description |
|--------|------|-------------|-------------|
| id | INTEGER | PK AUTOINCREMENT | |
| worker_id | INTEGER | NOT NULL FK → workers(id) | Submitting worker |
| session_id | TEXT | NOT NULL | Originating session |
| job_id | TEXT | NOT NULL | Target job ID |
| template_id | INTEGER | NOT NULL | Parsed from job_id |
| template_epoch | INTEGER | NOT NULL | Parsed from job_id |
| extranonce1 | TEXT | NOT NULL | Session's extranonce1 |
| extranonce2 | TEXT | NOT NULL | Miner's chosen extranonce2 |
| ntime_hex_6b | TEXT | NOT NULL | 6-byte hex ntime |
| nonce_hex_8b | TEXT | NOT NULL | 8-byte hex nonce |
| difficulty | REAL | NOT NULL | P_diff at job assignment time |
| dedupe_key | TEXT | NOT NULL UNIQUE | Deduplication key |
| created_at | DATETIME | NOT NULL DEFAULT CURRENT_TIMESTAMP | |

**Unique:** `dedupe_key` → `worker_id:template_id:template_epoch:extranonce2:ntime:nonce`

**Indexes:** `worker_id`, `created_at`, `dedupe_key`

---

## Table: `share_outcomes`

Share validation results. One-to-one with shares (via dedupe_key).

| Column | Type | Constraints | Description |
|--------|------|-------------|-------------|
| id | INTEGER | PK AUTOINCREMENT | |
| share_id | INTEGER | NOT NULL FK → shares(id) | |
| session_id | TEXT | NOT NULL | |
| worker_id | INTEGER | NOT NULL FK → workers(id) | |
| job_id | TEXT | NOT NULL | |
| round_id | INTEGER | FK → rounds(id) | Round at insert time (nullable for initial shares before round is created) |
| dedupe_key | TEXT | NOT NULL UNIQUE | Same as shares.dedupe_key |
| status | TEXT | NOT NULL | 'accepted', 'rejected', 'stale' |
| reject_reason | TEXT | | NULL when accepted; see rejection reasons below |
| node_result | TEXT | | lotusd submitblock result (NULL for non-block shares) |
| low_diff_ok | INTEGER | | 1=hash met P_diff target |
| network_target_ok | INTEGER | | 1=hash met N_diff target (high-hash share) |
| block_hash | TEXT | | Non-NULL if share found a block candidate |
| created_at | DATETIME | NOT NULL DEFAULT CURRENT_TIMESTAMP | |

**Unique:** `dedupe_key`

**Rejection reasons:** `stale-job`, `ntime-mismatch`, `invalid-submit-shape`, `low-difficulty-share`, `unauthorized-worker`

---

## Table: `rounds`

Payout rounds tracking share accumulation periods.

| Column | Type | Constraints | Description |
|--------|------|-------------|-------------|
| id | INTEGER | PK AUTOINCREMENT | |
| start_template_id | INTEGER | NOT NULL | Template at round open |
| end_template_id | INTEGER | | Template at round close |
| status | TEXT | NOT NULL DEFAULT 'open' | 'open', 'found', 'closed', 'paid', 'orphaned' |
| found_block_hash | TEXT | | Block that ended this round |
| created_at | DATETIME | NOT NULL DEFAULT CURRENT_TIMESTAMP | |

**Status lifecycle:** `open → found → closed → paid` or `open → orphaned` (reorg)

---

## Table: `found_blocks`

Blocks found by the pool and submitted to lotusd.

| Column | Type | Constraints | Description |
|--------|------|-------------|-------------|
| id | INTEGER | PK AUTOINCREMENT | |
| round_id | INTEGER | NOT NULL FK → rounds(id) | |
| block_hash | TEXT | NOT NULL UNIQUE | |
| height | INTEGER | NOT NULL | |
| status | TEXT | NOT NULL | 'pending', 'confirmed', 'orphaned', 'paid' |
| worker_id | INTEGER | FK → workers(id) | Finding worker |
| template_id | INTEGER | | |
| persist_source | TEXT | | 'json-rpc' or 'nng' |
| orphan_reason | TEXT | | Populated when status='orphaned' |
| matured_at | DATETIME | | When block reached maturity |
| coinbase_value | INTEGER | | Total coinbase output value in satoshis |
| network_target_hex | TEXT | | N_diff at find time |

**Unique:** `block_hash`

---

## Table: `accounting_events`

Append-only operational audit log.

| Column | Type | Constraints | Description |
|--------|------|-------------|-------------|
| id | INTEGER | PK AUTOINCREMENT | |
| event_type | TEXT | NOT NULL | Event discriminator |
| status | TEXT | NOT NULL | |
| session_id | TEXT | | |
| worker_id | INTEGER | FK → workers(id) | |
| worker_name | TEXT | | |
| payout_address | TEXT | | |
| round_id | INTEGER | FK → rounds(id) | |
| template_id | INTEGER | | |
| template_epoch | INTEGER | | |
| job_id | TEXT | | |
| block_hash | TEXT | | |
| height | INTEGER | | |
| payload_json | TEXT | | Arbitrary event payload |
| created_at | DATETIME | NOT NULL DEFAULT CURRENT_TIMESTAMP | |

---

## Table: `payout_batches`

A payout batch created when a found block matures and PPLNS payout is calculated.

| Column | Type | Constraints | Description |
|--------|------|-------------|-------------|
| id | INTEGER | PK AUTOINCREMENT | |
| round_id | INTEGER | NOT NULL FK → rounds(id) | |
| status | TEXT | NOT NULL | 'pending', 'signed', 'submitted', 'confirmed', 'failed' |
| total_amount | INTEGER | NOT NULL | Net reward distributed |
| pool_fee_amount | INTEGER | NOT NULL | Fee deducted |
| pool_fee_address | TEXT | | Where fee was sent |
| miner_count | INTEGER | NOT NULL | Number of payees |
| retry_key | TEXT | UNIQUE | `{block_hash}:{num_outputs}` |
| last_error | TEXT | | Last submission error |
| next_retry_at | DATETIME | | |
| attempt_count | INTEGER | NOT NULL DEFAULT 0 | |
| signed_payload_ref | TEXT | | Reference to signed tx |
| submitted_txid | TEXT | | On-chain txid |
| created_at | DATETIME | NOT NULL DEFAULT CURRENT_TIMESTAMP | |

**Unique:** `retry_key`

---

## Table: `payouts`

Individual miner payouts within a batch.

| Column | Type | Constraints | Description |
|--------|------|-------------|-------------|
| id | INTEGER | PK AUTOINCREMENT | |
| batch_id | INTEGER | NOT NULL FK → payout_batches(id) | |
| worker_id | INTEGER | NOT NULL | |
| payout_address | TEXT | NOT NULL | |
| amount | INTEGER | NOT NULL | Payout in satoshis |
| dust_carried_forward | INTEGER | NOT NULL | Dust included in this payout |

---

## Table: `payout_share_snapshots`

Captures which shares were in the PPLNS window when the payout was calculated.

| Column | Type | Constraints | Description |
|--------|------|-------------|-------------|
| id | INTEGER | PK AUTOINCREMENT | |
| batch_id | INTEGER | NOT NULL FK → payout_batches(id) | |
| share_id | INTEGER | NOT NULL FK → shares(id) | |
| share_outcome_id | INTEGER | NOT NULL FK → share_outcomes(id) | |
| worker_id | INTEGER | NOT NULL | |
| payout_address | TEXT | NOT NULL | |
| difficulty | REAL | NOT NULL | |
| share_created_at | DATETIME | NOT NULL | |

---

## Table: `dust_balances`

Per-address dust accumulation (see ADR 002).

| Column | Type | Constraints | Description |
|--------|------|-------------|-------------|
| id | INTEGER | PK AUTOINCREMENT | |
| payout_address | TEXT | NOT NULL UNIQUE | |
| balance | INTEGER | NOT NULL DEFAULT 0 | Cumulative dust in satoshis |
| updated_at | DATETIME | NOT NULL DEFAULT CURRENT_TIMESTAMP | |

**Unique:** `payout_address`

---

## Entity Relationships

```
workers ──< shares ──> share_outcomes ──> rounds
  │                                      │
  │                                      └──< found_blocks
  │                                      │
  │                                      └──< accounting_events
  │
  └──< payouts ──> payout_batches ──< payout_share_snapshots
                    │
                    └──< dust_balances
```

- `──>` = foreign key (many-to-one)
- `──<` = inverse (one-to-many)

## Key Invariants

1. `shares.dedupe_key` = `share_outcomes.dedupe_key` — matched pair, one-to-one.
2. `shares` is insert-only — records are never updated or deleted.
3. `share_outcomes.round_id` is resolved at insert time. NULL until Slice 5 round tracking is active.
4. `found_blocks.block_hash` is globally unique — no duplicate submissions.
5. `payout_batches.retry_key` is globally unique — no duplicate payout batches for the same block.
6. `dust_balances.balance` is additive-only (never decreases except via payout weight calculation).
7. WAL checkpoint is performed on graceful shutdown to prevent WAL file growth.
