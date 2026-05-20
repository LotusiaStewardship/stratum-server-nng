# Ubiquitous Language

Canonical terms used throughout the stratum-server-nng codebase. These definitions are shared across all bounded contexts.

## Core Mining Terms

### Share
A miner's submission via `mining.submit` Stratum method. The system persists two related records:

**Share (raw submission):** Captures the fact that the miner submitted work.
- worker_id, template_id, P_diff at submission time, accepted/rejected/stale, dedupe_key
- Immutable once persisted. Never deleted.
- Dedupe key ensures idempotent insertion for identical submissions.

**Share Outcome (validation pipeline result):** Captures the full validation result.
- session_id, worker_id, job_id, round_id, dedupe_key
- Validation status (accepted/rejected/stale)
- Validation details: `node_result`, `low_diff_ok`, `network_target_ok`, `block_hash` (if block found)
- Linked to the raw share via `share_id` foreign key
- Unique on `dedupe_key` — one outcome per unique submission

**Key invariant:** A share is immutable once persisted. Shares are never deleted — they are marked orphaned when a reorg invalidates them.

**Key invariant:** Each share belongs to exactly one round (via `round_id`). Round membership is resolved at insert time via `resolve_round_for_template(template_id)`, NOT backfilled.

**Key invariant:** When a round is orphaned, its shares remain valid and stay in the PPLNS window. The `round_id` is historical accounting only — PPLNS window calculation is share-ID-based, not round-based. Orphan cost is absorbed by pool fees over time, not by invalidating shares.

**Key invariant:** A share whose header hash meets P_diff but NOT N_diff (high-hash share) is still recorded as accepted with `network_target_ok=false`. These shares count toward PPLNS work units.

### Accounting Service
Facade that orchestrates accounting operations across multiple repositories. The service:
- Owns multi-repository transactions (e.g., record share + update worker stats)
- Handles CRUD operations for blockchain reorgs (orphan blocks, reverse payouts)
- Provides single interface for share recording, round management, payout tracking

**Key invariant:** Share Processing validates shares; Accounting Service persists them. This separation allows validation logic to evolve independently from persistence logic.

**Key invariant:** Accounting Service owns all multi-step accounting operations. For example, orphaning a block requires updating found_blocks, rounds, shares, and payouts — this orchestration lives in Accounting Service, not in callers.

**Related terms:**
- **Accepted Share:** Share whose header hash meets the session's P_diff target
- **High-Hash Share:** Share whose header hash meets P_diff but does NOT meet N_diff (the network target). Lotusd returns "high-hash" on submission. These shares are credited to the miner with `network_target_ok=false` — they contributed work to the pool even though they didn't find a block.
- **Rejected Share:** Share that failed validation (stale-job, low-difficulty, unauthorized-worker, invalid format, ntime-mismatch)
- **Stale Share:** Share submitted for a job that is no longer active (miner was working on outdated template)
- **Low-Difficulty Share:** Share whose header hash does not meet the session's P_diff target

### Job (MiningJob)
A unit of mining work distributed to miners via `mining.notify`. Contains:
- Block template data (prevhash, coinbase parts, merkle branches, version, nbits, ntime)
- Lotus-specific fields (height, epoch_hash, extended_metadata_hash, block_size)
- Job identifier and clean_jobs flag
- Template epoch (monotonically increasing counter)

**Key invariant:** A job is valid only while it is in the session's `active_jobs` set. A job becomes stale when:
1. `clean_jobs=true` job arrives (immediate staleness — Lotus header includes block_size which changes with every mempool update)
2. Job evicted from server's job cache (LRU eviction)
3. Session explicitly replaced with newer job

**Key invariant:** When `clean_jobs=true`, ALL previous jobs become stale immediately. This differs from Bitcoin where only merkle_root changes and miners could theoretically continue working.

**Key invariant:** Job identifier format is `job-{template_id}-{epoch}`. Same template can have multiple jobs (different epochs) as mempool changes.

### Assigned Job
A session-local record of a job that was dispatched to the miner via `mining.notify`. Each assigned job captures:
- **job_id:** The job's identifier
- **P_diff:** The pool difficulty at the moment the job was assigned (this becomes the share's difficulty if the miner submits against this job)
- **ntime:** The ntime from the template, frozen at assignment time

**Key invariant:** When a miner submits against a job, the server checks that the submitted ntime matches the frozen ntime from assignment. If different, the share is rejected as `ntime-mismatch`. This prevents miners from reusing valid nonces across different ntime values.

**Key invariant:** The share's difficulty is set at assignment time, not at submission time. If P_diff changes between assignment and submission, the share still gets the P_diff from when the job was assigned.

**Key invariant:** A session caps assigned jobs at MAX_ASSIGNED_JOBS_PER_SESSION (default 128) to bound memory.

### Worker
A persistent mining identity that spans sessions. A worker has:
- Payout address (Lotus address string)
- Optional worker suffix (e.g., "rig01" in `address.rig01`)
- Share history across all sessions that authorized this worker
- Hashrate tracking (rolling window across sessions)

**Key invariant:** A worker is identified by `(payout_address, worker_suffix)` tuple. Multiple sessions can authorize the same worker concurrently.

**Key invariant:** Worker is a global entity, not owned by any session. When a session disconnects, the worker persists with its historical data.

### Session
A TCP connection from a miner to the pool server. A session has:
- Unique session identifier
- Assigned extranonce1 (4 bytes, globally unique across all active sessions)
- Extranonce2 size (fixed at 4 bytes per Stratum V1 standard)
- Variable difficulty (per-session VarDiff controller — P_diff)
- Subscription and authorization state
- Active job set (jobs assigned to this session)
- Set of authorized workers (workers that can submit shares in this session)
- Assigned job bookkeeping (tracking `(job_id, P_diff, ntime)` per dispatched job)
- Rate limiter (`per_conn_req_per_sec`)
- Idle timeout (`conn_idle_timeout_secs`)

**Key invariant:** Each session has exactly one extranonce1 value for its lifetime. Extranonce1 is never reused across sessions.

**Key invariant:** Extranonce1 is derived from the session counter (monotonically increasing u64), guaranteeing global uniqueness across all active sessions without collision-checking overhead. The counter wraps at u32::MAX (~4B connections).

**Key invariant:** The server sends `mining.set_extranonce` immediately after the `mining.subscribe` response, communicating the session's `[extranonce1, extranonce2_size]` as a notification. This is the standard Stratum V1 mechanism for delivering extranonce parameters.

**Key invariant:** `mining.extranonce.subscribe` is accepted (returns success) but does not trigger any follow-up updates because extranonce1 is fixed per session.

**Key invariant:** Session is transient (TCP connection lifetime). Worker is persistent (exists across sessions).

**Key invariant:** VarDiff operates at session level (per TCP connection), NOT per worker. All workers authorized on the same session share the same P_diff. Mining proxies that aggregate multiple workers behind one connection are responsible for internal difficulty management.

**Key invariant:** The session maintains an `assigned_jobs` map tracking `(job_id, P_diff, ntime)` for every dispatched `mining.notify`. This enables:
  - Correct P_diff recording on shares (share difficulty = P_diff at assignment, not submission)
  - ntime-mismatch detection (reject submissions with different ntime than assigned)
  - Bounding memory via MAX_ASSIGNED_JOBS_PER_SESSION cap (default 128)

### Network Difficulty (N_diff)
The global canonical difficulty of the Lotus blockchain, derived from the mining template's `target` field. N_diff is:
- Read from the latest lotusd template via NNG RPC
- Updated only when a new template arrives (block found or mempool change)
- Read-only from the pool's perspective — the pool cannot influence N_diff

**Key invariant:** N_diff is the authoritative difficulty target for valid network-proof-of-work. A share whose hash meets N_diff is a valid network block candidate.

**Key invariant:** All per-session pool difficulties (P_diff) are bounded by N_diff as an absolute ceiling. P_diff ∈ [vardiff_min_floor, N_diff].

### Pool Difficulty (P_diff)
The per-session difficulty assigned to a miner via `mining.set_difficulty`. P_diff is:
- Dynamic per-session, managed by each session's independent VarDiff controller
- Initialized at session start: `P_diff_initial = N_diff × vardiff_initial_pct` (default 1% of network)
- Clamped to `[vardiff_min_floor, N_diff]` at all times
- Retargeted every `vardiff_retarget_secs` based on observed share rate
- Never exceeds N_diff (the network difficulty ceiling)

**Relationship to N_diff:** P_diff is a *session-local fraction* of N_diff. Miners with lower hashrate get lower P_diff so they can submit shares at a reasonable rate (target: 1 share per 20s). P_diff ramps up automatically as share rate increases. When N_diff changes (new template), P_diff's ceiling adjusts immediately via `update_max()`.

### Share Difficulty
The P_diff value recorded on a share at the moment it was submitted. A share's difficulty equals the session's P_diff when the share was validated. Used for:
- PPLNS work unit calculation (share contributes `difficulty` work units)
- Hashrate estimation
- Miner performance tracking

**Key invariant:** Share difficulty is immutable once recorded. Even if P_diff changes later (due to VarDiff retarget or N_diff update), the share retains its original difficulty.

**Key invariant:** Share difficulty represents work relative to P_diff, NOT N_diff. A share at difficulty 512 means the miner proved 512× the minimum P_diff work, which may be far below the network target.

### Round (Payout Round)
The period during which shares accumulate toward the next pool-found block. A round has:
- Start template ID (when round opened)
- End template ID (when pool found block)
- Status (open/found/closed/paid/orphaned)
- Associated found block (only when pool finds the block)

**Key invariant:** A round closes ONLY when the pool finds a block. External blocks (found by other pools) do NOT close the round — they only close the current template epoch group. This means a single round can span multiple external blocks.

**PPLNS Relationship:** The PPLNS window is calculated at round-close time, looking backward from the found block's template. Shares submitted during external-block periods within the round still count toward payout (they proved work for the pool).

### Template Epoch Group
The work period between any two blocks (pool or external). Used for accounting granularity and template management. When an external block is found:
- Template epoch group closes
- Round continues accumulating shares
- No payout calculation occurs

**Key invariant:** Multiple template epoch groups can belong to a single round. Template epoch groups are internal accounting; rounds are payout-relevant.

### Found Block
A block discovered by the pool via miner submission and accepted by the network. A found block has:
- Block hash and height
- Associated round ID (exactly one round per found block)
- Status lifecycle: confirmed → matured → paid, or orphaned
- Template ID and worker who submitted the winning share

**Key invariant:** A found block's status can change due to blockchain reorgs. Orphaned blocks are logged but do NOT invalidate shares. Shares from orphaned rounds remain in the PPLNS window and contribute to the next found block's payout calculation. The pool's fee covers orphan risk over time (typical orphan rate: 0.5-1% of blocks).

**Key invariant:** Exactly one found block per round (first-come-first-accepted). If multiple miners submit valid blocks for the same round, only the first accepted submission is recorded. Subsequent submissions are duplicates (network rejects them).

**Future consideration:** Bonus payouts for workers who submit valid blocks that are not first-accepted could be added later. This would require tracking multiple found_blocks per round with a `is_winning` flag. Out of scope for initial implementation.

### Dedupe Key
A unique string that identifies a share submission for idempotent insertion. Format:
```
worker_id:template_id:template_epoch:extranonce2:ntime:nonce
```

**Key invariant:** Both the `shares` table and `share_outcomes` table enforce `UNIQUE(dedupe_key)`. If a miner sends the exact same submission twice (e.g., due to network retry), the second insert is ignored (INSERT OR IGNORE).

**Key invariant:** The dedupe key is computed server-side at submission time, not provided by the miner.

### Invalid Share Banning
A per-session policy that disconnects miners who submit too many invalid shares. Configured via:
- `enabled` (default: true)
- `check_threshold` (default: 50 validated shares before checking)
- `invalid_percent` (default: 50% — if rejection rate exceeds this, the session is terminated)

The check runs at session-end: `total_validated = accepted + rejected`. If `total_validated >= check_threshold` and `rejected / total_validated > invalid_percent`, the session is terminated with a ban reason.

**Key invariant:** Banning is per-session, not per-worker. A worker reconnecting with a new TCP connection starts fresh. Global bans are not implemented — the MinerPolicy trait is scaffolded for future use.

**Key invariant:** Only validated shares (accepted + rejected) count toward the threshold. Format errors (invalid-submit-shape) and internal errors do not count.

## Payout Terms

### Payout Scheme
Algorithm for distributing block rewards among miners. Supported schemes:
- **PPLNS (Pay Per Last N Shares):** Rewards proportional to work in trailing window
- **PPS (Pay Per Share):** Fixed reward per accepted share (future)
- **PROP (Proportional):** Rewards proportional to shares in round (future)

### PPLNS Window
The trailing set of difficulty-weighted shares used for PPLNS payout calculations. The window:
- Ends at the found block's submission time
- Extends backward until cumulative work units reaches `n_multiplier × N_diff`
- Is share-count-based, NOT template-based (shares from any template can be in window)
- Aggregates shares by payout address for proportional distribution
- Excludes orphaned shares (shares from orphaned rounds)
- Spans across round boundaries (shares from previous rounds can still be in window)

**Key invariant:** The PPLNS window is a rolling window — shares "fall out" as newer shares arrive, regardless of round boundaries. When a block is found, the window is "snapshotted" for payout calculation.

**Work Units:** Each share contributes `share.difficulty` work units. A share at difficulty 512 counts twice as much as a share at difficulty 256.

**Example:** If `n_multiplier = 2.0` and N_diff = 100, the window targets 200 work units. This might be 200 shares at P_diff=1, or 100 shares at P_diff=2, or any combination totaling 200 work units.

### Payout Plan
The computed distribution of a block reward. Contains:
- Outputs: (address, amount) pairs for payments >= min_payout
- Dust: (address, amount) pairs for payments < min_payout (carried forward)
- Fee: Pool fee amount and address
- Gross/net reward accounting

### Dust
Sub-satoshi or sub-threshold amounts that cannot be paid out individually. Dust:
- Accumulates per address across multiple rounds
- Is added to future payout calculations
- Is never discarded (unless explicitly configured)

**Dust Ledger:** Dust is tracked in a `payout_dust_ledger` table with per-address FIFO ordering. When dust is paid out (accumulated above min_payout), the oldest dust entries are consumed first via `reduce_dust_ledger(address, amount)`. This ensures fair and auditable dust accounting.

### Payout Share Snapshot
An auditable record of which shares were in the PPLNS window when a payout was calculated. Stored in the `payout_share_snapshots` table, each entry captures:
- share_id, payout_address, work_units, share_created_at
- The ordering criterion used for window calculation (e.g., `share_id DESC`)
- The truncation reason (if the window was truncated by the hard limit)

**Key invariant:** The snapshot is created atomically with the payout batch. If the payout batch exists, its share snapshot is a complete record of what was paid.

**Key invariant:** The snapshot enables post-hoc audit: given a payout batch, you can reconstruct exactly which shares contributed and verify the distribution.

### Payout Batch Retry
A fault-tolerance mechanism for payout creation and submission. Each payout batch has:
- **retry_key:** Unique string (`block_hash:num_outputs`) preventing duplicate batch creation if the scheduler retries
- **last_error:** Human-readable error message if the last attempt failed
- **next_retry_at:** Timestamp for the next retry attempt
- **attempt_count:** Number of attempts so far (for exponential backoff)
- **signed_payload_ref:** Reference to the signed transaction (e.g., `rawtx:<txid>`) for manual recovery

**Key invariant:** The retry_key ensures idempotent batch creation. If the scheduler crashes mid-payout and restarts, it will find the existing batch instead of creating a duplicate.

## Node Integration Terms

### Mining Template
A block template from lotusd via NNG RPC. Contains:
- Block header fields (prevhash, version, nbits, ntime)
- Coinbase parts (coinbase1, coinbase2)
- Merkle branches
- Network target (difficulty)
- Serialized block with precomputed size

**Key invariant:** Template validity is signaled by `miningwrkchg` events. All template changes invalidate in-flight work (Lotus header includes block_size, which changes with every mempool update).

### Template Epoch
Monotonically increasing counter from lotusd. Each `miningwrkchg` event increments the epoch. Used for:
- Job cache invalidation
- Missed event detection (gaps > 1 indicate dropped events)
- Round boundaries

### Event Coalescing
Debouncing strategy for `miningwrkchg` events. Multiple rapid events are merged into single template refresh. Prevents excessive job broadcasts during high mempool variance.

### Mining Work Change Reason
An enum indicating why the mining template was invalidated. Received from lotusd via the `miningwrkchg` flatbuffer message. Four reasons:
- **NewTip:** A new block was connected at the chain tip. Template invalid due to prevhash change.
- **Reorg:** A chain reorganization occurred. Template invalid due to chain switch.
- **MempoolRefresh:** The mempool changed (tx added/removed). Template invalid due to merkle root and block size change.
- **ManualInvalidation:** An operator invoked an RPC to invalidate the template.

**Key invariant:** Regardless of reason, ALL `miningwrkchg` events trigger `clean_jobs=true` because the Lotus header includes `block_size`, which changes with any mempool update. The reason code is for logging and observability only, not for behavioral branching.

### Block Reconciliation
A startup-time procedure that validates the pool's `found_blocks` against the current node state. For each found_block:
1. Fetch the node's block at the same height
2. Compare hashes — if mismatch, mark as orphaned with reason `reorg_detected`
3. If node has no block at that height, mark as orphaned with reason `block_not_found`

**Key invariant:** Reconciliation runs once at startup before accepting miner connections. It ensures the pool's found_blocks are consistent with the canonical chain.

## Accounting Terms

### Orphaned
Status applied to blocks/shares when blockchain reorg invalidates them. An orphaned:
- Block: Was found but is no longer on canonical chain
- Share: Contributed to an orphaned block (has no payout value)

**Key invariant:** Orphaned status is permanent. Orphaned blocks/shares are never "un-orphaned" even if chain reorgs again.

### Matured
Status applied to found blocks when they reach minimum confirmations (default: 100). Matured blocks:
- Are eligible for payout
- Have coinbase locked per consensus rules
- Can be safely paid without reorg risk (beyond configured tolerance)

### Confirmed
Status applied to found blocks that are on canonical chain but not yet matured. Confirmed blocks:
- Have `confirmations = tip_height - block_height + 1`
- Accumulate confirmations as chain extends
- Transition to matured at confirmation threshold

### Scheduler Lease
An exclusive lock stored in the database that prevents double-payouts in HA (high-availability) deployments. One pool instance holds the lease at a time:
- `acquire_lease(instance_id, duration_mins) → bool` — claim exclusive payout rights
- `renew_lease(instance_id, duration_mins) → bool` — extend the lease before it expires
- `release_lease(instance_id) → bool` — voluntarily relinquish (on graceful shutdown)

**Key invariant:** The lease auto-expires after `duration_minutes` (default: 5 min) without renewal. This provides fail-safe behavior if the lease-holding instance crashes.

**Key invariant:** The Payout Scheduler acquires the lease at the start of each interval, processes all matured blocks, then renews the lease. If another instance holds the lease, the current instance skips its interval.

### Authorization Event
A record of every `mining.authorize` attempt, stored in the `authorization_events` table. Each event captures:
- session_id, worker_name
- resolved payout_address and worker_suffix (parsed from the worker name)
- authorized (boolean — whether the auth succeeded)
- reason (if auth failed, e.g., invalid Lotus address format)

**Key invariant:** Every authorize request produces exactly one authorization event, regardless of success or failure. This provides an operational audit trail for diagnosing miner connection issues.

**Key invariant:** Authorization events are write-only — they are never updated or deleted. The historical record is immutable.

### Accounting Event
A general-purpose audit log recording significant pool operations, stored in the `accounting_events` table. Each event captures:
- event_type (e.g., `share_outcome`, `round_opened`, `round_closed`, `found_block_observed`, `found_block_orphaned`)
- status (e.g., `accepted`, `orphaned`)
- Optional context fields: session_id, worker_id, worker_name, payout_address, round_id, template_id, template_epoch, job_id, block_hash, height
- payload_json: Arbitrary JSON payload for extensibility (e.g., `{"reject_reason":"high-hash"}`)

**Key invariant:** Accounting events are append-only. They provide a complete chronological record of the pool's operations for financial auditing and incident investigation.

**Key invariant:** The event_type is the primary access path. All event types should be documented as they are added, and existing types should never be removed (only deprecated).
