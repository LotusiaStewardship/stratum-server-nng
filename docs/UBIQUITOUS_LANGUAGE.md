# Ubiquitous Language

Canonical terms used throughout the stratum-server-nng codebase. These definitions are shared across all bounded contexts.

## Core Mining Terms

### Share
A miner's submission via `mining.submit` Stratum method. A share record captures:
- The submission metadata (worker, job, extranonce2, ntime, nonce)
- Validation result (accepted/rejected/stale/low-difficulty)
- Lifecycle status (accepted → orphaned if block reorgs)
- Round membership (via `round_id` foreign key)

**Key invariant:** A share is immutable once persisted, except for status changes due to blockchain reorgs. Shares are never deleted — they are marked orphaned.

**Key invariant:** Each share belongs to exactly one round (via `round_id` foreign key). Round membership is assigned when the round closes (backfilled based on template_id range).

**Key invariant:** When a round is orphaned, its shares remain valid and stay in the PPLNS window. The `round_id` is historical accounting only — PPLNS window calculation is time-based, not round-based. Orphan cost is absorbed by pool fees over time, not by invalidating shares.

### Accounting Service
Facade that orchestrates accounting operations across multiple repositories. The service:
- Owns multi-repository transactions (e.g., record share + update worker stats)
- Handles CRUD operations for blockchain reorgs (orphan blocks, reverse payouts)
- Provides single interface for share recording, round management, payout tracking

**Key invariant:** Share Processing validates shares; Accounting Service persists them. This separation allows validation logic to evolve independently from persistence logic.

**Key invariant:** Accounting Service owns all multi-step accounting operations. For example, orphaning a block requires updating found_blocks, rounds, shares, and payouts — this orchestration lives in Accounting Service, not in callers.

**Related terms:**
- **Accepted Share:** Share whose header hash meets the session's difficulty target
- **Rejected Share:** Share that failed validation (stale-job, low-difficulty, unauthorized-worker, invalid format)
- **Stale Share:** Share submitted for a job that is no longer active (miner was working on outdated template)
- **Low-Difficulty Share:** Share that does not meet the session's assigned difficulty target

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

### Session
A TCP connection from a miner to the pool server. A session has:
- Unique session identifier
- Assigned extranonce1 (4 bytes, unique per session)
- Variable difficulty (per-session VarDiff controller)
- Subscription and authorization state
- Active job set (jobs assigned to this session)

**Key invariant:** Each session has exactly one extranonce1 value for its lifetime. Extranoce1 is never reused across sessions.

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
- Assigned extranonce1 (4 bytes, unique per session)
- Variable difficulty (per-session VarDiff controller)
- Subscription and authorization state
- Active job set (jobs assigned to this session)
- Set of authorized workers (workers that can submit shares in this session)

**Key invariant:** Each session has exactly one extranonce1 value for its lifetime. Extranoce1 is never reused across sessions.

**Key invariant:** Session is transient (TCP connection lifetime). Worker is persistent (exists across sessions).

**Key invariant:** VarDiff operates at session level (per TCP connection), NOT per worker. All workers authorized on the same session share the same difficulty. Mining proxies that aggregate multiple workers behind one connection are responsible for internal difficulty management.

### Share Difficulty
The difficulty assigned to a share at submission time. A share's difficulty equals the session's current VarDiff difficulty when the share was submitted. Used for:
- PPLNS work unit calculation (share contributes `difficulty` work units)
- Hashrate estimation
- Miner performance tracking

**Key invariant:** Share difficulty is immutable once recorded. If session VarDiff changes, previously submitted shares retain their original difficulty.

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

## Payout Terms

### Payout Scheme
Algorithm for distributing block rewards among miners. Supported schemes:
- **PPLNS (Pay Per Last N Shares):** Rewards proportional to work in trailing window
- **PPS (Pay Per Share):** Fixed reward per accepted share (future)
- **PROP (Proportional):** Rewards proportional to shares in round (future)

### PPLNS Window
The trailing set of difficulty-weighted shares used for PPLNS payout calculations. The window:
- Ends at the found block's submission time
- Extends backward until cumulative work units reaches `n_multiplier × network_difficulty`
- Is share-count-based, NOT template-based (shares from any template can be in window)
- Aggregates shares by payout address for proportional distribution
- Excludes orphaned shares (shares from orphaned rounds)
- Spans across round boundaries (shares from previous rounds can still be in window)

**Key invariant:** The PPLNS window is a rolling window — shares "fall out" as newer shares arrive, regardless of round boundaries. When a block is found, the window is "snapshotted" for payout calculation.

**Work Units:** Each share contributes `share.difficulty` work units. A share at difficulty 512 counts twice as much as a share at difficulty 256.

**Example:** If `n_multiplier = 2.0` and network difficulty = 100, the window targets 200 work units. This might be 200 shares at diff=1, or 100 shares at diff=2, or any combination totaling 200 work units.

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
