# Phase 3: Job Deduplication in `publish_job`

**Plan:** [003](./003-nng-event-pipeline-fixes.md)  
**Priority:** High — Defense-in-depth against duplicate broadcasts  
**Risk:** Low — Adds a guard; doesn't change normal behavior  
**Effort:** Small — ~15 lines added to `publish_job`, ~10 lines in `MiningJob`  
**Dependencies:** [Phase 2](./003-phase2-event-coalescing.md) (but can land independently)

---

## Objective

Add deduplication to `StratumRuntime::publish_job`. Before broadcasting a new job, compare its **work content** (`prevhash` + `ntime` + `coinbase1` + `coinbase2` + `merkle_branches`) against the last published job. If identical, skip the broadcast.

This is a **defense-in-depth** measure. Phase 2's coalescing should prevent duplicate events from reaching this point, but if the coalescing window is too short or edge cases exist, deduplication catches any remaining duplicates.

## Problem

Even with Phase 1 (accounting events don't refresh) and Phase 2 (coalescing), there's still a window for duplicate broadcasts:

1. **Edge case: Identical templates from different causes.** A mempool change that adds then removes the same transaction could produce an identical template. Two `miningwrkchg` events fire >100ms apart (outside debounce window), each triggers a refresh with identical work.

2. **Edge case: lotusd restart.** If lotusd restarts, the `template_epoch` counter resets. Events before/after restart could produce identical templates.

3. **No validation at publication point.** `publish_job` blindly accepts and broadcasts whatever job it's given. There's no check at the publication gate.

### Current code

```rust
// src/stratum/server.rs:94-108
pub fn publish_job(&self, job: MiningJob) {
    let mut jobs = self.jobs.lock().expect("jobs lock");
    jobs.push_back(job.clone());
    while jobs.len() > self.max_jobs {
        jobs.pop_front();
    }
    debug!(
        job_id = %job.job_id,
        template_id = job.template_id,
        template_epoch = job.template_epoch,
        clean_jobs = job.clean_jobs,
        cached_jobs = jobs.len(),
        "published mining job"
    );
    let _ = self.tx.send(job);  // ← Broadcasts to ALL miners, no dedup check
}
```

### Why `prevhash` + `ntime` is the right dedup key

The mining work is defined by the block header fields that determine what the miner is hashing:

| Field | Source | Role in dedup |
|-------|--------|---------------|
| `prevhash` | `template.prev_hash_stratum` | **Primary key** — different prevhash = different block = different work |
| `ntime` | `template.ntime_stratum` | **Secondary key** — same prevhash but different ntime = different work (time changed) |
| `coinbase1` | `template.coinbase1` | Part of work but always changes with prevhash (payout script same, but block content differs) |
| `coinbase2` | `template.coinbase2` | Same as coinbase1 |
| `merkle_branches` | `template.merkle_branches` | Changes with mempool, but if prevhash is same, we need to check |

The minimal dedup key is **`prevhash` + `ntime`**. If both match, the work is identical regardless of other fields:
- Same `prevhash` = same block being extended
- Same `ntime` = same timestamp window
- If both match, even if mempool changed slightly, the resulting work is functionally identical for the miner (the `prevhash` determines what the miner is hashing against)

We also check `coinbase1` + `coinbase2` because in Lotus, the block size is part of the header, and coinbase changes affect block size.

---

## Implementation

### Step 1: Add `WorkKey` struct to `MiningJob`

**File: `src/stratum/job.rs`**

Add a `PartialEq` implementation and a `work_key()` method to `MiningJob`:

```rust
use serde_json::json;

#[derive(Debug, Clone)]
pub struct MiningJob {
    pub job_id: String,
    pub template_id: u64,
    pub prevhash: String,
    pub coinbase1: String,
    pub coinbase2: String,
    pub merkle_branches: Vec<String>,
    pub version: String,
    pub nbits: String,
    pub ntime: String,
    pub network_target_hex: String,
    pub clean_jobs: bool,
    pub template_epoch: u64,
    pub template_block: Vec<u8>,
    pub block_height: i32,
    pub epoch_hash_hex: String,
    pub extended_metadata_hash_hex: String,
    pub block_size: u64,
}

/// A minimal key that uniquely identifies the mining work in a job.
/// Two jobs with the same WorkKey represent identical mining work,
/// regardless of their job_id or template_epoch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkKey {
    pub prevhash: String,
    pub ntime: String,
    pub coinbase1: String,
    pub coinbase2: String,
    pub merkle_branches_hash: u64,
}

impl MiningJob {
    /// Returns a key that uniquely identifies the mining work content.
    /// Jobs with the same WorkKey are functionally equivalent for miners.
    pub fn work_key(&self) -> WorkKey {
        // Hash the merkle branches to a compact u64 for comparison.
        // This avoids comparing potentially-large Vec<String> on every publish.
        let merkle_hash = {
            use std::collections::hash_map::DefaultHasher;
            use std::hash::{Hash, Hasher};
            let mut hasher = DefaultHasher::new();
            self.merkle_branches.hash(&mut hasher);
            hasher.finish()
        };
        WorkKey {
            prevhash: self.prevhash.clone(),
            ntime: self.ntime.clone(),
            coinbase1: self.coinbase1.clone(),
            coinbase2: self.coinbase2.clone(),
            merkle_branches_hash: merkle_hash,
        }
    }

    /// Returns mining.notify params with Lotus extensions.
    /// Standard params (9) + Lotus extensions (height, epoch_hash, extended_metadata_hash)
    pub fn notify_params(&self) -> serde_json::Value {
        json!([
            self.job_id,
            self.prevhash,
            self.coinbase1,
            self.coinbase2,
            self.merkle_branches,
            self.version,
            self.nbits,
            self.ntime,
            self.clean_jobs,
            // Lotus-specific extensions
            self.block_height,
            self.epoch_hash_hex,
            self.extended_metadata_hash_hex,
            self.block_size,
        ])
    }
}
```

### Step 2: Add deduplication to `publish_job`

**File: `src/stratum/server.rs`**

Add a `last_published_key` field to `StratumRuntime` and check it in `publish_job`:

```rust
// Add to the StratumRuntime struct
#[derive(Clone)]
pub struct StratumRuntime {
    jobs: Arc<Mutex<VecDeque<MiningJob>>>,
    tx: broadcast::Sender<MiningJob>,
    max_jobs: usize,
    epoch_counter: Arc<AtomicU64>,
    last_published_key: Arc<Mutex<Option<WorkKey>>>,  // ← NEW
}

// In StratumRuntime::new()
pub fn new(max_jobs: usize) -> Self {
    let (tx, _) = broadcast::channel(1024);
    Self {
        jobs: Arc::new(Mutex::new(VecDeque::new())),
        tx,
        max_jobs,
        epoch_counter: Arc::new(AtomicU64::new(0)),
        last_published_key: Arc::new(Mutex::new(None)),  // ← NEW
    }
}

// In publish_job()
pub fn publish_job(&self, job: MiningJob) {
    let new_key = job.work_key();
    
    // Check for duplicate work
    let mut last_key = self.last_published_key.lock().expect("last_published_key lock");
    if let Some(ref prev_key) = *last_key {
        if *prev_key == new_key {
            info!(
                job_id = %job.job_id,
                template_id = job.template_id,
                template_epoch = job.template_epoch,
                prevhash = %job.prevhash,
                "skipping duplicate job broadcast (identical work)"
            );
            // Still add to job cache for submit validation, but don't broadcast
            let mut jobs = self.jobs.lock().expect("jobs lock");
            jobs.push_back(job.clone());
            while jobs.len() > self.max_jobs {
                jobs.pop_front();
            }
            return;
        }
    }
    *last_key = Some(new_key);
    drop(last_key);
    
    let mut jobs = self.jobs.lock().expect("jobs lock");
    jobs.push_back(job.clone());
    while jobs.len() > self.max_jobs {
        jobs.pop_front();
    }
    debug!(
        job_id = %job.job_id,
        template_id = job.template_id,
        template_epoch = job.template_epoch,
        clean_jobs = job.clean_jobs,
        cached_jobs = jobs.len(),
        "published mining job"
    );
    let _ = self.tx.send(job);
}
```

### Key design decisions

1. **Job is still added to the cache even when skipped.** Miners may submit shares for this job_id, so we need it available for `runtime.find_job()`. We just skip the broadcast.

2. **`last_published_key` is updated only on broadcast.** If we skip a duplicate, we don't update the key — it stays pointing to the original job's key. This means a third non-duplicate job will update it correctly.

3. **Merkle branches are hashed to `u64`.** Comparing `Vec<String>` on every publish is expensive. We hash them to a compact value. Collision probability with `u64` is negligible for this use case (we're comparing 2 items, not doing a lookup).

4. **Info-level log on dedup skip.** This is useful operational data — it tells us when the coalescing/debouncing is working as expected, or when edge cases produce duplicates.

---

## Expected Behavior

### Normal operation (different work)

```
publish_job(job-8685-8675):
  prevhash=44b5714995... ntime=c631046a0000
  last_published_key = None
  → Broadcast job-8685-8675
  → last_published_key = {prevhash: 44b5..., ntime: c631046a...}

publish_job(job-8686-8676):
  prevhash=aabbccdd99... ntime=d742056b0000  ← different prevhash
  last_published_key = {prevhash: 44b5...}
  → Different work → Broadcast job-8686-8676
  → last_published_key = {prevhash: aabb..., ntime: d742056b...}
```

### Duplicate work (same template, different epoch)

```
publish_job(job-8685-8675):
  prevhash=44b5714995... ntime=c631046a0000
  → Broadcast job-8685-8675

publish_job(job-8685-8676):
  prevhash=44b5714995... ntime=c631046a0000  ← SAME work, different epoch
  → DUPLICATE DETECTED
  → Skip broadcast (add to cache only)
  → Log: "skipping duplicate job broadcast (identical work)"
```

---

## Verification

1. **Unit test:** Create two jobs with identical `prevhash` + `ntime` + `coinbase` + `merkle_branches` but different `job_id`/`template_epoch`. Publish both, verify only one broadcast is sent.

2. **Unit test:** Create two jobs with different `prevhash`. Publish both, verify both broadcasts are sent.

3. **Integration:** Run stratum server, trigger two rapid `miningwrkchg` events with identical template content (can be simulated), verify miner receives only one `mining.notify`.

---

## Files Modified

| File | Lines | Change |
|------|-------|--------|
| `src/stratum/job.rs` | Add `WorkKey` struct + `work_key()` method | New types |
| `src/stratum/server.rs` | `StratumRuntime` struct + `new()` + `publish_job()` | Add `last_published_key` field and dedup check |
