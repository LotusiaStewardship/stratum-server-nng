# Phase 2: Coalescing Event Queue

**Plan:** [003](./003-nng-event-pipeline-fixes.md)  
**Priority:** Critical — Prevents burst of template refreshes under rapid NNG events  
**Risk:** Medium — Changes event timing semantics; requires careful debounce tuning  
**Effort:** Medium — ~40 lines added, ~15 lines modified  
**Dependencies:** [Phase 1](./003-phase1-accounting-events.md)

---

## Objective

Replace the raw `mpsc::unbounded_channel` + sequential event processing with a **coalescing event queue**. When multiple NNG events arrive in quick succession (e.g., rapid mempool updates, block bursts), only the **most recent** event is processed. Intermediate events are discarded because they are superseded by the latest template state.

## Problem

Even after Phase 1 (only `MiningWorkChanged` triggers refresh), rapid event bursts still cause problems:

### Scenario: Mempool storm

Lotusd emits multiple `miningwrkchg` events within milliseconds:

```
t=0ms  miningwrkchg[mempool@1000]  ← event 1
t=5ms  miningwrkchg[mempool@1001]  ← event 2
t=10ms miningwrkchg[mempool@1002]  ← event 3
t=15ms miningwrkchg[new_tip@1003]  ← event 4
```

With the current sequential `mpsc::unbounded_channel` processing:

```
Consumer processes:
  1. Event @1000 → RPC to lotusd (takes ~50ms) → publishes job A → broadcasts
  2. Event @1001 → RPC to lotusd (takes ~50ms) → publishes job B → broadcasts
  3. Event @1002 → RPC to lotusd (takes ~50ms) → publishes job C → broadcasts
  4. Event @1003 → RPC to lotusd (takes ~50ms) → publishes job D → broadcasts
```

Total: **4 RPC calls + 4 broadcasts** for what should be **1 broadcast** (the final state). Each broadcast sends ~900 bytes of JSON to every connected miner. On WAN, this burst of writes amplifies the TCP interleaving problem.

### Root cause in current code

```rust
// server.rs:329-330
let (tx, mut rx) = mpsc::unbounded_channel::<NodeEvent>();
// ...
while let Some(event) = rx.recv().await {
    // Every event is processed immediately and sequentially
    // No coalescing, no debouncing, no deduplication
    refresh_job_from_node(...).await;
}
```

The `mpsc::unbounded_channel` queues all events and processes them one-by-one. There's no mechanism to:
1. **Coalesce** — combine multiple pending events into one
2. **Debounce** — wait a short window before processing, collecting any events that arrive during the window
3. **Deduplicate** — skip events that would produce identical results

---

## Solution Design

### Coalescing pattern

Replace the `while let Some(event) = rx.recv().await` loop with a **debounced coalescing loop**:

```
┌─────────────────────────────────────────────┐
│ Coalescing Event Loop                       │
│                                             │
│  pending: Option<NodeEvent> = None          │
│  debounce: Duration = 100ms (configurable)  │
│                                             │
│  loop {                                     │
│    tokio::select! {                         │
│      // Collect incoming events             │
│      Some(event) = rx.recv() => {           │
│        pending = Some(event);  // replace   │
│      }                                      │
│                                             │
│      // Debounce timer fires                │
│      _ = debounce_timer.tick() => {         │
│        if let Some(event) = pending.take()  │
│          process_event(event);              │
│      }                                      │
│    }                                        │
│  }                                          │
└─────────────────────────────────────────────┘
```

### How it works

1. **Collect phase:** When an event arrives, store it in `pending`. If another event arrives before the debounce timer fires, **replace** the pending event with the newer one.
2. **Fire phase:** When the debounce timer fires, take the pending event and process it. This triggers exactly one `refresh_job_from_node` call.
3. **Result:** A burst of N events in rapid succession results in **one** template refresh with the **latest** event data.

### Debounce tuning

Default: **100ms**. This is a balance between:
- **Responsiveness:** Miners get new work quickly after a block is found
- **Coalescing:** Rapid mempool events (which can fire at 10-50ms intervals) are coalesced into one refresh

The 100ms window means:
- If 5 mempool events arrive within 100ms → 1 template refresh
- If events arrive 200ms apart → each triggers its own refresh (correct behavior)
- Miner work latency impact: +100ms worst case (negligible vs. typical block times)

---

## Implementation

### Target: `src/stratum/server.rs`

**Current code (lines ~328-476):**

```rust
tokio::spawn(async move {
    let (tx, mut rx) = mpsc::unbounded_channel::<NodeEvent>();
    let runtime_events = runtime_nng.clone();
    let adapter_events_inner = adapter_events.clone();
    tokio::spawn(async move {
        let mut last_template_epoch: Option<u64> = None;
        
        while let Some(event) = rx.recv().await {
            match &event {
                NodeEvent::MiningWorkChanged { ... } => {
                    // ... (Phase 1 already moved refresh_job_from_node here)
                    refresh_job_from_node(...).await;
                }
                NodeEvent::BlockConnected { ... } => {
                    // accounting only
                }
                NodeEvent::BlockDisconnected { ... } => {
                    // accounting only
                }
            }
        }
    });
    // ...
});
```

**Replacement code:**

```rust
tokio::spawn(async move {
    let (tx, mut rx) = mpsc::unbounded_channel::<NodeEvent>();
    let runtime_events = runtime_nng.clone();
    let adapter_events_inner = adapter_events.clone();

    tokio::spawn(async move {
        // Track last seen template_epoch for missed-event detection
        let mut last_template_epoch: Option<u64> = None;

        // Coalescing state: pending event + debounce timer
        let mut pending_mining_work: Option<NodeEvent> = None;
        let debounce_duration = Duration::from_millis(100);
        let mut debounce_timer = tokio::time::interval(debounce_duration);
        debounce_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        /// Process a single MiningWorkChanged event.
        /// Called after debounce fires, with the most recent event.
        async fn process_mining_work_change(
            event: &NodeEvent,
            last_template_epoch: &mut Option<u64>,
            runtime_events: &StratumRuntime,
            adapter: &Arc<dyn NodeMiningAdapter>,
            pool_scripts: &ResolvedPoolScripts,
            stats: &Arc<RuntimeStats>,
            diff_cache: &DifficultyCache,
            tip_height_events: &Arc<std::sync::atomic::AtomicI64>,
            db_events: &AccountingDb,
            min_confirmations: i64,
            debug: bool,
        ) {
            if let NodeEvent::MiningWorkChanged {
                reason,
                tip_height,
                template_epoch,
                ..
            } = event
            {
                // Detect missed events (gaps in template_epoch sequence)
                if let Some(last_epoch) = *last_template_epoch {
                    if *template_epoch > last_epoch + 1 {
                        let missed = template_epoch - last_epoch - 1;
                        warn!(
                            last_epoch,
                            current_epoch = template_epoch,
                            missed,
                            "missed miningwrkchg events from lotusd"
                        );
                    }
                    if *template_epoch <= last_epoch {
                        debug!(
                            last_epoch,
                            current_epoch = template_epoch,
                            "duplicate or out-of-order miningwrkchg event"
                        );
                    }
                }
                *last_template_epoch = Some(*template_epoch);

                // Update tip height tracker
                tip_height_events.store(*tip_height, Ordering::SeqCst);

                // Mark matured blocks
                if let Err(err) = db_events.mark_blocks_matured(*tip_height, min_confirmations) {
                    error!(error = %err, "failed marking matured blocks after miningwrkchg");
                }

                // Refresh template and broadcast
                if let Err(err) = refresh_job_from_node(
                    runtime_events,
                    adapter.clone(),
                    pool_scripts,
                    stats.clone(),
                    diff_cache,
                    true,  // clean_jobs
                    reason.as_str(),
                    debug,
                ).await {
                    warn!(error = %err, reason = reason.as_str(),
                        "template refresh failed after miningwrkchg");
                }
            }
        }

        loop {
            tokio::select! {
                // Collect incoming NNG events — always replace pending with latest
                Some(event) = rx.recv() => {
                    match &event {
                        NodeEvent::MiningWorkChanged { template_epoch, .. } => {
                            // Coalesce: replace pending with newer event
                            let prev = pending_mining_work
                                .as_ref()
                                .and_then(|e| match e {
                                    NodeEvent::MiningWorkChanged { template_epoch: e, .. } => Some(*e),
                                    _ => None,
                                });
                            debug!(
                                previous_epoch = prev.as_ref().map(|e| e.to_string()).as_deref().unwrap_or("none"),
                                new_epoch = template_epoch,
                                "coalescing miningwrkchg event"
                            );
                            pending_mining_work = Some(event);
                        }
                        NodeEvent::BlockConnected { height, hash: _, prev_hash: _ } => {
                            // Accounting events are processed immediately — they don't
                            // need coalescing since they don't trigger expensive RPCs
                            tip_height_events.store(*height, Ordering::SeqCst);
                            if let Err(err) = db_events.mark_blocks_matured(*height, min_confirmations) {
                                error!(error = %err, "failed marking matured blocks after blkconnected");
                            }
                        }
                        NodeEvent::BlockDisconnected { height, hash, prev_hash: _ } => {
                            match db_events.find_found_block_by_height_and_hash(*height, hash) {
                                Ok(Some(found_block)) => {
                                    if let Err(err) = db_events.mark_found_block_orphaned(hash, "blkdisconctd") {
                                        error!(error = %err, block_hash = %hash, "failed marking found_block orphaned");
                                    } else {
                                        warn!(
                                            block_hash = %hash,
                                            height = height,
                                            round_id = found_block.round_id,
                                            worker = %found_block.worker_name.unwrap_or_default(),
                                            "pool-mined block orphaned via blkdisconctd"
                                        );
                                    }
                                    let _ = db_events.close_round(
                                        found_block.round_id,
                                        found_block.template_id.map(|v| v as u64),
                                        "round_closed_orphaned",
                                        Some(hash),
                                    );
                                    let _ = db_events.record_accounting_event(
                                        "found_block_orphaned", Some("orphaned"),
                                        None, None, None, None,
                                        Some(found_block.round_id), None, None, None,
                                        Some(hash),
                                        Some(&format!("{{\"height\":{}}}", height)),
                                    );
                                }
                                Ok(None) => {
                                    debug!(block_hash = %hash, height = height, "external block disconnected");
                                }
                                Err(err) => error!(error = %err, "failed checking orphaned found_block"),
                            }
                            tip_height_events.store(height - 1, Ordering::SeqCst);
                            if let Err(err) = db_events.mark_blocks_matured(height - 1, min_confirmations) {
                                error!(error = %err, "failed marking matured blocks after reorg");
                            }
                        }
                    }
                }

                // Debounce timer fires — process the latest pending event
                _ = debounce_timer.tick() => {
                    if let Some(event) = pending_mining_work.take() {
                        process_mining_work_change(
                            &event,
                            &mut last_template_epoch,
                            &runtime_events,
                            &adapter_events_inner,
                            &pool_scripts_events,
                            &stats_events,
                            &diff_cache_events,
                            &tip_height_events,
                            &db_events,
                            min_confirmations,
                            debug,
                        ).await;
                    }
                }
            }
        }
    });

    // NNG pub loop (unchanged)
    if let Err(err) = rpc_adapter
        .run_pub_loop(&nng_pub_url, move |ev| {
            let event_type = match &ev {
                NodeEvent::MiningWorkChanged { reason, template_epoch, tip_height: _, .. } => {
                    format!("miningwrkchg[{}@{}]", reason.as_str(), template_epoch)
                }
                NodeEvent::BlockConnected { height, .. } => format!("blkconnected@{}", height),
                NodeEvent::BlockDisconnected { height, .. } => format!("blkdisconctd@{}", height),
            };
            if tx.send(ev).is_err() {
                warn!(event_type, "NNG event dropped; stratum event consumer not running");
            }
        })
        .await
    {
        warn!(error = %err, nng_pub = %nng_pub_url, "NNG pub loop exited unexpectedly");
    }
});
```

### Key design decisions

1. **Only `MiningWorkChanged` is debounced.** `BlockConnected` and `BlockDisconnected` are processed immediately because:
   - They don't trigger expensive RPC calls
   - They're pure accounting operations (DB writes)
   - Delaying them provides no benefit

2. **Debounced events are replaced, not queued.** When a new `MiningWorkChanged` arrives while one is pending, the old one is discarded. This is correct because:
   - The newer event represents a more recent template state
   - Processing the old event would produce stale work

3. **Debounce timer uses `MissedTickBehavior::Skip`.** If the debounce window elapses while another event is being processed, we skip the missed tick. This prevents timer backlog.

4. **`process_mining_work_change` is a helper function.** Extracted from the loop body to keep the `select!` block clean. Takes all captured variables as parameters.

---

## Expected Behavior

### Before (sequential processing)

```
t=0ms   miningwrkchg@1000 arrives → queued
t=5ms   miningwrkchg@1001 arrives → queued
t=10ms  miningwrkchg@1002 arrives → queued
t=15ms  miningwrkchg@1003 arrives → queued

Processing (each ~100ms RPC):
t=20ms  Process @1000 → broadcast job A
t=120ms Process @1001 → broadcast job B  ← stale!
t=220ms Process @1002 → broadcast job C  ← stale!
t=320ms Process @1003 → broadcast job D  ← correct, but late

Total: 4 RPCs, 4 broadcasts, 320ms latency
```

### After (coalescing + debounce)

```
t=0ms   miningwrkchg@1000 arrives → pending = @1000
t=5ms   miningwrkchg@1001 arrives → pending = @1001 (replaces)
t=10ms  miningwrkchg@1002 arrives → pending = @1002 (replaces)
t=15ms  miningwrkchg@1003 arrives → pending = @1003 (replaces)
t=100ms debounce fires → process @1003 → broadcast job D

Total: 1 RPC, 1 broadcast, 100ms latency
```

### With spaced-out events (normal operation)

```
t=0ms     miningwrkchg@1000 arrives → pending = @1000
t=100ms   debounce fires → process @1000 → broadcast job A
t=5000ms  miningwrkchg@1001 arrives → pending = @1001
t=5100ms  debounce fires → process @1001 → broadcast job B

Total: 2 RPCs, 2 broadcasts — correct behavior
```

---

## Verification

1. **Test burst coalescing:** Send 5 `miningwrkchg` events within 50ms, verify only 1 job is published
2. **Test normal operation:** Send events 5 seconds apart, verify each triggers its own refresh
3. **Test accounting events:** Send `blkconnected` while `miningwrkchg` is pending, verify accounting runs immediately and mining refresh still debounces correctly
4. **Measure latency:** Verify debounce adds ≤100ms to work delivery (acceptable vs. typical 60s block times)

---

## Files Modified

| File | Lines | Change |
|------|-------|--------|
| `src/stratum/server.rs` | ~328-476 | Replace sequential event loop with coalescing select! loop + debounce timer |
