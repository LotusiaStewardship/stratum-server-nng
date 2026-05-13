# Phase 1: Stop Accounting Events from Triggering Template Refresh

**Plan:** [003](./003-nng-event-pipeline-fixes.md)  
**Priority:** Critical — Root cause of duplicate job broadcasts  
**Risk:** Low — Isolated change; comments already document this intent  
**Effort:** Small — ~10 lines changed

---

## Objective

Decouple accounting events (`blkconnected`, `blkdisconctd`) from the template refresh pipeline. Only `MiningWorkChanged` events should trigger `refresh_job_from_node`. Accounting operations (mark blocks matured, orphan found blocks) run but do **not** initiate a new mining template fetch or broadcast.

## Problem

In `src/stratum/server.rs`, the NNG event processing loop (lines 336–476) has a `match` block that handles three event types:

1. `MiningWorkChanged` — primary signal for template refresh
2. `BlockConnected` — accounting only (per comments)
3. `BlockDisconnected` — accounting only (per comments)

**But `refresh_job_from_node` is called OUTSIDE the match block**, so ALL event types trigger a full template refresh. The `match` only sets a `reason` string for logging.

### Current code structure (simplified)

```rust
// server.rs:336-476
while let Some(event) = rx.recv().await {
    let clean = true;

    let reason = match &event {
        NodeEvent::MiningWorkChanged { .. } => {
            // ... accounting ops ...
            "new_tip"  // or "reorg" / "mempool"
        }
        NodeEvent::BlockConnected { .. } => {
            // ... accounting ops (mark matured) ...
            "blkconnected"
        }
        NodeEvent::BlockDisconnected { .. } => {
            // ... accounting ops (orphan, close round, etc.) ...
            "blkdisconctd"
        }
    };

    // BUG: This runs for ALL events, not just MiningWorkChanged!
    if let Err(err) = refresh_job_from_node(
        &runtime_events,
        adapter_events_inner.clone(),
        &pool_scripts_events,
        stats_events.clone(),
        &diff_cache_events,
        clean,
        reason,
        debug,
    ).await {
        warn!(error = %err, reason, "template refresh failed after event");
    }
}
```

### Why this causes duplicate broadcasts

When lotusd processes a new block, it emits both:
- `miningwrkchg` (reason=NEW_TIP)
- `blkconnected`

Both arrive within microseconds. The sequential event consumer processes:

1. **`miningwrkchg`** → `refresh_job_from_node()` → RPC → template → `job-8685-8675` → broadcast
2. **`blkconnected`** → `refresh_job_from_node()` → RPC → **SAME template** → `job-8685-8676` (next epoch) → broadcast

Every miner receives two `mining.notify` messages for identical mining work. On LAN, both JSON messages arrive cleanly. On WAN, the second send can interleave with the client's read buffer, producing a malformed partial line.

---

## Fix

Move `refresh_job_from_node` **inside** the `MiningWorkChanged` arm only. Remove it from the other arms entirely. The accounting operations in those arms continue to run — they just don't trigger a template refresh.

### Target: `src/stratum/server.rs`

**Current code (lines ~336–476):**

```rust
tokio::spawn(async move {
    // Track last seen template_epoch for missed-event detection
    let mut last_template_epoch: Option<u64> = None;
    
    while let Some(event) = rx.recv().await {
        let clean = true;
        
        let reason = match &event {
            NodeEvent::MiningWorkChanged {
                reason,
                tip_height,
                template_epoch,
                ..
            } => {
                // Detect missed events
                if let Some(last_epoch) = last_template_epoch {
                    if *template_epoch > last_epoch + 1 {
                        let missed = template_epoch - last_epoch - 1;
                        warn!(last_epoch, current_epoch = template_epoch, missed,
                            "missed miningwrkchg events from lotusd");
                    }
                }
                last_template_epoch = Some(*template_epoch);
                tip_height_events.store(*tip_height, Ordering::SeqCst);
                
                if let Err(err) = db_events.mark_blocks_matured(
                    *tip_height, min_confirmations,
                ) {
                    error!(error = %err, "failed marking matured blocks after miningwrkchg");
                }
                
                reason.as_str()
            }
            NodeEvent::BlockConnected { height, hash: _, prev_hash: _ } => {
                tip_height_events.store(*height, Ordering::SeqCst);
                if let Err(err) = db_events.mark_blocks_matured(
                    *height, min_confirmations,
                ) {
                    error!(error = %err, "failed marking matured blocks after blkconnected");
                }
                "blkconnected"
            }
            NodeEvent::BlockDisconnected { height, hash, prev_hash: _ } => {
                match db_events.find_found_block_by_height_and_hash(*height, hash) {
                    Ok(Some(found_block)) => {
                        if let Err(err) = db_events.mark_found_block_orphaned(hash, "blkdisconctd") {
                            error!(error = %err, block_hash = %hash, "failed marking found_block orphaned");
                        } else {
                            warn!(block_hash = %hash, height = height,
                                round_id = found_block.round_id,
                                worker = %found_block.worker_name.unwrap_or_default(),
                                "pool-mined block orphaned via blkdisconctd");
                        }
                        let _ = db_events.close_round(
                            found_block.round_id,
                            found_block.template_id.map(|v| v as u64),
                            "round_closed_orphaned", Some(hash),
                        );
                        let _ = db_events.record_accounting_event(
                            "found_block_orphaned", Some("orphaned"),
                            None, None, None, None,
                            Some(found_block.round_id), None, None, None,
                            Some(hash), Some(&format!("{{\"height\":{}}}", height)),
                        );
                    }
                    Ok(None) => {
                        debug!(block_hash = %hash, height = height, "external block disconnected");
                    }
                    Err(err) => error!(error = %err, "failed checking orphaned found_block"),
                }
                tip_height_events.store(height - 1, Ordering::SeqCst);
                if let Err(err) = db_events.mark_blocks_matured(
                    height - 1, min_confirmations,
                ) {
                    error!(error = %err, "failed marking matured blocks after reorg");
                }
                "blkdisconctd"
            }
        };
        if let Err(err) = refresh_job_from_node(
            &runtime_events, adapter_events_inner.clone(),
            &pool_scripts_events, stats_events.clone(),
            &diff_cache_events, clean, reason, debug,
        ).await {
            warn!(error = %err, reason, "template refresh failed after event");
        }
    }
});
```

**Replacement code:**

```rust
tokio::spawn(async move {
    // Track last seen template_epoch for missed-event detection
    let mut last_template_epoch: Option<u64> = None;
    
    while let Some(event) = rx.recv().await {
        match &event {
            NodeEvent::MiningWorkChanged {
                reason,
                tip_height,
                template_epoch,
                ..
            } => {
                // === PRIMARY: Template refresh path ===
                // MiningWorkChanged is the purpose-built signal from lotusd
                // for stratum servers. Only this event triggers a template refresh.
                
                // Detect missed events (gaps in template_epoch sequence)
                if let Some(last_epoch) = last_template_epoch {
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
                        // Still process - lotusd may restart and reset epoch counter
                    }
                }
                last_template_epoch = Some(*template_epoch);
                
                // Update tip height tracker for confirmation computation
                tip_height_events.store(*tip_height, Ordering::SeqCst);
                
                // Mark matured blocks based on new tip using configured min_confirmations
                if let Err(err) = db_events.mark_blocks_matured(
                    *tip_height,
                    min_confirmations,
                ) {
                    error!(error = %err, "failed marking matured blocks after miningwrkchg");
                }
                
                // Refresh the mining template and broadcast to all miners
                // clean=true because Lotus header includes block_size, so ALL
                // work changes (mempool, new tip, reorg) invalidate in-flight work
                if let Err(err) = refresh_job_from_node(
                    &runtime_events,
                    adapter_events_inner.clone(),
                    &pool_scripts_events,
                    stats_events.clone(),
                    &diff_cache_events,
                    true,  // clean_jobs
                    reason.as_str(),
                    debug,
                ).await {
                    warn!(error = %err, reason = reason.as_str(),
                        "template refresh failed after miningwrkchg");
                }
            }
            NodeEvent::BlockConnected { height, hash: _, prev_hash: _ } => {
                // === SECONDARY: Accounting only ===
                // BlockConnected is NOT used for template refresh.
                // miningwrkchg handles that. This event exists for backward
                // compatibility and accounting operations (marking blocks matured).
                
                tip_height_events.store(*height, Ordering::SeqCst);
                
                if let Err(err) = db_events.mark_blocks_matured(
                    *height,
                    min_confirmations,
                ) {
                    error!(error = %err, "failed marking matured blocks after blkconnected");
                }
                // NO refresh_job_from_node here — template refresh handled by miningwrkchg
            }
            NodeEvent::BlockDisconnected { height, hash, prev_hash: _ } => {
                // === SECONDARY: Accounting only ===
                // BlockDisconnected is NOT used for template refresh.
                // miningwrkchg handles that. This event exists for orphaning
                // found blocks and closing rounds.
                
                match db_events.find_found_block_by_height_and_hash(*height, hash) {
                    Ok(Some(found_block)) => {
                        // 1. Mark ONLY this block as orphaned
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
                        
                        // 2. Close the round
                        let _ = db_events.close_round(
                            found_block.round_id,
                            found_block.template_id.map(|v| v as u64),
                            "round_closed_orphaned",
                            Some(hash),
                        );
                        
                        // 3. Record accounting event
                        let _ = db_events.record_accounting_event(
                            "found_block_orphaned",
                            Some("orphaned"),
                            None, None, None, None,
                            Some(found_block.round_id),
                            None, None, None,
                            Some(hash),
                            Some(&format!("{{\"height\":{}}}", height)),
                        );
                    }
                    Ok(None) => {
                        debug!(block_hash = %hash, height = height, "external block disconnected");
                    }
                    Err(err) => error!(error = %err, "failed checking orphaned found_block"),
                }
                
                // Update tip height tracker (reorg: tip goes back to prev block)
                tip_height_events.store(height - 1, Ordering::SeqCst);
                
                // Mark matured blocks based on new tip using configured min_confirmations
                if let Err(err) = db_events.mark_blocks_matured(
                    height - 1,
                    min_confirmations,
                ) {
                    error!(error = %err, "failed marking matured blocks after reorg");
                }
                // NO refresh_job_from_node here — template refresh handled by miningwrkchg
            }
        }
    }
});
```

### Key changes

1. **Moved `refresh_job_from_node` inside `MiningWorkChanged` arm** — only the primary event triggers template refresh
2. **Removed `reason` variable** — no longer needed as a shared variable; passed directly as `reason.as_str()` in the MiningWorkChanged arm
3. **Added clarifying comments** in each arm explaining its purpose
4. **Removed the `clean` variable** — hardcoded as `true` in the `refresh_job_from_node` call (it was always `true` for all event types anyway)

---

## Expected Behavior After Fix

| NNG Event | Template Refresh? | Accounting Ops? |
|-----------|-------------------|-----------------|
| `miningwrkchg` (new_tip) | ✅ Yes | Yes (mark matured) |
| `miningwrkchg` (reorg) | ✅ Yes | Yes (mark matured) |
| `miningwrkchg` (mempool) | ✅ Yes | Yes (mark matured) |
| `blkconnected` | ❌ No | Yes (mark matured) |
| `blkdisconctd` | ❌ No | Yes (orphan, close round) |

Each new mining work generates **exactly one** `mining.notify` broadcast per connected miner.

---

## Verification

1. **Unit test:** Send a `miningwrkchg` event, verify `refresh_job_from_node` is called (can mock or observe job publication)
2. **Unit test:** Send a `blkconnected` event, verify `refresh_job_from_node` is NOT called (no new job published)
3. **Integration:** Run stratum server with a test miner, trigger rapid NNG events, verify miner receives exactly one `mining.notify` per block event

---

## Files Modified

| File | Lines | Change |
|------|-------|--------|
| `src/stratum/server.rs` | ~336–476 | Restructure event processing loop: move `refresh_job_from_node` into `MiningWorkChanged` arm only |
