# Plan 003: NNG Event Pipeline Fixes

**Date:** 2026-05-13  
**Status:** Proposed  
**Scope:** `src/stratum/server.rs`, `src/stratum/job.rs`  
**Trigger:** WAN miner disconnects with "trailing characters" JSON parse errors  

---

## Motivation

WAN miners (connecting to `pool.lotusia.org:3334`) experience intermittent disconnects with:

```
[ERROR] Session error: trailing characters at line 1 column 4
```

The same miners work perfectly on localhost/LAN. Investigation revealed that when `lotusd` emits NNG events (e.g., new block), multiple events fire in rapid succession — `miningwrkchg` and `blkconnected` arrive within microseconds. Each event triggers a full template refresh + broadcast pipeline, causing **duplicate `mining.notify` messages** to be sent to every connected miner.

### Forensic Evidence

Client-side log (all at `01:09:42`):

```
[DEBUG] RECV [908 bytes]: {"id":null,"method":"mining.notify","params":["job-8684-8674", ...4847]}    ← OK
[DEBUG] RECV [908 bytes]: {"id":null,"method":"mining.notify","params":["job-8685-8675", ...5079]}    ← OK
[DEBUG] RECV [368 bytes]: 583cf2f45b3df714ac07be6e0044bbdf5ce2e065","3c4c2357...5079]}                 ← CRASH
```

Server-side log (all at `08:09:42.427`):

```
sent mining.notify (standard Stratum V1) job_id=job-8685-8675   → session sdf7596f6102f0c7a
sent mining.notify (standard Stratum V1) job_id=job-8685-8675   → session s6b5ac651417513af
```

**The 368-byte fragment is byte offset 540–907 of the full 908-byte JSON for `job-8685-8675`.** It was the tail end of a duplicate send that arrived mid-read — the client's `BufReader` had already consumed the first 540 bytes, leaving 368 bytes as a malformed "next line."

### Root Cause Chain

```
NNG events:  miningwrkchg ──┐
             blkconnected  ──┤──→ mpsc::unbounded_channel ──→ sequential processing
                             │                                  ↓
                             │                         for EACH event:
                             │                         ┌──────────────────────────┐
                             │                         │ refresh_job_from_node()  │
                             │                         │ → RPC to lotusd          │
                             │                         │ → make_job_from_template │
                             │                         │ → publish_job(job)       │  ← SAME template
                             │                         │ → broadcast to ALL miners│     TWICE!
                             │                         └──────────────────────────┘
                             ↓
                    Each handle_conn:
                      pub_rx.recv() → send_mining_notify(job)  ← TWO sends per event
```

The code comments explicitly state `blkconnected`/`blkdisconctd` are "for accounting only" and "NOT used for template refresh" — but the code calls `refresh_job_from_node` for **all** event types. The call sits outside the `match` block.

---

## Bugs Identified

| # | Bug | Severity | Location | Description |
|---|-----|----------|----------|-------------|
| 1 | Accounting events trigger template refresh | **Critical** | `server.rs:462` | `blkconnected`/`blkdisconctd` call `refresh_job_from_node` despite comments saying they shouldn't |
| 2 | No event coalescing/debouncing | **Critical** | `server.rs:332–476` | Burst of NNG events → burst of async RPC → burst of broadcasts |
| 3 | No job deduplication in `publish_job` | **High** | `server.rs:94–108` | Same template + different epoch = duplicate broadcast |
| 4 | No TCP `flush()` after `write_all` | **Medium** | `server.rs:676–749` | Multiple rapid writes buffer together at TCP level, causing interleaving on WAN |

---

## Remediation Phases

| Phase | Fix | Risk | Effort | Dependencies |
|-------|-----|------|--------|--------------|
| [Phase 1](./003-phase1-accounting-events.md) | Stop accounting events from triggering template refresh | Low | Small | None |
| [Phase 2](./003-phase2-event-coalescing.md) | Replace raw channel with coalescing event queue | Medium | Medium | Phase 1 |
| [Phase 3](./003-phase3-job-deduplication.md) | Add job deduplication in `publish_job` | Low | Small | Phase 2 |
| [Phase 4](./003-phase4-tcp-flush.md) | Add `flush()` after all TCP `write_all` calls | Low | Trivial | None (can land independently) |

---

## Architecture Overview (Current)

```
┌──────────────────────────────────────────────────────────────────────┐
│ run_stratum_server()                                                 │
│                                                                      │
│  ┌─ NNG pub loop (spawn_blocking) ──────────────────────────────┐   │
│  │  recv_raw() → parse flatbuffer → tx.send(NodeEvent)           │   │
│  └──────────────────────────────────────────────────────────────┘   │
│                                      │                              │
│                                      ▼                              │
│  ┌─ event consumer (tokio::spawn) ──────────────────────────────┐   │
│  │  rx.recv() → match event → reason string                     │   │
│  │  refresh_job_from_node(clean=true, reason)  ← ALL events     │   │
│  │    → adapter.get_mining_template()  ← async RPC to lotusd     │   │
│  │    → make_job_from_template(template, next_epoch())           │   │
│  │    → runtime.publish_job(job)  ← broadcast::send()            │   │
│  └──────────────────────────────────────────────────────────────┘   │
│                                                                      │
│  ┌─ handle_conn (per miner, tokio::spawn) ──────────────────────┐   │
│  │  select! {                                                    │   │
│  │    biased;                                                    │   │
│  │    pub_rx.recv() → send_mining_notify(job)  ← writes to TCP   │   │
│  │    diff_rx.recv()  → send_set_difficulty()   ← writes to TCP   │   │
│  │    reader.read_line() → handle miner requests                 │   │
│  │  }                                                            │   │
│  └──────────────────────────────────────────────────────────────┘   │
│                                                                      │
│  (N copies of handle_conn — one per connected miner)                │
└──────────────────────────────────────────────────────────────────────┘
```

## Architecture Overview (After All Phases)

```
┌──────────────────────────────────────────────────────────────────────┐
│ run_stratum_server()                                                 │
│                                                                      │
│  ┌─ NNG pub loop (spawn_blocking) ──────────────────────────────┐   │
│  │  recv_raw() → parse flatbuffer → tx.send(NodeEvent)           │   │
│  └──────────────────────────────────────────────────────────────┘   │
│                                      │                              │
│                                      ▼                              │
│  ┌─ event consumer (tokio::spawn) ──────────────────────────────┐   │
│  │  rx.recv() → match event                                     │   │
│  │    MiningWorkChanged → pending = Some(event)  ← ONLY this!   │   │
│  │    blkconnected/blkdisconctd → accounting only, NO refresh   │   │
│  │                                                              │   │
│  │  debounce_timer → if pending:                                │   │
│  │    → coalesce: only keep latest pending event                │   │
│  │    → refresh_job_from_node()  ← ONCE per debounce window     │   │
│  │       → prevhash/ntime check → skip if same as last          │   │
│  │       → runtime.publish_job(job)  ← broadcast::send()        │   │
│  └──────────────────────────────────────────────────────────────┘   │
│                                                                      │
│  ┌─ handle_conn (per miner, tokio::spawn) ──────────────────────┐   │
│  │  select! {                                                    │   │
│  │    biased;                                                    │   │
│  │    pub_rx.recv() → send_mining_notify(job) + flush()          │   │
│  │    diff_rx.recv()  → send_set_difficulty() + flush()          │   │
│  │    reader.read_line() → handle miner requests                 │   │
│  │  }                                                            │   │
│  └──────────────────────────────────────────────────────────────┘   │
└──────────────────────────────────────────────────────────────────────┘
```

---

## Files Modified

| File | Phases |
|------|--------|
| `src/stratum/server.rs` | 1, 2, 3, 4 |
| `src/stratum/job.rs` | 3 |
