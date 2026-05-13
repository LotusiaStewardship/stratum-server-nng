# Phase 4: TCP `flush()` After All `write_all` Calls

**Plan:** [003](./003-nng-event-pipeline-fixes.md)  
**Priority:** Medium — Hardens TCP delivery semantics for WAN clients  
**Risk:** Low — Adding flush is always safe; may slightly increase latency  
**Effort:** Trivial — Add 4 lines across 4 functions  
**Dependencies:** None — Can land independently of other phases

---

## Objective

Add `writer.flush().await` after every `write_all` call in the stratum server's TCP send functions. This ensures each JSON line is pushed to the network immediately, preventing TCP buffer coalescing that can cause interleaved reads on WAN connections.

## Problem

The current send functions call `write_all` but never `flush`:

```rust
async fn send_json_line(
    writer: &mut tokio::net::tcp::OwnedWriteHalf,
    v: &StratumResponse,
) -> Result<()> {
    let mut data = serde_json::to_vec(v)?;
    data.push(b'\n');
    writer.write_all(&data).await?;
    // ← No flush — data may sit in OS TCP buffer
    Ok(())
}

async fn send_set_difficulty(
    writer: &mut tokio::net::tcp::OwnedWriteHalf,
    diff: f64,
) -> Result<()> {
    let v = serde_json::json!({...});
    let mut data = serde_json::to_vec(&v)?;
    data.push(b'\n');
    writer.write_all(&data).await?;
    // ← No flush
    Ok(())
}

async fn send_set_extranonce(...) -> Result<()> { ... }  // ← No flush
async fn send_mining_notify(...) -> Result<()> { ... }   // ← No flush
```

### Why this matters on WAN

Without `flush`, the OS TCP stack may:
1. **Nagle's algorithm** — buffer small writes to combine into fewer packets
2. **Delayed ACK** — wait for more data before sending
3. **Coalesce multiple writes** — if two `write_all` calls happen close together, the kernel may combine them into one TCP segment

On LAN, the TCP buffer empties quickly (low latency, high throughput), so messages arrive as discrete lines. On WAN:
- Higher RTT means buffers fill longer
- TCP may combine `send_mining_notify(job-8685-8675)` + `send_mining_notify(job-8685-8676)` into one packet
- The client's `read_line` reads the combined data, processes the first message, and the **remainder** of the second message becomes a malformed "next line"

This is exactly what happened in the forensic evidence:
- The 368-byte fragment was the tail of a duplicate `mining.notify` 
- The first 540 bytes of the duplicate were consumed as part of the previous read
- The remaining 368 bytes became a standalone malformed line

### Nagle's algorithm interaction

Nagle's algorithm delays sending small packets until either:
1. A full packet (typically 1460 bytes MSS) is ready, OR
2. All previously sent data has been ACKed

On high-latency WAN links, this can cause significant delays. Combined with the lack of `flush`, multiple stratum messages can sit in the TCP buffer together and arrive as one blob.

---

## Implementation

**File: `src/stratum/server.rs`**

### 1. `send_json_line` (line ~676)

```rust
async fn send_json_line(
    writer: &mut tokio::net::tcp::OwnedWriteHalf,
    v: &StratumResponse,
) -> Result<()> {
    let mut data = serde_json::to_vec(v)?;
    data.push(b'\n');
    writer.write_all(&data).await?;
    writer.flush().await?;  // ← ADD
    Ok(())
}
```

### 2. `send_set_difficulty` (line ~703)

```rust
async fn send_set_difficulty(
    writer: &mut tokio::net::tcp::OwnedWriteHalf,
    diff: f64,
) -> Result<()> {
    let v = serde_json::json!({
        "id": serde_json::Value::Null,
        "method": "mining.set_difficulty",
        "params": [diff],
    });
    let mut data = serde_json::to_vec(&v)?;
    data.push(b'\n');
    writer.write_all(&data).await?;
    writer.flush().await?;  // ← ADD
    debug!(difficulty = diff, "sent mining.set_difficulty");
    Ok(())
}
```

### 3. `send_set_extranonce` (line ~718)

```rust
async fn send_set_extranonce(
    writer: &mut tokio::net::tcp::OwnedWriteHalf,
    extranonce1: &str,
    extranonce2_size: usize,
) -> Result<()> {
    let v = serde_json::json!({
        "id": serde_json::Value::Null,
        "method": "mining.set_extranonce",
        "params": [extranonce1, extranonce2_size],
    });
    let mut data = serde_json::to_vec(&v)?;
    data.push(b'\n');
    writer.write_all(&data).await?;
    writer.flush().await?;  // ← ADD
    info!(extranonce1 = %extranonce1, extranonce2_size = extranonce2_size, "sent mining.set_extranonce");
    Ok(())
}
```

### 4. `send_mining_notify` (line ~734)

```rust
async fn send_mining_notify(
    writer: &mut tokio::net::tcp::OwnedWriteHalf,
    job: &MiningJob,
) -> Result<()> {
    let params = job.notify_params();
    let v = serde_json::json!({
        "id": serde_json::Value::Null,
        "method": "mining.notify",
        "params": params,
    });
    let mut data = serde_json::to_vec(&v)?;
    data.push(b'\n');
    writer.write_all(&data).await?;
    writer.flush().await?;  // ← ADD
    debug!(job_id = %job.job_id, "sent mining.notify (standard Stratum V1)");
    Ok(())
}
```

---

## Impact Analysis

### Performance

| Metric | Before | After | Notes |
|--------|--------|-------|-------|
| LAN latency | ~0.1ms | ~0.1ms | No measurable difference |
| WAN latency (single message) | ~50-200ms | ~50-200ms | No change; flush just ensures delivery |
| WAN burst latency | Variable | ~+RTT per message | Each message now gets its own packet |
| Throughput | Higher | Slightly lower | More TCP packets = slightly more overhead |

### Trade-offs

**Pro:**
- Each JSON line is guaranteed to be a complete TCP segment
- Eliminates interleaved reads on WAN
- Predictable delivery timing
- Aligns with stratum protocol expectations (one JSON object per line)

**Con:**
- Slightly more TCP packets on high-frequency updates
- Negligible overhead for typical stratum traffic (~1 message per 10-60 seconds)

### Why we don't disable Nagle's (`TCP_NODELAY`)

An alternative approach is to set `TCP_NODELAY` on the socket to disable Nagle's algorithm. We chose explicit `flush()` because:

1. **More surgical** — Only flushes when we actually want to send; doesn't affect every write
2. **Protocol-aware** — We know stratum is line-oriented; we flush at line boundaries
3. **Less invasive** — No socket option changes; works with default TCP behavior
4. **Composable** — Can be combined with `TCP_NODELAY` later if needed

---

## Verification

1. **Integration test:** Connect a miner over WAN (or simulate with `tc` netem adding latency). Trigger a job update, verify miner receives clean, complete JSON lines with no fragmentation.

2. **Packet capture:** Use `tcpdump` on the server to verify each `mining.notify` appears as a discrete TCP segment, not combined with other messages.

3. **Stress test:** Rapidly trigger multiple job updates, verify the miner never receives a partial JSON line.

---

## Files Modified

| File | Lines | Change |
|------|-------|--------|
| `src/stratum/server.rs` | ~683 | Add `writer.flush().await?` to `send_json_line` |
| `src/stratum/server.rs` | ~712 | Add `writer.flush().await?` to `send_set_difficulty` |
| `src/stratum/server.rs` | ~728 | Add `writer.flush().await?` to `send_set_extranonce` |
| `src/stratum/server.rs` | ~749 | Add `writer.flush().await?` to `send_mining_notify` |
