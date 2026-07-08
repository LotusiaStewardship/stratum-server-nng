# Phase 3: Un-Orphan Logic + Payout Confirmation

**Status:** Completed  
**Bullets:** 5 (Un-orphan logic), 6 (Payout confirmation via blkconnected)  
**Theme:** Correctness — fills functional gaps in reorg and payout lifecycle  
**Risk:** Medium  
**Estimated effort:** 1-2 sessions  
**Completed:** 2026-05-29  

---

## Overview

Phase 3 addresses the two most significant functional correctness gaps:

1. **Bullet 5:** When a block is orphaned by a reorg (`BlockDisconnected`) and later reconnected (`BlockConnected`), we must restore it rather than leaving it permanently orphaned. Verified against lotusd's NNG interface: both events carry the full `Block` with `header.hash`, so hash-based matching is reliable.

2. **Bullet 6:** When a payout transaction is submitted, we must wait for on-chain confirmation before marking the found_block as `'paid'`. The `BlockConnected` event includes the full transaction set (`block.txs: Vec<BlockTx>`) — we scan these for our submitted txids. No JSON-RPC calls needed.

---

## Bullet 5: Un-Orphan Logic for Reconnected Blocks

### Context From Reference Review

**Lotusd NNG interface** (`../lotusd/src/nng_interface/nng_interface.cpp`):
- `BlockConnected(block, pindex)` — fires when a block is connected to the active chain
- `BlockDisconnected(block, pindex)` — fires when a block is disconnected from the active chain
- Both carry the full `Block` with `header.hash` (Sha256d) and `txs: Vec<BlockTx>`
- Reorg sequence: disconnected blocks fire first (tip→fork), connected blocks fire second (fork→tip)

**Safety constraint:** A block that was orphaned and then reconnected might have had a payout batch created and submitted. If the batch was already submitted (`status='submitted'`), we must NOT un-orphan — the payout tx could confirm on the orphan chain, leading to a double-spend.

### Domain Position
- **Context:** Node Integration (consumer.rs) + Accounting (found_block_repository, round_repository)
- **Role:** Core correctness — prevents "lost blocks" when temporary reorgs resolve
- **Concepts:** Un-orphan, block reconnection, payout batch guard

### Data Flow
```
BlockConnected{block} arrives
  → extract block_hash = block.header.hash.to_hex_be()
  → found_block_repo.get_by_hash(&block_hash)
  → if status == 'orphaned':
      → payout_repo.get_by_block_hash(&block_hash)
        → if no batch exists: un_orphan to 'immature', leave round closed
        → if batch exists + 'pending': un_orphan to 'matured', process_pending handles it
        → if batch exists + 'submitted': DON'T un_orphan, log warning
        → if batch exists + 'confirmed': shouldn't happen, log warning
  → proceed with normal maturation check
```

### Implementation Steps

1. **Add `get_by_block_hash` to `PayoutRepository`** (or port to `payout::Repository` after Phase 2)
   ```sql
   SELECT pb.* FROM payout_batches pb
   JOIN found_blocks fb ON fb.round_id = pb.round_id
   WHERE fb.block_hash = ?1
   ```

2. **Add `un_orphan()` to `FoundBlockRepository`**
   ```sql
   UPDATE found_blocks SET status = 'immature', orphan_reason = NULL
   WHERE block_hash = ?1 AND status = 'orphaned'
   ```

3. **Add orphan check in `handle_block_connected`** in `consumer.rs`
   - After extracting height, before checking maturation
   - Extract `block_hash = event.block.header.hash.to_hex_be()`
   - Call `accounting.found_block_repo.get_by_hash(&block_hash)`
   - If orphaned: check payout batches, un-orphan if safe
   - Log each decision

4. **No round reopen needed** — The UBQ invariant states orphaned-round shares remain valid in the PPLNS window. Keeping the round closed is correct. If the block matures and generates a payout, it creates a new payout associated with the existing (closed) round, which is fine.

### Files Modified/Created
| File | Change |
|---|---|
| `src/node_integration/nng/consumer.rs` | Add orphan check in `handle_block_connected` |
| `src/accounting/found_block_repository.rs` | Add `un_orphan()` method |
| `src/accounting/payout_repository.rs` | Add `get_by_block_hash()` method |
| `src/node_integration/nng/consumer.rs` tests | Add test: orphaned block without batch → un-orphaned; orphaned block with submitted batch → stays orphaned |

### Test Scenarios
1. Block not in found_blocks → no-op (already handled — maturation check ignores unknown blocks)
2. Block in found_blocks, status='immature' → no-op (normal path, already handled)
3. Block in found_blocks, status='orphaned', no payout batch → un-orphan to 'immature'
4. Block in found_blocks, status='orphaned', payout='pending' → un-orphan to 'matured'
5. Block in found_blocks, status='orphaned', payout='submitted' → stay orphaned, warn

### Blast Radius
- **If removed:** Blocks stay orphaned after reconnection — same as current behavior. No regression.
- **Reversible with:** `git revert`

---

## Bullet 6: Payout Confirmation via BlockConnected Transaction Scanning

### Context From Reference Review

**Lotusd NNG interface** (`../bitcoinsuite/bitcoinsuite-bitcoind-nng/src/structs.rs`):
```rust
pub struct Block {
    pub header: BlockHeader,
    pub metadata: Vec<BlockMetadata>,
    pub txs: Vec<BlockTx>,   // <-- ALL transactions in the block
    pub file_num: u32,
    pub data_pos: u32,
    pub undo_pos: u32,
}

pub struct BlockTx {
    pub tx: Tx,              // tx.txid: Sha256d
    pub data_pos: u32,
    pub undo_pos: u32,
    pub undo_size: u32,
}
```

Txid format: `Sha256d::to_hex_be()` — big-endian hex, matching `submitted_txid` storage.

### Domain Position
- **Context:** Payout (PayoutHandler) + Node Integration (consumer)
- **Role:** Core correctness — payout is only complete when the tx confirms on-chain
- **Concepts:** Confirmation, `PayoutEvent::BlockConnected`

### Data Flow
```
NNG blkconnected → consumer.rs::handle_block_connected
  → extracts txids: event.block.txs.iter().map(|tx| tx.tx.txid.to_hex_be()).collect()
  → sends PayoutEvent::BlockConnected(txids)  // NEW: carries txids
  → PayoutHandler::run()
  → on BlockConnected(txids):
      → query all batches with status='submitted'
      → for each batch, check if submitted_txid ∈ txids
      → if match:
          update_batch_status(id, 'confirmed')
          found_block_repo.update_status(found_block.id, 'paid')
      → also run existing process_pending_payouts (retry failed submissions)
```

### Implementation Steps

1. **Change `PayoutEvent::BlockConnected` in `src/payout/mod.rs`**
   ```rust
   pub enum PayoutEvent {
       BlockMatured(String),
       BlockConnected(Vec<String>),  // hex txids in the connected block
   }
   ```

2. **Update `consumer.rs` — extract txids**
   ```rust
   let txids: Vec<String> = event.block.txs.iter()
       .map(|tx| tx.tx.txid.to_hex_be())
       .collect();
   let _ = maturation_tx.send(PayoutEvent::BlockConnected(txids));
   ```

3. **Add index on `payout_batches.submitted_txid`** in `payout/schema.rs`:
   ```sql
   CREATE INDEX IF NOT EXISTS idx_payout_batches_submitted_txid
       ON payout_batches(submitted_txid);
   ```
   The confirmation scan does `list_batches(Some("submitted"))` and then matches against `submitted_txid` in application code (not SQL). The index enables efficient lookups if we later add a `get_by_submitted_txid` query. For the current approach (iterate over submitted batches), the index on `status` already covers the initial filter.

4. **Update `PayoutHandler::run()` in `handler.rs`**
   ```rust
   PayoutEvent::BlockConnected(txids) => {
       // NEW: check for confirmed payouts
       let submitted = self.payout_repo.list_batches(Some("submitted"))?;
       for batch in submitted {
           if let Some(ref submitted_txid) = batch.submitted_txid {
               if txids.contains(submitted_txid) {
                   self.payout_repo.update_batch_status(batch.id, "confirmed")?;
                   if let Some(fb) = self.found_block_repo.get_by_round_id(batch.round_id)? {
                       self.found_block_repo.update_status(fb.id, "paid")?;
                   }
               }
           }
       }
       // EXISTING: retry pending submissions
       self.process_pending_payouts_locked().await;
   }
   ```

4. **Remove premature `'paid'` from submit path** in `accounting/service.rs` line 909:
   ```rust
   // BEFORE:
   let _ = self.found_block_repo.update_status(found_block.id, "paid");
   // AFTER: remove this line — 'paid' is set only after on-chain confirmation
   ```

5. **Update tests** in `consumer.rs` that match on `BlockConnected`:
   - Line 777: `matches!(second, PayoutEvent::BlockConnected)` → `matches!(second, PayoutEvent::BlockConnected(_))`
   - Line 807: `Ok(Some(PayoutEvent::BlockConnected))` → `Ok(Some(PayoutEvent::BlockConnected(_)))`

### Files Modified/Created
| File | Change |
|---|---|
| `src/payout/mod.rs` | `BlockConnected` → `BlockConnected(Vec<String>)` |
| `src/node_integration/nng/consumer.rs` | Extract txids, send with event |
| `src/payout/handler.rs` | Match on txids, scan for confirmation |
| `src/accounting/service.rs` | Remove premature `found_block.status = 'paid'` |
| `src/payout/schema.rs` | Add index on `submitted_txid` |

### Test Scenarios
1. Submitted batch with txid X, BlockConnected with X in block_txs → batch='confirmed', block='paid'
2. Submitted batch with txid X, BlockConnected without X → batch stays 'submitted'
3. No submitted batches → no-op (scan finds nothing)
4. Multiple submitted batches, one matches → only the matching one transitions

### Blast Radius
- **If removed:** Found_block stays 'matured' after payout submission (no 'paid' transition). The premature 'paid' is already removed. Batches stay 'submitted' indefinitely without confirmation scanning.
- **Reversible with:** `git revert`

---

## Phase 3 Verification

```
cargo build      # must pass clean
cargo test       # must pass — new tests for un-orphan + confirmation; updated BlockConnected match patterns
```

### Healing Check
After Phase 3:
- A `blkdisconctd` then later `blkconnected` with the same hash restores the block (unless a payout was already submitted)
- A payout batch transitions to `'confirmed'` when its txid appears in a connected block
- `found_block.status = 'paid'` is only set AFTER on-chain confirmation, not before

## Completion Checklist

- [x] `FoundBlockRepository::un_orphan()` exists and is tested
- [x] `PayoutRepository::get_batches_by_block_hash()` exists and is tested
- [x] `handle_block_connected` checks for orphaned blocks and safe-un-orphans
- [x] `PayoutEvent::BlockConnected` carries `Vec<String>` txids
- [x] `consumer.rs` extracts and passes txids from `BlockConnected` events
- [x] `PayoutHandler` scans txids for submitted payout confirmation
- [x] Premature `found_block.status = 'paid'` removed from submit path
- [x] All tests pass
