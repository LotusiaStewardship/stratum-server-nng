# Phase 4: Pagination, Hashrate, Config Docs, Tracing Alignment

**Status:** Proposed  
**Bullets:** 7 (Pagination), 8 (Config docs), 9 (Hashrate), 10 (Tracing)  
**Theme:** Surface — API completeness, observability, documentation  
**Risk:** Low  
**Estimated effort:** 1 session  

---

## Overview

Phase 4 addresses remaining surface-level gaps. These are independent of the structural changes in Phases 1-3 and can be done in any order (including before or in parallel with earlier phases).

1. **Bullet 7:** Add pagination to `GET /api/v1/blocks`, `/api/v1/rounds`, `/api/v1/payouts`
2. **Bullet 8:** Document `FeeSettings`, `PplnsSettings`, `BanningSettings`, `SigningSettings`, `MiningIdentity` in CONTEXT.md files
3. **Bullet 9:** Add pool hashrate computation to `GET /api/v1/stats`
4. **Bullet 10:** Correct `logging.rs` comment about tracing domains

---

## Bullet 7: Pagination on Remaining List Endpoints

### Domain Position
- **Context:** HTTP API
- **Role:** Surface/UI — prevents unbounded response growth
- **Concepts:** `PaginationParams`, `PaginatedResponse`

### Current State

| Endpoint | Pagination | Wrapper |
|----------|-----------|---------|
| `GET /api/v1/workers` | Yes (`PaginationParams`) | `PaginatedResponse` |
| `GET /api/v1/shares` | Yes (inline limit/offset) | `PaginatedResponse` |
| `GET /api/v1/share-outcomes` | Yes (inline limit/offset) | `PaginatedResponse` |
| `GET /api/v1/blocks` | **No** | Raw `Vec` |
| `GET /api/v1/rounds` | **No** | Raw `Vec` |
| `GET /api/v1/payouts` | **No** | Raw `Vec` |

### Implementation Steps

For each of `list_blocks`, `list_rounds`, `list_payouts`:

1. Accept optional query params via `PaginationParams` (already available in `pagination.rs`)
2. Query the full list (as currently done — these datasets are small enough)
3. Wrap in `PaginatedResponse::new(all, total, &params)`
4. Add test verifying pagination behavior

### Data Flow
```
Request: GET /api/v1/blocks?limit=5&offset=0
Handler: list_blocks(State(state), Query(PaginationParams { limit: Some(5), offset: Some(0) }))
  → repo.list(None) (full list)
  → PaginatedResponse::new(all, total, &params)
  → { data: [...first 5...], total: 42, has_more: true }
```

### Breaking Change Notice
**Response shape changes** from `[...]` to `{ data: [...], total, has_more }`. Any API consumer reading the raw array will break. Update the HTTP API CONTEXT.md to document the change.

### Files Modified
| File | Change |
|---|---|
| `src/http_api/routes/blocks.rs` | Add `Query<PaginationParams>`, wrap response |
| `src/http_api/routes/rounds.rs` | Same |
| `src/http_api/routes/payouts.rs` | Same |

### Test Criterion
`GET /api/v1/blocks?limit=1&offset=0` returns `{ data: [block1], total: N, has_more: true/false }`.

---

## Bullet 8: Document Config Structures

### Domain Position
- **Context:** Cross-cutting (documentation)
- **Role:** First entry point for operators
- **Concepts:** Config structs defined in `src/config.rs`

### Current Documentation Gaps

| Config Struct | Documented Where? |
|---------------|-------------------|
| `VarDiffSettings` | Partially in `stratum-core/CONTEXT.md` |
| `FeeSettings` (enabled, fee_bps, fee_address, fee_script_hex) | **Nowhere** |
| `PplnsSettings` (n_multiplier, min_payout_sat, payout_enabled, min_confirmations) | Partially in `payout/CONTEXT.md` |
| `BanningSettings` (enabled, check_threshold, invalid_percent) | **Nowhere** |
| `SigningSettings` (private_key, webhook_url, signing_mode) | **Nowhere** |
| `MiningIdentity` (payout_address, script_sig_tag) | **Nowhere** |
| `BitcoindRpcSettings` (url, rpc_user, rpc_pass) | Partially in `node-integration/CONTEXT.md` |

### Implementation

Add a "Configuration" section to each CONTEXT.md.

**`stratum-core/CONTEXT.md`:**
```markdown
## Configuration

### `[vardiff]`
| Key | Type | Default | Description |
|-----|------|---------|-------------|
| min_floor | f64 | 0.001 | Absolute minimum P_diff floor |
| initial_pct | f64 | 0.01 | Initial P_diff as fraction of N_diff |
| target_secs | f64 | 20.0 | Target seconds between shares |
| retarget_secs | f64 | 60.0 | Retarget interval in seconds |
```

**`payout/CONTEXT.md`:**
```markdown
## Configuration

### `[pool.fee]`
| Key | Type | Default | Description |
|-----|------|---------|-------------|
| enabled | bool | true | Enable fee collection |
| fee_bps | u32 | 100 | Fee in basis points (100 = 1%) |
| fee_address | string? | null | Lotus address for fee output |
| fee_script_hex | string? | null | Raw output script hex for fee |

### `[pool.pplns]`
| Key | Type | Default | Description |
|-----|------|---------|-------------|
| n_multiplier | f64 | 5.0 | PPLNS window multiplier × N_diff |
| min_payout_sat | i64 | 546 | Minimum miner payout threshold |
| payout_enabled | bool | true | Auto-create payout batches |
| min_confirmations | u64 | 100 | Blocks before payout-eligible |

### `[pool.pplns.banning]`
| Key | Type | Default | Description |
|-----|------|---------|-------------|
| enabled | bool | false | Enable miner banning |
| check_threshold | u64 | 10 | Shares before checking ratio |
| invalid_percent | f64 | 50.0 | Max rejection % before ban |

### `[pool.signing]`
| Key | Type | Default | Description |
|-----|------|---------|-------------|
| signing_mode | string | "internal" | 'internal' or 'external' |
| private_key | string? | null | Private key hex/WIF (internal mode) |
| webhook_url | string? | null | Webhook URL (external mode) |
| fee_per_kb | i64 | 1000 | Tx fee in sat/kB |

### `[pool.mining_identity]`
| Key | Type | Default | Description |
|-----|------|---------|-------------|
| payout_address | string | required | Pool's Lotus payout address |
| script_sig_tag | string? | null | Optional coinbase scriptSig tag |
```

**`node-integration/CONTEXT.md`:**
```markdown
## Configuration

### `[bitcoind_rpc]`
| Key | Type | Default | Description |
|-----|------|---------|-------------|
| url | string | "http://127.0.0.1:18332" | JSON-RPC endpoint |
| rpc_user | string | "lotus" | RPC auth username |
| rpc_pass | string | "lotus" | RPC auth password |
```

**`docs/CONSTITUTION.md`:** Add reference: "See CONTEXT.md files for per-context config documentation."

### Files Modified
| File | Change |
|---|---|
| `docs/contexts/stratum-core/CONTEXT.md` | Add VarDiff config section |
| `docs/contexts/payout/CONTEXT.md` | Add fee, pplns, banning, signing, mining_identity config |
| `docs/contexts/node-integration/CONTEXT.md` | Add bitcoind_rpc config section |
| `docs/CONSTITUTION.md` | Add config reference pointer |

---

## Bullet 9: Add Pool Hashrate Endpoint

### Context From Reference Review

**Lotusd methodology** (`../lotusd/src/rpc/mining.cpp`):
```cpp
static UniValue GetNetworkHashPS(int lookup, int height) {
    // ...
    arith_uint256 workDiff = pb->nChainWork - pb0->nChainWork;
    int64_t timeDiff = maxTime - minTime;
    return workDiff.getdouble() / timeDiff;
}
```
Network hashrate = cumulative work over window / time span. Work is measured in N_diff-weighted units: `work ≈ sum(N_diff_i × 2^32)`.

**Adaptation to stratum:** Each share with difficulty D represents `D × 2^32` hashes of work. Pool hashrate = `sum(accepted_share.difficulty) × 2^32 / window_seconds`.

### Domain Position
- **Context:** HTTP API (stats handler) + Accounting (share repository)
- **Role:** Surface/UI — operator monitoring
- **Concepts:** Pool hashrate, rolling window, share difficulty

### Data Flow
```sql
SELECT SUM(s.difficulty)
FROM shares s
JOIN share_outcomes so ON s.id = so.share_id
WHERE so.status = 'accepted'
  AND so.created_at >= ?1    -- 5 minutes ago
```

```rust
let sum_diff = share_repo.sum_difficulty_since(five_min_ago)?;
let hashrate = sum_diff.map(|s| (s * 4_294_967_296.0 / 300.0) as u64);
// 2^32 = 4,294,967,296; 300s = 5 minute window
```

### Implementation Steps

1. **Add `sum_difficulty_since()` to `ShareRepository`**
   ```rust
   pub fn sum_difficulty_since(&self, since: &str) -> Result<f64> {
       let conn = self.conn.lock();
       let sql = "
           SELECT COALESCE(SUM(s.difficulty), 0.0)
           FROM shares s
           JOIN share_outcomes so ON s.id = so.share_id
           WHERE so.status = 'accepted'
             AND so.created_at >= ?1
       ";
       conn.query_row(sql, params![since], |row| row.get(0))
           .map_err(Into::into)
   }
   ```

2. **Add `pool_hashrate` to `StatsResponse` in `stats.rs`**
   ```rust
   #[derive(Serialize)]
   pub struct StatsResponse {
       pub total_shares: i64,
       pub accepted_shares: i64,
       pub rejected_shares: i64,
       pub accepted_pct: f64,
       pub rejection_breakdown: HashMap<String, i64>,
       pub network_difficulty: Option<String>,
       pub pool_hashrate: Option<String>,  // NEW: in H/s, as string for JSON safety
   }
   ```

3. **Compute hashrate in `stats_handler`**
   ```rust
   let now = Utc::now().naive_utc();
   let five_min_ago = (now - chrono::Duration::seconds(300))
       .format("%Y-%m-%d %H:%M:%S")
       .to_string();
   let sum_diff = share_repo
       .sum_difficulty_since(&five_min_ago)
       .unwrap_or(0.0);
   let pool_hashrate = if sum_diff > 0.0 {
       Some(((sum_diff * 4_294_967_296.0) / 300.0) as u64)
   } else {
       None
   };
   ```

### Files Modified
| File | Change |
|---|---|
| `src/accounting/share_repository.rs` | Add `sum_difficulty_since()` |
| `src/http_api/routes/stats.rs` | Add `pool_hashrate: Option<String>` to response, compute value |

### Test Criterion
Insert 100 shares with difficulty 1.0 and `created_at` within last 5 minutes, all accepted. `sum_difficulty_since` returns ~100.0. `stats_handler` returns `pool_hashrate: "1431655765"` (100 × 2^32 / 300 ≈ 1.43 GH/s).

---

## Bullet 10: Tracing Domain Comment Alignment

### Domain Position
- **Context:** Cross-cutting (observability)
- **Role:** Documentation — prevents developer confusion about domain mapping

### Current State
```rust
// Domain-tagged tracing macros.
//
// Each macro wraps the corresponding tracing:: macro with a `target:` field
// matching the bounded context (see docs/CONTEXT_MAP.md). This enables
// per-domain filtering via RUST_LOG, e.g.:
//   RUST_LOG=stratum=debug,accounting=info
```

### Problem
Tracing domains don't match bounded contexts 1:1. `validator` has no context, `shutdown` has no context, `main` has no context.

### Implementation
Update the comment to:
```rust
// Domain-tagged tracing macros.
//
// Each macro wraps the corresponding tracing:: macro with a `target:` field
// matching the module area. These targets are finer-grained than the bounded
// contexts defined in docs/CONTEXT_MAP.md, enabling more precise filtering
// via RUST_LOG.
//
// Domain → context mapping:
//   stratum     → Stratum Core (stratum_protocol module)
//   validator   → Stratum Core sub-domain (share_processing/validator)
//   node_int    → Node Integration
//   accounting  → Accounting
//   payout      → Payout
//   http_api    → HTTP API
//   shutdown    → Infrastructure / cross-cutting
//   main        → Application entry point (main.rs)
//
// Filtering example:
//   RUST_LOG=stratum=debug,accounting=info,node_int=warn
```

### Files Modified
| File | Change |
|---|---|
| `src/logging.rs` | Update comment header |

---

### Documentation Updates

| Document | Change |
|----------|--------|
| `docs/contexts/stratum-core/CONTEXT.md` | Add VarDiff config section. |
| `docs/contexts/payout/CONTEXT.md` | Add fee, pplns, banning, signing, mining_identity config sections. Document hashrate endpoint. |
| `docs/contexts/node-integration/CONTEXT.md` | Add bitcoind_rpc config section. |
| `docs/contexts/http-api/CONTEXT.md` | Update route table with pagination changes (response shape change). Document hashrate field in stats response. |
| `docs/CONSTITUTION.md` | Add config reference pointer. |
| `src/logging.rs` | Fix comment (Bullet 10). |

---

## Phase 4 Verification

```
cargo build      # must pass clean (no new deps needed)
cargo test       # must pass
```

## Completion Checklist

- [ ] `list_blocks`, `list_rounds`, `list_payouts` accept `PaginationParams` and return `PaginatedResponse`
- [ ] Config structs documented in CONTEXT.md files (FeeSettings, PplnsSettings, BanningSettings, SigningSettings, MiningIdentity)
- [ ] `ShareRepository.sum_difficulty_since()` exists and returns correct values
- [ ] `GET /api/v1/stats` returns `pool_hashrate` field (accepted shares, 5-min window, 2^32 multiplier)
- [ ] `logging.rs` comment corrected to describe domain→context mapping accurately
- [ ] Documentation updated per table above
- [ ] All tests pass
