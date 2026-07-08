# Phase 1: PayoutScheme Trait + TransactionBuilder

**Status:** Proposed  
**Bullets:** 3 (PayoutScheme trait), 4 (TransactionBuilder)  
**Theme:** Additive foundation — no code moved  
**Risk:** Low  
**Estimated effort:** 1 session  

---

## Overview

Phase 1 establishes the abstraction foundation for the payout module. Two purely additive changes that introduce no breaking changes and require no data migration:

1. **Bullet 3:** Extract a `PayoutScheme` trait so callers are scheme-agnostic (PPLNS today, PPS tomorrow).
2. **Bullet 4:** Extract coinbase-spending transaction construction from `InternalSigner` into a standalone `TransactionBuilder`, separating construction from signing.

Neither bullet moves existing code between modules. Both are pure extract-refactors that leave existing interfaces intact while adding new ones.

---

## Bullet 3: PayoutScheme Trait

### Domain Position
- **Context:** Payout
- **Role:** Contract/boundary — abstracts payout calculation
- **Concepts:** `PayoutScheme`, `PplnsScheme`

### Data Flow
```
Current:                           Intended:
PayoutService                      PayoutService
  └─ calls build_payout_plan()       └─ calls scheme.calculate()
                                           ↑
                                    PayoutScheme trait
                                           ↑
                                    PplnsScheme::calculate()
```

### Implementation Steps

1. **Create `src/payout/scheme/mod.rs`**
   ```rust
   use crate::payout::plan::PayoutPlan;
   use crate::payout::pplns::PplnsShareEntry;

   pub trait PayoutScheme: Send + Sync {
       fn calculate(
           &self,
           round_id: i64,
           block_height: i32,
           block_hash: &str,
           network_difficulty: f64,
           gross_reward: i64,
           fee_bps: u32,
           fee_address: Option<&str>,
           min_payout_sat: i64,
           dust_balances: &[(String, i64)],
           shares: &[PplnsShareEntry],
       ) -> PayoutPlan;
   }
   ```

   The trait takes **individual fields** (matching `build_payout_plan`'s actual signature in `plan.rs`), not a `FoundBlock` struct. This keeps the trait generic — callers destructure whatever data source they have.

2. **Create `src/payout/scheme/pplns.rs`**
   - Move the body of `build_payout_plan()` from `plan.rs` into `PplnsScheme::calculate()`
   - Keep `PayoutPlan`, `PayoutOutput` structs in `plan.rs` (they're data types, not logic)

3. **Update `src/payout/service.rs` (created in Phase 2)** to hold `Box<dyn PayoutScheme>`

4. **Update `src/main.rs`** to inject `PplnsScheme` into `PayoutService`

5. **Add `payout/scheme` to `payout/mod.rs`**

### Files Modified/Created
| File | Action |
|---|---|
| `src/payout/scheme/mod.rs` | **Create** — trait definition |
| `src/payout/scheme/pplns.rs` | **Create** — PPLNS implementation |
| `src/payout/plan.rs` | **Modify** — extract `build_payout_plan` body (or keep as thin wrapper calling `PplnsScheme`) |
| `src/payout/mod.rs` | **Modify** — add `pub mod scheme` |
| `src/payout/service.rs` | **Modify** — accept `Box<dyn PayoutScheme>` in constructor (created in Phase 2) |
| `src/main.rs` | **Modify** — inject `PplnsScheme` |

### Test Impact
- Existing payout calculation tests continue to pass (they test the same logic through the trait)
- Add a test verifying a mock scheme is called and its plan is used
- `cargo test` must pass

### Blast Radius
- **If removed:** Callers go back to calling `build_payout_plan()` directly. No data loss. No behavior change for PPLNS. Adding a second scheme would require re-introducing the trait.
- **Reversible with:** `git revert`

---

### Documentation Updates

| Document | Change |
|----------|--------|
| `docs/contexts/payout/CONTEXT.md` | Add `scheme/` sub-module description. Document `PayoutScheme` trait as the contract for payout calculation. |
| `docs/contexts/payout/CONTEXT.md` | Add `transaction.rs` description. Note `TransactionBuilder` is separated from `InternalSigner`. |

---

## Bullet 4: TransactionBuilder Extraction

### Domain Position
- **Context:** Payout
- **Role:** Infrastructure — builds coinbase-spending `Tx` from `PayoutPlan` + UTXO data
- **Concepts:** `TransactionBuilder`, `SignedBatchData`

### Data Flow
```
Current:                           Intended:
InternalSigner                     TransactionBuilder
  ├─ build_payout_tx()               ├─ build(SignedBatchData) -> Tx
  └─ sign_and_submit()               └─ used by InternalSigner
                                       InternalSigner
                                         └─ sign_and_submit() uses TransactionBuilder
```

### Implementation Steps

1. **Create `src/payout/transaction.rs`**
   - Define `TransactionBuilder` struct (no fields needed — it's stateless)
   - Move `build_payout_tx()` logic from `signer/internal.rs` into `TransactionBuilder::build(SignedBatchData) -> Tx`
   - Move `build_vout_spendable()` or equivalent vout-scanning logic
   - Note: `build_payout_tx` in `internal.rs:85` is a **private** method (`fn`, not `pub fn`). No callers outside `InternalSigner` exist. The extraction is purely internal refactoring.

2. **Update `src/payout/signer/internal.rs`**
   - `InternalSigner` holds `TransactionBuilder`
   - `sign_and_submit()` calls `self.tx_builder.build(data)` then signs the resulting `Tx`

3. **Update `src/payout/mod.rs`** to expose `transaction`

### Files Modified/Created
| File | Action |
|---|---|
| `src/payout/transaction.rs` | **Create** — `TransactionBuilder` |
| `src/payout/signer/internal.rs` | **Modify** — use `TransactionBuilder` |
| `src/payout/mod.rs` | **Modify** — add `pub mod transaction` |
| `src/payout/signer/mod.rs` | **No change** — `Signer` trait unchanged |

### Test Impact
- Extract existing `build_payout_tx` test as unit tests for `TransactionBuilder::build()`
- Integration tests in `internal.rs` continue to pass through `InternalSigner`
- `cargo test` must pass

### Blast Radius
- **If removed:** `InternalSigner` goes back to doing its own construction. No external callers affected — `TransactionBuilder` is used only by `InternalSigner`.
- **Reversible with:** `git revert`

---

### Documentation Updates

| Document | Change |
|----------|--------|
| `docs/contexts/payout/CONTEXT.md` | Add `scheme/` sub-module description. Document `PayoutScheme` trait as the contract for payout calculation. |
| `docs/contexts/payout/CONTEXT.md` | Add `transaction.rs` description. Note `TransactionBuilder` is separated from `InternalSigner`. |

---

## Phase 1 Verification

```
cargo build      # must pass clean
cargo test       # must pass — all payout tests exercise same logic through new abstractions
```

## Completion Checklist

- [ ] `PayoutScheme` trait defined in `payout/scheme/mod.rs`
- [ ] `PplnsScheme` implements `PayoutScheme` in `payout/scheme/pplns.rs`
- [ ] `TransactionBuilder` extracted in `payout/transaction.rs`
- [ ] `InternalSigner` uses `TransactionBuilder`
- [ ] `mod.rs` files updated for new modules
- [ ] `cargo test` passes
