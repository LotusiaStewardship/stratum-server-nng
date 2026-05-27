# ADR 006: Canonical Type Alignment with Lotusd

**Context:** Node Integration, Accounting, Payout (cross-context)
**Date:** 2026-05-25
**Status:** Accepted

## Decision

Every domain field uses the Rust integer type that matches its canonical type in the lotusd FlatBuffers schema. Type conversions occur only at two infrastructure boundaries: SQLite reads (i64 → target type with debug_assert) and JSON API serialization (u64 → String for safe roundtrips). No conversions happen in the Rust domain layer.

Type map:

| Field | Canonical (lotusd FBS) | Domain (Rust) | API (JSON) |
|---|---|---|---|
| `template_id`, `template_epoch`, `curtime`, `coinbase_value` | `uint64` | `u64` | `String` |
| `height` | `int32` | `i32` | number |
| `fee`, `sigops`, CAmount, monetary amounts | `int64` / `CAmount = int64_t` | `i64` | number |
| `node_time` | `int64` | `i64` | number |

## Rationale

The stratum server is subordinate to lotusd and bitcoinsuite — it translates data between them. Every unnecessary type widening (i64 where the source says uint64) erases information about the data's origin and invites questions about which representation is authoritative. By matching the canonical type exactly at every domain layer, a reader can trace any field back to its source schema and know the conversion cost (none, in the domain). The SQLite boundary is exempted because SQLite has no unsigned integer type — the debug_assert guard substitutes for a database constraint SQLite cannot provide. The API boundary is exempted because JSON numbers lose precision for u64 values above 2^53.

The original codebase used i64 for most numeric fields (height, template_id, template_epoch, coinbase_value), which was a valid simplification but concealed which fields came from uint64 sources vs. int32 sources vs. CAmount sources. The refactored model makes these distinctions explicit at the type level, enforced by the compiler.

## Considered Options

1. **Canonical type alignment with infrastructure casts (chosen):** Every domain field uses the exact type from the lotusd FlatBuffers schema. i64 → u64 or i64 → i32 casts are explicit at the SQLite read boundary with debug_assert guards. API DTOs serialize u64 as String.

2. **Widen everything to i64 (previous approach):** Use i64 for all integer fields regardless of source canonical type. Simplifies SQLite interaction (no boundary casts needed) and reduces churn when types change. Loses the connection to the canonical source — a reader cannot tell whether a field was originally uint64, int32, or int64 without reading the NNG schema.

3. **Separate type aliases per schema:** Define `type TemplateId = u64`, `type Height = i32`, `type Amount = i64` etc. Provides the same type safety as Option 1 but with named types that carry semantic meaning. Not chosen because it increases type scaffolding without a clear payoff given that the canonical schema is not expected to change.

## Consequences

- Every SQLite read of a u64 or i32 field requires an explicit i64 → target type cast with a debug_assert. This adds ~3 lines per field at each read site. The pattern is uniform and reviewable.
- API consumers see u64 fields as JSON strings. Deserializing clients must parse these strings into their integer representation. This is an explicit API contract — u64 values that happen to fit in JSON's safe integer range (< 2^53) will still be strings for consistency.
- Adding a new field from the NNG schema requires checking its FBS type and using the corresponding Rust integer type. The cost of getting it wrong is a compilation error (mismatched types in struct construction) rather than a silent truncation.
- The CAmount convention (i64 for monetary values) is preserved as a special case — coinbase_value enters the system as u64 from the NNG raw template but converts to i64 at the boundary where it becomes `gross_reward` in the PayoutPlan. This single conversion point is documented with a safety invariant.
- ChainTip (chain tip height tracker) uses AtomicI32 to match lotusd's `int32_t` return from `ChainActive().Height()`, eliminating the unsafe `i32 as u64` cast that existed previously.

### 2026-05-26 update: debug_assert promoted to assert

All SQLite boundary `debug_assert!` guards were promoted to `assert!` (18 call sites across `accounting_event_repository.rs`, `found_block_repository.rs`, `stratum_protocol/server.rs`, and `block_builder.rs`). In release builds, `debug_assert!` is stripped — corrupt DB data (negative i64 in unsigned columns) would silently produce wrong u64 values. Since these represent unrecoverable data corruption, `assert!` is appropriate: crash with diagnostics rather than propagate garbage.
