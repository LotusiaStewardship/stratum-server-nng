# ADR 004: PPLNS-Only Payout Scheme (no PayoutScheme trait)

**Context:** Payout  
**Date:** 2026-05-22  
**Status:** Accepted

## Decision

PPLNS (Pay Per Last N Shares) is the only payout scheme. No `PayoutScheme` trait or alternative scheme implementation exists. The database schema is scheme-agnostic (`payout_batches`, `payouts`, `payout_share_snapshots` use generic field names) so a trait can be extracted later without migration.

## Rationale

PPLNS is the dominant payout scheme for pool mining because it aligns miner incentives with pool health — miners are rewarded for sustained participation and orphan risk is socialized via window dilution. PPS (Pay Per Share) requires the pool to carry variance risk and demands a much larger reserve. PROP (Proportional) has known gameability issues (pool hopping). Starting with a single scheme avoids premature abstraction and lets the payout data model prove itself before committing to a trait interface.

## Considered Options

1. **PPLNS-only, schema scheme-agnostic (chosen):** Implement PPLNS specifically. Design tables generically so switching schemes or extracting a trait is additive, not breaking.
2. **PayoutScheme trait from day one:** Abstract the scheme interface, implement PPLNS as one variant. Guarantees clean separation but adds indirection before the second consumer exists.
3. **PPLNS with scheme-specific schema:** Optimize tables for PPLNS window logic. Fastest to build but would require schema migration for a second scheme.

## Consequences

- Adding PPS or PROP requires extracting a `PayoutScheme` trait and implementing a second variant. The schema supports this without migration.
- No runtime scheme selection or configuration. The payout method is fixed at compile time.
- The PPLNS window (cumulative difficulty threshold via `n_multiplier × N_diff`) is the only payout algorithm. Configuration is limited to `n_multiplier`, `min_payout_sat`, and fee parameters.
