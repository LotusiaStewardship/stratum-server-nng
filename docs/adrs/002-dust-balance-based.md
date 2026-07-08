# ADR 002: Dust Tracking Balance-Based (not FIFO Ledger)

**Context:** Payout  
**Date:** 2026-05-22  
**Status:** Accepted

## Decision

Dust (payout amounts below `min_payout_sat`) is tracked per-address as a single running balance in `dust_balances` table. Dust is added as bonus work weight proportional to `gross_reward` in the next payout calculation.

## Rationale

A FIFO dust ledger (tracking each individual sub-payout amount with its originating round) is more auditable but adds significant schema and computation complexity — every payout must create individual dust line items, and dust inclusion must match FIFO ordering. The balance-based approach is simpler to implement and sufficient for initial operations. The PPLNS window already socializes orphan risk across all participants, so the exact provenance of each dust satoshi is less critical than it would be under PPS.

## Considered Options

1. **Balance-based (chosen):** Single `balance` integer per `payout_address`. Updated additively. Dust weight proportional to gross_reward in next payout.
2. **FIFO ledger:** Each dust event tracked individually with round origin. Dust inclusion respects FIFO ordering. Full audit trail.

## Consequences

- Dust provenance is opaque — audit requires reconstructing the computation, not querying individual records.
- A future migration to FIFO-ledger is possible without breaking existing data (balance can serve as the starting point).
- The `dust_balances` table uses additive updates (`balance = balance + delta`), which is safe for concurrent access because writes are serialized through SQLite's single connection.
