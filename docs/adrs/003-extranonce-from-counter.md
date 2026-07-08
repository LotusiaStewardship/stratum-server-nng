# ADR 003: Extranonce1 from Session Counter (not Random)

**Context:** Stratum Core  
**Date:** 2026-05-22  
**Status:** Accepted

## Decision

Each session's `extranonce1` is derived from a monotonically increasing `session_counter` (modulo `2^(EXTRANONCE_1_SIZE * 8)`), formatted as hex. The counter starts at 0 on server startup.

## Rationale

Per UBQ, extranonce1 must be globally unique across active sessions. Random generation would require collision checking against all active sessions on every new connection, adding latency and complexity. A counter-based approach guarantees uniqueness without any collision detection — the counter is monotonically increasing and wraps at a value that far exceeds practical connection counts (4 bytes → 4B unique values per server lifetime). The wrap is safe because old sessions with wrapped extranonce1 values will have disconnected long before the counter wraps.

## Considered Options

1. **Counter-based (chosen):** Monotonic counter, mask to `EXTRANONCE_1_SIZE` bytes, format as hex. O(1) allocation, no collision checks.
2. **Random without collision check:** Generate random bytes. Collision probability is negligible at low connection counts but grows with session churn.
3. **Random with collision check:** Generate random bytes, verify uniqueness against active session map. Bounded but adds latency per connection.

## Consequences

- extranonce1 values are sequential and predictable, which reveals session count. This is acceptable for Stratum V1 — extranonce1 is not a security boundary.
- On server restart, the counter resets to 0. This is safe because there are no active sessions from the previous lifetime.
- The wrap mask and hex width are derived from `params::EXTRANONCE_1_SIZE`, so changing that constant automatically adjusts extranonce1 encoding.
