# ADR 001: VarDiff Per-Session (not Per-Worker)

**Context:** Stratum Core  
**Date:** 2026-05-22  
**Status:** Accepted

## Decision

VarDiff operates per TCP connection (session), not per worker identity. A miner with multiple rigs on one connection shares a single VarDiff instance; a miner with one rig per connection gets independent VarDiff per rig.

## Rationale

Per-worker VarDiff would require tracking worker identity across share submissions and maintaining separate difficulty state per worker within a single TCP stream. This adds complexity to the session state machine and creates ambiguity when a worker is not yet authorized. Per-session VarDiff matches the natural lifecycle boundary (TCP connection) and simplifies the state model: one `VarDiff` instance per `SessionState`, initialized at connection time, retargeted on share rate.

## Considered Options

1. **Per-session (chosen):** One VarDiff per TCP connection. Simple state model, matches connection lifecycle.
2. **Per-worker:** Track worker identity on each share, maintain separate difficulty per worker within a session. More precise tuning for shared connections, but adds authorization-dependent complexity and edge cases (worker changes mid-session).

## Consequences

- A miner running multiple rigs on one connection gets a single difficulty target for all rigs, which may not be optimal for heterogeneous hardware.
- For per-rig difficulty tuning, operators should configure miners to use separate connections per rig.
- Session-level granularity is the industry convention for Stratum V1 pools.
