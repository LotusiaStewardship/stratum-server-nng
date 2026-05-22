# Project Constitution

**Last updated:** 2026-05-22

This document governs all development work in `stratum-server-nng`. Every agent and contributor must follow its rules. If a rule here conflicts with a skill's instructions, this document takes precedence.

---

## Table of Contents

1. [Documentation](#1-documentation)
2. [Testing & Quality](#2-testing--quality)
3. [Implementation Workflow](#3-implementation-workflow)
4. [Skill Activation Guide](#4-skill-activation-guide)
5. [Code Standards](#5-code-standards)
6. [Review Gates](#6-review-gates)

---

## 1. Documentation

### 1.1 The rule

Every implementation plan MUST include a **"Documentation Updates"** subsection. Documentation is a first-class deliverable, not an afterthought.

### 1.2 Update matrix

| If you modified... | You MUST update |
|---|---|
| A table, column, constraint, or index (in `schema.rs`) | `docs/SCHEMA.md` |
| A reject reason, validation rule, protocol message, or domain term | `docs/UBIQUITOUS_LANGUAGE.md` |
| A bounded context's boundary, ownership, dependencies, or invariants | That context's `CONTEXT.md` in `docs/contexts/<name>/` |
| The relationship between bounded contexts or added/removed a context | `docs/CONTEXT_MAP.md` |
| An architectural decision meeting all 3 ADR criteria (hard to reverse, surprising, trade-off) | Create an ADR in `docs/adrs/<nnn>-slug.md` |
| Slice acceptance criteria (implemented a spec item) | Mark `[x]` in `docs/contexts/stratum-core/specs/*.md` |
| The configuration schema or added a config field | `config.example.toml` and `docs/contexts/stratum-core/CONTEXT.md` (dependencies table) |

### 1.3 What triggers an ADR

Create an ADR ONLY when ALL THREE conditions are met:

1. **Hard to reverse** — the cost of changing your mind later is meaningful
2. **Surprising without context** — a future reader will wonder "why did they do it this way?"
3. **Result of a real trade-off** — there were genuine alternatives and you picked one for specific reasons

Format: Title + context + decision + considered options + consequences. Keep it minimal.

### 1.4 Timing

Documentation MUST be updated in the same session as the code change. Batching doc updates is not allowed. If the implementation plan spans multiple sessions, documentation todos must be interleaved with code todos, not deferred to the end.

### 1.5 Fallback

If you're unsure whether a change requires documentation updates, add a "Documentation Updates" subsection to your plan listing all potentially affected files and let the reviewer decide.

---

## 2. Testing & Quality

### 2.1 Test philosophy (from `/tdd` skill)

Tests describe WHAT the system does, not HOW it does it. Good tests exercise real code paths through public interfaces and survive refactors. Bad tests mock internal collaborators or verify implementation details.

### 2.2 Rules

1. **Test through public interfaces.** Never mock your own modules. Mock only at system boundaries (external APIs, time, randomness).
2. **Test-first for core logic and domain rules.** Implementation-first is acceptable for glue code and UI, but tests must follow immediately.
3. **Every new behavior gets at minimum one test.** If you're adding a validation rule, write a test for it. If you're adding a repository method, write a test for it.
4. **No nondeterministic tests.** No `sleep()`, no race-via-timing. For async behavior, subscribe to the exact event or state change before triggering the action. Unless time itself is the behavior under test, fixed sleeps and polling delays are forbidden.
5. **Run `cargo test` before marking work complete.** All tests must pass. Pre-existing failures must be noted explicitly — do not suppress or delete failing tests.

### 2.3 What to test

| Layer | Test approach |
|-------|--------------|
| Validation logic (difficulty, format, staleness) | Test-first, each rule independently |
| Repository CRUD (insert, query, upsert) | Test-first with in-memory SQLite |
| Protocol parsing (valid/invalid requests) | Test-first, edge cases |
| VarDiff retarget math | Test-first, deterministic |
| PPLNS window calculation | Test-first, deterministic with known data |
| HTTP API routes | Implementation-first, test after for each handler |
| Glue code, wiring, configuration | Test only if logic is non-trivial |

### 2.4 Integration tests

Integration tests must:
- Start a real TCP server on a random port
- Connect with a real TCP client
- Exercise the full subscribe → authorize → submit flow
- Verify persistence in the database

The existing integration tests in `server.rs` serve as templates. Follow their patterns.

---

## 3. Implementation Workflow

Every implementation follows the **EXPLORE → DEFINE → PLAN → TODO → EXECUTE** workflow. Documentation is built into every phase.

### Phase 1: EXPLORE

Before touching any code:
1. Read `CLAUDE.md` and this constitution
2. Read relevant source files — never assume you know what's there
3. Read relevant documentation (`contexts/*/CONTEXT.md`, `specs/*.md`, `SCHEMA.md`)
4. Read existing tests to understand patterns
5. Check `docs/CONSTITUTION.md §1.2` to identify which docs might need updating

### Phase 2: DEFINE

State explicitly:
- **WHAT** is the final deliverable?
- **WHY** does this exist? (infer from context)
- **SUCCESS CRITERIA** — how will we know it's done?

### Phase 3: PLAN

Present the plan to the user. The plan MUST include:

1. **Technical approach** — steps in order, why each matters
2. **Documentation Updates** — every document that will be created or modified (reference §1.2)
3. **Verification** — how you'll confirm it works

### Phase 4: TODO

Create atomic todos in the project's todo system. Each todo must encode WHERE, WHY, HOW, and EXPECTED RESULT. Documentation todos MUST be interleaved with code todos — do not batch them at the end.

### Phase 5: EXECUTE

Work through todos. Mark completion immediately after each. Run through [Review Gates](#6-review-gates) before declaring done.

---

## 4. Skill Activation Guide

The agent has seven skills (sourced from [github.com/AgentiveStack/skills](https://github.com/AgentiveStack/skills)). Use this decision tree to choose which to invoke:

| When you're... | Invoke this skill | What it produces |
|---|---|---|
| Starting a new feature, enhancement, or significant change | `<skill name="spec">` | Structured feature spec (PRD) grounded in domain model |
| Breaking a spec into independently-implementable pieces | `<skill name="slice">` | Ordered task list with dependency graph |
| Implementing a slice, fixing a bug, or writing any code that needs tests | `<skill name="tdd">` | Tested implementation via RED-GREEN-REFACTOR loop |
| Designing module interfaces or untangling messy code | `<skill name="architect">` | Interface analysis with alternative designs |
| Stress-testing a plan against domain terminology and invariants | `<skill name="domain">` | Updated CONTEXT.md, UBIQUITOUS_LANGUAGE.md, ADRs |
| Needing a system-level view — understanding blast radius, data flow, or an unfamiliar module | `<skill name="holistic">` | Multi-layer context map (domain position, data flow, cross-context impact) |
| QA testing, reporting bugs, or filing issues from observations | `<skill name="qa">` | Durable GitHub issues in domain language |

### Cross-cutting: documentation

Every skill updates documentation inline as it works. If a skill produces a decision that meets the ADR criteria, it creates the ADR. If it touches a bounded context, it updates the CONTEXT.md. This is not optional — it is part of the skill's contract.

---

## 5. Code Standards

### 5.1 Module architecture

The codebase follows a bounded-context module structure defined in `docs/CONTEXT_MAP.md`:

```
src/
├── stratum_protocol/       # Stratum Core: TCP server, sessions, protocol parsing
├── share_processing/       # Stratum Core: validation, VarDiff
├── accounting/             # Accounting: schema, repositories, service facade
├── http_api/               # HTTP API: routes, auth, server
├── node_integration/       # Node Integration: NNG, JSON-RPC, block building
├── payout/                 # Payout: PPLNS, plan construction
├── shutdown/               # Cross-cutting: graceful shutdown coordinator
├── config.rs               # Configuration loading
├── main.rs                 # Entry point, wiring, startup orchestration
└── lib.rs                  # Public module exports
```

Each module has clear ownership boundaries documented in its `CONTEXT.md`. Cross-module dependencies should go through the context boundaries defined in `CONTEXT_MAP.md`, not directly between arbitrary modules.

### 5.2 Database patterns

- **Connection:** Single `rusqlite::Connection` wrapped in `Arc<Mutex<Connection>>` (see ADR 005). No connection pools.
- **Writes:** Serialized through the `Mutex`. No concurrent writes.
- **Schema:** All CREATE TABLE statements in `accounting/schema.rs`. `init_schema()` must be idempotent.
- **Migrations:** Not yet needed — schema evolves via idempotent CREATE TABLE IF NOT EXISTS. Add columns via ALTER TABLE IF NOT EXISTS.
- **Dedupe key:** Format is `worker_id:template_id:template_epoch:extranonce2:ntime:nonce`. Used with INSERT OR IGNORE.
- **WAL checkpoint:** Performed on graceful shutdown via `ShutdownCoordinator`.

### 5.3 Error handling

- Application boundaries (main.rs, route handlers): `anyhow::Result`
- Domain errors (validation, business logic): `thiserror` derive enum (see `StratumError` in `protocol.rs`)
- Internal errors (non-recoverable): `anyhow::bail!` or `anyhow::anyhow!`
- Warnings (recoverable): `tracing::warn!` — do not crash, do not silently swallow

### 5.4 Session state patterns

- Sessions are per-TCP-connection. `SessionState` holds: extranonce1, authorized workers, assigned_jobs map, VarDiff instance.
- `assigned_jobs` is capped at `MAX_ASSIGNED_JOBS_PER_SESSION` (128) with FIFO eviction.
- `clean_jobs=true` clears ALL assigned jobs immediately.
- Extranonce1 is derived from a monotonic counter (see ADR 003).
- P_diff is captured in `AssignedJob` at notify dispatch time — immutable on shares.

### 5.5 Share lifecycle

```
mining.submit → parse params → validate (authorization, format, staleness, ntime, difficulty)
    → ValidationResult { accepted, reject_reason, low_diff_ok, network_target_ok, block_hash }
    → INSERT share (immutable) + INSERT share_outcome (atomically via transaction)
    → respond to miner (true/false)
    → if network_target_ok: build_submit_block → submitblock JSON-RPC
```

All shares are persisted regardless of acceptance status. Shares are never updated or deleted.

### 5.6 Naming conventions

- Test names describe behavior in domain language: `test_rejected_share_unauthorized_persists_outcome`
- Variables and types use the canonical term from `docs/UBIQUITOUS_LANGUAGE.md`
- Job ID format: `job-{template_id}-{epoch}` (per UBQ)
- SQL table names: snake_case, plural
- Rust types: PascalCase, descriptive

### 5.7 Configuration

- Config structs in `config.rs` with `#[serde(default)]` for optional fields
- Config loaded from `config.toml` with env var overrides (see `STRATUM_API_TOKEN`, `NNG_PUB_URL`, `BITCOIND_RPC_*`)
- Failing early on missing critical config (e.g., `mining_identity.payout_address` prevents startup with a clear error)

---

## 6. Review Gates

Before marking any work complete, verify:

### Completeness

- [ ] All todos in the task list are marked complete
- [ ] No remaining `TODO`, `FIXME`, `HACK`, or `XXX` comments were introduced (existing ones are noted)
- [ ] The implementation plan's "Documentation Updates" subsection is fully executed

### Correctness

- [ ] `cargo test` passes (all tests, not just the ones you wrote)
- [ ] `cargo check` produces no errors or warnings
- [ ] If you changed schema or data access: integration test covers the new path
- [ ] If you changed validation: unit tests cover all rejection reasons
- [ ] If you changed protocol handling: integration test covers the flow

### Documentation

- [ ] `docs/SCHEMA.md` matches the actual database schema (if schema changed)
- [ ] `docs/UBIQUITOUS_LANGUAGE.md` is accurate (if terms changed)
- [ ] Per-context `CONTEXT.md` files are accurate (if boundaries or invariants changed)
- [ ] `docs/CONTEXT_MAP.md` is accurate (if module relationships changed)
- [ ] Spec acceptance criteria are marked `[x]` (if slice items were completed)
- [ ] ADR was created if the new decision meets all 3 criteria

### No regressions

- [ ] Pre-existing tests were not deleted or modified to pass
- [ ] No new `#[ignore]` or `#[should_panic]` attributes were added without justification
- [ ] No silent error suppression (`.ok()`, `.unwrap_or_default()` without deliberate reason)
