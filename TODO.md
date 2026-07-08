- [X] (complete-refactor:edaae56) implement window for vardiff calculations (e.g. track 100 shares and/or 300s, calculate diff based on worker diff over time compared to network diff, etc.)
  - For example, should the target be 20s or an *average of 20s over X number of seconds*?
- [X] (release-readiness:2026-05-26) All debug_assert! promoted to assert! (18 sites); debug!() calls gated behind config.debug; domain-tagged logging via src/logging.rs; dynamic log level via EnvFilter + config.debug
- [ ] Add JSON log format option (config.log_format = "json") for log aggregator compatibility
- [X] (complete-refactor:3739f06) Payouts: instead of 1h interval, we can check maturation for all immature blocks on each `BlockConnected`. we're tracking latest tip height (per payout/specs/maturation-check.md). we can check on `BlockConnected` for
  1. immature blocks that are now matured
  2. for all matured blocks, send event to payout handler with relevant block data
  3. payout scheduler accepts this event, parses it, then decides how to proceed
  4. timer architecture completely eliminated; miners begin receiving consistent streams of income vs batches of income every X minutes/hours/etc.
- [X] (complete-refactor:1e7ae30) send workers reason for `miningwrkchg` along with new `MiningJob`
- [ ] To expand to different payout schemes, we will need to 
  1. host payout schemes on different TCP ports
  2. differentiate between blocks found for each payout scheme in the db schema (e.g. add `mined_for_scheme` column?)
- [X] (startup-reconciliation:2026-05-31) payout: reconcile `submitted` payouts with blockchain state
  - example scenario:
    1. pool submits payout; marked as `submitted`
    2. pool goes down
    3. payout tx confirmed
    4. pool starts up
    5. payout remains `submitted` and not moved to `confirmed`
  - solution: `AccountingService::reconcile_submitted_payouts()` called at startup
    after block reconciliation, before maturation check. Uses JSON-RPC
    `getrawtransaction` (already available) to check tx confirmation status.
    Confirmed tx → batch → `confirmed`, found_block → `paid`.
    Mempool / not-found / RPC-error → leave as `submitted` (safe default).
- [ ] On macbook, when laptop sleeps and awakes, Ctrl+C initiates but doesn't complete; shutdown persists forever and timeouts don't trigger
- [ ] Make sure mainnet, testnet, and regtest databases are PROPERLY SEPARATED
  - Tested by switching from mainnet to testnet and back, and it appears the same database is being used for BOTH NETWORKS