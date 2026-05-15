# Stratum Mining Pool — Frequently Asked Questions

Welcome! This FAQ answers common questions about mining with the Lotus pool, the Stratum V1 protocol, payout methods, and how the pool works under the hood.

---

## Table of Contents

- [General](#general)
  - [What is this pool?](#what-is-this-pool)
  - [What is Lotus?](#what-is-lotus)
  - [Do I need to run a Lotus node to mine?](#do-i-need-to-run-a-lotus-node-to-mine)
- [Stratum V1 Protocol](#stratum-v1-protocol)
  - [What is Stratum V1?](#what-is-stratum-v1)
  - [How does a miner connect to the pool?](#how-does-a-miner-connect-to-the-pool)
  - [What is extranonce?](#what-is-extranonce)
  - [What does a mining.notify message contain?](#what-does-a-miningnotify-message-contain)
  - [What is a "share"?](#what-is-a-share)
- [Difficulty & VarDiff](#difficulty--vardiff)
  - [What is mining difficulty?](#what-is-mining-difficulty)
  - [What is VarDiff?](#what-is-vardiff)
  - [How does VarDiff adjust my difficulty?](#how-does-vardiff-adjust-my-difficulty)
  - [What difficulty should I start with?](#what-difficulty-should-i-start-with)
  - [Can I suggest a difficulty?](#can-i-suggest-a-difficulty)
- [Payout Methods](#payout-methods)
  - [What is PPLNS?](#what-is-pplns)
  - [How does PPLNS reward me?](#how-does-pplns-reward-me)
  - [What does "N" mean in PPLNS?](#what-does-n-mean-in-pplns)
  - [How does PPLNS compare to PPS?](#how-does-pplns-compare-to-pps)
  - [How does PPLNS compare to proportional payouts?](#how-does-pplns-compare-to-proportional-payouts)
  - [Does the pool charge a fee?](#does-the-pool-charge-a-fee)
  - [What is dust, and how does the pool handle it?](#what-is-dust-and-how-does-the-pool-handle-it)
- [Payouts](#payouts)
  - [When do I get paid?](#when-do-i-get-paid)
  - [Why is there a minimum payout threshold?](#why-is-there-a-minimum-payout-threshold)
  - [What does "matured" mean?](#what-does-matured-mean)
  - [How are payouts distributed?](#how-are-payouts-distributed)
- [Mining Setup](#mining-setup)
  - [What mining software should I use?](#what-mining-software-should-i-use)
  - [How do I configure my miner?](#how-do-i-configure-my-miner)
  - [What worker name format should I use?](#what-worker-name-format-should-i-use)
  - [What port should I connect to?](#what-port-should-i-connect-to)
- [Shares & Validation](#shares--validation)
  - [What is a stale share?](#what-is-a-stale-share)
  - [What is a duplicate share?](#what-is-a-duplicate-share)
  - [What is a low-difficulty share?](#what-is-a-low-difficulty-share)
  - [What is an orphaned block?](#what-is-an-orphaned-block)
  - [What causes rejected shares?](#what-causes-rejected-shares)
- [Advanced / Technical](#advanced--technical)
  - [How does the pool communicate with Lotus?](#how-does-the-pool-communicate-with-lotus)
  - [Does the pool support high availability (HA)?](#does-the-pool-support-high-availability-ha)
  - [What networks are supported?](#what-networks-are-supported)
  - [Where is accounting data stored?](#where-is-accounting-data-stored)
  - [Is there an API for monitoring?](#is-there-an-api-for-monitoring)

---

## General

### What is this pool?

This is a **production-grade Stratum V1 mining pool server** for the Lotus blockchain. It implements the PPLNS (Pay Per Last N Shares) reward system, variable difficulty (VarDiff) for miners, and automated payout processing via a built-in scheduler.

The pool connects to a Lotus daemon (`lotusd`) via **NNG (nanomsg-next-generation)** for mining template retrieval and block submission, and uses **SQLite** for accounting (tracking shares, workers, rounds, and payouts).

### What is Lotus?

Lotus is a blockchain based on the Bitcoin codebase with modifications. It uses the same proof-of-work mining fundamentals — miners compete to find valid blocks by performing hash computations. The pool described here is specifically designed for Lotus's consensus rules, including its block structure, coinbase transaction format, and address encoding.

### Do I need to run a Lotus node to mine?

**No.** You only need mining hardware (ASIC or CPU/GPU miner) and mining software that supports Stratum V1. The pool operator runs the Lotus node (`lotusd`) and this pool server. You simply point your miner to the pool's Stratum endpoint.

For GPU mining, the recommended software is **lotus-gpu-miner** (see [What mining software should I use?](#what-mining-software-should-i-use)).

---

## Stratum V1 Protocol

### What is Stratum V1?

**Stratum V1** is the dominant mining pool protocol, introduced in late 2012 as a replacement for the older `getwork` protocol. It uses **plain-text JSON messages over a persistent TCP connection** and is supported by virtually all mining software and hardware.

Key characteristics:
- **Persistent connection** — the miner maintains one open TCP socket to the pool.
- **JSON-RPC style** — messages are JSON objects with an `id`, `method`, and `params`.
- **Server-driven** — the pool pushes new work (`mining.notify`) to miners as blocks are found or mempool changes.
- **No formal BIP** — the protocol evolved through implementation rather than a formal standard, so minor variations exist between pools.

### How does a miner connect to the pool?

The connection follows a standard Stratum V1 handshake:

1. **Subscribe** — The miner sends `mining.subscribe` to register with the pool. The pool responds with a unique `extranonce1`, subscription IDs, and the expected `extranonce2` size.
2. **Authorize** — The miner sends `mining.authorize` with its worker name (e.g., `lotus_YourAddress.rig0`). The pool validates the worker name and begins tracking shares for that worker.
3. **Receive work** — The pool sends `mining.notify` with a new mining job (block header template, merkle branches, etc.).
4. **Submit shares** — The miner sends `mining.submit` with a candidate share (nonce, timestamp, etc.). The pool validates it and responds with accepted/rejected.
5. **Difficulty adjustment** — The pool may send `mining.set_difficulty` at any time to adjust the miner's required share difficulty.

### What is extranonce?

The **extranonce** is a pool-assigned unique value that ensures each miner searches a different portion of the nonce space, preventing miners from duplicating each other's work.

- **extranonce1** — A per-connection unique hex string assigned by the pool during subscription.
- **extranonce2** — A value chosen and incremented by the miner itself (typically 4 bytes).

Together with the standard nonce, these values create a unique search space for each miner. The pool combines `extranonce1 + extranonce2` into the coinbase transaction, which affects the merkle root and therefore the block header hash.

### What does a `mining.notify` message contain?

A `mining.notify` is the pool's way of sending new work to miners. It includes:

| Field | Description |
|-------|-------------|
| **Job ID** | Unique identifier; miners include this when submitting shares |
| **Previous block hash** | Used to construct the block header |
| **Coinbase part 1** | First portion of the coinbase transaction (before extranonce) |
| **Coinbase part 2** | Second portion of the coinbase transaction (after extranonce) |
| **Merkle branches** | Hashes used to build the merkle root |
| **Block version** | Version number for the block header |
| **nBits** | Encoded network difficulty target |
| **nTime** | Current time (miners may use time rolling) |
| **Clean jobs** | If `true`, miners should drop current work and start the new job immediately |

New `mining.notify` messages are sent whenever the network finds a new block, the mempool changes significantly, or the pool needs to update the difficulty.

### What is a "share"?

A **share** is a valid block header hash that meets the **pool's difficulty target** (which is lower than the network's difficulty target). It proves the miner performed work, even though the share itself may not meet the full network difficulty required to find a block.

- Shares are the **proof of work** miners submit to the pool.
- The pool uses shares to measure each miner's contribution and determine payout proportions.
- Shares are classified as **accepted**, **stale**, **duplicate**, **invalid**, or **low-difficulty** (see [Shares & Validation](#shares--validation)).

---

## Difficulty & VarDiff

### What is mining difficulty?

**Difficulty** is a measure of how hard it is to find a valid block hash. It's a number — higher means harder. The network adjusts difficulty periodically to maintain a consistent block time.

For mining pools, there are **two difficulty levels**:
- **Network difficulty** — The target set by the blockchain. A block must meet this to be valid on the network.
- **Share difficulty** — The (lower) target set by the pool. Shares meeting this difficulty are accepted as proof of work.

For example, if the network difficulty is 100,000, the pool might set a share difficulty of 1.0, meaning miners submit ~100,000 shares for every block found.

### What is VarDiff?

**VarDiff (Variable Difficulty)** is an automatic per-miner difficulty adjustment system. Instead of using a single fixed difficulty for all miners, VarDiff dynamically adjusts each miner's difficulty based on their observed **share submission rate**.

This pool uses **network-aware dynamic difficulty**:
1. The pool's baseline difficulty **automatically tracks the network difficulty** from `lotusd`.
2. VarDiff then **fine-tunes per-miner** from that baseline based on how fast shares arrive.

### How does VarDiff adjust my difficulty?

VarDiff works on a simple principle: **target a specific time between accepted shares**.

- The pool has a **target share interval** (default: 15 seconds).
- If your shares arrive **faster** than the target, VarDiff **increases** your difficulty (you're too fast).
- If your shares arrive **slower** than the target, VarDiff **decreases** your difficulty (you're too slow).
- Adjustments are **clamped** to ±50% per retarget to prevent wild swings.
- Retargeting happens every 90 seconds (configurable), giving the system enough data for stable decisions.

Your difficulty is also **capped** at the current network difficulty — it can never exceed that.

### What difficulty should I start with?

You don't need to choose! New miners start at **1% of the current network difficulty** and VarDiff ramps up based on your actual hashrate. This means:
- Small miners (CPU/mobile) get easy initial shares and ramp up quickly.
- Large miners (ASICs) get their difficulty increased rapidly to match their hashrate.

You can optionally suggest a difficulty using `mining.suggest_difficulty`, but the pool's VarDiff system will still make the final decision.

### Can I suggest a difficulty?

Yes. Stratum V1 supports `mining.suggest_difficulty`, where a miner can request a preferred share difficulty. However, this pool treats suggestions as **advisory only** — VarDiff makes the final call based on observed share rates.

---

## Payout Methods

### What is PPLNS?

**PPLNS** stands for **"Pay Per Last N Shares"**. It is the payout method used by this pool.

Under PPLNS, when the pool finds a block, the reward is distributed among miners who contributed shares within a **rolling window of work** (the "last N shares"). The window size is measured in **work units** (share difficulty × number of shares), not a fixed count.

Key properties:
- **No rounds** — PPLNS doesn't have discrete "rounds" like proportional pools. The window continuously rolls forward.
- **Pool-friendly** — Miners who stay connected long-term benefit more, as they always have shares in the window.
- **Anti-pool-hopping** — Unlike proportional systems, PPLNS discourages pool-hopping (jumping between pools based on luck).

### How does PPLNS reward me?

When the pool finds a block:
1. The pool looks back at the **PPLNS window** (configured as `N × the expected work for one block`).
2. All **accepted shares** within that window are collected, weighted by their difficulty.
3. Each miner receives a **proportional share** of the block reward based on their work units.

For example, if the window contains 10,000 work units total and you contributed 500, you receive **5%** of the net reward.

### What does "N" mean in PPLNS?

**N** is a multiplier that defines the size of the PPLNS window:

- **N = 1.0** → The window covers roughly the expected amount of work needed to find **one block**.
- **N > 1.0** → Larger window, more smoothing, slower responsiveness.
- **N < 1.0** → Smaller window, less smoothing, faster responsiveness.

This pool defaults to **N = 2.0** (industry standard), meaning the window spans approximately two blocks' worth of work. This provides better variance smoothing while remaining responsive to miner contributions.

### How does PPLNS compare to PPS?

| Feature | PPLNS | PPS (Pay Per Share) |
|---------|-------|---------------------|
| **Payout timing** | When pool finds a block | Immediately per share |
| **Pool risk** | Shared with miners | Borne by pool operator |
| **Fees** | Lower (1–3%) | Higher (2–5%+) |
| **Pool-hopping** | Discouraged | Encouraged |
| **Variance** | Higher for miners | None for miners |
| **Simplicity** | Moderate | Simple for miners |

**PPS** pays miners a fixed rate per accepted share regardless of whether the pool finds blocks. The pool operator absorbs the variance risk (and charges higher fees for it). **PPLNS** ties payouts to actual blocks found, meaning miners share in the pool's luck.

### How does PPLNS compare to proportional payouts?

**Proportional (Prop)** divides each block's reward among miners who submitted shares **during that round only** (from the previous block to the current one).

| Feature | PPLNS | Proportional |
|---------|-------|-------------|
| **Window** | Rolling (N blocks of work) | Discrete rounds |
| **Pool-hopping** | Discouraged | Exploitable |
| **Long-term miner bonus** | Yes — always in window | No — only active rounds |
| **Round orphaning** | Partially protected | Full loss if round is orphaned |

PPLNS is generally preferred by long-term miners because it provides more consistent income and protects against pool-hoppers.

### Does the pool charge a fee?

Yes. The pool charges a configurable fee in **basis points (bps)**. The default is **100 bps (1.00%)**.

- The fee is deducted from the **gross block reward** before miner payouts.
- If the fee is 1% and the block reward is 1,000,000 satoshis, the pool takes 10,000 satoshis and distributes 990,000 among miners.
- Fee collection can be **disabled** in the configuration.

### What is dust, and how does the pool handle it?

**Dust** refers to tiny payout amounts below the minimum payout threshold (default: **546 satoshis**, the standard Bitcoin/Lotus dust limit).

Instead of creating unspendable dust outputs on-chain, the pool:
1. **Tracks dust** per-address in a separate ledger.
2. **Carries forward** dust to the next payout — it accumulates until it reaches the minimum threshold.
3. When accumulated dust + new payout ≥ minimum, the combined amount is paid out.

This ensures no value is lost to dust while avoiding bloated transactions with tiny outputs.

---

## Payouts

### When do I get paid?

Payouts are processed on a **scheduled interval** (default: every **3,600 seconds / 1 hour**). The payout scheduler:
1. Checks for **matured blocks** (blocks old enough that their coinbase reward can be spent).
2. Calculates each miner's share of the reward using PPLNS.
3. Builds and broadcasts a payout transaction to the network.

If multiple mature blocks are waiting, the scheduler processes them one at a time (up to 10 per interval) to avoid overwhelming the node.

### Why is there a minimum payout threshold?

The minimum payout threshold (default: **546 satoshis**) prevents the creation of **dust outputs** — tiny transaction outputs that cost more in fees than they're worth to spend.

If your share of a block reward is below this threshold, the amount is stored as **dust** and carried forward to future payouts (see [What is dust?](#what-is-dust-and-how-does-the-pool-handle-it)).

### What does "matured" mean?

Per Bitcoin/Lotus consensus rules, **coinbase rewards cannot be spent until 100 blocks** have been mined after the block containing them. This is called **coinbase maturity**.

A block goes through this lifecycle:
1. **Found** — The pool submits a valid block to the network.
2. **Confirmed** — The block has at least one confirmation (another block built on top).
3. **Matured** — The block has ≥100 confirmations (configurable minimum, floored at 100). The coinbase reward can now be spent.
4. **Paid** — The payout scheduler has distributed the reward to miners.

If the block is **orphaned** (replaced by a competing chain), it is marked as orphaned and no payouts are made for it.

### How are payouts distributed?

Payouts are distributed via a **single transaction** that spends the block's coinbase output:
- **Input:** The coinbase transaction's payout output (vout[1] of the found block).
- **Outputs:** One output per eligible miner + one pool fee output.
- **Fee:** The transaction pays a network fee of ~2 sat/byte; this is deducted from the pool fee output.

Each miner's payout amount is calculated proportionally from their PPLNS weighted shares, with remainder satoshis distributed deterministically (largest fractional parts first).

---

## Mining Setup

### What mining software should I use?

The canonical mining software for the Lotus pool is **lotus-gpu-miner**, a GPU-optimized miner specifically designed for Lotus's proof-of-work algorithm.

- **Repository:** [`lotus-gpu-miner`](https://github.com/LotusiaStewardship/lotus-gpu-miner)
- **Features:**
  - GPU-accelerated mining (OpenCL for AMD/NVIDIA, Metal for Apple Silicon)
  - Native Stratum V1 support
  - Automatic difficulty adjustment
  - Cross-platform (Windows, macOS, Linux)
  - CLI and GUI interfaces

### How do I configure my miner?

#### For lotus-gpu-miner

**Configuration file** (`~/.lotus-miner/config.toml`):

```toml
mine_to_address = "lotus_16PSJNf1EDEfGvaYzaXJCJZrXH4pgiTo7kyW61iGi"
stratum_url = "pool.lotusia.org:3334"
stratum_worker_name = "rig01"
stratum_password = "x"
```

**Command-line**:

```bash
./lotus-miner-cli --stratum-url pool.lotusia.org:3334 --mine-to-address lotus_16PSJNf1EDEfGvaYzaXJCJZrXH4pgiTo7kyW61iGi --stratum-worker-name rig01 --stratum-password x
```

Note: The miner combines `mine_to_address` and `stratum_worker_name` automatically as `<address>.<worker_name>`.

#### For other Stratum V1 miners

Point your miner to the pool's endpoint:

```
URL:      <pool-ip>:<port>  (or stratum+tcp://<pool-ip>:<port>)
Username: <your-lotus-address>.<worker-name>
Password: x  (or leave blank)
```

For example:
```
URL:       pool.example.com:3334
Username:  lotus_16PSJNf1EDEfGvaYzaXJCJZrXH4pgiTo7kyW61iGi.rig01
Password:  x
```

### What worker name format should I use?

The pool expects worker names in the format:

```
<lotus-address>.<worker-suffix>
```

- **Lotus address** — Your payout address (e.g., `lotus_16PSJN...`).
- **Worker suffix** — A label for your miner (e.g., `rig01`, `asic-03`, `cpu`).

The dot separator is required. If no suffix is provided, the address alone is accepted.

### What port should I connect to?

Ports are network-specific:

| Network | Stratum Port | API Port |
|---------|-------------|----------|
| **Mainnet** | 3334 | 18080 |
| **Testnet** | 13334 | 18081 |
| **Regtest** | 23334 | 18082 |

These are the defaults — the pool operator may configure different ports.

---

## Shares & Validation

### What is a stale share?

A **stale share** is a share submitted for a mining job that is no longer active — typically because a new block was found (by the pool or another pool) and the pool has already moved on to a new job.

Stale shares are **not rewarded** because the work they represent is no longer relevant to the current chain. High stale rates can indicate:
- High network latency between your miner and the pool.
- The pool finding blocks frequently (a good problem!).

### What is a duplicate share?

A **duplicate share** is a share that has already been submitted and recorded. This can happen if:
- Your miner resends a share due to a connection hiccup.
- Two miners are misconfigured with the same credentials and submit the same work.

Duplicates are rejected to prevent double-counting.

### What is a low-difficulty share?

A **low-difficulty share** is a share whose hash does not meet the current difficulty assigned to your miner. The pool's VarDiff system sets your minimum difficulty, and shares below that threshold are rejected.

This usually means your miner is misconfigured or your difficulty setting hasn't been updated after a VarDiff adjustment.

### What is an orphaned block?

An **orphaned block** (more accurately, a **stale block**) is a valid block that was found but **did not become part of the longest chain**. This happens when another miner finds a block at nearly the same time, and the network adopts the competing block.

If the pool finds a block that later becomes orphaned:
- The block is marked as **orphaned** in the accounting database.
- No payouts are distributed for that block.
- Any work miners contributed toward that block was not rewarded (this risk is inherent in mining).

### What causes rejected shares?

Shares can be rejected for several reasons:

| Reason | Description |
|--------|-------------|
| **Stale** | The job was replaced by a newer block |
| **Duplicate** | The share was already submitted |
| **Invalid** | The share has structural errors (bad nonce, extranonce, etc.) |
| **Low-difficulty** | The share doesn't meet your assigned difficulty |
| **Not subscribed** | Miner didn't complete the subscribe handshake |
| **Unauthorized** | Miner didn't complete the authorize handshake |
| **Unauthorized worker** | The worker name format is invalid |

---

## Advanced / Technical

### How does the pool communicate with Lotus?

The pool uses **two independent channels** to communicate with `lotusd`, each serving distinct purposes:

#### NNG (nanomsg-next-generation) — Mining Templates & Real-Time Events

NNG handles the high-performance, low-latency mining path:

- **NNG RPC** (`nng_rpc_url`, e.g. `ipc://datadir/nngrpc.pipe`) — Used for:
  - **Fetching mining templates** (`get_mining_template`) — the pool requests block header templates, coinbase parts, merkle branches, and difficulty targets. This is the primary source of mining work.
  - **Fetching full blocks** (`get_block`) — used during payout processing to retrieve matured found blocks and extract the coinbase reward.

- **NNG Pub/Sub** (`nng_pub_url`, e.g. `ipc://datadir/nngpub.pipe`) — The pool subscribes to three topics from `lotusd`:
  - **`miningwrkchg`** (primary) — A purpose-built signal emitted when mining work is invalidated. Covers new block tips, chain reorgs, mempool changes, and manual invalidations. Each message includes a monotonically increasing `template_epoch` counter so the pool can detect missed events. On receiving this, the pool fetches a fresh mining template and broadcasts `mining.notify` to all connected miners.
  - **`blkconnected`** (accounting only) — Emitted when a new block is connected to the chain. Used to mark found blocks as matured and update the chain tip height.
  - **`blkdisconctd`** (accounting only) — Emitted when a block is disconnected (reorg). Used to mark found blocks as orphaned and adjust maturity tracking.
  
  NNG pub/sub messages use **FlatBuffers** serialization for efficiency. The pool intentionally does *not* subscribe to per-transaction mempool topics (`mempooltxadd`, `mempooltxrem`) because those are too granular — `miningwrkchg` already consolidates mempool changes into a single efficient signal.

#### JSON-RPC (HTTP) — Block Submission & Transaction Operations

JSON-RPC handles standard Bitcoin-style RPC operations over HTTP:

- **`submitblock`** — When a miner finds a valid block, the pool submits the full serialized block to `lotusd` via JSON-RPC `submitblock`. This uses BIP22-style responses (null = accepted, string = rejection reason).
- **`sendrawtransaction`** — Used by the payout scheduler to broadcast signed payout transactions to the network.
- **`getrawtransaction`** — Used to check transaction confirmation counts (to verify payout transactions have been confirmed).
- **`getblockcount`** — Used at startup to fetch the current chain tip height and during reconciliation to validate found blocks.

#### Why Two Channels?

NNG provides **low-latency, event-driven** mining template delivery with FlatBuffers serialization — critical for keeping miners fed with fresh work. JSON-RPC provides **standard RPC operations** that are simpler to implement for transaction-level tasks like block submission and payout broadcasting. Together they form a `BitcoindMiningAdapter` that composes NNG (templates, blocks, pub/sub) with JSON-RPC (submit, broadcast, confirmations).

### Does the pool support high availability (HA)?

Yes. The payout scheduler implements a **lease-based coordination mechanism** using SQLite:

- Only one instance can hold the **scheduler lease** at a time.
- If multiple instances are running, the one with the lease processes payouts.
- Leases are automatically renewed and expire if the holder goes offline.
- This prevents **double-spending** in multi-instance deployments.

The Stratum server itself can also run in multiple instances, though each would maintain its own set of connected miners.

### What networks are supported?

Three networks are supported, **auto-detected** from the `bitcoind_rpc.url` JSON-RPC port:

| Network | JSON-RPC Port | Stratum Port | Database Path |
|---------|--------------|-------------|---------------|
| **Mainnet** | 10604 | 3334 | `./dbs/mainnet/stratum-accounting.sqlite3` |
| **Testnet** | 11604 | 13334 | `./dbs/testnet/stratum-accounting.sqlite3` |
| **Regtest** | 12604 | 23334 | `./dbs/regtest/stratum-accounting.sqlite3` |

Network can be explicitly overridden in the config, but this is not recommended.

### Where is accounting data stored?

All accounting data is stored in a **SQLite database** (`stratum-accounting.sqlite3`). The database tracks:
- **Workers** — Payout addresses and worker suffixes.
- **Shares** — Accepted, rejected, and stale shares with difficulty weights.
- **Rounds** — Mining rounds (for PPLNS window management).
- **Found blocks** — Block lifecycle (confirmed → matured → paid, or orphaned).
- **Payout batches** — Transaction creation, signing, submission, and confirmation status.
- **Dust ledger** — Accumulated dust per address for carry-forward.
- **Scheduler lease** — HA coordination state.

### Is there an API for monitoring?

Yes. The pool exposes a **REST API** (default: `127.0.0.1:18080`) with the following endpoints:

| Endpoint | Auth | Description |
|----------|------|-------------|
| `GET /healthz` | No | Simple health check — returns `200 OK` |
| `GET /readyz` | No | Readiness check — verifies the database is accessible |
| `GET /status` | Yes | Full pool status: stats, found blocks, payout state |
| `GET /workers` | Yes | List of registered workers |
| `GET /workers/summary` | Yes | Worker accounting summary (shares, payouts) |
| `GET /rounds` | Yes | Recent mining rounds |
| `GET /shares` | Yes | Recent shares |
| `GET /shares/rejected-reasons` | Yes | Breakdown of rejection reasons |
| `GET /payouts` | Yes | Recent payout batches |
| `GET /health/payout-scheduler` | Yes | Payout scheduler health (lease status, confirmed blocks) |
| `GET /reconciliation/missing-found-blocks` | Yes | Data reconciliation details |

Authenticated endpoints require a `Bearer` token in the `Authorization` header (configured via `api_token` in the config file or `STRATUM_API_TOKEN` environment variable).

---

*This FAQ is maintained alongside the `stratum-server-nng` codebase. If you have additional questions, please consult the source code or contact the pool operator.*
