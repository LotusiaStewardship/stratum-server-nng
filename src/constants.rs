//! Lotus-specific constants for monetary units, dust limits, and fee defaults.
//!
//! Lotus uses a different base unit than Bitcoin:
//!   1 XPI (Lotus) = 1,000,000 satoshis (base unit)
//!   (Bitcoin: 1 BTC = 100,000,000 satoshis)
//!
//! All constants are derived from lotusd's consensus rules (see amount.h).

/// Base unit conversion: satoshis (base unit) per XPI (Lotus).
/// Lotus defines 1 LOTUS = 1,000,000 SATOSHI.
pub const SATS_PER_XPI: i64 = 1_000_000;

/// Float version for runtime conversions involving f64 values.
pub const SATS_PER_XPI_F64: f64 = 1_000_000.0;

/// Minimum non-dust output value for P2PKH outputs.
/// Standard across Bitcoin-derived chains for P2PKH (34 byte script).
pub const DUST_LIMIT: i64 = 546;

/// Default transaction fee rate in satoshis per kilobyte.
/// 1000 sat/kB = 1 sat/vByte (standard relay minimum).
pub const DEFAULT_TX_FEE_PER_KB: i64 = 1000;

/// Default time window for calculating pool hashrate in `/api/v1/stats`
/// 1800 seconds = 30 minutes / 2-minute block time = 15 block window average
pub const DEFAULT_POOL_HASHRATE_WINDOW_SECS: i64 = 1800;

/// Chain fork id committed to in every sighash once replay protection is
/// active (lotusd `src/script/sighashtype.h`: `CHAIN_FORK_ID = 0x4c4f54`,
/// "LOT"). The pool's payout signer must append this value to the sighash
/// type (`SigHashType::with_fork_value`) when the node's median time past
/// has reached the replay-protection activation time, or lotusd rejects
/// the payout transaction.
pub const CHAIN_FORK_ID: u32 = 0x4c_4f_54;

/// Mainnet replay-protection activation time: the second Kings protocol
/// upgrade, 2026-12-21T20:50:00Z (lotusd `src/chainparams.cpp`
/// `consensus.secondKingsActivationTime`). When the node's
/// `getblockchaininfo.mediantime` reaches this timestamp, every signature
/// must commit to [`CHAIN_FORK_ID`].
pub const REPLAY_PROTECTION_ACTIVATION_MAINNET: i64 = 1_797_886_200;

/// Testnet replay-protection activation time: mainnet minus 21 days
/// (lotusd `src/chainparams.cpp`: `testnetActivationOffset = 21 * 24 * 3600`).
pub const REPLAY_PROTECTION_ACTIVATION_TESTNET: i64 =
    REPLAY_PROTECTION_ACTIVATION_MAINNET - 21 * 24 * 3600;

/// Extra leeway past the activation time before the stale-node guard fires.
///
/// If the pool's node still reports a fork-0 chain (`mediantime <
/// activation_time`) while the wall clock is more than this margin past the
/// activation time, the node is stale or misconfigured (e.g. wrong
/// `-replayprotectionactivationtime`). Signing fork 0 then would produce
/// payouts the activated network rejects (or worse, replayable ones), so the
/// batch is skipped instead. 3600 seconds = 1 hour.
pub const REPLAY_PROTECTION_SAFETY_MARGIN: i64 = 3_600;

/// Default replay-protection activation time for the pool's configured
/// network. Mirrors lotusd's chainparams: mainnet activates at
/// [`REPLAY_PROTECTION_ACTIVATION_MAINNET`], testnet 21 days earlier, and
/// regtest uses the mainnet time (lotusd's `CRegTestParams` copies the
/// mainnet activation; regtest runs activate early via the node's
/// `-replayprotectionactivationtime` override). Unknown networks fall back
/// to the mainnet value.
///
/// The pool's own node is the authority on when replay protection is active:
/// the activation time is only compared against the node's `mediantime`.
/// Set `pool.signing.replay_protection_activation_time` in config.toml when
/// the node runs with a custom `-replayprotectionactivationtime`.
pub fn default_replay_protection_activation_time(network: &str) -> i64 {
    match network {
        "testnet" => REPLAY_PROTECTION_ACTIVATION_TESTNET,
        _ => REPLAY_PROTECTION_ACTIVATION_MAINNET,
    }
}
