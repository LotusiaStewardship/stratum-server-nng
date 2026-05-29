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