/// Stratum V1 protocol parameters and constants.
///
/// All runtime stratum parameters are defined here as a single source of truth.
/// To change a parameter, update it in one place and every consumer picks up the
/// new value.

/// Number of bytes for extranonce1 (assigned per-session by the pool).
/// Standard Stratum V1 value: 4 bytes (8 hex chars).
pub const EXTRANONCE_1_SIZE: u8 = 4;

/// Number of bytes for extranonce2 (chosen per-share by the miner).
/// Standard Stratum V1 value: 4 bytes (8 hex chars).
pub const EXTRANONCE_2_SIZE: u8 = 4;

/// Total extranonce bytes inserted between coinbase1 and coinbase2.
pub const EXTRANONCE_TOTAL_SIZE: u8 = EXTRANONCE_1_SIZE + EXTRANONCE_2_SIZE;

/// Number of hex characters for extranonce1 (bytes × 2).
pub const EXTRANONCE_1_HEX_CHARS: usize = (EXTRANONCE_1_SIZE as usize) * 2;

/// Maximum number of assigned jobs to track per session.
/// When this cap is reached, the oldest assigned job is evicted.
pub const MAX_ASSIGNED_JOBS_PER_SESSION: usize = 128;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_params_constants_have_expected_values() {
        assert_eq!(EXTRANONCE_1_SIZE, 4);
        assert_eq!(EXTRANONCE_2_SIZE, 4);
        assert_eq!(EXTRANONCE_TOTAL_SIZE, 8);
        assert_eq!(EXTRANONCE_1_HEX_CHARS, 8);
        assert_eq!(MAX_ASSIGNED_JOBS_PER_SESSION, 128);
    }
}
