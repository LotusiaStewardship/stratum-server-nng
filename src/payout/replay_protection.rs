//! Replay protection: which sighash fork value a payout signature must use.
//!
//! Lotus replay protection activates at the First Chronicles upgrade. From the
//! block whose parent has a median time past (MTP) at or above the activation
//! time, the node validates every signature with the sighash fork value
//! `0xffdead` committed in the upper 24 bits of the 32-bit sighash type of the
//! BIP143 preimage (the signature itself still carries a single sighash byte).
//! A signature hashed with fork value 0 is refused from then on, and a
//! signature hashed with `0xffdead` is refused before.
//!
//! The node decides the rule for the next block from the current tip's MTP, so
//! the decision here is made from `getblockchaininfo.mediantime`, fetched right
//! before signing — never from the wall clock.

/// Sighash fork value required once replay protection is active.
///
/// The node derives it as `0xff0000 | (0 ^ 0xdead)` from a signature whose
/// sighash byte carries fork value 0.
pub const REPLAY_PROTECTION_FORK_VALUE: u32 = 0xff_dead;

/// Sighash fork value used before replay protection is active.
pub const DEFAULT_FORK_VALUE: u32 = 0;

/// Mainnet First Chronicles activation time (MTP), 2027-06-21 14:10:00 UTC.
pub const MAINNET_ACTIVATION_TIME: i64 = 1_813_587_000;

/// Testnet activation time: mainnet minus 21 days, 2027-05-31 14:10:00 UTC.
pub const TESTNET_ACTIVATION_TIME: i64 = MAINNET_ACTIVATION_TIME - 21 * 24 * 3600;

/// Regtest uses the mainnet activation time unless lotusd is started with
/// `-replayprotectionactivationtime`.
pub const REGTEST_ACTIVATION_TIME: i64 = MAINNET_ACTIVATION_TIME;

/// Payouts are held while the tip's MTP is within this many seconds of the
/// activation time (about 30 blocks). Near the boundary a reorg or a late
/// block can flip the rule between signing and mining, which would leave the
/// transaction unminable.
pub const BOUNDARY_HOLD_SECS: i64 = 3600;

/// Default activation time for a network name as used in `config.network`.
pub fn default_activation_time(network: &str) -> i64 {
    match network {
        "testnet" => TESTNET_ACTIVATION_TIME,
        "regtest" => REGTEST_ACTIVATION_TIME,
        _ => MAINNET_ACTIVATION_TIME,
    }
}

/// The fork value with which the node will verify signatures in the next
/// block, given the current tip's median time past.
pub fn fork_value_for_next_block(tip_median_time: i64, activation_time: i64) -> u32 {
    if tip_median_time >= activation_time {
        REPLAY_PROTECTION_FORK_VALUE
    } else {
        DEFAULT_FORK_VALUE
    }
}

/// The fork value of the other side of the activation.
pub fn other_fork_value(fork_value: u32) -> u32 {
    if fork_value == REPLAY_PROTECTION_FORK_VALUE {
        DEFAULT_FORK_VALUE
    } else {
        REPLAY_PROTECTION_FORK_VALUE
    }
}

/// Whether a broadcast error is a script verification failure, the refusal
/// a node gives for a signature hashed with the wrong fork value
/// (`mandatory-script-verify-flag-failed (...)` or
/// `non-mandatory-script-verify-flag (...)`).
pub fn is_script_verify_rejection(error: &str) -> bool {
    error.contains("script-verify-flag")
}

/// Signing is held because the chain tip is too close to the activation.
///
/// Returned by signers instead of broadcasting a transaction that the next
/// blocks may refuse. The batch stays `pending` and is retried on a later
/// block.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error(
    "payout held: chain tip median time {tip_median_time} is within {}s of the \
     replay-protection activation time {activation_time}",
    BOUNDARY_HOLD_SECS
)]
pub struct PayoutHeld {
    pub tip_median_time: i64,
    pub activation_time: i64,
}

/// Replay-protection settings for payout signing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReplayProtection {
    /// MTP at which replay protection activates.
    pub activation_time: i64,
}

impl ReplayProtection {
    pub fn new(activation_time: i64) -> Self {
        Self { activation_time }
    }

    /// Settings for a network name as used in `config.network`.
    pub fn for_network(network: &str) -> Self {
        Self::new(default_activation_time(network))
    }

    /// Decide the fork value for a signature made now, or hold.
    ///
    /// Returns [`PayoutHeld`] while `|tip_median_time - activation_time|` is
    /// at most [`BOUNDARY_HOLD_SECS`].
    pub fn fork_value_for_signing(&self, tip_median_time: i64) -> Result<u32, PayoutHeld> {
        if (tip_median_time - self.activation_time).abs() <= BOUNDARY_HOLD_SECS {
            return Err(PayoutHeld {
                tip_median_time,
                activation_time: self.activation_time,
            });
        }
        Ok(fork_value_for_next_block(
            tip_median_time,
            self.activation_time,
        ))
    }
}

impl Default for ReplayProtection {
    fn default() -> Self {
        Self::new(MAINNET_ACTIVATION_TIME)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const A: i64 = MAINNET_ACTIVATION_TIME;

    #[test]
    fn test_fork_value_is_zero_before_activation() {
        assert_eq!(fork_value_for_next_block(A - 1, A), DEFAULT_FORK_VALUE);
        assert_eq!(fork_value_for_next_block(0, A), DEFAULT_FORK_VALUE);
    }

    #[test]
    fn test_fork_value_is_ffdead_from_activation() {
        assert_eq!(fork_value_for_next_block(A, A), 0xffdead);
        assert_eq!(fork_value_for_next_block(A + 1, A), 0xffdead);
    }

    #[test]
    fn test_other_fork_value_swaps_sides() {
        assert_eq!(
            other_fork_value(DEFAULT_FORK_VALUE),
            REPLAY_PROTECTION_FORK_VALUE
        );
        assert_eq!(
            other_fork_value(REPLAY_PROTECTION_FORK_VALUE),
            DEFAULT_FORK_VALUE
        );
    }

    #[test]
    fn test_default_activation_time_per_network() {
        assert_eq!(default_activation_time("mainnet"), 1_813_587_000);
        assert_eq!(default_activation_time("testnet"), 1_811_772_600);
        assert_eq!(default_activation_time("regtest"), 1_813_587_000);
        assert_eq!(ReplayProtection::default().activation_time, 1_813_587_000);
    }

    #[test]
    fn test_signing_well_before_activation_uses_zero() {
        let rp = ReplayProtection::new(A);
        assert_eq!(rp.fork_value_for_signing(A - BOUNDARY_HOLD_SECS - 1), Ok(0));
    }

    #[test]
    fn test_signing_well_after_activation_uses_ffdead() {
        let rp = ReplayProtection::new(A);
        assert_eq!(
            rp.fork_value_for_signing(A + BOUNDARY_HOLD_SECS + 1),
            Ok(0xffdead)
        );
    }

    #[test]
    fn test_signing_is_held_within_the_boundary_window() {
        let rp = ReplayProtection::new(A);
        for mtp in [
            A - BOUNDARY_HOLD_SECS,
            A - 1,
            A,
            A + 1,
            A + BOUNDARY_HOLD_SECS,
        ] {
            assert_eq!(
                rp.fork_value_for_signing(mtp),
                Err(PayoutHeld {
                    tip_median_time: mtp,
                    activation_time: A,
                }),
                "mtp {} should be held",
                mtp
            );
        }
    }

    #[test]
    fn test_overridden_activation_time_moves_the_boundary() {
        let rp = ReplayProtection::new(1_000_000);
        assert_eq!(rp.fork_value_for_signing(1_000_000 - 3601), Ok(0));
        assert!(rp.fork_value_for_signing(1_000_000).is_err());
        assert_eq!(rp.fork_value_for_signing(1_000_000 + 3601), Ok(0xffdead));
    }

    #[test]
    fn test_script_verify_rejections_are_detected() {
        assert!(is_script_verify_rejection(
            "JSON-RPC error (code=-26, http=500): mandatory-script-verify-flag-failed \
             (Signature must be zero for failed CHECK(MULTI)SIG operation)"
        ));
        assert!(is_script_verify_rejection(
            "non-mandatory-script-verify-flag (Signature must be zero for failed CHECK(MULTI)SIG operation)"
        ));
        assert!(!is_script_verify_rejection(
            "JSON-RPC error (code=-26, http=500): insufficient priority"
        ));
        assert!(!is_script_verify_rejection("txn-already-known"));
    }

    #[test]
    fn test_payout_held_message_names_both_times() {
        let msg = PayoutHeld {
            tip_median_time: A - 10,
            activation_time: A,
        }
        .to_string();
        assert!(msg.contains("payout held"));
        assert!(msg.contains(&(A - 10).to_string()));
        assert!(msg.contains(&A.to_string()));
    }
}
