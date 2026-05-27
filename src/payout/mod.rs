pub mod handler;
pub mod plan;
pub mod pplns;
pub mod signer;

/// Events delivered from the Node Integration context to the Payout context
/// through the maturation channel.
///
/// - `BlockMatured(hash)`: a pool block reached maturity depth — create
///   a payout batch for it.
/// - `BlockConnected`: a new block arrived on the chain — retry any
///   pending (failed) payout submissions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PayoutEvent {
    BlockMatured(String),
    BlockConnected,
}

/// Given the total coinbase value (subsidy + fees), compute the pool's
/// actual spendable reward after minerfund deduction.
///
/// Lotusd computes: minerfund = coinbase_value / 2 (integer truncation),
/// then deducts it from the miner output. The miner keeps the remainder
/// satoshi when coinbase_value is odd.
pub fn reward_from_coinbase(coinbase_value: u64) -> i64 {
    let minerfund = coinbase_value / 2;
    (coinbase_value - minerfund) as i64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_reward_from_coinbase_even_value() {
        // Lotus minerfund = total / 2, miner_reward = total - minerfund
        // For even values, reward is exactly half.
        assert_eq!(reward_from_coinbase(10000), 5000);
    }

    #[test]
    fn test_reward_from_coinbase_odd_value_miner_gets_remainder() {
        // minerfund = 9 / 2 = 4 (integer truncation)
        // miner_reward = 9 - 4 = 5  (miner gets the remainder satoshi)
        assert_eq!(reward_from_coinbase(9), 5);
    }

    #[test]
    fn test_reward_from_coinbase_zero() {
        assert_eq!(reward_from_coinbase(0), 0);
    }

    #[test]
    fn test_reward_from_coinbase_one_sat() {
        // minerfund = 1 / 2 = 0 (integer truncation)
        // miner_reward = 1 - 0 = 1
        assert_eq!(reward_from_coinbase(1), 1);
    }

    #[test]
    fn test_reward_from_coinbase_three_sat() {
        // minerfund = 3 / 2 = 1, miner_reward = 3 - 1 = 2
        assert_eq!(reward_from_coinbase(3), 2);
    }

    #[test]
    fn test_reward_from_coinbase_large_value() {
        // ~399.49 BTC in satoshis — realistic coinbase value
        let coinbase = 39_949_002_900u64;
        let reward = reward_from_coinbase(coinbase);
        // miner_reward = total - total/2 = ceil(total/2)
        // 39949002900 / 2 = 19974501450
        // 39949002900 - 19974501450 = 19974501450
        // For even values, reward == total/2
        assert_eq!(reward, 19_974_501_450);
    }

    #[test]
    fn test_reward_from_coinbase_large_odd_value() {
        let coinbase = 39_949_002_901u64;
        let reward = reward_from_coinbase(coinbase);
        // minerfund = 39949002901 / 2 = 19974501450
        // miner_reward = 39949002901 - 19974501450 = 19974501451
        assert_eq!(reward, 19_974_501_451);
    }

    #[test]
    fn test_reward_never_exceeds_coinbase_value() {
        // Invariant: reward_from_coinbase(x) <= x as i64 for all x
        for val in [0u64, 1, 2, 3, 100, 999, 1_000_000, 39_949_002_900] {
            let reward = reward_from_coinbase(val);
            assert!(
                reward <= val as i64,
                "reward {} exceeds coinbase {} for val {}",
                reward,
                val,
                val
            );
        }
    }
}
