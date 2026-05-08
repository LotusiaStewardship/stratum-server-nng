pub mod scheduler;
pub mod scheduler_lease;

use std::collections::HashMap;

use anyhow::Result;

/// Represents a complete payout plan for distributing block rewards.
/// 
/// Contains the final outputs to be paid, dust amounts below minimum payout,
/// and accounting information for the reward distribution.
#[derive(Debug, Clone)]
pub struct PayoutPlan {
    /// List of (address, amount) pairs that will be included in the payout transaction.
    /// Only includes outputs >= min_payout_sat threshold.
    pub outputs: Vec<(String, i64)>,
    /// List of (address, amount) pairs that are below the minimum payout threshold.
    /// These are tracked but not paid out until they accumulate sufficiently.
    pub dust: Vec<(String, i64)>,
    /// Total block reward in satoshis before any fees are deducted.
    pub gross_reward_sat: i64,
    /// Pool fee amount in satoshis (calculated as fee_bps of gross_reward).
    pub fee_sat: i64,
    /// Net reward available to miners after fee deduction (gross_reward - fee).
    pub net_reward_sat: i64,
}

/// Trait for abstracting the transaction signing and submission mechanism.
/// 
/// Allows the payout system to support different signing modes (e.g., internal,
/// external HSM, multisig) without coupling to specific implementation details.
pub trait OptionalSigner: Send + Sync {
    /// Sign the payout transaction and broadcast it to the network.
    /// 
    /// # Arguments
    /// * `plan` - The payout plan containing outputs to be paid
    /// 
    /// # Returns
    /// The transaction ID (txid) of the submitted transaction as a hex string.
    fn sign_and_submit(&self, _plan: &PayoutPlan) -> Result<String>;
}

/// Represents a miner's share of work in the PPLNS window.
/// 
/// Used to calculate proportional payout amounts based on contributed work.
#[derive(Debug, Clone)]
pub struct WeightedShare {
    /// The miner's payout address (Lotus address string).
    pub payout_address: String,
    /// Amount of work units contributed by this miner in the PPLNS window.
    /// Higher work_units = larger share of the reward.
    pub work_units: f64,
}

/// Calculate the pool fee from a gross reward amount.
/// 
/// Uses 128-bit intermediate arithmetic to prevent overflow during multiplication.
/// 
/// # Arguments
/// * `gross_reward_sat` - Total reward in satoshis before fee deduction
/// * `fee_bps` - Fee in basis points (1/100th of a percent). E.g., 100 = 1%
/// 
/// # Returns
/// Fee amount in satoshis, truncated to nearest whole satoshi.
pub fn compute_fee(gross_reward_sat: i64, fee_bps: u32) -> i64 {
    ((gross_reward_sat as i128 * fee_bps as i128) / 10_000) as i64
}

/// Build a PPLNS (Pay Per Last N Shares) payout plan for a found block.
/// 
/// Distributes the net reward (after fees) proportionally among miners based on
/// their work units in the PPLNS window. Handles fractional satoshi remainders
/// deterministically by distributing them to miners with largest fractional parts.
/// 
/// Includes accumulated dust from previous payouts (dust carry-forward).
/// 
/// # Arguments
/// * `gross_reward_sat` - Total block reward in satoshis
/// * `fee_bps` - Pool fee in basis points (e.g., 100 = 1%)
/// * `fee_address` - Optional address to receive the pool fee
/// * `shares` - List of weighted shares from miners in the PPLNS window
/// * `min_payout_sat` - Minimum payout threshold; amounts below this become dust
/// * `dust_by_address` - Map of address → accumulated dust from previous payouts
/// 
/// # Returns
/// A `PayoutPlan` containing outputs, dust, and accounting information.
/// 
/// # Edge Cases
/// - If total_work <= 0 or net_reward <= 0, returns plan with only fee output (if any)
/// - Remainder satoshis from floor division are distributed deterministically
/// - Dust outputs are tracked separately for future accumulation
pub fn build_pplns_payout_plan(
    gross_reward_sat: i64,
    fee_bps: u32,
    fee_address: Option<&str>,
    shares: &[WeightedShare],
    min_payout_sat: i64,
    dust_by_address: &HashMap<String, i64>,
) -> PayoutPlan {
    // Calculate fee and net reward available to miners
    let fee_sat = compute_fee(gross_reward_sat, fee_bps);
    let net_reward_sat = gross_reward_sat - fee_sat;

    // Aggregate work units by payout address (multiple shares may map to same address)
    let mut work_by_address: HashMap<String, f64> = HashMap::new();
    for s in shares {
        *work_by_address
            .entry(s.payout_address.clone())
            .or_insert(0.0) += s.work_units;
    }

    // Calculate total work across all miners
    let total_work: f64 = work_by_address.values().sum();
    // Edge case: no valid work or no reward to distribute - return early with fee-only output
    if total_work <= 0.0 || net_reward_sat <= 0 {
        return PayoutPlan {
            outputs: fee_address
                .filter(|_| fee_sat > 0)
                .map(|addr| vec![(addr.to_string(), fee_sat)])
                .unwrap_or_default(),
            dust: Vec::new(),
            gross_reward_sat,
            fee_sat,
            net_reward_sat,
        };
    }

    // Calculate each miner's raw payout: floor(amount) + fractional remainder
    // Store (address, floored_amount, fractional_part) for remainder distribution
    // Include accumulated dust from previous payouts (dust carry-forward)
    let mut staged: Vec<(String, i64, f64)> = work_by_address
        .into_iter()
        .map(|(addr, w)| {
            let raw = (net_reward_sat as f64) * (w / total_work);
            let floor_amount = raw.floor() as i64;
            // Add accumulated dust from previous payouts
            let dust_amount = dust_by_address.get(&addr).copied().unwrap_or(0);
            (addr, floor_amount + dust_amount, raw.fract())
        })
        .collect();
    // Sort by fractional part descending (largest fractions get remainder first),
    // then by address ascending for deterministic tie-breaking
    staged.sort_by(|a, b| {
        b.2.partial_cmp(&a.2)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.0.cmp(&b.0))
    });

    // Calculate total satoshis distributed by floor division
    let paid_floor: i64 = staged.iter().map(|(_, v, _)| *v).sum();
    // Remainder = satoshis lost to floor division; distribute 1 sat each to miners
    // with largest fractional parts until remainder is exhausted
    let mut remainder = net_reward_sat - paid_floor;
    for item in staged.iter_mut() {
        if remainder <= 0 {
            break;
        }
        item.1 += 1;
        remainder -= 1;
    }

    // Separate outputs into payable amounts and dust (below minimum threshold)
    let mut outputs: Vec<(String, i64)> = Vec::new();
    let mut dust: Vec<(String, i64)> = Vec::new();
    for (addr, sat, _) in staged {
        if sat >= min_payout_sat {
            outputs.push((addr, sat));
        } else if sat > 0 {
            // Track dust for future accumulation but don't pay out yet
            dust.push((addr, sat));
        }
    }

    // Append fee output to the end of the outputs list (if fee is configured)
    if fee_sat > 0 {
        if let Some(addr) = fee_address {
            outputs.push((addr.to_string(), fee_sat));
        }
    }

    PayoutPlan {
        outputs,
        dust,
        gross_reward_sat,
        fee_sat,
        net_reward_sat,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pplns_is_deterministic() {
        let shares = vec![
            WeightedShare {
                payout_address: "a".into(),
                work_units: 10.0,
            },
            WeightedShare {
                payout_address: "b".into(),
                work_units: 10.0,
            },
        ];
        let dust = HashMap::new();
        let p1 = build_pplns_payout_plan(1000, 0, None, &shares, 0, &dust);
        let p2 = build_pplns_payout_plan(1000, 0, None, &shares, 0, &dust);
        assert_eq!(p1.outputs, p2.outputs);
        assert_eq!(p1.outputs.iter().map(|(_, v)| *v).sum::<i64>(), 1000);
    }

    #[test]
    fn fee_is_accounted() {
        let shares = vec![WeightedShare {
            payout_address: "a".into(),
            work_units: 1.0,
        }];
        let dust = HashMap::new();
        let p = build_pplns_payout_plan(1000, 100, Some("fee"), &shares, 0, &dust);
        assert_eq!(p.fee_sat, 10);
        assert_eq!(p.net_reward_sat, 990);
    }

    #[test]
    fn remainder_distribution_is_deterministic() {
        let shares = vec![
            WeightedShare {
                payout_address: "b".into(),
                work_units: 1.0,
            },
            WeightedShare {
                payout_address: "a".into(),
                work_units: 1.0,
            },
            WeightedShare {
                payout_address: "c".into(),
                work_units: 1.0,
            },
        ];
        let dust = HashMap::new();
        let p = build_pplns_payout_plan(10, 0, None, &shares, 0, &dust);
        assert_eq!(p.outputs.iter().map(|(_, v)| *v).sum::<i64>(), 10);
        assert_eq!(
            p.outputs,
            vec![("a".into(), 4), ("b".into(), 3), ("c".into(), 3)]
        );
    }

    #[test]
    fn dust_carry_forward_included_in_payout() {
        let shares = vec![WeightedShare {
            payout_address: "a".into(),
            work_units: 1.0,
        }];
        let mut dust = HashMap::new();
        dust.insert("a".to_string(), 100); // 100 sats accumulated dust
        
        let p = build_pplns_payout_plan(1000, 0, None, &shares, 0, &dust);
        // Should receive 1000 (current) + 100 (dust) = 1100
        assert_eq!(p.outputs, vec![("a".into(), 1100)]);
        assert_eq!(p.dust, vec![]); // No new dust
    }
}
