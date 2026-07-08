use crate::payout::pplns::PplnsShareEntry;

/// An output in a payout plan — one miner's payment.
#[derive(Debug, Clone, PartialEq)]
pub struct PayoutOutput {
    pub payout_address: String,
    pub worker_id: i64,
    pub amount: i64,
    pub dust_carried_forward: i64,
}

/// A complete payout plan for a found block.
#[derive(Debug, Clone)]
pub struct PayoutPlan {
    pub round_id: i64,
    pub block_height: i32,
    pub block_hash: String,
    pub network_difficulty: f64,
    pub total_work_units: f64,
    pub gross_reward: i64,
    pub pool_fee_amount: i64,
    pub pool_fee_address: Option<String>,
    pub outputs: Vec<PayoutOutput>,
    pub dust_carried_forward_total: i64,
    pub retry_key: String,
}

/// Build a payout plan for a found block using PPLNS distribution.
///
/// # Arguments
///
/// * `round_id` - The round ID the found block belongs to.
/// * `block_height` - Height of the found block.
/// * `block_hash` - Hash of the found block.
/// * `network_difficulty` - N_diff at the time of the found block (for window calculation metadata).
/// * `gross_reward` - Total coinbase value in satoshis (from coinbase_value).
/// * `fee_bps` - Pool fee in basis points (e.g. 100 = 1.00%).
/// * `fee_address` - Optional separate address for pool fee output.
/// * `min_payout_sat` - Minimum payout in satoshis; outputs below this become dust.
/// * `dust_balances` - Current dust carry-forward per address: `(address, balance_sat)`.
/// * `shares` - Shares in the PPLNS window, from `calculate_pplns_window`.
///
/// # Algorithm
///
/// 1. Aggregate work units by payout_address (sum difficulty per address).
/// 2. Add existing dust balance as bonus weight for each address.
/// 3. Calculate fee: `pool_fee_amount = gross_reward * fee_bps / 10000`.
/// 4. Net reward = gross_reward - pool_fee_amount.
/// 5. Distribute proportionally using integer arithmetic.
/// 6. Handle remainder satoshis (distribute to largest fractional parts).
/// 7. Clip outputs below min_payout_sat to dust.
/// 8. Build the fee output if fee_address is configured.
///
/// Returns a PayoutPlan with deterministic outputs (same inputs => same outputs).
pub fn build_payout_plan(
    round_id: i64,
    block_height: i32,
    block_hash: &str,
    network_difficulty: f64,
    gross_reward: i64,
    fee_bps: u32,
    fee_address: Option<&str>,
    min_payout_sat: i64,
    dust_balances: &[(String, i64)],
    shares: &[PplnsShareEntry],
) -> PayoutPlan {
    // 1. Aggregate work units by payout_address
    let mut work_by_addr: std::collections::BTreeMap<String, (i64, f64)> = BTreeMap::new();
    // worker_id is the first share's worker_id for this address (they should all be the same)
    for share in shares {
        let entry = work_by_addr
            .entry(share.payout_address.clone())
            .or_insert_with(|| (share.worker_id, 0.0));
        entry.1 += share.difficulty;
    }

    // 2. Add existing dust as bonus weight
    let dust_map: std::collections::HashMap<&str, i64> = dust_balances
        .iter()
        .map(|(a, b)| (a.as_str(), *b))
        .collect();
    for (addr, (_worker_id, work_units)) in work_by_addr.iter_mut() {
        if let Some(dust) = dust_map.get(addr.as_str()) {
            // Dust is converted to fractional work units proportional to the gross_reward.
            // A 1-dust bonus = (1 / gross_reward) * total_work_units additional weight.
            // This effectively gives the dust holder a proportional "bonus" in the window.
            if gross_reward > 0 && *dust > 0 {
                let dust_weight = *dust as f64 / gross_reward as f64;
                *work_units += dust_weight;
            }
        }
    }

    let total_work_units: f64 = work_by_addr.values().map(|(_, w)| w).sum();

    // 3. Calculate pool fee
    let pool_fee_amount = if gross_reward > 0 {
        (gross_reward as u64 * fee_bps as u64 / 10000) as i64
    } else {
        0
    };
    let net_reward = gross_reward - pool_fee_amount;

    // 4-6. Distribute proportionally with remainder handling
    let mut outputs: Vec<PayoutOutput> = Vec::new();
    let mut dust_carried_forward_total: i64 = 0;

    if total_work_units > 0.0 && net_reward > 0 {
        // Calculate exact amounts with fractional parts
        #[allow(dead_code)]
        struct Alloc {
            address: String,
            worker_id: i64,
            whole: i64,
            fractional: f64,
        }
        let mut allocs: Vec<Alloc> = work_by_addr
            .iter()
            .map(|(addr, (wid, work))| {
                let fractional_reward = *work as f64 / total_work_units * net_reward as f64;
                let whole = fractional_reward.floor() as i64;
                let fractional = fractional_reward - whole as f64;
                Alloc {
                    address: addr.clone(),
                    worker_id: *wid,
                    whole,
                    fractional,
                }
            })
            .collect();

        // Handle remainder: distribute remaining satoshis to largest fractional parts
        let allocated_sum: i64 = allocs.iter().map(|a| a.whole).sum();
        let mut remainder = net_reward - allocated_sum;

        // Sort by fractional part descending for remainder distribution
        allocs.sort_by(|a, b| {
            b.fractional
                .partial_cmp(&a.fractional)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        for alloc in &mut allocs {
            if remainder <= 0 {
                break;
            }
            alloc.whole += 1;
            remainder -= 1;
        }

        // Sort back by original order (by address) for determinism
        allocs.sort_by(|a, b| a.address.cmp(&b.address));

        // 7. Apply min_payout_sat — clip below threshold to dust
        for alloc in allocs {
            if alloc.whole < min_payout_sat {
                // Below minimum — becomes dust
                dust_carried_forward_total += alloc.whole;
                // Still record the payout with 0 amount and the dust carried forward
                outputs.push(PayoutOutput {
                    payout_address: alloc.address,
                    worker_id: alloc.worker_id,
                    amount: 0,
                    dust_carried_forward: alloc.whole,
                });
            } else {
                outputs.push(PayoutOutput {
                    payout_address: alloc.address,
                    worker_id: alloc.worker_id,
                    amount: alloc.whole,
                    dust_carried_forward: 0,
                });
            }
        }
    }

    // 8. Fee is tracked via pool_fee_amount / pool_fee_address on PayoutPlan.
    // It is NOT added to outputs — outputs are miner-only.
    // If no fee_address is configured the amount stays unallocated on the batch.

    let retry_key = format!("{}:{}", block_hash, outputs.len());

    PayoutPlan {
        round_id,
        block_height,
        block_hash: block_hash.to_string(),
        network_difficulty,
        total_work_units,
        gross_reward,
        pool_fee_amount,
        pool_fee_address: fee_address.map(|s| s.to_string()),
        outputs,
        dust_carried_forward_total,
        retry_key,
    }
}

use std::collections::BTreeMap;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::payout::pplns::PplnsShareEntry;

    fn make_share(worker_id: i64, addr: &str, diff: f64) -> PplnsShareEntry {
        PplnsShareEntry {
            share_id: 1,
            share_outcome_id: 1,
            worker_id,
            payout_address: addr.to_string(),
            difficulty: diff,
            created_at: "2026-05-20T12:00:00".to_string(),
        }
    }

    #[test]
    fn test_simple_50_50_split_no_fee() {
        let shares = vec![make_share(1, "addr1", 10.0), make_share(2, "addr2", 10.0)];
        let plan = build_payout_plan(
            1,
            1000,
            "blockhash",
            100.0,
            100000, // 1000 sat gross
            0,      // 0 bps fee
            None,
            546,
            &[],
            &shares,
        );
        assert_eq!(plan.pool_fee_amount, 0);
        assert_eq!(plan.outputs.len(), 2);
        // 50/50 split of 100000 = 50000 each
        assert_eq!(plan.outputs[0].amount, 50000);
        assert_eq!(plan.outputs[1].amount, 50000);
    }

    #[test]
    fn test_fee_deduction_100_bps() {
        let shares = vec![make_share(1, "addr1", 10.0)];
        let plan = build_payout_plan(
            1,
            1000,
            "blockhash",
            100.0,
            100000, // 1000 sat gross
            100,    // 1% fee
            None,
            546,
            &[],
            &shares,
        );
        assert_eq!(plan.pool_fee_amount, 1000); // 100000 * 100 / 10000 = 1000
        assert_eq!(plan.outputs.len(), 1);
        assert_eq!(plan.outputs[0].amount, 99000); // 100000 - 1000
    }

    #[test]
    fn test_fee_output_with_fee_address() {
        let shares = vec![make_share(1, "addr1", 10.0)];
        let plan = build_payout_plan(
            1,
            1000,
            "blockhash",
            100.0,
            100000,
            100,
            Some("fee_addr"),
            546,
            &[],
            &shares,
        );
        // Fee is tracked on the plan, not in outputs
        assert_eq!(plan.pool_fee_amount, 1000);
        assert_eq!(plan.pool_fee_address.as_deref(), Some("fee_addr"));
        assert_eq!(
            plan.outputs.len(),
            1,
            "outputs should contain only the miner payout"
        );
    }

    #[test]
    fn test_remainder_distribution() {
        // 3 miners, net = 11 sat, work units 5:3:2
        // addr1: 5.5, addr2: 3.3, addr3: 2.2
        // whole = 5+3+2 = 10, remainder = 1 -> goes to addr1 (largest frac)
        let shares = vec![
            make_share(1, "addr1", 5.0),
            make_share(2, "addr2", 3.0),
            make_share(3, "addr3", 2.0),
        ];
        let plan = build_payout_plan(1, 1000, "blockhash", 100.0, 11, 0, None, 1, &[], &shares);
        assert_eq!(plan.outputs.len(), 3);
        let total: i64 = plan.outputs.iter().map(|o| o.amount).sum();
        assert_eq!(total, 11, "remainder satoshis must sum to gross");
        // addr1 gets largest remainder -> 6, others their whole amounts
        assert_eq!(plan.outputs[0].amount, 6);
        assert_eq!(plan.outputs[1].amount, 3);
        assert_eq!(plan.outputs[2].amount, 2);
    }

    #[test]
    fn test_dust_filtering_below_min_payout() {
        let shares = vec![make_share(1, "addr1", 10.0), make_share(2, "addr2", 0.1)];
        let plan = build_payout_plan(
            1,
            1000,
            "blockhash",
            100.0,
            1000,
            0,
            None,
            500, // min payout = 500 sat
            &[],
            &shares,
        );
        // addr1 gets most (should be >= 500)
        // addr2 gets very little (< 500) -> becomes dust
        assert!(plan.dust_carried_forward_total > 0);
        let addr2_output = plan
            .outputs
            .iter()
            .find(|o| o.payout_address == "addr2")
            .unwrap();
        assert_eq!(addr2_output.amount, 0, "addr2 output should be dusted to 0");
        assert!(addr2_output.dust_carried_forward > 0);
    }

    #[test]
    fn test_dust_carry_forward_included_in_plan() {
        let shares = vec![make_share(1, "addr1", 10.0)];
        let dust = vec![("addr1".to_string(), 500)];
        let plan = build_payout_plan(
            1,
            1000,
            "blockhash",
            100.0,
            10000,
            0,
            None,
            1,
            &dust,
            &shares,
        );
        // Dust carry-forward should affect the plan but total should still sum to gross
        // Dust weight gives a tiny bonus but should not break total
        let total: i64 = plan.outputs.iter().map(|o| o.amount).sum();
        assert_eq!(total, 10000, "total must equal gross reward");
    }

    #[test]
    fn test_zero_work_units_returns_empty_outputs() {
        let plan = build_payout_plan(1, 1000, "blockhash", 100.0, 100000, 0, None, 546, &[], &[]);
        assert!(plan.outputs.is_empty());
        assert_eq!(plan.total_work_units, 0.0);
    }

    #[test]
    fn test_single_miner_gets_all_net() {
        let shares = vec![make_share(1, "addr1", 42.0)];
        let plan = build_payout_plan(
            1,
            1000,
            "blockhash",
            100.0,
            50000,
            200, // 2% fee
            None,
            crate::constants::DUST_LIMIT,
            &[],
            &shares,
        );
        assert_eq!(plan.pool_fee_amount, 1000); // 50000 * 200 / 10000 = 1000
        assert_eq!(plan.outputs[0].amount, 49000); // 50000 - 1000
    }

    #[test]
    fn test_deterministic_identical_inputs() {
        let shares = vec![
            make_share(1, "addr1", 10.0),
            make_share(2, "addr2", 5.0),
            make_share(3, "addr3", 2.5),
        ];
        let plan1 = build_payout_plan(1, 1000, "hash", 100.0, 10000, 50, None, 100, &[], &shares);
        let plan2 = build_payout_plan(1, 1000, "hash", 100.0, 10000, 50, None, 100, &[], &shares);
        assert_eq!(plan1.outputs.len(), plan2.outputs.len());
        for (o1, o2) in plan1.outputs.iter().zip(plan2.outputs.iter()) {
            assert_eq!(o1.amount, o2.amount);
            assert_eq!(o1.payout_address, o2.payout_address);
        }
    }
}
