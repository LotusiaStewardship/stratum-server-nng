pub mod scheduler;

use std::collections::HashMap;

use anyhow::Result;

#[derive(Debug, Clone)]
pub struct PayoutPlan {
    pub outputs: Vec<(String, i64)>,
    pub gross_reward_sat: i64,
    pub fee_sat: i64,
    pub net_reward_sat: i64,
}

pub trait OptionalSigner: Send + Sync {
    fn sign_and_submit(&self, _plan: &PayoutPlan) -> Result<String>;
}

#[derive(Debug, Clone)]
pub struct WeightedShare {
    pub payout_address: String,
    pub work_units: f64,
}

pub fn compute_fee(gross_reward_sat: i64, fee_bps: u32) -> i64 {
    ((gross_reward_sat as i128 * fee_bps as i128) / 10_000) as i64
}

pub fn build_pplns_payout_plan(
    gross_reward_sat: i64,
    fee_bps: u32,
    fee_address: Option<&str>,
    shares: &[WeightedShare],
    min_payout_sat: i64,
) -> PayoutPlan {
    let fee_sat = compute_fee(gross_reward_sat, fee_bps);
    let net_reward_sat = gross_reward_sat - fee_sat;

    let mut work_by_address: HashMap<String, f64> = HashMap::new();
    for s in shares {
        *work_by_address.entry(s.payout_address.clone()).or_insert(0.0) += s.work_units;
    }

    let total_work: f64 = work_by_address.values().sum();
    if total_work <= 0.0 {
        return PayoutPlan {
            outputs: vec![],
            gross_reward_sat,
            fee_sat,
            net_reward_sat,
        };
    }

    let mut staged: Vec<(String, i64, f64)> = work_by_address
        .into_iter()
        .map(|(addr, w)| {
            let raw = (net_reward_sat as f64) * (w / total_work);
            (addr, raw.floor() as i64, raw.fract())
        })
        .collect();
    staged.sort_by(|a, b| b.2.partial_cmp(&a.2).unwrap_or(std::cmp::Ordering::Equal).then_with(|| a.0.cmp(&b.0)));

    let paid_floor: i64 = staged.iter().map(|(_, v, _)| *v).sum();
    let mut remainder = net_reward_sat - paid_floor;
    for i in 0..staged.len() {
        if remainder <= 0 { break; }
        staged[i].1 += 1;
        remainder -= 1;
    }

    let mut outputs: Vec<(String, i64)> = staged
        .into_iter()
        .filter_map(|(addr, sat, _)| (sat >= min_payout_sat).then_some((addr, sat)))
        .collect();

    if fee_sat > 0 {
        if let Some(addr) = fee_address {
            outputs.push((addr.to_string(), fee_sat));
        }
    }

    PayoutPlan {
        outputs,
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
            WeightedShare { payout_address: "a".into(), work_units: 10.0 },
            WeightedShare { payout_address: "b".into(), work_units: 10.0 },
        ];
        let p1 = build_pplns_payout_plan(1000, 0, None, &shares, 0);
        let p2 = build_pplns_payout_plan(1000, 0, None, &shares, 0);
        assert_eq!(p1.outputs, p2.outputs);
        assert_eq!(p1.outputs.iter().map(|(_, v)| *v).sum::<i64>(), 1000);
    }

    #[test]
    fn fee_is_accounted() {
        let shares = vec![WeightedShare { payout_address: "a".into(), work_units: 1.0 }];
        let p = build_pplns_payout_plan(1000, 100, Some("fee"), &shares, 0);
        assert_eq!(p.fee_sat, 10);
        assert_eq!(p.net_reward_sat, 990);
    }
}
