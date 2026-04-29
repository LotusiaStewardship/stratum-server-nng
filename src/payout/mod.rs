use anyhow::Result;

/// Unsigned payout plan built from accounting obligations.
#[derive(Debug, Clone)]
pub struct PayoutPlan {
    pub outputs: Vec<(String, i64)>,
}

/// Optional signer integration point.
///
/// This keeps key custody out of core accounting by default while allowing
/// operators to plug in online or offline signing workflows.
pub trait OptionalSigner: Send + Sync {
    fn sign_and_submit(&self, _plan: &PayoutPlan) -> Result<String>;
}

/// Planner stub for Phase S5; accounting and queue state is prepared in S4.
pub fn build_payout_plan() -> PayoutPlan {
    PayoutPlan { outputs: vec![] }
}
