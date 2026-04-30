use bitcoinsuite_core::LotusAddress;

/// Enforced worker authorization grammar:
/// `<lotus_address>[.<worker>]`
///
/// The pool uses this to split payout destination and human worker suffix.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerName {
    pub payout_address: String,
    pub worker_suffix: Option<String>,
}

pub fn parse_worker_name(input: &str) -> anyhow::Result<WorkerName> {
    let mut parts = input.splitn(2, '.');
    let address = parts.next().unwrap_or_default();
    let suffix = parts.next();

    let _: LotusAddress = address
        .parse()
        .map_err(|e| anyhow::anyhow!("invalid lotus address: {e}"))?;
    if suffix.is_some_and(|s| s.is_empty()) {
        anyhow::bail!("worker suffix must not be empty when '.' is present")
    }

    Ok(WorkerName {
        payout_address: address.to_string(),
        worker_suffix: suffix.map(|s| s.to_string()),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_worker() {
        let w = parse_worker_name("lotus_16PSJNf1EDEfGvaYzaXJCJZrXH4pgiTo7kyW61iGi.rig01").unwrap();
        assert_eq!(w.payout_address, "lotus_16PSJNf1EDEfGvaYzaXJCJZrXH4pgiTo7kyW61iGi");
        assert_eq!(w.worker_suffix.as_deref(), Some("rig01"));
        assert!(parse_worker_name("bad.worker").is_err());
        assert!(parse_worker_name("lotusabc.r1").is_err());
        assert!(parse_worker_name("lotus_.r1").is_err());
        assert!(parse_worker_name("lotus_16PSJNf1EDEfGvaYzaXJCJZrXH4pgiTo7kyW61iGi.").is_err());
    }
}
