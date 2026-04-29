use regex::Regex;

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

    // Placeholder Lotus address sanity check (tighten with canonical parser in
    // next protocol-hardening phase if required by operators).
    let re = Regex::new(r"^lotus[_A-Za-z0-9]+$").unwrap();
    if !re.is_match(address) {
        anyhow::bail!("worker name must begin with a lotus address")
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
        let w = parse_worker_name("lotus_abc.rig01").unwrap();
        assert_eq!(w.payout_address, "lotus_abc");
        assert_eq!(w.worker_suffix.as_deref(), Some("rig01"));
        assert!(parse_worker_name("bad.worker").is_err());
    }
}
