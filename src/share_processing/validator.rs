use crate::share_processing::VarDiffConfig;
use crate::stratum_protocol::job::MiningJob;
use crate::stratum_protocol::session::SessionState;
use bitcoinsuite_bitcoind_stratum::{build_stratum_header, header_meets_difficulty};
use bitcoinsuite_core::{BitcoinCode, Bytes, Hashed, LotusHeader};
use primitive_types::U256;

/// Result of validating a share submission.
///
/// Per UBQ §Share Outcome: captures the full validation result,
/// including difficulty check outcomes and block detection.
#[derive(Debug, Clone)]
pub struct ValidationResult {
    /// Whether the share is accepted (meets P_diff target and all checks pass).
    pub accepted: bool,
    /// Human-readable rejection reason. Non-NULL when `accepted` is false.
    pub reject_reason: Option<String>,
    /// 1 if hash met P_diff target, 0 otherwise.
    pub low_diff_ok: bool,
    /// 1 if hash met N_diff target (high-hash share / block candidate).
    pub network_target_ok: bool,
    /// Non-NULL if share found a block candidate (network_target_ok=true).
    pub block_hash: Option<String>,
}

impl ValidationResult {
    /// Create a rejected validation result with the given reason.
    pub(crate) fn rejected(reason: &str) -> Self {
        Self {
            accepted: false,
            reject_reason: Some(reason.to_string()),
            low_diff_ok: false,
            network_target_ok: false,
            block_hash: None,
        }
    }
}

/// Validate the format of share submission parameters.
///
/// Checks:
/// - extranonce2: hex string, even length, max 16 chars (8 bytes)
/// - ntime: 6 bytes = 12 hex chars
/// - nonce: 8 bytes = 16 hex chars
pub fn validate_share_format(
    extranonce2: &str,
    ntime: &str,
    nonce: &str,
) -> Result<(), String> {
    // extranonce2: hex string, even length, max 16 chars (8 bytes)
    if extranonce2.is_empty() || extranonce2.len() % 2 != 0 || extranonce2.len() > 16 {
        return Err("invalid-submit-shape".into());
    }
    if !extranonce2.chars().all(|c| c.is_ascii_hexdigit()) {
        return Err("invalid-submit-shape".into());
    }

    // ntime: 6 bytes = 12 hex chars
    if ntime.len() != 12 {
        return Err("invalid-submit-shape".into());
    }
    if !ntime.chars().all(|c| c.is_ascii_hexdigit()) {
        return Err("invalid-submit-shape".into());
    }

    // nonce: 8 bytes = 16 hex chars
    if nonce.len() != 16 {
        return Err("invalid-submit-shape".into());
    }
    if !nonce.chars().all(|c| c.is_ascii_hexdigit()) {
        return Err("invalid-submit-shape".into());
    }

    Ok(())
}

/// Validate a share submission against all rules in the validation pipeline.
///
/// This is the authoritative validation function per the Slice 3 spec.
/// It checks:
/// 1. Worker is authorized for this session
/// 2. Submit params format (extranonce2, ntime, nonce)
/// 3. Job exists and is active in session's assigned_jobs
/// 4. Submitted ntime matches frozen ntime from assigned job
/// 5. Header hash meets P_diff target
/// 6. Header hash meets N_diff target (block candidate detection)
///
/// The result is used for:
/// - Protocol response (accepted → true, rejected → false with error code)
/// - Share outcome recording (status, reject_reason, low_diff_ok, network_target_ok, block_hash)
pub fn validate_share(
    worker_name: &str,
    job_id: &str,
    extranonce2: &str,
    ntime: &str,
    nonce: &str,
    session: &SessionState,
    job: &MiningJob,
) -> ValidationResult {
    // 1. Check worker authorization
    if !session.authorized_workers.contains(worker_name) {
        return ValidationResult::rejected("unauthorized-worker");
    }

    // 2. Parse and validate submit params format
    if let Err(reason) = validate_share_format(extranonce2, ntime, nonce) {
        return ValidationResult::rejected(&reason);
    }

    // 3. Look up assigned job in session
    let assigned = match session.get_assigned_job(job_id) {
        Some(a) => a,
        None => return ValidationResult::rejected("stale-job"),
    };

    // 4. Check ntime matches frozen ntime from assignment
    // Per UBQ §Assigned Job: prevents miners from reusing valid nonces across different ntime values
    if ntime != assigned.ntime {
        return ValidationResult::rejected("ntime-mismatch");
    }

    // 5. Build stratum header using bitcoinsuite's builder
    let header_bytes = match build_stratum_header(
        &job.coinbase1,
        &session.extranonce1,
        extranonce2,
        &job.coinbase2,
        &job.merkle_branches,
        &job.prevhash,
        &job.version,
        &job.nbits,
        ntime,
        nonce,
        Some(job.height),
        Some(&job.epoch_hash),
        Some(&job.extended_metadata_hash),
        Some(job.block_size),
    ) {
        Ok(h) => h,
        Err(_) => return ValidationResult::rejected("invalid-submit-shape"),
    };

    // Compute the Lotus-specific block hash (merkle-tree-style, not SHA256d)
    let hash_le = match LotusHeader::deser(&mut Bytes::from_slice(&header_bytes)) {
        Ok(header) => header.calc_hash().as_slice().to_vec(),
        Err(_) => return ValidationResult::rejected("invalid-submit-shape"),
    };

    // Convert hash to big-endian for U256 difficulty comparison
    // (header_meets_difficulty expects big-endian)
    let mut hash_bytes = [0u8; 32];
    hash_bytes.copy_from_slice(&hash_le);
    let mut hash_be = hash_bytes;
    hash_be.reverse();

    // 6. Check P_diff target
    // Per UBQ: share difficulty = P_diff at assignment time (from assigned_jobs)
    let meets_pdiff = match header_meets_difficulty(&hash_be, assigned.p_diff) {
        Ok(v) => v,
        Err(_) => return ValidationResult::rejected("low-difficulty-share"),
    };

    if !meets_pdiff {
        return ValidationResult::rejected("low-difficulty-share");
    }

    // 7. Check N_diff target (block candidate detection)
    // Per UBQ: high-hash shares (meet P_diff but not N_diff) are accepted
    // with network_target_ok=false
    let network_target_bytes = match hex::decode(&job.network_target_hex) {
        Ok(b) => b,
        Err(_) => return ValidationResult::rejected("invalid-submit-shape"),
    };

    let network_target: [u8; 32] = match network_target_bytes.as_slice().try_into() {
        Ok(t) => t,
        Err(_) => return ValidationResult::rejected("invalid-submit-shape"),
    };

    let hash_u256 = U256::from_big_endian(&hash_be);
    let ntarget_u256 = U256::from_big_endian(&network_target);
    let meets_network = hash_u256 <= ntarget_u256;

    // 8. Build the final result
    // Convert hash to big-endian for block hash display (standard hex format)
    let mut hash_be_display = [0u8; 32];
    hash_be_display.copy_from_slice(&hash_le);
    hash_be_display.reverse();
    let block_hash = if meets_network {
        Some(hex::encode(hash_be_display))
    } else {
        None
    };

    ValidationResult {
        accepted: true,
        reject_reason: None,
        low_diff_ok: true,
        network_target_ok: meets_network,
        block_hash,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stratum_protocol::session::SessionState;

    // Real-world MiningJob reconstructed from lotusd template at height 1292529
    // Source: template_id=890, epoch=100, prevhash in stratum format
    fn create_test_job() -> MiningJob {
        MiningJob {
            job_id: "job-890-100".to_string(),
            template_id: 890,
            prevhash: "4f7bcee63a20eff92f69a7f0e74af36a9f1e60ee7ecc5b0506e1ae3600000000".to_string(),
            coinbase1: "02000000010000000000000000000000000000000000000000000000000000000000000000ffffffff1900000e2f4c6f747573696120506f6f6c2f".to_string(),
            coinbase2: "ffffffff0300000000000000000b6a056c6f676f7303f1b8137ecf360d000000001976a914ad8b796954a46f0f32a867d3fd8855043cc506ba88ac7ecf360d000000001976a914053d4d0c28d299dc5c2be1ce5d29bf00cdb61b4088ac00000000".to_string(),
            merkle_branches: vec![
                "796f6be745741765f8b19cfa4209ff68447d9e76198fee5d33fbe2c944224f16".to_string(),
                "4b0ce2ddbf0f5352b721b7688109a1e1007722f96fa07f61ea8e655ac804964f".to_string(),
                "c3899f315bc3b284015819a8d77404b4e179528d62559886babf89884966a172".to_string(),
            ],
            version: "00000001".to_string(),
            nbits: "10d0091c".to_string(),
            ntime: "6adc0c6a0000".to_string(),
            network_target_hex: "0000000009d01000000000000000000000000000000000000000000000000000".to_string(),
            clean_jobs: true,
            template_epoch: 100,
            height: 1292529,
            epoch_hash: "00000000061fb84d2a1d30d8767f629a08904b0e70f84587008fd9e91f1583f7".to_string(),
            extended_metadata_hash: "9a538906e6466ebd2617d321f71bc94e56056ce213d366773699e28158e00614".to_string(),
            block_size: 2588,
        }
    }

    fn create_authorized_session() -> SessionState {
        let mut session = SessionState::new("sess-1".to_string(), VarDiffConfig::default(), 100.0);
        session.is_subscribed = true;
        session.is_authorized = true;
        session.authorized_workers.insert("lotus_16PSJNf1EDEfGvaYzaXJCJZrXH4pgiTo7kyW61iGi.rig".to_string());
        session.record_assigned_job("job-890-100".to_string(), 1.0, "6adc0c6a0000".to_string());
        session
    }

    #[test]
    fn test_validate_format_valid_params() {
        assert!(validate_share_format("00000003", "6adc0c6a0000", "B02B4ABB3DD6E835").is_ok());
        assert!(validate_share_format("aabb", "6adc0c6a0000", "B02B4ABB3DD6E835").is_ok());
        assert!(validate_share_format("0011223344556677", "6adc0c6a0000", "B02B4ABB3DD6E835").is_ok());
    }

    #[test]
    fn test_validate_format_invalid_extranonce2() {
        assert!(validate_share_format("", "6adc0c6a0000", "B02B4ABB3DD6E835").is_err());
        assert!(validate_share_format("0000000", "6adc0c6a0000", "B02B4ABB3DD6E835").is_err());
        assert!(validate_share_format("000000xx", "6adc0c6a0000", "B02B4ABB3DD6E835").is_err());
        assert!(validate_share_format("000000030000000000", "6adc0c6a0000", "B02B4ABB3DD6E835").is_err());
    }

    #[test]
    fn test_validate_format_invalid_ntime() {
        assert!(validate_share_format("00000003", "5f5f5f", "B02B4ABB3DD6E835").is_err());
        assert!(validate_share_format("00000003", "5f5f5f5f5f", "B02B4ABB3DD6E835").is_err());
        assert!(validate_share_format("00000003", "zzzzzzzzzzzz", "B02B4ABB3DD6E835").is_err());
    }

    #[test]
    fn test_validate_format_invalid_nonce() {
        assert!(validate_share_format("00000003", "6adc0c6a0000", "00112233").is_err());
        assert!(validate_share_format("00000003", "6adc0c6a0000", "zzzzzzzzzzzzzzzz").is_err());
    }

    #[test]
    fn test_validate_share_unauthorized_worker() {
        let session = create_authorized_session();
        let job = create_test_job();

        let result = validate_share(
            "unauthorized_worker",
            "job-890-100",
            "00000003",
            "6adc0c6a0000",
            "B02B4ABB3DD6E835",
            &session,
            &job,
        );

        assert!(!result.accepted);
        assert_eq!(result.reject_reason.as_deref(), Some("unauthorized-worker"));
    }

    #[test]
    fn test_validate_share_invalid_format() {
        let session = create_authorized_session();
        let job = create_test_job();

        let result = validate_share(
            "lotus_16PSJNf1EDEfGvaYzaXJCJZrXH4pgiTo7kyW61iGi.rig",
            "job-890-100",
            "00000003",
            "ssss",
            "B02B4ABB3DD6E835",
            &session,
            &job,
        );

        assert!(!result.accepted);
        assert_eq!(result.reject_reason.as_deref(), Some("invalid-submit-shape"));
    }

    #[test]
    fn test_validate_share_stale_job() {
        let session = create_authorized_session();
        let job = create_test_job();

        let result = validate_share(
            "lotus_16PSJNf1EDEfGvaYzaXJCJZrXH4pgiTo7kyW61iGi.rig",
            "unknown-job",
            "00000003",
            "6adc0c6a0000",
            "B02B4ABB3DD6E835",
            &session,
            &job,
        );

        assert!(!result.accepted);
        assert_eq!(result.reject_reason.as_deref(), Some("stale-job"));
    }

    #[test]
    fn test_validate_share_ntime_mismatch() {
        let session = create_authorized_session();
        let job = create_test_job();

        let result = validate_share(
            "lotus_16PSJNf1EDEfGvaYzaXJCJZrXH4pgiTo7kyW61iGi.rig",
            "job-890-100",
            "00000003",
            "aaaaaaaaaaaa",
            "B02B4ABB3DD6E835",
            &session,
            &job,
        );

        assert!(!result.accepted);
        assert_eq!(result.reject_reason.as_deref(), Some("ntime-mismatch"));
    }

    #[test]
    fn test_validate_share_format_error_codes() {
        let session = create_authorized_session();
        let job = create_test_job();

        let cases = vec![
            ("00000003", "6adc0c6a0000", "xx", "invalid-submit-shape"),
            ("00000003", "6adc0c6a0000", "B02B4ABB3DD6E835xx", "invalid-submit-shape"),
            ("00000003", "zz", "B02B4ABB3DD6E835", "invalid-submit-shape"),
            ("zz", "6adc0c6a0000", "B02B4ABB3DD6E835", "invalid-submit-shape"),
        ];

        for (extranonce2, ntime, nonce, expected_reason) in cases {
            let result = validate_share(
                "lotus_16PSJNf1EDEfGvaYzaXJCJZrXH4pgiTo7kyW61iGi.rig",
                "job-890-100",
                extranonce2,
                ntime,
                nonce,
                &session,
                &job,
            );
            assert!(!result.accepted, "expected rejection for {:?}/{:?}/{:?}", extranonce2, ntime, nonce);
            assert_eq!(result.reject_reason.as_deref(), Some(expected_reason));
        }
    }

    #[test]
    fn test_validate_share_pipeline_consistent() {
        // Verify pipeline runs and produces internally-consistent fields
        let session = create_authorized_session();
        let job = create_test_job();

        let result = validate_share(
            "lotus_16PSJNf1EDEfGvaYzaXJCJZrXH4pgiTo7kyW61iGi.rig",
            "job-890-100",
            "00000003",
            "6adc0c6a0000",
            "B02B4ABB3DD6E835",
            &session,
            &job,
        );

        if result.accepted {
            assert!(result.reject_reason.is_none());
            assert!(result.low_diff_ok);
            if result.network_target_ok {
                assert!(result.block_hash.is_some());
            }
        } else {
            assert!(result.reject_reason.is_some());
            assert!(!result.low_diff_ok);
            assert!(!result.network_target_ok);
            assert!(result.block_hash.is_none());
        }
    }

    #[test]
    fn test_validate_share_rejects_when_not_authorized() {
        let session = SessionState::new("sess-unauth".to_string(), VarDiffConfig::default(), 100.0);
        let job = create_test_job();

        let result = validate_share(
            "lotus_16PSJNf1EDEfGvaYzaXJCJZrXH4pgiTo7kyW61iGi.rig",
            "job-890-100",
            "00000003",
            "6adc0c6a0000",
            "B02B4ABB3DD6E835",
            &session,
            &job,
        );

        assert!(!result.accepted);
        assert_eq!(result.reject_reason.as_deref(), Some("unauthorized-worker"));
    }

    #[test]
    fn test_validate_share_empty_extranonce2() {
        let session = create_authorized_session();
        let job = create_test_job();

        let result = validate_share(
            "lotus_16PSJNf1EDEfGvaYzaXJCJZrXH4pgiTo7kyW61iGi.rig",
            "job-890-100",
            "",
            "6adc0c6a0000",
            "B02B4ABB3DD6E835",
            &session,
            &job,
        );

        assert!(!result.accepted);
        assert_eq!(result.reject_reason.as_deref(), Some("invalid-submit-shape"));
    }

    #[test]
    fn test_known_valid_share_accepted() {
        // Deterministic test using the actual block-finding share data.
        // The block at height 1292529 was found with:
        //   extranonce1=79aca8e7, extranonce2=00000003, nonce=13573272464251480634
        //   block_hash=0000000008c2e07a429d877d5f35c07893f2b4693fb708033cf293f52568a3cc
        // Extracted from the block's real coinbase script (inserted between coinbase1/2):
        //   coinbase1 ends '...2f', then extranonce1+extranonce2 '79aca8e700000003', then coinbase2
        let job = create_test_job();

        // Real block-finding share params (from lotusd at height 1292529)
        let worker_name = "lotus_16PSJM7tLsgvi6BNER9VKtb8duMiuFfs2ab7Q1sQd.mainnet";
        let extranonce1 = "79aca8e7";
        let extranonce2 = "00000003";
        let ntime = "6adc0c6a0000";
        // Nonce 13573272464251480634 as LE bytes (matches Lotus header serialization)
        let nonce = hex::encode(13573272464251480634u64.to_le_bytes());

        // Build the session with the exact extranonce1 from the block-finding share
        let mut session = SessionState::new("sess-finder".to_string(), VarDiffConfig::default(), 100.0);
        session.is_subscribed = true;
        session.is_authorized = true;
        session.extranonce1 = extranonce1.to_string();
        // Authorize the exact worker that submitted the block-finding share
        session.authorized_workers.insert(worker_name.to_string());
        session.record_assigned_job(
            "job-890-100".to_string(),
            0.5887084205325228,
            ntime.to_string(),
        );

        let result = validate_share(
            worker_name,
            "job-890-100",
            extranonce2,
            ntime,
            &nonce,
            &session,
            &job,
        );

        assert!(
            result.accepted,
            "block-finding share rejected: reason={:?} low_diff={:?} net_ok={:?} hash={:?}",
            result.reject_reason,
            result.low_diff_ok,
            result.network_target_ok,
            result.block_hash,
        );
        assert!(result.low_diff_ok);
        assert!(result.network_target_ok, "block-finding share must meet network target");
        assert!(result.reject_reason.is_none());
        assert!(
            result.block_hash.is_some(),
            "block-finding share must have a block_hash"
        );
        // Verify the block hash matches
        assert_eq!(
            result.block_hash.as_deref(),
            Some("0000000008c2e07a429d877d5f35c07893f2b4693fb708033cf293f52568a3cc"),
            "block hash must match expected"
        );
    }

    #[test]
    fn test_header_fields_affect_hash() {
        // Verify that build_stratum_header produces different output
        // when real header fields (height, epoch_hash, etc.) are provided
        // vs when None is passed for them.
        let job = create_test_job();

        let header_with_fields = build_stratum_header(
            &job.coinbase1,
            "00112233",
            "00000003",
            &job.coinbase2,
            &job.merkle_branches,
            &job.prevhash,
            &job.version,
            &job.nbits,
            "6adc0c6a0000",
            "B02B4ABB3DD6E835",
            Some(job.height),
            Some(&job.epoch_hash),
            Some(&job.extended_metadata_hash),
            Some(job.block_size),
        )
        .unwrap();

        let header_without_fields = build_stratum_header(
            &job.coinbase1,
            "00112233",
            "00000003",
            &job.coinbase2,
            &job.merkle_branches,
            &job.prevhash,
            &job.version,
            &job.nbits,
            "6adc0c6a0000",
            "B02B4ABB3DD6E835",
            None,
            None,
            None,
            None,
        )
        .unwrap();

        assert_ne!(
            header_with_fields, header_without_fields,
            "header with real height/epoch_hash/ext_metadata/size should differ from defaults"
        );
    }

    #[test]
    fn test_low_difficulty_share_rejection() {
        let mut session = create_authorized_session();
        session.record_assigned_job("job-890-100".to_string(), f64::MAX, "6adc0c6a0000".to_string());
        let job = create_test_job();

        let result = validate_share(
            "lotus_16PSJNf1EDEfGvaYzaXJCJZrXH4pgiTo7kyW61iGi.rig",
            "job-890-100",
            "00000003",
            "6adc0c6a0000",
            "B02B4ABB3DD6E835",
            &session,
            &job,
        );

        assert!(!result.accepted);
        assert_eq!(result.reject_reason.as_deref(), Some("low-difficulty-share"));
        assert!(!result.low_diff_ok);
        assert!(!result.network_target_ok);
        assert!(result.block_hash.is_none());
    }
}
