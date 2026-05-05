//! Payout scheduler lease mechanism for HA deployments.
//! 
//! This module provides lease acquisition, renewal, and release functionality
//! to prevent double payouts when running multiple pool instances.
//!
//! These are convenience wrappers around AccountingDb methods.

use anyhow::Result;

use crate::accounting::{AccountingDb, LeaseInfo};

/// Default lease duration in minutes
pub const DEFAULT_LEASE_DURATION_MINS: i64 = 5;

/// Acquire a scheduler lease for the given instance.
/// 
/// Returns `Ok(true)` if lease was acquired successfully.
/// Returns `Ok(false)` if another instance currently holds the lease.
/// 
/// The lease will automatically expire after `duration_minutes` if not renewed.
pub fn acquire_scheduler_lease(
    db: &AccountingDb,
    instance_id: &str,
    duration_minutes: i64,
) -> Result<bool> {
    db.acquire_scheduler_lease(instance_id, duration_minutes)
}

/// Renew an existing scheduler lease.
/// 
/// Returns `Ok(true)` if lease was renewed successfully.
/// Returns `Ok(false)` if this instance doesn't hold the lease (may have expired or been taken by another instance).
pub fn renew_scheduler_lease(
    db: &AccountingDb,
    instance_id: &str,
    duration_minutes: i64,
) -> Result<bool> {
    db.renew_scheduler_lease(instance_id, duration_minutes)
}

/// Release a scheduler lease voluntarily.
/// 
/// Returns `Ok(true)` if lease was released.
/// Returns `Ok(false)` if this instance didn't hold the lease.
pub fn release_scheduler_lease(
    db: &AccountingDb,
    instance_id: &str,
) -> Result<bool> {
    db.release_scheduler_lease(instance_id)
}

/// Check if a lease is currently held and by whom.
/// 
/// Returns `Ok(Some(owner))` if lease is held (not expired).
/// Returns `Ok(None)` if no lease exists or lease has expired.
pub fn check_lease_status(db: &AccountingDb) -> Result<Option<String>> {
    db.check_lease_status()
}

/// Get detailed lease information including expiration time.
pub fn get_lease_info(db: &AccountingDb) -> Result<Option<LeaseInfo>> {
    db.get_lease_info()
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn create_test_db() -> (TempDir, AccountingDb) {
        let tmp_dir = TempDir::new().expect("failed to create temp dir");
        let db_path = tmp_dir.path().join("test.db");
        
        let db = AccountingDb::open(db_path.to_str().unwrap())
            .expect("failed to open db");
        db.init_schema().expect("failed to init schema");
        
        (tmp_dir, db)
    }

    #[test]
    fn test_acquire_lease_when_no_lease_exists() {
        let (_tmp, db) = create_test_db();
        
        let result = acquire_scheduler_lease(&db, "instance-1", DEFAULT_LEASE_DURATION_MINS);
        
        assert!(result.is_ok());
        assert!(result.unwrap());
        
        // Verify lease info
        let info = get_lease_info(&db).unwrap();
        assert!(info.is_some());
        let info = info.unwrap();
        assert_eq!(info.owner, "instance-1");
        assert!(info.is_valid);
        assert!(!info.is_expired);
    }

    #[test]
    fn test_acquire_lease_when_another_instance_holds_it() {
        let (_tmp, db) = create_test_db();
        
        // First instance acquires lease
        let result1 = acquire_scheduler_lease(&db, "instance-1", DEFAULT_LEASE_DURATION_MINS);
        assert!(result1.is_ok());
        assert!(result1.unwrap());
        
        // Second instance tries to acquire - should fail
        let result2 = acquire_scheduler_lease(&db, "instance-2", DEFAULT_LEASE_DURATION_MINS);
        assert!(result2.is_ok());
        assert!(!result2.unwrap());
        
        // Verify instance-1 still holds the lease
        let info = get_lease_info(&db).unwrap();
        assert!(info.is_some());
        assert_eq!(info.unwrap().owner, "instance-1");
    }

    #[test]
    fn test_acquire_lease_after_expiration() {
        let (_tmp, db) = create_test_db();
        
        // Acquire lease with very short duration
        let result1 = acquire_scheduler_lease(&db, "instance-1", 1);
        assert!(result1.is_ok());
        assert!(result1.unwrap());
        
        // Manually expire the lease
        db.expire_lease_for_test().unwrap();
        
        // Another instance should be able to acquire
        let result2 = acquire_scheduler_lease(&db, "instance-2", DEFAULT_LEASE_DURATION_MINS);
        assert!(result2.is_ok());
        assert!(result2.unwrap());
        
        // Verify instance-2 now holds the lease
        let info = get_lease_info(&db).unwrap();
        assert!(info.is_some());
        assert_eq!(info.unwrap().owner, "instance-2");
    }

    #[test]
    fn test_renew_lease_success() {
        let (_tmp, db) = create_test_db();
        
        // Acquire lease
        acquire_scheduler_lease(&db, "instance-1", DEFAULT_LEASE_DURATION_MINS).unwrap();
        
        // Get initial expiration
        let info1 = get_lease_info(&db).unwrap().unwrap();
        
        // Renew the lease
        let result = renew_scheduler_lease(&db, "instance-1", DEFAULT_LEASE_DURATION_MINS);
        assert!(result.is_ok());
        assert!(result.unwrap());
        
        // Verify expiration was extended
        let info2 = get_lease_info(&db).unwrap().unwrap();
        // Compare as strings since we store them that way
        assert!(info2.expires_at > info1.expires_at);
        assert_eq!(info2.owner, "instance-1");
    }

    #[test]
    fn test_renew_lease_wrong_instance() {
        let (_tmp, db) = create_test_db();
        
        // Instance-1 acquires lease
        acquire_scheduler_lease(&db, "instance-1", DEFAULT_LEASE_DURATION_MINS).unwrap();
        
        // Instance-2 tries to renew - should fail
        let result = renew_scheduler_lease(&db, "instance-2", DEFAULT_LEASE_DURATION_MINS);
        assert!(result.is_ok());
        assert!(!result.unwrap());
        
        // Verify instance-1 still holds the lease
        let info = get_lease_info(&db).unwrap();
        assert_eq!(info.unwrap().owner, "instance-1");
    }

    #[test]
    fn test_release_lease_success() {
        let (_tmp, db) = create_test_db();
        
        // Acquire lease
        acquire_scheduler_lease(&db, "instance-1", DEFAULT_LEASE_DURATION_MINS).unwrap();
        
        // Release the lease
        let result = release_scheduler_lease(&db, "instance-1");
        assert!(result.is_ok());
        assert!(result.unwrap());
        
        // Verify no lease exists
        let info = get_lease_info(&db).unwrap();
        assert!(info.is_none());
    }

    #[test]
    fn test_release_lease_wrong_instance() {
        let (_tmp, db) = create_test_db();
        
        // Instance-1 acquires lease
        acquire_scheduler_lease(&db, "instance-1", DEFAULT_LEASE_DURATION_MINS).unwrap();
        
        // Instance-2 tries to release - should fail
        let result = release_scheduler_lease(&db, "instance-2");
        assert!(result.is_ok());
        assert!(!result.unwrap());
        
        // Verify instance-1 still holds the lease
        let info = get_lease_info(&db).unwrap();
        assert_eq!(info.unwrap().owner, "instance-1");
    }

    #[test]
    fn test_check_lease_status() {
        let (_tmp, db) = create_test_db();
        
        // No lease initially
        let status = check_lease_status(&db).unwrap();
        assert!(status.is_none());
        
        // Acquire lease
        acquire_scheduler_lease(&db, "instance-1", DEFAULT_LEASE_DURATION_MINS).unwrap();
        
        // Check status
        let status = check_lease_status(&db).unwrap();
        assert_eq!(status, Some("instance-1".to_string()));
    }

    #[test]
    fn test_check_lease_status_expired() {
        let (_tmp, db) = create_test_db();
        
        // Acquire lease
        acquire_scheduler_lease(&db, "instance-1", DEFAULT_LEASE_DURATION_MINS).unwrap();
        
        // Manually expire the lease
        db.expire_lease_for_test().unwrap();
        
        // Check status - should return None for expired lease
        let status = check_lease_status(&db).unwrap();
        assert!(status.is_none());
    }

    #[test]
    fn test_lease_contention_multiple_instances() {
        let (_tmp, db) = create_test_db();
        
        // Simulate multiple instances trying to acquire
        let mut results = Vec::new();
        for i in 1..=5 {
            let instance_id = format!("instance-{}", i);
            let result = acquire_scheduler_lease(&db, &instance_id, DEFAULT_LEASE_DURATION_MINS).unwrap();
            results.push((instance_id, result));
        }
        
        // Only the first should succeed
        let success_count = results.iter().filter(|(_, r)| *r).count();
        assert_eq!(success_count, 1);
        
        // Verify which instance got the lease
        let winner = results.iter().find(|(_, r)| *r).unwrap();
        assert_eq!(winner.0, "instance-1");
        
        // Verify lease holder
        let info = get_lease_info(&db).unwrap().unwrap();
        assert_eq!(info.owner, "instance-1");
    }

    #[test]
    fn test_acquire_renew_release_lifecycle() {
        let (_tmp, db) = create_test_db();
        
        // Acquire
        let acquired = acquire_scheduler_lease(&db, "instance-1", DEFAULT_LEASE_DURATION_MINS).unwrap();
        assert!(acquired);
        
        // Renew multiple times
        for _ in 0..3 {
            let renewed = renew_scheduler_lease(&db, "instance-1", DEFAULT_LEASE_DURATION_MINS).unwrap();
            assert!(renewed);
        }
        
        // Verify still held
        let info = get_lease_info(&db).unwrap().unwrap();
        assert_eq!(info.owner, "instance-1");
        assert!(info.is_valid);
        
        // Release
        let released = release_scheduler_lease(&db, "instance-1").unwrap();
        assert!(released);
        
        // Verify released
        let info = get_lease_info(&db).unwrap();
        assert!(info.is_none());
        
        // Try to release again - should fail
        let released_again = release_scheduler_lease(&db, "instance-1").unwrap();
        assert!(!released_again);
    }
}
