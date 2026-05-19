use crate::stratum_protocol::job::MiningJob;
use moka::future::Cache;
use std::collections::VecDeque;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Job cache with LRU eviction for mining jobs.
///
/// Stores up to `max_size` jobs. When full, oldest jobs are evicted first.
/// Uses Moka's built-in LRU for eviction, with VecDeque only for tracking latest job.
pub struct JobCache {
    cache: Cache<String, MiningJob>,
    job_order: Arc<RwLock<VecDeque<String>>>,
    max_size: u64,
}

impl JobCache {
    /// Create a new job cache with the specified maximum size.
    pub fn new(max_size: u64) -> Self {
        Self {
            cache: Cache::new(max_size),
            job_order: Arc::new(RwLock::new(VecDeque::new())),
            max_size,
        }
    }

    /// Insert a job into the cache. If cache is full, evicts oldest job.
    pub async fn insert(&self, job: MiningJob) {
        let job_id = job.job_id.clone();

        // Track insertion order
        let mut order = self.job_order.write().await;
        order.push_back(job_id.clone());
        
        // Evict oldest if over capacity (synchronous LRU)
        while order.len() > self.max_size as usize {
            if let Some(oldest_id) = order.pop_front() {
                self.cache.invalidate(&oldest_id).await;
            }
        }
        
        // Insert into cache
        self.cache.insert(job_id.clone(), job).await;
    }

    /// Get a job by ID.
    pub async fn get(&self, job_id: &str) -> Option<MiningJob> {
        self.cache.get(job_id).await
    }

    /// Get the most recently inserted job.
    pub async fn get_latest(&self) -> Option<MiningJob> {
        let order = self.job_order.read().await;
        if let Some(latest_job_id) = order.back() {
            return self.cache.get(latest_job_id).await;
        }
        None
    }

    /// Get the current network difficulty target from the most recent job.
    pub async fn get_latest_target_hex(&self) -> Option<String> {
        let order = self.job_order.read().await;
        if let Some(latest_job_id) = order.back() {
            if let Some(job) = self.cache.get(latest_job_id).await {
                return Some(job.network_target_hex.clone());
            }
        }
        None
    }

    /// Get the number of jobs currently in the cache.
    pub async fn len(&self) -> usize {
        let order = self.job_order.read().await;
        order.len()
    }

    /// Check if the cache is empty.
    pub async fn is_empty(&self) -> bool {
        let order = self.job_order.read().await;
        order.is_empty()
    }

    /// Clear all jobs from the cache.
    pub async fn clear(&self) {
        self.cache.invalidate_all();
        let mut order = self.job_order.write().await;
        order.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_job(job_id: &str, template_id: u64) -> MiningJob {
        MiningJob {
            job_id: job_id.to_string(),
            template_id,
            prevhash: "0000000000000000000000000000000000000000000000000000000000000000".to_string(),
            coinbase1: "coinbase1".to_string(),
            coinbase2: "coinbase2".to_string(),
            merkle_branches: vec![],
            version: "00000000".to_string(),
            nbits: "1d00ffff".to_string(),
            ntime: "6adc0c6a0000".to_string(),
            network_target_hex: "ffffffff".to_string(),
            clean_jobs: false,
            template_epoch: 0,
        }
    }

    #[tokio::test]
    async fn test_insert_and_get() {
        let cache = JobCache::new(10);
        let job = create_test_job("job-1", 1);

        cache.insert(job.clone()).await;
        let retrieved = cache.get("job-1").await;

        assert!(retrieved.is_some());
        let job = retrieved.unwrap();
        assert_eq!(job.job_id, "job-1");
        assert_eq!(job.template_id, 1);
    }

    #[tokio::test]
    async fn test_get_nonexistent() {
        let cache = JobCache::new(10);
        let result = cache.get("nonexistent").await;

        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_lru_eviction() {
        let cache = JobCache::new(3);

        // Insert 4 jobs (max is 3)
        cache.insert(create_test_job("job-1", 1)).await;
        cache.insert(create_test_job("job-2", 2)).await;
        cache.insert(create_test_job("job-3", 3)).await;
        cache.insert(create_test_job("job-4", 4)).await;

        // job-1 should be evicted
        assert!(cache.get("job-1").await.is_none());

        // Others should still be present
        assert!(cache.get("job-2").await.is_some());
        assert!(cache.get("job-3").await.is_some());
        assert!(cache.get("job-4").await.is_some());
    }

    #[tokio::test]
    async fn test_len() {
        let cache = JobCache::new(10);
        assert_eq!(cache.len().await, 0);

        cache.insert(create_test_job("job-1", 1)).await;
        assert_eq!(cache.len().await, 1);

        cache.insert(create_test_job("job-2", 2)).await;
        assert_eq!(cache.len().await, 2);
    }

    #[tokio::test]
    async fn test_is_empty() {
        let cache = JobCache::new(10);
        assert!(cache.is_empty().await);

        cache.insert(create_test_job("job-1", 1)).await;
        assert!(!cache.is_empty().await);
    }

    #[tokio::test]
    async fn test_clear() {
        let cache = JobCache::new(10);
        cache.insert(create_test_job("job-1", 1)).await;
        cache.insert(create_test_job("job-2", 2)).await;

        cache.clear().await;

        assert!(cache.is_empty().await);
        assert!(cache.get("job-1").await.is_none());
        assert!(cache.get("job-2").await.is_none());
    }

    #[tokio::test]
    async fn test_get_latest_target() {
        let cache = JobCache::new(10);

        // Initially empty
        assert!(cache.get_latest_target_hex().await.is_none());

        // Insert jobs
        let mut job1 = create_test_job("job-1", 1);
        job1.network_target_hex = "target1".to_string();
        cache.insert(job1).await;

        let mut job2 = create_test_job("job-2", 2);
        job2.network_target_hex = "target2".to_string();
        cache.insert(job2).await;

        // Should return latest
        assert_eq!(cache.get_latest_target_hex().await, Some("target2".to_string()));
    }

    #[tokio::test]
    async fn test_lru_order_maintained() {
        let cache = JobCache::new(3);

        // Insert jobs 1, 2, 3
        cache.insert(create_test_job("job-1", 1)).await;
        cache.insert(create_test_job("job-2", 2)).await;
        cache.insert(create_test_job("job-3", 3)).await;

        // Insert job 4, should evict job-1
        cache.insert(create_test_job("job-4", 4)).await;

        // Insert job 5, should evict job-2
        cache.insert(create_test_job("job-5", 5)).await;

        // job-1 and job-2 should be gone
        assert!(cache.get("job-1").await.is_none());
        assert!(cache.get("job-2").await.is_none());

        // job-3, job-4, job-5 should remain
        assert!(cache.get("job-3").await.is_some());
        assert!(cache.get("job-4").await.is_some());
        assert!(cache.get("job-5").await.is_some());
    }

    #[tokio::test]
    async fn test_get_latest_returns_most_recent() {
        let cache = JobCache::new(10);

        // Initially empty
        assert!(cache.get_latest().await.is_none());

        // Insert jobs in order
        cache.insert(create_test_job("job-1", 1)).await;
        let latest = cache.get_latest().await;
        assert!(latest.is_some());
        assert_eq!(latest.unwrap().job_id, "job-1");

        cache.insert(create_test_job("job-2", 2)).await;
        let latest = cache.get_latest().await;
        assert_eq!(latest.unwrap().job_id, "job-2");

        cache.insert(create_test_job("job-3", 3)).await;
        let latest = cache.get_latest().await;
        assert_eq!(latest.unwrap().job_id, "job-3");
    }

    #[tokio::test]
    async fn test_len_matches_cache_size() {
        let cache = JobCache::new(10);
        
        assert_eq!(cache.len().await, 0);
        
        cache.insert(create_test_job("job-1", 1)).await;
        assert_eq!(cache.len().await, 1);
        
        cache.insert(create_test_job("job-2", 2)).await;
        cache.insert(create_test_job("job-3", 3)).await;
        assert_eq!(cache.len().await, 3);
    }
}
