//! HTTP Dashboard Database Layer
//!
//! Read-only queries for public dashboard. Wraps AccountingDb
//! and provides aggregated statistics.

use crate::accounting::AccountingDb;
use crate::http::models::*;
use anyhow::Result;
use chrono::{DateTime, Duration, Utc};
use moka::future::Cache;
use tracing::info;
use std::time::Duration as StdDuration;

/// Read-only database handle for HTTP dashboard
#[derive(Clone)]
pub struct PublicDb {
    inner: AccountingDb,
}

impl PublicDb {
    pub fn new(db: AccountingDb) -> Self {
        Self { inner: db }
    }

    /// Get aggregate pool statistics
    pub fn get_pool_stats(&self, network_difficulty: f64) -> Result<PoolStats> {
        let now = Utc::now();
        let day_ago = now - Duration::days(1);
        let week_ago = now - Duration::days(7);
        let ten_min_ago = now - Duration::minutes(10);

        // Get block counts by status
        let block_summary = self.inner.found_block_state_summary().unwrap_or_else(|_| {
            crate::accounting::FoundBlockStateSummary {
                confirmed: 0,
                matured: 0,
                orphaned: 0,
                paid: 0,
            }
        });

        // Count blocks found in last 24h and 7d
        let blocks_24h = self.inner.count_blocks_since(&day_ago.to_rfc3339())?;
        let blocks_7d = self.inner.count_blocks_since(&week_ago.to_rfc3339())?;

        // Get last block found
        let last_block = match self.inner.get_last_found_block()? {
            Some((height, hash, created_at)) => {
                Some(LastBlockInfo {
                    height,
                    hash,
                    found_at: DateTime::parse_from_rfc3339(&created_at)
                        .map(|dt| dt.with_timezone(&Utc))
                        .unwrap_or_else(|_| Utc::now()),
                })
            }
            None => None,
        };

        // Calculate pool hashrate from recent shares
        let (hashrate, active_miners) = self.inner.calculate_pool_hashrate(&ten_min_ago.to_rfc3339())?;

        info!(
            hashrate = hashrate,
            active_miners = active_miners,
            blocks_24h = blocks_24h,
            network_difficulty = network_difficulty,
            "pool stats calculated"
        );

        Ok(PoolStats {
            pool_hashrate: hashrate,
            network_difficulty,
            active_miners,
            blocks_found_total: block_summary.confirmed + block_summary.matured + block_summary.paid,
            blocks_found_24h: blocks_24h,
            blocks_found_7d: blocks_7d,
            last_block_found: last_block,
        })
    }

    /// List workers with statistics (paginated)
    pub fn list_workers(&self, limit: u32, offset: u32) -> Result<Vec<WorkerStats>> {
        let workers = self.inner.worker_accounting_summary(1000)?;

        Ok(workers
            .into_iter()
            .skip(offset as usize)
            .take(limit as usize)
            .map(|w| {
                // Get blocks found for this worker
                let blocks_found = self.count_worker_blocks(&w.payout_address).unwrap_or(0);

                WorkerStats {
                    id: w.worker_id,
                    payout_address: w.payout_address,
                    worker_suffix: w.worker_suffix,
                    shares_accepted: w.accepted,
                    shares_rejected: w.rejected,
                    shares_stale: w.stale,
                    blocks_found,
                    hashrate: 0.0, // TODO: Calculate from work_units
                }
            })
            .collect())
    }

    /// Get miner details by payout address
    pub fn get_worker_by_address(&self, address: &str) -> Result<Option<MinerDetail>> {
        let workers = self.inner.worker_accounting_summary(1000)?;
        let address_workers: Vec<_> = workers
            .into_iter()
            .filter(|w| w.payout_address == address)
            .map(|w| {
                let blocks_found = self.count_worker_blocks(&w.payout_address).unwrap_or(0);
                WorkerStats {
                    id: w.worker_id,
                    payout_address: w.payout_address.clone(),
                    worker_suffix: w.worker_suffix,
                    shares_accepted: w.accepted,
                    shares_rejected: w.rejected,
                    shares_stale: w.stale,
                    blocks_found,
                    hashrate: 0.0,
                }
            })
            .collect();

        if address_workers.is_empty() {
            return Ok(None);
        }

        let total_shares: u64 = address_workers.iter().map(|w| w.shares_accepted).sum();
        let total_blocks: u64 = address_workers.iter().map(|w| w.blocks_found).sum();

        Ok(Some(MinerDetail {
            payout_address: address.to_string(),
            total_hashrate: 0.0,
            workers: address_workers,
            total_shares_accepted: total_shares,
            total_blocks_found: total_blocks,
        }))
    }

    /// List recent rounds
    pub fn list_recent_rounds(&self, limit: u32) -> Result<Vec<RoundStats>> {
        let rounds = self.inner.list_recent_rounds(limit)?;

        Ok(rounds
            .into_iter()
            .map(|r| RoundStats {
                id: r.id,
                start_template_id: r.start_template_id,
                end_template_id: r.end_template_id,
                found_block_hash: r.found_block_hash,
                duration_seconds: None,
                started_at: r.created_at,
                ended_at: None,
            })
            .collect())
    }

    /// List found blocks with optional status filter
    pub fn list_found_blocks(
        &self,
        limit: u32,
        status: Option<&str>,
    ) -> Result<Vec<BlockInfo>> {
        let blocks = self.inner.list_found_blocks(limit, status)?;

        // Get current tip height for confirmations
        let tip_height = self.get_tip_height().unwrap_or(0);

        let block_infos: Vec<BlockInfo> = blocks
            .into_iter()
            .map(|b| {
                let confirmations = if b.status == "orphaned" {
                    -1
                } else {
                    (tip_height - b.height + 1).max(0)
                };

                BlockInfo {
                    height: b.height,
                    hash: b.block_hash,
                    status: b.status,
                    confirmations,
                    found_by: b.worker_name,
                    payout_address: b.payout_address,
                    found_at: b.created_at,
                    matured_at: b.matured_at,
                }
            })
            .collect();

        info!(count = block_infos.len(), "found blocks retrieved");
        Ok(block_infos)
    }

    /// List payout batches
    pub fn list_payout_batches(&self, limit: u32) -> Result<Vec<PayoutInfo>> {
        let batches = self.inner.list_recent_payout_batches(limit)?;

        Ok(batches
            .into_iter()
            .map(|b| PayoutInfo {
                id: b.id,
                method: b.method,
                status: b.status,
                total_amount: 0.0, // TODO: Sum from payout_batch_items
                miner_count: 0,    // TODO: Count from payout_batch_items
                submitted_txid: b.submitted_txid,
                created_at: b.created_at,
                confirmed_at: None,
            })
            .collect())
    }

    /// Health check
    pub fn health_check(&self) -> Result<HealthResponse> {
        let _ = self.inner.active_payout_method()?;

        let last_share = match self.inner.get_last_share_time()? {
            Some(ts) => DateTime::parse_from_rfc3339(&ts)
                .map(|dt| dt.with_timezone(&Utc))
                .ok(),
            None => None,
        };

        Ok(HealthResponse {
            status: "ok".to_string(),
            database: "connected".to_string(),
            last_share,
        })
    }

    // Helper methods

    fn count_worker_blocks(&self, payout_address: &str) -> Result<u64> {
        self.inner.count_blocks_by_address(payout_address)
    }

    fn get_tip_height(&self) -> Result<i64> {
        self.inner.get_tip_height()
    }
}

/// Cached database wrapper for expensive queries
#[derive(Clone)]
pub struct CachedDb {
    inner: PublicDb,
    stats_cache: Cache<String, PoolStats>,
}

impl CachedDb {
    pub fn new(inner: PublicDb) -> Self {
        let stats_cache = Cache::builder()
            .max_capacity(10)
            .time_to_live(StdDuration::from_secs(10))  // 2x broadcast interval for overlap
            .build();
        Self { inner, stats_cache }
    }

    /// Get pool stats with caching
    pub async fn get_pool_stats(&self, network_difficulty: f64) -> Result<PoolStats> {
        // Use rounded network_difficulty for cache key to avoid precision issues
        // Network difficulty changes slowly, so 2 decimal places is sufficient
        let cache_key = format!("stats:{:.2}", network_difficulty);
        
        // Try cache first
        if let Some(cached) = self.stats_cache.get(&cache_key).await {
            return Ok(cached);
        }

        // Query database
        let stats = self.inner.get_pool_stats(network_difficulty)?;
        
        // Cache the result
        self.stats_cache.insert(cache_key, stats.clone()).await;
        
        Ok(stats)
    }

    /// Delegate other methods to inner PublicDb
    pub fn list_workers(&self, limit: u32, offset: u32) -> Result<Vec<WorkerStats>> {
        self.inner.list_workers(limit, offset)
    }

    pub fn get_worker_by_address(&self, address: &str) -> Result<Option<MinerDetail>> {
        self.inner.get_worker_by_address(address)
    }

    pub fn list_recent_rounds(&self, limit: u32) -> Result<Vec<RoundStats>> {
        self.inner.list_recent_rounds(limit)
    }

    pub fn list_found_blocks(&self, limit: u32, status: Option<&str>) -> Result<Vec<BlockInfo>> {
        self.inner.list_found_blocks(limit, status)
    }

    pub fn list_payout_batches(&self, limit: u32) -> Result<Vec<PayoutInfo>> {
        self.inner.list_payout_batches(limit)
    }

    pub fn health_check(&self) -> Result<HealthResponse> {
        self.inner.health_check()
    }

    /// Invalidate the stats cache - called when events are broadcast
    /// This is synchronous (moka future::Cache uses async for get/insert, but invalidate_all is sync)
    pub fn invalidate_stats_cache(&self) {
        self.stats_cache.invalidate_all();
    }
}
