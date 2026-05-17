//! HTTP Dashboard Application State

use crate::accounting::AccountingDb;
use crate::config::PoolConfig;
use crate::stratum::diff_cache::DifficultyCache;
use crate::stratum::server::RuntimeStats;
use std::sync::Arc;

/// Application state shared across all HTTP handlers
#[derive(Clone)]
pub struct AppState {
    pub db: AccountingDb,
    pub stats: Arc<RuntimeStats>,
    pub pool_config: PoolConfig,
    pub diff_cache: DifficultyCache,
}

impl AppState {
    pub fn new(db: AccountingDb, stats: Arc<RuntimeStats>, pool_config: PoolConfig, diff_cache: DifficultyCache) -> Self {
        Self {
            db,
            stats,
            pool_config,
            diff_cache,
        }
    }
}
