//! HTTP Dashboard Server Startup

use axum::{Router, http::StatusCode, extract::State};
use axum::routing::get;
use tower_http::{compression::CompressionLayer, trace::TraceLayer, services::ServeDir};
use tokio::net::TcpListener;
use tokio::time::{interval, Duration};
use anyhow::Result;
use tracing::{info, error};

use super::state::AppState;
use super::routes;
use super::{PublicDb, CachedDb, DashboardEventSender, DashboardEvent, StatsUpdateEvent};
use super::routes::errors::error_response;

/// Start the HTTP dashboard server
///
/// This function is called from main.rs in a tokio::spawn task.
/// It blocks until the server shuts down or an error occurs.
pub async fn start_http_dashboard(
    bind: String,
    db: crate::accounting::AccountingDb,
    stats: std::sync::Arc<crate::stratum::server::RuntimeStats>,
    pool_config: crate::config::PoolConfig,
    config: crate::config::Config,
    diff_cache: crate::stratum::diff_cache::DifficultyCache,
    events_tx: DashboardEventSender,
) -> Result<()> {
    // Create cached database wrapper for efficient page rendering
    let public_db = PublicDb::new(db.clone(), stats.clone());
    let cached_db = CachedDb::new(public_db.clone());
    
    // Spawn background task to periodically broadcast FRESH stats updates
    // (don't use cache - WebSocket should always send latest data)
    let events_tx_clone = events_tx.clone();
    let diff_cache_clone = diff_cache.clone();
    tokio::spawn(async move {
        let mut interval = interval(Duration::from_secs(5));
        loop {
            interval.tick().await;
            
            let network_difficulty = diff_cache_clone.network_diff();
            
            // Query fresh data for WebSocket broadcast
            match public_db.get_pool_stats(network_difficulty) {
                Ok(pool_stats) => {
                    let update = StatsUpdateEvent {
                        pool_hashrate: pool_stats.pool_hashrate,
                        network_difficulty: pool_stats.network_difficulty,
                        active_miners: pool_stats.active_miners,
                        blocks_found_total: pool_stats.blocks_found_total,
                        blocks_found_24h: pool_stats.blocks_found_24h,
                        blocks_found_7d: pool_stats.blocks_found_7d,
                    };
                    events_tx_clone.send(DashboardEvent::StatsUpdate(update));
                }
                Err(e) => {
                    error!(error = %e, "failed to get pool stats for broadcast");
                }
            }
        }
    });
    
    // Spawn cache invalidation listener - invalidates cache when events are broadcast
    if let Some(events_rx_for_invalidate) = events_tx.subscribe() {
        let cached_db_invalidate = cached_db.clone();
        tokio::spawn(async move {
            let mut rx = events_rx_for_invalidate;
            while let Ok(event) = rx.recv().await {
                match event {
                    DashboardEvent::BlockFound(_) | DashboardEvent::ShareUpdate(_) => {
                        // Invalidate stats cache so next fetch gets fresh data
                        cached_db_invalidate.invalidate_stats_cache();
                    }
                    DashboardEvent::StatsUpdate(_) => {
                        // Stats updates don't require invalidation - they're the result of a cache fetch
                    }
                }
            }
        });
    }
    
    // Create application state with cached_db for page rendering
    let state = AppState::new(db.clone(), stats, pool_config, config, diff_cache, events_tx, cached_db);

    // Build router with all routes
    let app = Router::new()
        // API endpoints (JSON) - nested under /api
        .nest("/api", routes::api::api_routes())
        // Page endpoints (HTML)
        .merge(routes::pages::page_routes())
        // WebSocket endpoint for real-time updates
        .route("/ws", get(routes::ws::ws_handler))
        // Static assets
        .nest_service("/static/http", ServeDir::new("static/http"))
        // 404 fallback
        .fallback(not_found_handler)
        // Add middleware layers
        .layer(CompressionLayer::new())
        .layer(TraceLayer::new_for_http())
        // Add application state (must be last before serve)
        .with_state(state);

    // Bind to address
    let listener = TcpListener::bind(&bind).await?;

    // Log startup
    info!(
        bind = %bind,
        http_enabled = true,
        "HTTP dashboard listening"
    );

    // Serve requests
    if let Err(e) = axum::serve(listener, app).await {
        error!(error = %e, "HTTP dashboard server failed");
        return Err(e.into());
    }

    Ok(())
}

/// Handle 404 Not Found errors
async fn not_found_handler(State(state): State<AppState>) -> impl axum::response::IntoResponse {
    error_response(&state, StatusCode::NOT_FOUND, "Page Not Found", "The page you're looking for doesn't exist.")
}
