//! HTTP Dashboard Server Startup

use axum::Router;
use tower_http::{compression::CompressionLayer, trace::TraceLayer};
use tokio::net::TcpListener;
use anyhow::Result;
use tracing::{info, error};

use super::state::AppState;
use super::routes;

/// Start the HTTP dashboard server
///
/// This function is called from main.rs in a tokio::spawn task.
/// It blocks until the server shuts down or an error occurs.
pub async fn start_http_dashboard(
    bind: String,
    db: crate::accounting::AccountingDb,
    stats: std::sync::Arc<crate::stratum::server::RuntimeStats>,
    pool_config: crate::config::PoolConfig,
    diff_cache: crate::stratum::diff_cache::DifficultyCache,
) -> Result<()> {
    // Create application state
    let state = AppState::new(db.clone(), stats, pool_config, diff_cache);

    // Build router with all routes
    let app = Router::new()
        // API endpoints (JSON) - nested under /api
        .nest("/api", routes::api::api_routes())
        // Page endpoints (HTML) - placeholder for now
        .merge(routes::pages::page_routes())
        // Add application state
        .with_state(state)
        // Add middleware layers
        .layer(CompressionLayer::new())
        .layer(TraceLayer::new_for_http());

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
