//! HTTP Dashboard Server
//!
//! Public-facing HTTP server for pool statistics and miner information.
//!
//! This module provides:
//! - JSON API endpoints for pool data (`/api/*`)
//! - HTML pages for user-facing dashboard (`/`)
//! - Static asset serving (CSS, JS, images)
//!
//! Unlike the Operator API (`crate::api`), this server:
//! - Requires NO authentication for public endpoints
//! - Serves both HTML and JSON
//! - Provides read-only access to accounting data

mod db;
pub mod models;
mod routes;
mod server;
mod state;

pub use db::PublicDb;
pub use server::start_http_dashboard;
pub use state::AppState;
