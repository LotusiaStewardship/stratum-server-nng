//! HTTP Dashboard Server
//!
//! Public-facing HTTP server for pool statistics and miner information.
//!
//! This module provides:
//! - JSON API endpoints for pool data (`/api/*`)
//! - HTML pages for user-facing dashboard (`/`)
//! - Static asset serving (CSS, JS, images)
//! - Real-time WebSocket updates for live stats
//!
//! Unlike the Operator API (`crate::api`), this server:
//! - Requires NO authentication for public endpoints
//! - Serves both HTML and JSON
//! - Provides read-only access to accounting data

mod db;
pub mod events;
pub mod models;
mod routes;
mod server;
mod state;

pub use db::{PublicDb, CachedDb};
pub use events::{DashboardEvent, DashboardEventSender, BlockFoundEvent, ShareUpdateEvent, StatsUpdateEvent};
pub use server::start_http_dashboard;
pub use state::AppState;
