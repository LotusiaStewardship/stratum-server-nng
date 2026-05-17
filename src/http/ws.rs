//! WebSocket real-time updates for HTTP dashboard
//!
//! Provides a WebSocket endpoint at /ws that broadcasts pool stats
//! to connected clients in real-time.

use axum::{
    extract::{ws::{Message, WebSocket, WebSocketUpgrade}, State},
    response::IntoResponse,
};
use futures_util::{sink::SinkExt, stream::StreamExt};
use tokio::sync::broadcast;
use serde::Serialize;
use tracing::{info, debug, error};

use super::state::AppState;

/// Stats update message sent to WebSocket clients
#[derive(Serialize, Clone, Debug)]
pub struct WsStatsUpdate {
    pub pool_hashrate: f64,
    pub network_difficulty: f64,
    pub active_miners: u64,
    pub blocks_found_total: u64,
    pub blocks_found_24h: u64,
    pub blocks_found_7d: u64,
}

/// Broadcast channel size for WebSocket stats updates
const BROADCAST_CHANNEL_SIZE: usize = 16;

/// Create a new broadcast channel for stats updates
pub fn create_stats_channel() -> broadcast::Sender<WsStatsUpdate> {
    let (tx, _rx) = broadcast::channel(BROADCAST_CHANNEL_SIZE);
    tx
}

/// WebSocket handler for /ws endpoint
pub async fn ws_handler(
    ws: WebSocketUpgrade,
    State(state): State<AppState>,
) -> impl IntoResponse {
    ws.on_upgrade(|socket| handle_socket(socket, state))
}

/// Handle individual WebSocket connection
async fn handle_socket(socket: WebSocket, state: AppState) {
    let (mut sender, mut receiver) = socket.split();
    
    // Subscribe to stats broadcast channel
    let mut rx = state.stats_tx.subscribe();
    
    // Spawn task to receive messages from client (we ignore them, just keep connection alive)
    let recv_task = tokio::spawn(async move {
        while let Some(msg) = receiver.next().await {
            match msg {
                Ok(Message::Ping(data)) => {
                    debug!("WebSocket ping received");
                    // Pongs are handled automatically by axum
                }
                Ok(Message::Close(_)) => {
                    debug!("WebSocket close received");
                    break;
                }
                Ok(Message::Text(text)) => {
                    debug!("Received text message: {}", text);
                }
                Ok(Message::Binary(data)) => {
                    debug!("Received binary message: {} bytes", data.len());
                }
                Err(e) => {
                    error!("WebSocket error: {}", e);
                    break;
                }
                _ => {}
            }
        }
    });

    // Send initial stats immediately on connect
    if let Ok(stats) = build_stats_update(&state).await {
        if let Ok(json) = serde_json::to_string(&stats) {
            if sender.send(Message::Text(json)).await.is_err() {
                debug!("Failed to send initial stats");
                recv_task.abort();
                return;
            }
        }
    }

    // Listen for broadcast updates and forward to WebSocket
    let send_task = tokio::spawn(async move {
        while let Ok(stats) = rx.recv().await {
            match serde_json::to_string(&stats) {
                Ok(json) => {
                    if sender.send(Message::Text(json)).await.is_err() {
                        debug!("Failed to send stats update, client disconnected");
                        break;
                    }
                }
                Err(e) => {
                    error!("Failed to serialize stats: {}", e);
                }
            }
        }
    });

    // Wait for either task to complete
    tokio::select! {
        _ = recv_task => {
            debug!("WebSocket receive task completed");
        }
        _ = send_task => {
            debug!("WebSocket send task completed");
        }
    }
}

/// Build stats update from current state
async fn build_stats_update(state: &AppState) -> Result<WsStatsUpdate, anyhow::Error> {
    let db = &state.db;
    
    // Calculate pool hashrate (5min window)
    let pool_hashrate = db.calculate_pool_hashrate()?;
    
    // Get network difficulty from diff cache
    let network_difficulty = state.diff_cache.get_network_difficulty().unwrap_or(0.0);
    
    // Count active miners (seen in last 5 minutes)
    let active_miners = db.count_active_miners(300)? as u64;
    
    // Get block statistics
    let blocks_found_total = db.count_total_blocks_found()? as u64;
    let blocks_found_24h = db.count_blocks_found_hours(24)? as u64;
    let blocks_found_7d = db.count_blocks_found_hours(168)? as u64;
    
    Ok(WsStatsUpdate {
        pool_hashrate,
        network_difficulty,
        active_miners,
        blocks_found_total,
        blocks_found_24h,
        blocks_found_7d,
    })
}

/// Background task that periodically broadcasts stats updates
pub async fn stats_broadcast_loop(
    state: AppState,
    stats_tx: broadcast::Sender<WsStatsUpdate>,
    interval_secs: u64,
) {
    let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(interval_secs));
    
    loop {
        interval.tick().await;
        
        match build_stats_update(&state).await {
            Ok(stats) => {
                // Send to broadcast channel (ignore if no subscribers)
                let _ = stats_tx.send(stats);
                debug!("Broadcast stats update");
            }
            Err(e) => {
                error!("Failed to build stats update: {}", e);
            }
        }
    }
}
