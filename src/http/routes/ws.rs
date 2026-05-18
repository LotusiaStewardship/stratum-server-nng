//! WebSocket Handler - Real-time stats updates
//!
//! Provides live pool statistics via WebSocket connection.
//! Clients connect to /ws and receive stats updates every 5 seconds.

use axum::{
    extract::{ws::{Message, WebSocket, WebSocketUpgrade}, State},
    response::IntoResponse,
};
use crate::http::AppState;
use futures_util::{SinkExt, StreamExt};
use tracing::{info, error, warn};

/// WebSocket upgrade handler
pub async fn ws_handler(
    ws: WebSocketUpgrade,
    State(state): State<AppState>,
) -> impl IntoResponse {
    ws.on_upgrade(|socket| handle_socket(socket, state))
}

/// Handle individual WebSocket connections
async fn handle_socket(socket: WebSocket, state: AppState) {
    let (mut sender, mut receiver) = socket.split();
    
    // Subscribe to dashboard events
    let Some(mut events_rx) = state.events_tx.subscribe() else {
        warn!("WebSocket client connected but events disabled");
        return;
    };
    
    info!("WebSocket client connected");
    
    loop {
        tokio::select! {
            // Receive dashboard events from broadcast channel
            Ok(event) = events_rx.recv() => {
                match serde_json::to_string(&event) {
                    Ok(msg) => {
                        if let Err(e) = sender.send(Message::Text(msg)).await {
                            warn!(error = %e, "failed to send event, client may have disconnected");
                            break;
                        }
                    }
                    Err(e) => {
                        error!(error = %e, "failed to serialize event");
                    }
                }
            }
            msg = receiver.next() => {
                match msg {
                    Some(Ok(Message::Ping(data))) => {
                        if let Err(e) = sender.send(Message::Pong(data)).await {
                            warn!(error = %e, "failed to send pong");
                            break;
                        }
                    }
                    Some(Ok(Message::Close(_))) => {
                        info!("WebSocket client disconnected");
                        break;
                    }
                    Some(Err(e)) => {
                        error!(error = %e, "WebSocket error");
                        break;
                    }
                    None => {
                        info!("WebSocket stream ended");
                        break;
                    }
                    _ => {}
                }
            }
        }
    }
    
    info!("WebSocket connection closed");
}
