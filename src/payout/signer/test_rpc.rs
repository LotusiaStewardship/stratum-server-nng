//! Test support: a mock lotusd JSON-RPC server for signer tests.

use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// Extract the body from an HTTP request string.
fn extract_json_body(http_request: &str) -> Option<&str> {
    http_request.split("\r\n\r\n").nth(1)
}

/// Read one HTTP request (headers and body) from a keep-alive stream.
async fn read_http_request(stream: &mut TcpStream) -> Option<String> {
    let mut data = Vec::new();
    let mut buf = [0u8; 8192];
    loop {
        if let Some(pos) = data.windows(4).position(|w| w == b"\r\n\r\n") {
            let headers = String::from_utf8_lossy(&data[..pos]).to_ascii_lowercase();
            let content_length = headers
                .lines()
                .find_map(|l| l.strip_prefix("content-length:"))
                .and_then(|v| v.trim().parse::<usize>().ok())
                .unwrap_or(0);
            if data.len() >= pos + 4 + content_length {
                return Some(
                    String::from_utf8_lossy(&data[..pos + 4 + content_length]).to_string(),
                );
            }
        }
        let n = stream.read(&mut buf).await.ok()?;
        if n == 0 {
            return None;
        }
        data.extend_from_slice(&buf[..n]);
    }
}

/// Helper: spawn a mock JSON-RPC server. `respond` maps
/// `(method, params, raw HTTP request)` to the full JSON-RPC response
/// object; it runs before the response is sent. Every request is captured
/// in order. Serves any number of requests and connections.
pub(crate) async fn spawn_rpc_server<F>(respond: F) -> (String, Arc<std::sync::Mutex<Vec<String>>>)
where
    F: Fn(&str, &serde_json::Value, &str) -> serde_json::Value + Send + Sync + 'static,
{
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let captured = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
    let captured_clone = captured.clone();
    let respond = Arc::new(respond);

    tokio::spawn(async move {
        while let Ok((mut stream, _)) = listener.accept().await {
            let captured = captured_clone.clone();
            let respond = respond.clone();
            tokio::spawn(async move {
                while let Some(request) = read_http_request(&mut stream).await {
                    let body: serde_json::Value =
                        serde_json::from_str(extract_json_body(&request).unwrap_or("{}"))
                            .unwrap_or_default();
                    let method = body["method"].as_str().unwrap_or("").to_string();
                    let response = respond(&method, &body["params"], &request);
                    captured.lock().unwrap().push(request);

                    let response_body = serde_json::to_string(&response).unwrap();
                    let http_response = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
                        response_body.len(),
                        response_body,
                    );
                    if stream.write_all(http_response.as_bytes()).await.is_err() {
                        break;
                    }
                    let _ = stream.flush().await;
                }
            });
        }
    });

    (url, captured)
}
