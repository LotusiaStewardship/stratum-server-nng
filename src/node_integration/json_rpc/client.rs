use anyhow::Result;
use serde_json::Value;

/// Result of a submitblock call to lotusd.
#[derive(Debug, Clone, PartialEq)]
pub struct SubmitBlockResult {
    pub accepted: bool,
    pub block_hash: Option<String>,
    pub error: Option<String>,
}

/// JSON-RPC HTTP client for communicating with lotusd.
///
/// Supports Basic Auth and standard JSON-RPC 2.0 request/response format.
/// Single `reqwest::Client` is shared across all requests.
pub struct JsonRpcClient {
    client: reqwest::Client,
    url: String,
    rpc_user: String,
    rpc_pass: String,
}

impl JsonRpcClient {
    /// Create a new JSON-RPC client.
    pub fn new(url: &str, rpc_user: &str, rpc_pass: &str) -> Self {
        Self {
            client: reqwest::Client::new(),
            url: url.to_string(),
            rpc_user: rpc_user.to_string(),
            rpc_pass: rpc_pass.to_string(),
        }
    }

    /// Submit a mined block to lotusd via `submitblock` JSON-RPC method.
    ///
    /// `mined_block_hex` is the raw serialized block in hex encoding.
    /// Returns the result indicating whether the block was accepted.
    pub async fn submitblock(&self, mined_block_hex: &str) -> Result<SubmitBlockResult> {
        let response = self
            .call(
                "submitblock",
                vec![Value::String(mined_block_hex.to_string())],
            )
            .await?;

        // lotusd's submitblock returns null on success, or an error object on failure
        match response {
            Value::Null => Ok(SubmitBlockResult {
                accepted: true,
                block_hash: None,
                error: None,
            }),
            Value::String(s) => {
                // May return a string description (e.g., "duplicate", "inconclusive")
                Ok(SubmitBlockResult {
                    accepted: false,
                    block_hash: None,
                    error: Some(s),
                })
            }
            other => {
                // Unexpected response format
                Ok(SubmitBlockResult {
                    accepted: false,
                    block_hash: None,
                    error: Some(format!("unexpected response: {}", other)),
                })
            }
        }
    }

    /// Get the current chain tip block count.
    pub async fn getblockcount(&self) -> Result<i32> {
        let response = self.call("getblockcount", vec![]).await?;
        let count = response.as_i64().ok_or_else(|| {
            anyhow::anyhow!("getblockcount: unexpected response type: {}", response)
        })?;
        Ok(count as i32)
    }

    /// Get the block hash at a given height via `getblockhash`.
    /// Returns `Ok(None)` if the height is above the chain tip (error code -8).
    pub async fn getblockhash(&self, height: i32) -> Result<Option<String>> {
        let response = self
            .call("getblockhash", vec![serde_json::json!(height)])
            .await;

        match response {
            Ok(val) => {
                // On success, getblockhash returns a hex string
                match val.as_str() {
                    Some(hash) => Ok(Some(hash.to_string())),
                    None => {
                        anyhow::bail!("getblockhash: unexpected response type: {}", val);
                    }
                }
            }
            Err(e) => {
                // Check if error is "Block not found" (height above tip, error code -8)
                let err_str = e.to_string();
                if err_str.contains("-8") || err_str.contains("Block not found") {
                    return Ok(None);
                }
                Err(e)
            }
        }
    }

    /// Get block data by hash, with transaction details (verbosity=2).
    ///
    /// Returns the full block JSON including the tx list. The first tx is the
    /// coinbase transaction. Used by the payout signer to resolve coinbase txid.
    pub async fn get_block(&self, block_hash: &str) -> Result<Value> {
        self.call(
            "getblock",
            vec![Value::String(block_hash.to_string()), serde_json::json!(2)],
        )
        .await
    }

    /// Get a raw transaction by txid, with verbose details (verbosity=true).
    ///
    /// Returns the transaction JSON including `vout` array with `scriptPubKey`.
    /// Used by the payout signer to get the coinbase output script and amount.
    pub async fn get_raw_transaction(&self, txid: &str) -> Result<Value> {
        self.call(
            "getrawtransaction",
            vec![Value::String(txid.to_string()), serde_json::json!(true)],
        )
        .await
    }

    /// Broadcast a signed raw transaction via `sendrawtransaction` JSON-RPC method.
    ///
    /// `signed_tx_hex` is the raw serialized transaction in hex encoding.
    /// Returns the transaction ID (txid) on success.
    pub async fn send_raw_transaction(&self, signed_tx_hex: &str) -> Result<String> {
        let response = self
            .call(
                "sendrawtransaction",
                vec![Value::String(signed_tx_hex.to_string())],
            )
            .await?;

        match response {
            Value::String(txid) => Ok(txid),
            Value::Null => {
                anyhow::bail!("sendrawtransaction returned null")
            }
            other => {
                anyhow::bail!("sendrawtransaction: unexpected response type: {}", other)
            }
        }
    }

    /// Make a generic JSON-RPC 2.0 call.
    ///
    /// Formats the request, sends it via HTTP POST with Basic Auth,
    /// parses the response, and returns the result value or an error.
    async fn call(&self, method: &str, params: Vec<Value>) -> Result<Value> {
        let request_body = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": method,
            "params": params,
        });

        let response = self
            .client
            .post(&self.url)
            .basic_auth(&self.rpc_user, Some(&self.rpc_pass))
            .json(&request_body)
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("JSON-RPC HTTP error: {}", e))?;

        let status = response.status();
        let body: Value = response
            .json()
            .await
            .map_err(|e| anyhow::anyhow!("JSON-RPC response parse error: {}", e))?;

        // Check for JSON-RPC error
        if let Some(error) = body.get("error").and_then(|e| e.as_object()) {
            let code = error.get("code").and_then(|c| c.as_i64()).unwrap_or(-1);
            let message = error
                .get("message")
                .and_then(|m| m.as_str())
                .unwrap_or("unknown error");
            return Err(anyhow::anyhow!(
                "JSON-RPC error (code={}, http={}): {}",
                code,
                status.as_u16(),
                message
            ));
        }

        // Extract result
        body.get("result")
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("JSON-RPC response missing 'result' field: {}", body))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    /// Helper: spawn a mock JSON-RPC server that returns a canned response.
    /// Returns the URL for the client to connect to.
    async fn spawn_mock_server(response: serde_json::Value) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let url = format!("http://{}", addr);

        tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut buf = vec![0u8; 4096];
            let n = stream.read(&mut buf).await.unwrap();
            let _request = String::from_utf8_lossy(&buf[..n]);

            let response_body = serde_json::to_string(&response).unwrap();
            let http_response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
                response_body.len(),
                response_body,
            );
            stream.write_all(http_response.as_bytes()).await.unwrap();
            stream.flush().await.unwrap();
        });

        url
    }

    #[tokio::test]
    async fn test_submitblock_success() {
        // lotusd returns null on successful block submission
        let url = spawn_mock_server(json!({
            "result": null,
            "error": null,
            "id": 1
        }))
        .await;

        let client = JsonRpcClient::new(&url, "lotus", "lotus");
        let result = client.submitblock("deadbeef").await.unwrap();

        assert!(result.accepted, "block should be accepted");
        assert!(result.error.is_none(), "no error expected");
    }

    #[tokio::test]
    async fn test_submitblock_rejected_duplicate() {
        // lotusd returns a string description on rejection
        let url = spawn_mock_server(json!({
            "result": "duplicate",
            "error": null,
            "id": 1
        }))
        .await;

        let client = JsonRpcClient::new(&url, "lotus", "lotus");
        let result = client.submitblock("deadbeef").await.unwrap();

        assert!(!result.accepted, "duplicate should not be accepted");
        assert_eq!(result.error.as_deref(), Some("duplicate"));
    }

    #[tokio::test]
    async fn test_getblockcount_success() {
        let url = spawn_mock_server(json!({
            "result": 1292529,
            "error": null,
            "id": 1
        }))
        .await;

        let client = JsonRpcClient::new(&url, "lotus", "lotus");
        let count = client.getblockcount().await.unwrap();

        assert_eq!(count, 1292529);
    }

    #[tokio::test]
    async fn test_json_rpc_error_response() {
        let url = spawn_mock_server(json!({
            "result": null,
            "error": {
                "code": -32601,
                "message": "Method not found"
            },
            "id": 1
        }))
        .await;

        let client = JsonRpcClient::new(&url, "lotus", "lotus");
        let err = client.submitblock("deadbeef").await.unwrap_err();

        assert!(err.to_string().contains("-32601"));
        assert!(err.to_string().contains("Method not found"));
    }

    #[tokio::test]
    async fn test_http_connection_refused() {
        // Connect to a port that nothing is listening on
        let client = JsonRpcClient::new("http://127.0.0.1:1", "lotus", "lotus");
        let err = client.submitblock("deadbeef").await.unwrap_err();

        assert!(err.to_string().contains("HTTP error"));
    }
}
