use super::{SignedBatchData, Signer};
use anyhow::Result;
use async_trait::async_trait;

/// Signer that delegates payout transaction signing to an external webhook.
///
/// Posts the payout plan as JSON to a configured URL and returns the txid
/// from the response. The external service is responsible for building,
/// signing, and broadcasting the transaction.
///
/// This is a scaffold — full retry/poll logic will be added later.
pub struct ExternalSigner {
    /// Webhook URL to POST payout plans to.
    webhook_url: String,
    /// Shared HTTP client.
    client: reqwest::Client,
}

impl ExternalSigner {
    /// Create a new external signer pointing at the given webhook URL.
    pub fn new(webhook_url: String) -> Self {
        Self {
            webhook_url,
            client: reqwest::Client::new(),
        }
    }
}

#[async_trait]
impl Signer for ExternalSigner {
    async fn sign_and_submit(&self, data: &SignedBatchData) -> Result<String> {
        // Build the JSON payload — everything the external service needs
        // to construct, sign, and broadcast the payout transaction.
        let outputs: Vec<serde_json::Value> = data
            .plan
            .outputs
            .iter()
            .map(|o| {
                serde_json::json!({
                    "payout_address": o.payout_address,
                    "worker_id": o.worker_id,
                    "amount": o.amount,
                    "dust_carried_forward": o.dust_carried_forward,
                })
            })
            .collect();

        let payload = serde_json::json!({
            "retry_key": data.plan.retry_key,
            "block_hash": data.plan.block_hash,
            "block_height": data.plan.block_height,
            "gross_reward": data.plan.gross_reward,
            "pool_fee_amount": data.plan.pool_fee_amount,
            "pool_fee_address": data.plan.pool_fee_address,
            "coinbase_txid": data.coinbase_txid,
            "coinbase_vout": data.coinbase_vout,
            "coinbase_amount": data.coinbase_amount,
            "coinbase_script_pubkey_hex": data.coinbase_script_pubkey_hex,
            "outputs": outputs,
        });

        let response = self
            .client
            .post(&self.webhook_url)
            .json(&payload)
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("external signer HTTP request failed: {}", e))?;

        let status = response.status();
        if !status.is_success() {
            anyhow::bail!("external signer returned HTTP {}", status.as_u16());
        }

        let body: serde_json::Value = response
            .json()
            .await
            .map_err(|e| anyhow::anyhow!("external signer returned invalid JSON: {}", e))?;

        let txid = body
            .get("txid")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow::anyhow!("external signer response missing 'txid' field"))?;

        Ok(txid.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::payout::plan::{PayoutOutput, PayoutPlan};
    use std::sync::Arc;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    /// Helper: spawn a mock webhook server that returns a canned response.
    /// Returns the URL and a shared buffer capturing the POST body.
    async fn spawn_mock_webhook(
        response: serde_json::Value,
    ) -> (String, Arc<std::sync::Mutex<Option<String>>>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let url = format!("http://{}", addr);

        let captured = Arc::new(std::sync::Mutex::new(None::<String>));
        let captured_clone = captured.clone();

        tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut buf = vec![0u8; 8192];
            let n = stream.read(&mut buf).await.unwrap();
            let request = String::from_utf8_lossy(&buf[..n]).to_string();
            *captured_clone.lock().unwrap() = Some(request);

            let response_body = serde_json::to_string(&response).unwrap();
            let http_response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
                response_body.len(),
                response_body,
            );
            stream.write_all(http_response.as_bytes()).await.unwrap();
            stream.flush().await.unwrap();
        });

        (url, captured)
    }

    /// Helper: generate a valid Lotus address from a dummy pubkey hash.
    fn lotus_addr_from_hash(index: u8) -> String {
        use bitcoinsuite_core::{LotusAddress, Net, Script, ShaRmd160};
        let hash = ShaRmd160::new([index; 20]);
        let script = Script::p2pkh(&hash);
        let addr = LotusAddress::new("lotus", Net::Mainnet, script);
        addr.as_str().to_string()
    }

    fn test_signed_batch_data() -> SignedBatchData {
        let miner1 = lotus_addr_from_hash(1);
        let miner2 = lotus_addr_from_hash(2);
        let fee_addr = lotus_addr_from_hash(0xaa);

        SignedBatchData {
            plan: PayoutPlan {
                round_id: 1,
                block_height: 1000,
                block_hash: "0000000000000000000000000000000000000000000000000000000000000000"
                    .to_string(),
                network_difficulty: 100.0,
                total_work_units: 5000.0,
                gross_reward: 50000,
                pool_fee_amount: 500,
                pool_fee_address: Some(fee_addr),
                outputs: vec![
                    PayoutOutput {
                        payout_address: miner1,
                        worker_id: 1,
                        amount: 30000,
                        dust_carried_forward: 0,
                    },
                    PayoutOutput {
                        payout_address: miner2,
                        worker_id: 2,
                        amount: 19500,
                        dust_carried_forward: 0,
                    },
                ],
                dust_carried_forward_total: 0,
                retry_key: "test:2".to_string(),
            },
            coinbase_txid: "0000000000000000000000000000000000000000000000000000000000000001"
                .to_string(),
            coinbase_vout: 0,
            coinbase_amount: 50000,
            coinbase_script_pubkey_hex: "76a914000000000000000000000000000000000000000088ac"
                .to_string(),
        }
    }

    /// ExternalSigner POSTs payout plan to webhook and returns txid from response.
    #[tokio::test]
    async fn test_external_signer_posts_to_webhook() {
        let expected_txid = "ext-txid-12345";
        let (url, captured) = spawn_mock_webhook(serde_json::json!({
            "txid": expected_txid,
        }))
        .await;

        let signer = ExternalSigner::new(url);
        let data = test_signed_batch_data();
        let txid = signer
            .sign_and_submit(&data)
            .await
            .expect("external sign_and_submit should succeed");

        assert_eq!(txid, expected_txid);

        // Verify the POST body contains the payout plan data
        let body = captured
            .lock()
            .unwrap()
            .take()
            .expect("mock webhook should have received a request");
        assert!(body.contains("POST"), "should be a POST request");
        assert!(body.contains("retry_key"), "body should contain retry_key");
        assert!(
            body.contains("\"test:2\""),
            "body should contain the batch's retry_key value"
        );
        assert!(
            body.contains("coinbase_txid"),
            "body should contain coinbase_txid"
        );
        assert!(
            body.contains("outputs"),
            "body should contain outputs array"
        );
        assert!(
            body.contains("30000"),
            "body should contain miner payout amounts"
        );
    }

    /// ExternalSigner returns an error when the webhook returns non-200.
    #[tokio::test]
    async fn test_external_signer_handles_http_error() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let url = format!("http://{}", addr);

        tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut buf = vec![0u8; 4096];
            stream.read(&mut buf).await.unwrap();
            let http_response = "HTTP/1.1 500 Internal Server Error\r\nContent-Length: 0\r\n\r\n";
            stream.write_all(http_response.as_bytes()).await.unwrap();
            stream.flush().await.unwrap();
        });

        let signer = ExternalSigner::new(url);
        let data = test_signed_batch_data();
        let err = signer.sign_and_submit(&data).await.unwrap_err();
        assert!(
            err.to_string().contains("500"),
            "error should mention HTTP 500, got: {}",
            err
        );
    }

    /// ExternalSigner returns an error when the response is missing 'txid'.
    #[tokio::test]
    async fn test_external_signer_handles_missing_txid() {
        let (url, _captured) = spawn_mock_webhook(serde_json::json!({
            "status": "accepted",
        }))
        .await;

        let signer = ExternalSigner::new(url);
        let data = test_signed_batch_data();
        let err = signer.sign_and_submit(&data).await.unwrap_err();
        assert!(
            err.to_string().contains("txid"),
            "error should mention missing txid, got: {}",
            err
        );
    }
}
