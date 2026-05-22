use anyhow::Result;
use async_trait::async_trait;
use std::sync::Arc;

use bitcoinsuite_core::{
    ecc::{Ecc, SecKey, PubKey},
    BitcoinCode, BytesMut, Hashed, LotusAddress, OutPoint, Script, SequenceNo, Sha256d, SigHashType,
    TxInput, TxOutput,
    P2PKHSignatory, SignData, SignField, TxBuilder, TxBuilderInput, TxBuilderOutput,
};
use bitcoinsuite_ecc_secp256k1::EccSecp256k1;

use crate::node_integration::JsonRpcClient;
use super::{SignedBatchData, Signer};

/// Signer that builds, signs, and broadcasts payout transactions using an
/// in-process private key.
///
/// The private key is provided as 64-char hex at construction time.
/// Transaction building uses `bitcoinsuite-core` types. Broadcasting goes
/// through the configured Lotus JSON-RPC node.
pub struct InternalSigner {
    seckey: SecKey,
    pubkey: PubKey,
    rpc_client: Arc<JsonRpcClient>,
}

impl InternalSigner {
    /// Create a new internal signer.
    ///
    /// `private_key_hex` — 64-character hex string (32 bytes).
    /// `rpc_client` — shared JSON-RPC client for broadcasting.
    pub fn new(private_key_hex: &str, rpc_client: Arc<JsonRpcClient>) -> Result<Self> {
        let key_bytes = hex::decode(private_key_hex)
            .map_err(|e| anyhow::anyhow!("invalid private key hex: {}", e))?;
        if key_bytes.len() != 32 {
            anyhow::bail!(
                "private key must be 32 bytes (64 hex chars), got {} bytes",
                key_bytes.len()
            );
        }
        let mut arr = [0u8; 32];
        arr.copy_from_slice(&key_bytes);

        let ecc = EccSecp256k1::default();
        let seckey = ecc
            .seckey_from_array(arr)
            .map_err(|e| anyhow::anyhow!("invalid private key: {}", e))?;
        let pubkey = ecc.derive_pubkey(&seckey);
        Ok(Self {
            seckey,
            pubkey,
            rpc_client,
        })
    }
}

#[async_trait]
impl Signer for InternalSigner {
    async fn sign_and_submit(&self, data: &SignedBatchData) -> Result<String> {
        let ecc = EccSecp256k1::default();

        // Build and sign the payout transaction
        let tx_builder = self.build_payout_tx(data)?;
        let signed_tx = tx_builder
            .sign(&ecc, 0, 546)
            .map_err(|e| anyhow::anyhow!("transaction signing failed: {}", e))?;

        // Serialize to hex
        let mut buf = BytesMut::new();
        signed_tx.ser_to(&mut buf);
        let tx_hex = hex::encode(buf.freeze());

        // Broadcast via JSON-RPC and return txid
        let txid = self.rpc_client.send_raw_transaction(&tx_hex).await?;
        Ok(txid)
    }
}

impl InternalSigner {
    /// Build an unsigned payout transaction with signatories attached.
    fn build_payout_tx(&self, data: &SignedBatchData) -> Result<TxBuilder> {
        // -- Coinbase input --
        let coinbase_txid = Sha256d::from_hex_be(&data.coinbase_txid)
            .map_err(|e| anyhow::anyhow!("invalid coinbase txid: {}", e))?;

        let coinbase_script_bytes = hex::decode(&data.coinbase_script_pubkey_hex)
            .map_err(|e| anyhow::anyhow!("invalid coinbase script hex: {}", e))?;
        let coinbase_script = Script::from_slice(&coinbase_script_bytes);

        let signatory = P2PKHSignatory {
            seckey: self.seckey.clone(),
            pubkey: self.pubkey.clone(),
            sig_hash_type: SigHashType::ALL_BIP143,
        };

        let tx_input = TxInput {
            prev_out: OutPoint {
                txid: coinbase_txid,
                out_idx: data.coinbase_vout,
            },
            script: Script::default(),
            sequence: SequenceNo::finalized(),
            sign_data: Some(SignData::new(vec![
                SignField::Value(data.coinbase_amount),
                SignField::OutputScript(coinbase_script),
            ])),
        };

        let mut builder = TxBuilder {
            version: 2,
            inputs: vec![TxBuilderInput::new(tx_input, Box::new(signatory))],
            outputs: Vec::new(),
            lock_time: 0,
        };

        // -- Miner payout outputs --
        for output in &data.plan.outputs {
            let addr_script = address_to_script(&output.payout_address)?;
            builder
                .outputs
                .push(TxBuilderOutput::Fixed(TxOutput {
                    value: output.amount,
                    script: addr_script,
                }));
        }

        // -- Pool fee output (if configured) --
        if let Some(ref fee_address) = data.plan.pool_fee_address {
            if data.plan.pool_fee_amount > 0 {
                let fee_script = address_to_script(fee_address)?;
                builder
                    .outputs
                    .push(TxBuilderOutput::Fixed(TxOutput {
                        value: data.plan.pool_fee_amount,
                        script: fee_script,
                    }));
            }
        }

        Ok(builder)
    }
}

/// Convert a Lotus address string to its locking Script.
fn address_to_script(address: &str) -> Result<Script> {
    let lotus_addr: LotusAddress = address
        .parse()
        .map_err(|e| anyhow::anyhow!("invalid payout address '{}': {}", address, e))?;
    Ok(lotus_addr.script().clone())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node_integration::JsonRpcClient;
    use crate::payout::plan::PayoutPlan;
    use serde_json::json;
    use tokio::net::TcpListener;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    /// Helper: spawn a mock JSON-RPC server that returns a canned response.
    /// Returns the URL and a shared buffer capturing the request body.
    async fn spawn_mock_rpc(
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

    /// Generate a valid Lotus address from a dummy pubkey hash.
    fn lotus_addr_from_hash(index: u8) -> String {
        use bitcoinsuite_core::ShaRmd160;
        let hash = ShaRmd160::new([index; 20]);
        let script = Script::p2pkh(&hash);
        let addr = LotusAddress::new("lotus", bitcoinsuite_core::Net::Mainnet, script);
        addr.as_str().to_string()
    }

    /// Create a minimal SignedBatchData for testing.
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
                    crate::payout::plan::PayoutOutput {
                        payout_address: miner1,
                        worker_id: 1,
                        amount: 30000,
                        dust_carried_forward: 0,
                    },
                    crate::payout::plan::PayoutOutput {
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

    /// InternalSigner builds a signed transaction and broadcasts it via RPC.
    /// The mock RPC server captures the request so we can verify the tx hex.
    #[tokio::test]
    async fn test_internal_signer_signs_and_broadcasts() {
        let expected_txid = "a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0c1d2e3f4a5b6c7d8e9f0a1b2";
        let (url, captured) = spawn_mock_rpc(json!({
            "result": expected_txid,
            "error": null,
            "id": 1
        }))
        .await;

        let rpc_client = Arc::new(JsonRpcClient::new(
            &url,
            "lotus",
            "lotus",
        ));

        let signer = InternalSigner::new(
            "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f",
            rpc_client,
        )
        .expect("InternalSigner construction should succeed");

        let data = test_signed_batch_data();
        let txid = signer.sign_and_submit(&data).await
            .expect("sign_and_submit should succeed");

        assert_eq!(txid, expected_txid, "should return txid from RPC response");

        // Verify the mock RPC received a sendrawtransaction call with valid hex
        let captured_body = captured.lock().unwrap().take()
            .expect("mock RPC should have received a request");
        assert!(
            captured_body.contains("sendrawtransaction"),
            "RPC call should be sendrawtransaction"
        );
        assert!(
            captured_body.contains("params"),
            "should have params"
        );

        // Verify the sent hex is a valid serialized transaction by checking
        // it starts with the version field (02000000 for version 2)
        let captured_json: serde_json::Value = serde_json::from_str(
            extract_json_body(&captured_body).unwrap_or("{}")
        ).unwrap_or_default();
        let params = captured_json["params"].as_array()
            .expect("params should be an array");
        let tx_hex = params[0].as_str()
            .expect("first param should be tx hex");
        assert!(!tx_hex.is_empty(), "tx hex should not be empty");
        // Version 2 serialized in little-endian = "02000000"
        assert!(
            tx_hex.starts_with("02000000"),
            "tx hex should start with version=2 (02000000), got: {}...",
            &tx_hex[..std::cmp::min(16, tx_hex.len())]
        );
        // The hex should be a reasonable length for a tx with 1 input + 3 outputs
        assert!(
            tx_hex.len() > 200,
            "tx hex should be substantial (got {} chars)",
            tx_hex.len()
        );
    }

    /// Helper: extract JSON body from an HTTP request string.
    fn extract_json_body(http_request: &str) -> Option<&str> {
        let parts: Vec<&str> = http_request.split("\r\n\r\n").collect();
        parts.get(1).copied()
    }
}
