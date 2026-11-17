use anyhow::Result;
use async_trait::async_trait;
use std::sync::Arc;

use bitcoinsuite_core::{
    ecc::{Ecc, PubKey, SecKey},
    BitcoinCode, Bytes, BytesMut, Hashed, LotusAddress, OutPoint, Script, SequenceNo, Sha256d,
    SigHashType, SignData, SignField, Signatory, TxBuilder, TxBuilderInput, TxBuilderOutput,
    TxInput, TxOutput, UnsignedTxInput,
};
use bitcoinsuite_ecc_secp256k1::EccSecp256k1;

use super::{SignedBatchData, Signer};
use crate::node_integration::JsonRpcClient;
use crate::payout::replay_protection::{
    is_script_verify_rejection, other_fork_value, ReplayProtection,
};

/// Signer that builds, signs, and broadcasts payout transactions using an
/// in-process private key.
///
/// The private key is provided as hex (64 chars) or WIF format at
/// construction time. Transaction building uses `bitcoinsuite-core` types.
/// Broadcasting goes through the configured Lotus JSON-RPC node.
///
/// Signatures commit to the sighash fork value the node will require in the
/// next block (see [`ReplayProtection`]). The value is decided from the chain
/// tip's median time past, fetched right before each signature.
pub struct InternalSigner {
    seckey: SecKey,
    pubkey: PubKey,
    rpc_client: Arc<JsonRpcClient>,
    tx_fee_per_kb: i64,
    replay_protection: ReplayProtection,
}

impl InternalSigner {
    /// Create a new internal signer.
    ///
    /// `private_key` — 32-byte hex string (64 chars) or WIF-encoded secret key.
    /// `rpc_client` — shared JSON-RPC client for broadcasting.
    /// `tx_fee_per_kb` — fee rate in sat/kB for payout transactions.
    /// `replay_protection` — activation time used to pick the sighash fork value.
    pub fn new(
        private_key: &str,
        rpc_client: Arc<JsonRpcClient>,
        tx_fee_per_kb: i64,
        replay_protection: ReplayProtection,
    ) -> Result<Self> {
        let parsed = SecKey::from_hex_or_wif(private_key)
            .map_err(|e| anyhow::anyhow!("invalid private key: {}", e))?;
        let arr: [u8; 32] = parsed
            .as_slice()
            .try_into()
            .expect("SecKey::from_hex_or_wif always returns 32 bytes");

        let ecc = EccSecp256k1::default();
        let seckey = ecc
            .seckey_from_array(arr)
            .map_err(|e| anyhow::anyhow!("invalid private key: {}", e))?;
        let pubkey = ecc.derive_pubkey(&seckey);
        Ok(Self {
            seckey,
            pubkey,
            rpc_client,
            tx_fee_per_kb,
            replay_protection,
        })
    }
}

#[async_trait]
impl Signer for InternalSigner {
    async fn sign_and_submit(&self, data: &SignedBatchData) -> Result<String> {
        ensure_miner_outputs(data)?;

        // Decide the sighash fork value from the tip as the node will judge
        // the next block. Fetched fresh for every batch; never the wall clock.
        let tip_median_time = self.rpc_client.get_tip_median_time().await?;
        let fork_value = self
            .replay_protection
            .fork_value_for_signing(tip_median_time)?;

        let tx_hex = self.sign_payout_tx(data, fork_value)?;
        match self.rpc_client.send_raw_transaction(&tx_hex).await {
            Ok(txid) => Ok(txid),
            Err(e) if is_script_verify_rejection(&e.to_string()) => {
                // The node judged the signature with the other fork value
                // (e.g. its activation time differs from ours). Re-sign the
                // same transaction once with that value; never loop.
                let retry_fork_value = other_fork_value(fork_value);
                tracing::warn!(
                    error = %e,
                    fork_value = format_args!("{:#x}", fork_value),
                    retry_fork_value = format_args!("{:#x}", retry_fork_value),
                    "payout refused by script verification, re-signing once with the other sighash fork value",
                );
                let tx_hex = self.sign_payout_tx(data, retry_fork_value)?;
                self.rpc_client
                    .send_raw_transaction(&tx_hex)
                    .await
                    .map_err(|retry_err| {
                        anyhow::anyhow!(
                            "payout refused with sighash fork value {:#x} ({}) and {:#x} ({})",
                            fork_value,
                            e,
                            retry_fork_value,
                            retry_err,
                        )
                    })
            }
            Err(e) => Err(e),
        }
    }
}

impl InternalSigner {
    /// Build and sign the payout transaction with the given sighash fork
    /// value, and return it serialized as hex.
    fn sign_payout_tx(&self, data: &SignedBatchData, fork_value: u32) -> Result<String> {
        let ecc = EccSecp256k1::default();
        let signed_tx = self
            .build_payout_tx(data, fork_value)?
            .sign(&ecc, self.tx_fee_per_kb, crate::constants::DUST_LIMIT)
            .map_err(|e| anyhow::anyhow!("transaction signing failed: {}", e))?;

        let mut buf = BytesMut::new();
        signed_tx.ser_to(&mut buf);
        Ok(hex::encode(buf.freeze()))
    }

    /// Build an unsigned payout transaction with signatories attached.
    fn build_payout_tx(&self, data: &SignedBatchData, fork_value: u32) -> Result<TxBuilder> {
        ensure_miner_outputs(data)?;

        // -- Coinbase input --
        let coinbase_txid = Sha256d::from_hex_be(&data.coinbase_txid)
            .map_err(|e| anyhow::anyhow!("invalid coinbase txid: {}", e))?;

        let coinbase_script_bytes = hex::decode(&data.coinbase_script_pubkey_hex)
            .map_err(|e| anyhow::anyhow!("invalid coinbase script hex: {}", e))?;
        let coinbase_script = Script::from_slice(&coinbase_script_bytes);

        let signatory = ForkValueP2PKHSignatory {
            seckey: self.seckey.clone(),
            pubkey: self.pubkey,
            sig_hash_type: SigHashType::ALL_BIP143,
            fork_value,
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
            builder.outputs.push(TxBuilderOutput::Fixed(TxOutput {
                value: output.amount,
                script: addr_script,
            }));
        }

        // -- Pool fee output (if configured) — use Leftover so TxBuilder
        // deducts the transaction fee from the pool's share.
        if let Some(ref fee_address) = data.plan.pool_fee_address {
            if data.plan.pool_fee_amount > 0 {
                let fee_script = address_to_script(fee_address)?;
                builder.outputs.push(TxBuilderOutput::Leftover(fee_script));
            }
        }

        Ok(builder)
    }
}

/// Safety guard: refuse to build a tx with zero miner outputs.
/// Without this, a Leftover-only fee output would absorb the entire
/// coinbase value, sending the full block reward to the pool fee address.
fn ensure_miner_outputs(data: &SignedBatchData) -> Result<()> {
    if data.plan.outputs.is_empty() {
        anyhow::bail!(
            "refusing to build payout tx with zero miner outputs (batch has {} payees? pool_fee={})",
            data.plan.outputs.len(),
            data.plan.pool_fee_amount,
        );
    }
    Ok(())
}

/// P2PKH signatory that commits a sighash fork value.
///
/// Same as `bitcoinsuite_core::P2PKHSignatory`, except that the 32-bit sighash
/// type at the end of the BIP143 preimage carries `fork_value` in its upper 24
/// bits, as the node expects once replay protection is active. The signature
/// still ends with the single sighash byte. With `fork_value = 0` it signs
/// the same message as `P2PKHSignatory`.
struct ForkValueP2PKHSignatory {
    seckey: SecKey,
    pubkey: PubKey,
    sig_hash_type: SigHashType,
    fork_value: u32,
}

impl Signatory for ForkValueP2PKHSignatory {
    fn sign_input(
        &self,
        ecc: &dyn Ecc,
        mut input: UnsignedTxInput<'_>,
    ) -> bitcoinsuite_core::Result<()> {
        let preimage = input.sighash_preimage(self.sig_hash_type, None)?;
        let preimage = with_fork_value(&preimage.bytes, self.sig_hash_type, self.fork_value);
        let sighash = Sha256d::digest(preimage).byte_array().clone();
        let sig = ecc.schnorr_sign(&self.seckey, sighash);
        let mut sig_flagged = BytesMut::new();
        sig_flagged.put_bytes(sig);
        sig_flagged.put_slice(&[self.sig_hash_type.to_u32() as u8]);
        *input.input_script_mut() = Script::p2pkh_spend(&self.pubkey, sig_flagged.freeze());
        Ok(())
    }
}

/// Replace the trailing 32-bit sighash type of a BIP143 preimage with
/// `(fork_value << 8) | sig_hash_type`, little-endian.
fn with_fork_value(preimage: &[u8], sig_hash_type: SigHashType, fork_value: u32) -> Bytes {
    let mut bytes = preimage.to_vec();
    let start = bytes.len() - 4;
    let full_type = (fork_value << 8) | (sig_hash_type.to_u32() & 0xff);
    bytes[start..].copy_from_slice(&full_type.to_le_bytes());
    Bytes::from(bytes)
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
    use crate::payout::signer::test_rpc::spawn_rpc_server;
    use serde_json::json;

    /// Chain tip median time well before the default activation time.
    const PRE_ACTIVATION_MTP: i64 = 1_800_000_000;

    /// Helper: spawn a mock JSON-RPC server that returns a canned response.
    /// `getblockchaininfo` is answered with a tip median time before the
    /// replay-protection activation. Returns the URL and a shared buffer
    /// capturing the last other request.
    async fn spawn_mock_rpc(
        response: serde_json::Value,
    ) -> (String, Arc<std::sync::Mutex<Option<String>>>) {
        let captured = Arc::new(std::sync::Mutex::new(None::<String>));
        let captured_clone = captured.clone();
        let (url, _requests) = spawn_rpc_server(move |method, _params, raw| {
            if method == "getblockchaininfo" {
                json!({ "result": { "mediantime": PRE_ACTIVATION_MTP }, "error": null, "id": 1 })
            } else {
                *captured_clone.lock().unwrap() = Some(raw.to_string());
                response.clone()
            }
        })
        .await;

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

        let rpc_client = Arc::new(JsonRpcClient::new(&url, "lotus", "lotus"));

        let signer = InternalSigner::new(
            "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f",
            rpc_client,
            1000,
            ReplayProtection::default(),
        )
        .expect("InternalSigner construction should succeed");

        let data = test_signed_batch_data();
        let txid = signer
            .sign_and_submit(&data)
            .await
            .expect("sign_and_submit should succeed");

        assert_eq!(txid, expected_txid, "should return txid from RPC response");

        // Verify the mock RPC received a sendrawtransaction call with valid hex
        let captured_body = captured
            .lock()
            .unwrap()
            .take()
            .expect("mock RPC should have received a request");
        assert!(
            captured_body.contains("sendrawtransaction"),
            "RPC call should be sendrawtransaction"
        );
        assert!(captured_body.contains("params"), "should have params");

        // Verify the sent hex is a valid serialized transaction by checking
        // it starts with the version field (02000000 for version 2)
        let captured_json: serde_json::Value =
            serde_json::from_str(extract_json_body(&captured_body).unwrap_or("{}"))
                .unwrap_or_default();
        let params = captured_json["params"]
            .as_array()
            .expect("params should be an array");
        let tx_hex = params[0].as_str().expect("first param should be tx hex");
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

    /// InternalSigner can be constructed with a WIF-format private key.
    /// The full sign_and_submit pipeline works identically to hex keys.
    #[tokio::test]
    async fn test_internal_signer_with_wif_key() {
        let expected_txid = "b1b2b3b4b5b6b7b8b9c0c1c2c3c4c5c6c7c8c9d0d1d2d3d4d5d6d7d8d9e0e1e2";
        let (url, captured) = spawn_mock_rpc(json!({
            "result": expected_txid,
            "error": null,
            "id": 1
        }))
        .await;

        let rpc_client = Arc::new(JsonRpcClient::new(&url, "lotus", "lotus"));

        // Testnet WIF private key (from bitcoinsuite-core test fixture)
        let wif_key = "cPymiBZp9Ak8aVAmrnh8TL8E4yoibD61KE7weuhXNbaMsJt2murF";
        let signer = InternalSigner::new(wif_key, rpc_client, 1000, ReplayProtection::default())
            .expect("InternalSigner construction should succeed with WIF key");

        let data = test_signed_batch_data();
        let txid = signer
            .sign_and_submit(&data)
            .await
            .expect("sign_and_submit should succeed with WIF-derived key");

        assert_eq!(txid, expected_txid, "should return txid from RPC response");

        // Verify the mock RPC received a sendrawtransaction call
        let captured_body = captured
            .lock()
            .unwrap()
            .take()
            .expect("mock RPC should have received a request");
        assert!(
            captured_body.contains("sendrawtransaction"),
            "RPC call should be sendrawtransaction"
        );
        assert!(captured_body.contains("params"), "should have params");

        // Verify the tx hex is valid
        let captured_json: serde_json::Value =
            serde_json::from_str(extract_json_body(&captured_body).unwrap_or("{}"))
                .unwrap_or_default();
        let params = captured_json["params"]
            .as_array()
            .expect("params should be an array");
        let tx_hex = params[0].as_str().expect("first param should be tx hex");
        assert!(
            tx_hex.starts_with("02000000"),
            "tx hex should start with version=2 (02000000), got: {}...",
            &tx_hex[..std::cmp::min(16, tx_hex.len())]
        );
        assert!(
            tx_hex.len() > 200,
            "tx hex should be substantial (got {} chars)",
            tx_hex.len()
        );
    }

    /// InternalSigner rejects a payout plan with zero miner outputs — prevents
    /// accidentally sending the entire block reward to the pool fee address.
    #[tokio::test]
    async fn test_internal_signer_rejects_empty_outputs() {
        let expected_txid = "deadbeef00000000000000000000000000000000000000000000000000000000";
        let (url, _captured) = spawn_mock_rpc(json!({
            "result": expected_txid,
            "error": null,
            "id": 1
        }))
        .await;

        let rpc_client = Arc::new(JsonRpcClient::new(&url, "lotus", "lotus"));
        let signer = InternalSigner::new(
            "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f",
            rpc_client,
            1000,
            ReplayProtection::default(),
        )
        .expect("InternalSigner construction should succeed");

        let mut data = test_signed_batch_data();
        data.plan.outputs = vec![];
        let err = signer.sign_and_submit(&data).await.unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("zero miner outputs"),
            "error should mention 'zero miner outputs', got: {}",
            msg,
        );
    }

    /// InternalSigner rejects invalid private key material with a clear error.
    #[tokio::test]
    async fn test_internal_signer_rejects_invalid_key() {
        let (url, _captured) = spawn_mock_rpc(json!(null)).await;
        let rpc_client = Arc::new(JsonRpcClient::new(&url, "lotus", "lotus"));

        let result = InternalSigner::new(
            "not-a-valid-key",
            rpc_client,
            1000,
            ReplayProtection::default(),
        );
        match result {
            Ok(_) => panic!("InternalSigner should reject garbage input"),
            Err(err) => {
                let msg = err.to_string();
                assert!(
                    msg.contains("invalid private key"),
                    "error should mention 'invalid private key', got: {}",
                    msg
                );
            }
        }
    }

    const TEST_KEY: &str = "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f";
    const ACTIVATION: i64 = crate::payout::replay_protection::MAINNET_ACTIVATION_TIME;
    const SCRIPT_VERIFY_REFUSAL: &str = "mandatory-script-verify-flag-failed \
        (Signature must be zero for failed CHECK(MULTI)SIG operation)";

    fn test_signer(url: &str, activation_time: i64) -> InternalSigner {
        InternalSigner::new(
            TEST_KEY,
            Arc::new(JsonRpcClient::new(url, "lotus", "lotus")),
            1000,
            ReplayProtection::new(activation_time),
        )
        .expect("InternalSigner construction should succeed")
    }

    /// Mock node: reports `mediantime` as the tip MTP and answers each
    /// `sendrawtransaction` with the next entry of `broadcast_results`
    /// (`Ok(txid)` or `Err(reject reason)`, as lotusd does with code -26).
    async fn spawn_node(
        mediantime: i64,
        broadcast_results: Vec<Result<&'static str, &'static str>>,
    ) -> (String, Arc<std::sync::Mutex<Vec<String>>>) {
        let results = Arc::new(std::sync::Mutex::new(
            broadcast_results
                .into_iter()
                .collect::<std::collections::VecDeque<_>>(),
        ));
        spawn_rpc_server(move |method, _params, _raw| match method {
            "getblockchaininfo" => {
                json!({ "result": { "mediantime": mediantime }, "error": null, "id": 1 })
            }
            "sendrawtransaction" => match results.lock().unwrap().pop_front() {
                Some(Ok(txid)) => json!({ "result": txid, "error": null, "id": 1 }),
                Some(Err(reason)) => json!({
                    "result": null,
                    "error": { "code": -26, "message": reason },
                    "id": 1
                }),
                None => panic!("unexpected extra sendrawtransaction"),
            },
            other => panic!("unexpected RPC method {}", other),
        })
        .await
    }

    /// Methods of the captured requests, in order.
    fn methods(requests: &Arc<std::sync::Mutex<Vec<String>>>) -> Vec<String> {
        requests
            .lock()
            .unwrap()
            .iter()
            .map(|r| {
                let body: serde_json::Value =
                    serde_json::from_str(extract_json_body(r).unwrap_or("{}")).unwrap_or_default();
                body["method"].as_str().unwrap_or("").to_string()
            })
            .collect()
    }

    /// Hex of every broadcast transaction, in order.
    fn broadcast_txs(requests: &Arc<std::sync::Mutex<Vec<String>>>) -> Vec<String> {
        requests
            .lock()
            .unwrap()
            .iter()
            .filter_map(|r| {
                let body: serde_json::Value =
                    serde_json::from_str(extract_json_body(r).unwrap_or("{}")).unwrap_or_default();
                (body["method"] == "sendrawtransaction")
                    .then(|| body["params"][0].as_str().unwrap_or("").to_string())
            })
            .collect()
    }

    /// Sighash fork value (0 or 0xffdead) that the signature of a broadcast
    /// payout transaction verifies against, by recomputing the BIP143
    /// preimage of its input with each candidate fork value.
    fn signed_fork_value(signer: &InternalSigner, data: &SignedBatchData, tx_hex: &str) -> u32 {
        use bitcoinsuite_core::{UnhashedTx, UnsignedTx};

        let mut raw = Bytes::from(hex::decode(tx_hex).unwrap());
        let mut tx = UnhashedTx::deser(&mut raw).unwrap();
        let script_sig = tx.inputs[0].script.bytecode().to_vec();
        assert_eq!(
            script_sig[0], 65,
            "scriptSig starts with a 65-byte signature push"
        );
        assert_eq!(script_sig[65], 0x41, "sighash byte stays ALL|FORKID");
        let sig = Bytes::from(script_sig[1..65].to_vec());

        tx.inputs[0].script = Script::default();
        tx.inputs[0].sign_data = Some(SignData::new(vec![
            SignField::Value(data.coinbase_amount),
            SignField::OutputScript(Script::from_slice(
                &hex::decode(&data.coinbase_script_pubkey_hex).unwrap(),
            )),
        ]));
        let mut unsigned = UnsignedTx::new(tx);
        let preimage = unsigned
            .input_at(0)
            .sighash_preimage(SigHashType::ALL_BIP143, None)
            .unwrap()
            .bytes;

        let ecc = EccSecp256k1::default();
        let verifies = |fork_value: u32| {
            let msg = Sha256d::digest(with_fork_value(
                &preimage,
                SigHashType::ALL_BIP143,
                fork_value,
            ))
            .byte_array()
            .clone();
            ecc.schnorr_verify(&signer.pubkey, msg, &sig).is_ok()
        };
        match (verifies(0), verifies(0xffdead)) {
            (true, false) => 0,
            (false, true) => 0xffdead,
            other => panic!(
                "signature verifies as (fork 0, fork 0xffdead) = {:?}",
                other
            ),
        }
    }

    /// The signature produced for a fork value commits to
    /// `(fork_value << 8) | sighash` in the preimage, and the signature keeps a
    /// single sighash byte. With fork value 0 it signs the same message as
    /// bitcoinsuite's `P2PKHSignatory`.
    #[test]
    fn test_fork_value_signatory_commits_fork_value() {
        use bitcoinsuite_core::{P2PKHSignatory, ShaRmd160, UnhashedTx, UnsignedTx};

        let ecc = EccSecp256k1::default();
        let seckey = ecc
            .seckey_from_array(hex::decode(TEST_KEY).unwrap().try_into().unwrap())
            .unwrap();
        let pubkey = ecc.derive_pubkey(&seckey);
        let p2pkh_script = Script::p2pkh(&ShaRmd160::digest(pubkey.array().into()));
        let tx = UnhashedTx {
            version: 2,
            inputs: vec![TxInput {
                prev_out: OutPoint::default(),
                script: Script::default(),
                sequence: SequenceNo::finalized(),
                sign_data: Some(SignData::new(vec![
                    SignField::Value(50000),
                    SignField::OutputScript(p2pkh_script),
                ])),
            }],
            outputs: vec![TxOutput {
                value: 49000,
                script: Script::p2pkh(&ShaRmd160::new([1; 20])),
            }],
            lock_time: 0,
        };

        let sign_with = |fork_value: u32| {
            let mut unsigned = UnsignedTx::new(tx.clone());
            ForkValueP2PKHSignatory {
                seckey: seckey.clone(),
                pubkey,
                sig_hash_type: SigHashType::ALL_BIP143,
                fork_value,
            }
            .sign_input(&ecc, unsigned.input_at(0))
            .unwrap();
            unsigned.tx().inputs[0].script.clone()
        };
        let preimage_for = |full_type: u32| {
            let mut unsigned = UnsignedTx::new(tx.clone());
            let preimage = unsigned
                .input_at(0)
                .sighash_preimage(SigHashType::ALL_BIP143, None)
                .unwrap()
                .bytes
                .to_vec();
            assert_eq!(&preimage[preimage.len() - 4..], &0x41u32.to_le_bytes());
            let mut preimage = preimage;
            let n = preimage.len();
            preimage[n - 4..].copy_from_slice(&full_type.to_le_bytes());
            Sha256d::digest(Bytes::from(preimage)).byte_array().clone()
        };
        let signature_of = |script: &Script| {
            // scriptSig = <65-byte push: 64-byte Schnorr sig || sighash byte> <33-byte pubkey>
            let bytes = script.bytecode().to_vec();
            assert_eq!(bytes[0], 65);
            assert_eq!(
                bytes[65], 0x41,
                "signature keeps the single sighash byte ALL|FORKID"
            );
            Bytes::from(bytes[1..65].to_vec())
        };

        // Fork value 0: same message as bitcoinsuite's P2PKHSignatory.
        let script_0 = sign_with(0);
        let mut unsigned = UnsignedTx::new(tx.clone());
        P2PKHSignatory {
            seckey: seckey.clone(),
            pubkey,
            sig_hash_type: SigHashType::ALL_BIP143,
        }
        .sign_input(&ecc, unsigned.input_at(0))
        .unwrap();
        let reference = unsigned.tx().inputs[0].script.clone();
        assert_eq!(script_0.bytecode().len(), reference.bytecode().len());
        assert_eq!(script_0.bytecode()[65..], reference.bytecode()[65..]);
        let sig_reference = signature_of(&reference);
        assert!(ecc
            .schnorr_verify(&pubkey, preimage_for(0x0000_0041), &sig_reference)
            .is_ok());
        let sig_0 = signature_of(&script_0);
        assert!(ecc
            .schnorr_verify(&pubkey, preimage_for(0x0000_0041), &sig_0)
            .is_ok());
        assert!(ecc
            .schnorr_verify(&pubkey, preimage_for(0xffde_ad41), &sig_0)
            .is_err());

        // Fork value 0xffdead: the preimage ends with 0xffdead41.
        let script_rp = sign_with(0xffdead);
        let sig_rp = signature_of(&script_rp);
        assert!(ecc
            .schnorr_verify(&pubkey, preimage_for(0xffde_ad41), &sig_rp)
            .is_ok());
        assert!(ecc
            .schnorr_verify(&pubkey, preimage_for(0x0000_0041), &sig_rp)
            .is_err());
    }

    /// Before the activation, payouts are signed with fork value 0, after
    /// asking the node for the tip median time.
    #[tokio::test]
    async fn test_payout_before_activation_signs_with_fork_value_zero() {
        let (url, requests) = spawn_node(ACTIVATION - 7200, vec![Ok("txid-before")]).await;
        let signer = test_signer(&url, ACTIVATION);
        let data = test_signed_batch_data();

        let txid = signer.sign_and_submit(&data).await.unwrap();

        assert_eq!(txid, "txid-before");
        assert_eq!(
            methods(&requests),
            ["getblockchaininfo", "sendrawtransaction"]
        );
        let txs = broadcast_txs(&requests);
        assert_eq!(signed_fork_value(&signer, &data, &txs[0]), 0);
    }

    /// After the activation, payouts are signed with fork value 0xffdead.
    #[tokio::test]
    async fn test_payout_after_activation_signs_with_replay_protection_fork_value() {
        let (url, requests) = spawn_node(ACTIVATION + 7200, vec![Ok("txid-after")]).await;
        let signer = test_signer(&url, ACTIVATION);
        let data = test_signed_batch_data();

        let txid = signer.sign_and_submit(&data).await.unwrap();

        assert_eq!(txid, "txid-after");
        let txs = broadcast_txs(&requests);
        assert_eq!(txs.len(), 1);
        assert_eq!(signed_fork_value(&signer, &data, &txs[0]), 0xffdead);
    }

    /// The fork value follows the configured activation time, not the clock.
    #[tokio::test]
    async fn test_payout_follows_overridden_activation_time() {
        let (url, requests) = spawn_node(1_000_000 + 3601, vec![Ok("txid-override")]).await;
        let signer = test_signer(&url, 1_000_000);
        let data = test_signed_batch_data();

        signer.sign_and_submit(&data).await.unwrap();

        let txs = broadcast_txs(&requests);
        assert_eq!(signed_fork_value(&signer, &data, &txs[0]), 0xffdead);
    }

    /// Within the boundary window, nothing is signed or broadcast; the error
    /// is a `PayoutHeld` so the batch stays pending.
    #[tokio::test]
    async fn test_payout_is_held_near_activation() {
        for mtp in [
            ACTIVATION - 3600,
            ACTIVATION - 1,
            ACTIVATION,
            ACTIVATION + 3600,
        ] {
            let (url, requests) = spawn_node(mtp, vec![]).await;
            let signer = test_signer(&url, ACTIVATION);

            let err = signer
                .sign_and_submit(&test_signed_batch_data())
                .await
                .unwrap_err();

            let held = err
                .downcast_ref::<crate::payout::replay_protection::PayoutHeld>()
                .unwrap_or_else(|| panic!("expected PayoutHeld at mtp {}, got: {}", mtp, err));
            assert_eq!(held.tip_median_time, mtp);
            assert_eq!(held.activation_time, ACTIVATION);
            assert_eq!(methods(&requests), ["getblockchaininfo"], "mtp {}", mtp);
        }
    }

    /// A script-verify refusal is retried once with the other fork value.
    #[tokio::test]
    async fn test_script_verify_refusal_resigns_once_with_other_fork_value() {
        let (url, requests) = spawn_node(
            ACTIVATION + 7200,
            vec![Err(SCRIPT_VERIFY_REFUSAL), Ok("txid-retried")],
        )
        .await;
        let signer = test_signer(&url, ACTIVATION);
        let data = test_signed_batch_data();

        let txid = signer.sign_and_submit(&data).await.unwrap();

        assert_eq!(txid, "txid-retried");
        assert_eq!(
            methods(&requests),
            [
                "getblockchaininfo",
                "sendrawtransaction",
                "sendrawtransaction"
            ]
        );
        let txs = broadcast_txs(&requests);
        assert_eq!(txs.len(), 2);
        assert_eq!(signed_fork_value(&signer, &data, &txs[0]), 0xffdead);
        assert_eq!(signed_fork_value(&signer, &data, &txs[1]), 0);
        // Same inputs and outputs: only the 64-byte signature differs (hex
        // offsets 86..214: version, input count, prevout, scriptSig length
        // and push opcode come before it).
        let without_signature = |tx: &str| format!("{}{}", &tx[..86], &tx[214..]);
        assert_eq!(without_signature(&txs[0]), without_signature(&txs[1]));
    }

    /// The retry happens once: a second refusal is returned, never looped.
    #[tokio::test]
    async fn test_script_verify_refusal_twice_returns_error() {
        let (url, requests) = spawn_node(
            ACTIVATION - 7200,
            vec![Err(SCRIPT_VERIFY_REFUSAL), Err(SCRIPT_VERIFY_REFUSAL)],
        )
        .await;
        let signer = test_signer(&url, ACTIVATION);

        let err = signer
            .sign_and_submit(&test_signed_batch_data())
            .await
            .unwrap_err();

        let msg = err.to_string();
        assert!(
            msg.contains("0x0") && msg.contains("0xffdead"),
            "got: {}",
            msg
        );
        assert_eq!(broadcast_txs(&requests).len(), 2);
    }

    /// Other refusals are not retried with the other fork value.
    #[tokio::test]
    async fn test_other_refusal_is_not_retried() {
        let (url, requests) =
            spawn_node(ACTIVATION - 7200, vec![Err("insufficient priority")]).await;
        let signer = test_signer(&url, ACTIVATION);

        let err = signer
            .sign_and_submit(&test_signed_batch_data())
            .await
            .unwrap_err();

        assert!(
            err.to_string().contains("insufficient priority"),
            "got: {}",
            err
        );
        assert_eq!(broadcast_txs(&requests).len(), 1);
    }

    /// Helper: extract JSON body from an HTTP request string.
    fn extract_json_body(http_request: &str) -> Option<&str> {
        let parts: Vec<&str> = http_request.split("\r\n\r\n").collect();
        parts.get(1).copied()
    }
}
