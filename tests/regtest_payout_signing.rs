//! DEV-343 regtest acceptance: the pool's `InternalSigner` must sign payout
//! transactions that commit to `CHAIN_FORK_ID` once replay protection is
//! active, and a fork-0 signature must be rejected by the node.
//!
//! Drives the production `InternalSigner` + `JsonRpcClient` against a real
//! regtest lotusd run with `-replayprotectionactivationtime=0` (replay
//! protection active from genesis):
//!
//! 1. Mine 101 blocks to a P2PKH address derived from a known secret key.
//! 2. Resolve `chain_fork_id` through the pool's
//!    `JsonRpcClient::get_chain_fork_id` (must return `CHAIN_FORK_ID`).
//! 3. Build a `SignedBatchData` spending block 1's mature coinbase and sign
//!    it via `InternalSigner::sign_and_submit` at `CHAIN_FORK_ID` — the tx
//!    must be accepted into the mempool.
//! 4. Repeat with block 2's coinbase at fork value 0 — `sendrawtransaction`
//!    must reject it with a signature error.
//!
//! This test is `#[ignore]`d so the default `cargo test` never needs a node:
//!
//! ```sh
//! CC=gcc-13 cargo +1.99.0 test -p stratum-server-nng \
//!     --test regtest_payout_signing -- --ignored --nocapture
//! ```
//!
//! Env overrides:
//! - `LOTUSD_BIN`: lotusd binary (default: the sibling `../lotusd` build)
//! - `DEV343_RPC_PORT`: RPC port base (default: 38400 + pid % 2000)

use bitcoinsuite_core::{
    ecc::{Ecc, SecKey},
    Hashed, Script, Sha256, ShaRmd160,
};
use bitcoinsuite_ecc_secp256k1::EccSecp256k1;
use serde_json::{json, Value};
use std::{
    path::PathBuf,
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};
use stratum_server_nng::constants::CHAIN_FORK_ID as POOL_CHAIN_FORK_ID;
use stratum_server_nng::node_integration::JsonRpcClient;
use stratum_server_nng::payout::plan::{PayoutOutput, PayoutPlan};
use stratum_server_nng::payout::signer::internal::InternalSigner;
use stratum_server_nng::payout::signer::{SignedBatchData, Signer};

const RPC_USER: &str = "dev343";
const RPC_PASSWORD: &str = "dev343-regtest-password";
/// Known secret key the pool signer uses: scalar 1.
const SECKEY_HEX: &str = "0101010101010101010101010101010101010101010101010101010101010101";
const FEE_SATS: i64 = 1_000;

/// Locate the lotusd binary: `LOTUSD_BIN`, then the sibling checkout
/// relative to this crate (tests run with cwd = crate root).
fn lotusd_bin() -> String {
    if let Ok(path) = std::env::var("LOTUSD_BIN") {
        return path;
    }
    for candidate in [
        "../lotusd/build-clang/src/lotusd",
        "../../lotusd/build-clang/src/lotusd",
    ] {
        if std::path::Path::new(candidate).exists() {
            return candidate.to_string();
        }
    }
    panic!("lotusd not found; set LOTUSD_BIN or build the sibling ../lotusd checkout");
}

/// Kills the node and removes the temp datadir even when assertions fail.
struct NodeGuard {
    child: Child,
    datadir: PathBuf,
}

impl Drop for NodeGuard {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_dir_all(&self.datadir);
    }
}

/// Encode an XAddress in lotusd v12's native format: base58 over
/// `[type byte 0] ++ payload ++ checksum`, payload = full scriptPubKey,
/// checksum = SHA256("lotus" ++ net_char ++ type ++ payload)[..4].
///
/// This exists ONLY to fund the regtest node: lotusd v12.0.2's
/// `XAddress::Parse` expects the full scriptPubKey as the P2PKH payload.
/// The pool-side `LotusAddress` helper encodes the bare 20-byte hash160
/// (xpi-ts format) and is used for the payout plan output below, where only
/// the library parses the string and the node sees just the output script.
/// The encoding gap is tracked separately (DEV-355) and NOT fixed here.
fn lotusd_native_p2pkh_address(script: &Script) -> String {
    let payload = script.bytecode();
    let mut checksum_preimage = Vec::new();
    checksum_preimage.extend_from_slice(b"lotus");
    checksum_preimage.push(b'R');
    checksum_preimage.push(0); // AddressType SCRIPT_PUB_KEY
    checksum_preimage.extend_from_slice(payload.as_ref());
    let check = Sha256::digest(checksum_preimage.into());

    let mut data = Vec::new();
    data.push(0); // AddressType SCRIPT_PUB_KEY
    data.extend_from_slice(payload.as_ref());
    data.extend_from_slice(&check.as_slice()[..4]);
    format!("lotusR{}", base58_encode(&data))
}

fn base58_encode(input: &[u8]) -> String {
    const ALPHABET: &[u8; 58] = b"123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz";
    let zeros = input.iter().take_while(|&&byte| byte == 0).count();
    let mut digits: Vec<u8> = Vec::with_capacity(input.len() * 138 / 100 + 1);
    for &byte in &input[zeros..] {
        let mut carry = byte as u32;
        for digit in digits.iter_mut() {
            let value = (*digit as u32) * 256 + carry;
            *digit = (value % 58) as u8;
            carry = value / 58;
        }
        while carry > 0 {
            digits.push((carry % 58) as u8);
            carry /= 58;
        }
    }
    let mut out = String::with_capacity(zeros + digits.len());
    for _ in 0..zeros {
        out.push('1');
    }
    for digit in digits.iter().rev() {
        out.push(ALPHABET[*digit as usize] as char);
    }
    out
}

/// One-off JSON-RPC call via reqwest (for methods the pool's
/// `JsonRpcClient` does not expose, e.g. `generatetoaddress`).
async fn raw_rpc(
    client: &reqwest::Client,
    url: &str,
    method: &str,
    params: Value,
) -> Result<Value, String> {
    let body = json!({
        "jsonrpc": "2.0",
        "id": "dev343",
        "method": method,
        "params": params,
    });
    let response = client
        .post(url)
        .basic_auth(RPC_USER, Some(RPC_PASSWORD))
        .json(&body)
        .send()
        .await
        .map_err(|e| format!("RPC {method}: HTTP failed: {e}"))?;
    let value: Value = response
        .json()
        .await
        .map_err(|e| format!("RPC {method}: bad json: {e}"))?;
    if let Some(error) = value.get("error") {
        if !error.is_null() {
            return Err(format!("RPC {method} error: {error}"));
        }
    }
    value
        .get("result")
        .cloned()
        .ok_or_else(|| format!("RPC {method}: missing result"))
}

/// Extract the first spendable (non-OP_RETURN, value > 0) coinbase output:
/// the same shape `accounting::process_pending_payouts` consumes.
fn spendable_coinbase_vout(raw_tx: &Value) -> (u32, i64, String) {
    let vouts = raw_tx["vout"]
        .as_array()
        .expect("coinbase has a vout array");
    vouts
        .iter()
        .enumerate()
        .find(|(_idx, vout)| {
            vout["scriptPubKey"]["type"].as_str() != Some("nulldata")
                && vout["value"].as_f64().unwrap_or(0.0) > 0.0
        })
        .map(|(idx, vout)| {
            (
                idx as u32,
                // lotusd RPC `value` is in LOTUS units (6 decimals):
                // sats = value * 1_000_000.
                (vout["value"].as_f64().unwrap() * 1e6).round() as i64,
                vout["scriptPubKey"]["hex"].as_str().unwrap().to_string(),
            )
        })
        .expect("coinbase has a spendable vout")
}

/// Build a `SignedBatchData` spending `coinbase` back to the pool address
/// (`coinbase_amount - 1000` sats) with the given chain fork id.
fn batch_data(
    coinbase_txid: String,
    coinbase_vout: u32,
    coinbase_amount: i64,
    coinbase_script_pubkey_hex: String,
    payout_address: String,
    chain_fork_id: u32,
) -> SignedBatchData {
    SignedBatchData {
        plan: PayoutPlan {
            round_id: 1,
            block_height: 100,
            block_hash: "regtest-acceptance".to_string(),
            network_difficulty: 1.0,
            total_work_units: 0.0,
            gross_reward: coinbase_amount,
            pool_fee_amount: 0,
            pool_fee_address: None,
            outputs: vec![PayoutOutput {
                payout_address,
                worker_id: 1,
                amount: coinbase_amount - FEE_SATS,
                dust_carried_forward: 0,
            }],
            dust_carried_forward_total: 0,
            retry_key: format!("regtest-acceptance:{chain_fork_id}"),
        },
        coinbase_txid,
        coinbase_vout,
        coinbase_amount,
        coinbase_script_pubkey_hex,
        chain_fork_id,
    }
}

#[tokio::test]
#[ignore]
async fn regtest_payout_signing_acceptance() -> Result<(), Box<dyn std::error::Error>> {
    let lotusd_bin = lotusd_bin();
    let mut rpc_port: u16 = std::env::var("DEV343_RPC_PORT")
        .ok()
        .and_then(|port| port.parse().ok())
        .unwrap_or(38400 + (std::process::id() % 2000) as u16);
    // Skip ports already taken by unrelated local services: if something
    // answers on the candidate port, walk upward until it is free.
    while std::net::TcpStream::connect_timeout(
        &std::net::SocketAddr::from(([127, 0, 0, 1], rpc_port)),
        Duration::from_millis(250),
    )
    .is_ok()
    {
        rpc_port = rpc_port.wrapping_add(1);
    }
    let p2p_port = rpc_port + 1;
    let rpc_url = format!("http://127.0.0.1:{rpc_port}");
    let datadir = std::env::temp_dir().join(format!("dev343-regtest-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&datadir);
    std::fs::create_dir_all(&datadir)?;

    let child = Command::new(&lotusd_bin)
        .args([
            "-regtest",
            &format!("-datadir={}", datadir.display()),
            // Replay protection active from genesis: every sighash must
            // commit to CHAIN_FORK_ID.
            "-replayprotectionactivationtime=0",
            "-server",
            &format!("-rpcuser={RPC_USER}"),
            &format!("-rpcpassword={RPC_PASSWORD}"),
            &format!("-rpcport={rpc_port}"),
            &format!("-port={p2p_port}"),
            "-nolisten",
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()?;
    let _node = NodeGuard {
        child,
        datadir: datadir.clone(),
    };

    // Production RPC client — the same one process_pending_payouts uses.
    let rpc = JsonRpcClient::new(&rpc_url, RPC_USER, RPC_PASSWORD);
    let http = reqwest::Client::new();

    // Wait for the RPC interface
    let start = Instant::now();
    loop {
        match rpc.getblockcount().await {
            Ok(height) => {
                println!(
                    "node up ({}s): blockcount {}",
                    start.elapsed().as_secs(),
                    height
                );
                break;
            }
            Err(_) if start.elapsed() < Duration::from_secs(120) => {
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
            Err(err) => panic!("node RPC did not come up within 120s: {err}"),
        }
    }

    // P2PKH identity from a known secret key: the pool signer signs with it,
    // and the node mines coinbases to its script.
    let ecc = EccSecp256k1::default();
    let seckey: SecKey = ecc
        .seckey_from_array([1u8; 32])
        .map_err(|e| format!("seckey: {e}"))?;
    let pubkey = ecc.derive_pubkey(&seckey);
    let pubkey_hash = ShaRmd160::digest(pubkey.array().into());
    let p2pkh_script = Script::p2pkh(&pubkey_hash);

    // Funding address (lotusd-native XAddress encoding — see doc above)
    let funding_address = lotusd_native_p2pkh_address(&p2pkh_script);
    let validated = raw_rpc(&http, &rpc_url, "validateaddress", json!([funding_address])).await?;
    assert_eq!(
        validated["isvalid"].as_bool(),
        Some(true),
        "node rejected funding address: {validated}"
    );

    // The pool helper must derive the SAME output script for its own
    // encoding of the address (the node only ever sees the script).
    let pool_address = bitcoinsuite_core::LotusAddress::from_hash(
        "lotus",
        bitcoinsuite_core::Net::Regtest,
        pubkey_hash,
    );
    assert_eq!(
        pool_address.script().bytecode().hex(),
        p2pkh_script.bytecode().hex(),
        "pool LotusAddress helper must derive the funded P2PKH script"
    );
    println!("funded address (lotusd-native): {funding_address}");
    println!("payout address (pool helper)   : {}", pool_address.as_str());

    // Mine 101 blocks: block 1's coinbase reaches maturity (100 confs);
    // block 2's coinbase is also exactly spendable at 100 confs.
    let generated = raw_rpc(
        &http,
        &rpc_url,
        "generatetoaddress",
        json!([101, funding_address]),
    )
    .await?;
    assert_eq!(generated.as_array().map(Vec::len), Some(101));

    // Resolve the chain fork id through the production resolver: the node
    // runs with activation=0, so mediantime >= 0 must yield CHAIN_FORK_ID.
    let resolved = rpc.get_chain_fork_id(0).await?;
    assert_eq!(
        resolved, POOL_CHAIN_FORK_ID,
        "get_chain_fork_id must resolve CHAIN_FORK_ID against the active node"
    );

    // Fetch both coinbases: block 1 for the accepted (fork-id) spend,
    // block 2 for the rejected (fork-0) spend — separate outpoints so the
    // fork-0 rejection is a genuine signature failure, not a conflict.
    // Coinbases are read from `getblock <hash> 2` (verbosity 2 embeds full
    // tx JSON): this node build has no -txindex, so `getrawtransaction`
    // by txid only resolves mempool transactions.
    let mut coinbases = Vec::new();
    for height in [1, 2] {
        let block_hash = rpc
            .getblockhash(height)
            .await?
            .expect("block exists after mining");
        let block = rpc.get_block(&block_hash).await?;
        let coinbase = &block["tx"][0];
        let coinbase_txid = coinbase["txid"]
            .as_str()
            .expect("coinbase txid")
            .to_string();
        let (vout, amount_sat, script_hex) = spendable_coinbase_vout(coinbase);
        assert_eq!(
            script_hex,
            p2pkh_script.bytecode().hex(),
            "coinbase vout must pay to our P2PKH script"
        );
        println!("block {height} coinbase outpoint: {coinbase_txid}:{vout} ({amount_sat} sats)");
        coinbases.push((coinbase_txid, vout, amount_sat, script_hex));
    }

    // Build the production signer over the known key
    let signer = InternalSigner::new(SECKEY_HEX, std::sync::Arc::new(rpc), FEE_SATS)?;

    // 1) CHAIN_FORK_ID spend must be accepted into the mempool
    let (txid1, vout1, amount1, script1) = coinbases.remove(0);
    let data_fork_id = batch_data(
        txid1,
        vout1,
        amount1,
        script1,
        pool_address.as_str().to_string(),
        POOL_CHAIN_FORK_ID,
    );
    let accepted_txid = signer
        .sign_and_submit(&data_fork_id)
        .await
        .expect("CHAIN_FORK_ID spend must be accepted");
    println!("accepted txid (CHAIN_FORK_ID): {accepted_txid}");

    let mempool = raw_rpc(&http, &rpc_url, "getrawmempool", json!([])).await?;
    assert!(
        mempool
            .as_array()
            .expect("mempool array")
            .iter()
            .any(|entry| entry.as_str() == Some(accepted_txid.as_str())),
        "accepted txid not in mempool: {mempool}"
    );

    // 2) Fork-0 spend of the second coinbase must be rejected
    let (txid2, vout2, amount2, script2) = coinbases.remove(0);
    let data_fork_0 = batch_data(
        txid2,
        vout2,
        amount2,
        script2,
        pool_address.as_str().to_string(),
        0,
    );
    let fork_0_error = match signer.sign_and_submit(&data_fork_0).await {
        Ok(txid) => panic!("fork-0 spend unexpectedly accepted: {txid}"),
        Err(err) => err.to_string(),
    };
    println!("fork-0 rejection: {fork_0_error}");
    assert!(
        fork_0_error.contains("JSON-RPC error"),
        "fork-0 spend must be rejected by sendrawtransaction, got: {fork_0_error}"
    );

    // The mempool must still contain exactly the fork-id spend
    let mempool_after = raw_rpc(&http, &rpc_url, "getrawmempool", json!([])).await?;
    assert_eq!(
        mempool_after.as_array().map(Vec::len),
        Some(1),
        "only the CHAIN_FORK_ID spend should be in the mempool: {mempool_after}"
    );

    Ok(())
}
