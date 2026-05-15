use anyhow::{anyhow, Result};
use std::collections::HashMap;
use tracing::{debug, error, info, warn};

use bitcoinsuite_bitcoind::rpc_client::{BitcoindRpcClient, BitcoindRpcClientConf};
use bitcoinsuite_bitcoind_stratum::target_to_difficulty;
use bitcoinsuite_core::{
    ecc::{Ecc, SecKey},
    BitcoinCode, Bytes, Hashed, LotusAddress, OutPoint, P2PKHSignatory, Script, SequenceNo,
    SigHashType, SignData, SignField, Tx, TxBuilder, TxBuilderInput, TxBuilderOutput, TxInput,
    TxOutput, UnhashedTx,
};
use bitcoinsuite_ecc_secp256k1::EccSecp256k1;

use crate::nng::adapter::{BitcoindMiningAdapter, JsonRpcClient, NngAdapter, NodeMiningAdapter};

use crate::{
    accounting::AccountingDb,
    config::Config,
    payout::{build_pplns_payout_plan, scheduler_lease, WeightedShare},
};

/// Number of blocks required before a coinbase transaction can be spent.
/// Per Bitcoin consensus rules, coinbase rewards mature after 100 blocks.
const COINBASE_MATURITY_BLOCKS: u32 = 100;

/// Main entry point for the payout scheduler background task.
///
/// Runs an infinite loop that periodically:
/// 1. Acquires/renews a scheduler lease for HA coordination
/// 2. Syncs block confirmations to track maturity
/// 3. Processes matured found blocks and creates payout transactions
/// 4. Submits payouts to bitcoind and tracks confirmation
///
/// # Arguments
/// * `cfg` - Pool configuration including signing keys, fee settings, and PPLNS parameters
/// * `db` - Accounting database for tracking shares, found blocks, and payout batches
///
/// # Early Exit
/// Returns immediately if signing mode is not "internal" (external signing not yet supported).
///
/// # Lease Behavior
/// Uses scheduler lease mechanism to ensure only one instance processes payouts at a time
/// in HA deployments. Lease is acquired each interval and renewed after processing.
pub async fn run_payout_scheduler(cfg: Config, db: AccountingDb) -> Result<()> {
    // Only internal signing mode is supported for now
    if cfg.pool.signing.mode != "internal" {
        warn!(mode = %cfg.pool.signing.mode, "payout scheduler disabled: only internal signer mode is currently supported");
        return Ok(());
    }

    // Load the pool's payout script (used to identify coinbase outputs and for signing)
    let payout_script = cfg.resolve_pool_scripts()?.payout_script;
    // Load the signing private key from configuration
    let signing_key = cfg
        .pool
        .signing
        .private_key
        .as_deref()
        .ok_or_else(|| anyhow!("missing signing key"))?;
    let seckey = parse_hex_seckey(signing_key)?;

    // Create separate NNG and JSON-RPC clients, then compose them
    // NNG adapter for mining template and block retrieval
    let nng_adapter = NngAdapter::connect(&cfg.nng_rpc_url)?;
    // JSON-RPC client for transaction submission and confirmation checks
    let json_rpc_client = JsonRpcClient::new(
        cfg.bitcoind_rpc.url.clone(),
        cfg.bitcoind_rpc.rpc_user.clone(),
        cfg.bitcoind_rpc.rpc_pass.clone(),
    );
    let adapter = BitcoindMiningAdapter::new(nng_adapter, json_rpc_client);
    // Separate bitcoind client for sendrawtransaction and getrawtransaction calls
    let bitcoind = BitcoindRpcClient::new(BitcoindRpcClientConf {
        url: cfg.bitcoind_rpc.url.clone(),
        rpc_user: cfg.bitcoind_rpc.rpc_user.clone(),
        rpc_pass: cfg.bitcoind_rpc.rpc_pass.clone(),
    });
    // Payout interval with minimum 30-second safety floor
    let interval = std::time::Duration::from_secs(cfg.pool.pplns.payout_interval_secs.max(30));
    // Unique identifier for this instance (used for lease coordination)
    let instance_id = &cfg.pool.pplns.instance_id;
    // Lease duration before expiration (default 5 minutes)
    let lease_duration_mins = scheduler_lease::DEFAULT_LEASE_DURATION_MINS;

    // Main scheduler loop: wait for interval, then attempt payout processing
    loop {
        tokio::time::sleep(interval).await;

        // Try to acquire scheduler lease for HA coordination
        // Only one instance should process payouts at a time to prevent double-spending
        match scheduler_lease::acquire_scheduler_lease(&db, instance_id, lease_duration_mins) {
            Ok(true) => {
                // Successfully acquired lease - this instance will process payouts
                info!(instance_id = %instance_id, "acquired payout scheduler lease");
            }
            Ok(false) => {
                // Another instance holds the lease - skip this interval
                warn!(instance_id = %instance_id, "another instance holds the payout scheduler lease, skipping this interval");
                continue;
            }
            Err(err) => {
                // Database error during lease acquisition - log and retry next interval
                error!(error = %err, "failed to acquire payout scheduler lease");
                continue;
            }
        }

        // Get current blockchain tip height from mining template
        // Used to determine which found blocks have matured (coinbase + min_confirmations)
        let (tip_height, network_diff) = match adapter.get_mining_template(None, None).await {
            Ok(t) => {
                let target_bytes: [u8; 32] = t
                    .target
                    .to_vec_be()
                    .try_into()
                    .expect("template target must be 32 bytes");
                let diff = target_to_difficulty(&target_bytes)
                    .expect("failed to convert template target to difficulty");
                (t.height as i64, diff)
            }
            Err(err) => {
                error!(error = %err, "failed loading tip height for payout maturity sync");
                continue;
            }
        };
        // Mark matured blocks based on tip height
        // Blocks are matured when: tip_height - block.height + 1 >= coinbase_maturity
        if let Err(err) = db.mark_blocks_matured(
            tip_height,
            cfg.pool
                .pplns
                .min_confirmations
                .max(COINBASE_MATURITY_BLOCKS) as i64,
        ) {
            error!(error = %err, "failed marking matured blocks");
            continue;
        }

        // Catch-up logic: process ALL matured blocks (rate limited)
        // Prevents falling behind if the scheduler was down or overloaded
        let mut processed_count = 0;
        // Rate limit: process at most 10 blocks per interval to avoid overwhelming bitcoind
        let max_blocks_per_interval = 10;

        // Process each matured found block one at a time
        // take_next_matured_fetch returns blocks in order of maturity (oldest first)
        while let Some((found_block_id, block_hash)) = db.take_next_matured_found_block()? {
            // Check rate limit before processing
            if processed_count >= max_blocks_per_interval {
                info!(
                    processed_count,
                    "rate limit reached, will continue in next interval"
                );
                break;
            }
            processed_count += 1;

            // Log first block normally, subsequent blocks as "catching up"
            if processed_count == 1 {
                info!(block_hash, "processing matured found block");
            } else {
                info!(
                    block_hash,
                    processed_count, "catching up: processing additional matured found block"
                );
            }

            // Calculate target work units for PPLNS window.
            //
            // The DB returns work_units = difficulty / vardiff_min_floor (normalized).
            // N = n_multiplier × network_difficulty (in raw difficulty units).
            // To express N in normalized work_units: N / vardiff_min_floor.
            //
            // With n_multiplier=2.0 (industry standard), the window spans ~2 expected
            // rounds of work, providing variance smoothing while remaining responsive.
            let target_work_units = (cfg.pool.pplns.n_multiplier * network_diff
                / cfg.vardiff.vardiff_min_floor)
                .max(1.0);
            // Retrieve all weighted shares in the PPLNS window for this found block.
            // Third parameter (200_000) is the maximum number of shares to retrieve (safety cap).
            let window_shares = db.list_weighted_shares_for_pplns_window(
                found_block_id,
                target_work_units,
                200_000,
                cfg.vardiff.vardiff_min_floor,
            )?;

            // Window shares already have partial credit applied at the boundary
            // (handled by list_weighted_shares_for_pplns_window).
            let shares: Vec<WeightedShare> = window_shares
                .iter()
                .map(|s| WeightedShare {
                    payout_address: s.payout_address.clone(),
                    work_units: s.work_units,
                })
                .collect();

            // Debug: Log PPLNS window details
            let total_work_units: f64 = shares.iter().map(|s| s.work_units).sum();
            let num_shares = shares.len();
            let unique_addresses: std::collections::HashSet<&String> =
                shares.iter().map(|s| &s.payout_address).collect();
            let num_unique_addresses = unique_addresses.len();
            debug!(
                found_block_id,
                block_hash,
                target_work_units,
                actual_work_units = total_work_units,
                num_shares,
                num_unique_addresses,
                "PPLNS window details"
            );
            // Debug: Log top 10 shares by work units
            let mut shares_sorted = shares.clone();
            shares_sorted.sort_by(|a, b| {
                b.work_units
                    .partial_cmp(&a.work_units)
                    .unwrap_or(std::cmp::Ordering::Equal)
            });
            let top_shares: Vec<_> = shares_sorted
                .iter()
                .take(10)
                .map(|s| format!("{}:{:.4}", s.payout_address, s.work_units))
                .collect();
            debug!(
                found_block_id,
                top_shares = top_shares.join(", "),
                "top 10 shares by work units"
            );

            // Fetch the actual found block to extract the real coinbase reward
            let block = match adapter.get_block_by_hash(&block_hash).await {
                Ok(b) => b,
                Err(err) => {
                    error!(error = %err, block_hash, "failed loading found block for payout tx construction");
                    continue;
                }
            };
            // Coinbase transaction is always the first transaction in a block
            let Some(coinbase) = block.txs.first() else {
                error!(block_hash, "found block missing coinbase tx");
                continue;
            };
            // Deserialize the raw coinbase transaction for parsing
            let mut coinbase_raw = Bytes::from_slice(&coinbase.tx.raw);
            let parsed_coinbase = match Tx::deser(&mut coinbase_raw) {
                Ok(tx) => tx,
                Err(err) => {
                    error!(error = %err, block_hash, "failed decoding coinbase tx");
                    continue;
                }
            };
            // Coinbase structure (Lotus):
            //   vout[0]: OP_RETURN with block height encoding (nValue=0, not spendable)
            //   vout[1]: Pool payout output (subsidy + TX fees - miner fund) ← we spend this
            //   vout[2+]: Miner fund outputs (if enabled, ~50% of subsidy to predefined addresses)
            let Some(payout_output) = parsed_coinbase.outputs().get(1) else {
                error!(block_hash, "coinbase tx missing payout output vout[1]");
                continue;
            };
            // Verify the payout output script matches our expected pool payout script
            // This ensures we're paying out the correct coinbase (security check)
            if payout_output.script.bytecode().as_ref() != payout_script.as_slice() {
                error!(
                    block_hash,
                    "coinbase payout script mismatch for matured found block"
                );
                continue;
            }
            // Build payout plan with actual coinbase reward value
            let actual_coinbase_value = payout_output.value;
            let fee_address = cfg.pool.fee.fee_address.as_deref();
            let fee_enabled = cfg.pool.fee.enabled;
            let fee_bps = if fee_enabled { cfg.pool.fee.fee_bps } else { 0 };
            let min_payout_sat = cfg.pool.pplns.min_payout_sat;

            // Query accumulated dust for each unique address (dust carry-forward)
            let mut dust_by_address: HashMap<String, i64> = HashMap::new();
            for addr in &unique_addresses {
                if let Ok(dust) = db.get_accumulated_dust(addr) {
                    if dust > 0 {
                        dust_by_address.insert(addr.to_string(), dust);
                    }
                }
            }

            let plan = build_pplns_payout_plan(
                actual_coinbase_value,
                fee_bps,
                fee_address,
                &shares,
                min_payout_sat,
                &dust_by_address,
            );

            // Debug: Log payout plan
            debug!(
                found_block_id,
                block_hash,
                actual_coinbase_value,
                gross_reward_sat = plan.gross_reward_sat,
                fee_sat = plan.fee_sat,
                net_reward_sat = plan.net_reward_sat,
                outputs_count = plan.outputs.len(),
                dust_count = plan.dust.len(),
                fee_enabled,
                fee_bps,
                min_payout_sat,
                "payout plan built from actual coinbase value"
            );
            // Debug: Log payout outputs summary (top 10 by amount)
            let mut outputs_sorted = plan.outputs.clone();
            outputs_sorted.sort_by(|a, b| b.1.cmp(&a.1));
            let top_outputs: Vec<_> = outputs_sorted
                .iter()
                .take(10)
                .map(|(addr, amt)| format!("{}:{}", addr, amt))
                .collect();
            let total_outputs_sat: i64 = plan.outputs.iter().map(|(_, amt)| *amt).sum();
            let total_dust_sat: i64 = plan.dust.iter().map(|(_, amt)| *amt).sum();
            debug!(
                found_block_id,
                total_outputs_sat,
                total_dust_sat,
                top_outputs = top_outputs.join(", "),
                "payout outputs summary (top 10 by amount)"
            );

            // Create unique retry key to prevent duplicate payouts on retry
            // Format: block_hash:number_of_outputs
            let retry_key = format!("{}:{}", block_hash, plan.outputs.len());
            // Create payout batch record in database for tracking and retry logic
            let batch_id = db.create_payout_batch(
                found_block_id,
                &retry_key,
                plan.gross_reward_sat,
                plan.fee_sat,
                plan.net_reward_sat,
                &plan.outputs,
                &plan.dust,
                &window_shares,
            )?;

            // Debug: Log batch creation
            debug!(
                batch_id,
                found_block_id, retry_key, "payout batch created in database"
            );

            // Build and sign the payout transaction using the coinbase as input
            let coinbase_txid_hex = coinbase.tx.txid.to_hex_be();
            let coinbase_input_value = payout_output.value;

            // Separate worker outputs from fee output for transaction construction
            // plan.outputs includes fee for accounting, but TxBuilder handles fee as Leftover
            let fee_addr_str = cfg.pool.fee.fee_address.as_deref();
            let worker_outputs: Vec<(String, i64)> =
                if fee_addr_str.is_some() && !plan.outputs.is_empty() {
                    // Fee output is appended last by build_pplns_payout_plan
                    plan.outputs
                        .iter()
                        .take(plan.outputs.len() - 1)
                        .cloned()
                        .collect()
                } else {
                    plan.outputs.clone()
                };

            // Build fee script for Leftover output
            let fee_script = if let Some(fee_addr) = fee_addr_str {
                fee_addr.parse::<LotusAddress>()?.script().clone()
            } else {
                // Fallback to payout script if no fee address configured
                Script::from_slice(&payout_script)
            };

            let signed_tx = match build_and_sign_payout_tx(
                &worker_outputs,
                &fee_script,
                &payout_script,
                &seckey,
                coinbase.tx.txid.clone(),
                coinbase_input_value,
            ) {
                Ok(tx) => tx,
                Err(err) => {
                    // Schedule retry in 60 seconds on signing failure
                    let retry_at =
                        (chrono::Utc::now() + chrono::Duration::seconds(60)).to_rfc3339();
                    db.schedule_batch_retry(batch_id, &err.to_string(), &retry_at)?;
                    error!(error = %err, batch_id, found_block_id, block_hash, "failed signing payout tx");
                    continue;
                }
            };

            // Debug: Log transaction construction details
            let tx_serialized = signed_tx.ser();
            let tx_size_bytes = tx_serialized.as_ref().len();
            let worker_output_sat: i64 = worker_outputs.iter().map(|(_, amt)| *amt).sum();
            let pool_fee_output = signed_tx.outputs.last().map(|o| o.value).unwrap_or(0);
            let network_fee_sat = coinbase_input_value - worker_output_sat - pool_fee_output;
            debug!(
                batch_id,
                found_block_id,
                block_hash,
                coinbase_txid = coinbase_txid_hex,
                coinbase_input_value,
                worker_output_sat,
                pool_fee_output,
                network_fee_sat,
                tx_size_bytes,
                num_worker_outputs = worker_outputs.len(),
                "payout transaction construction details"
            );

            // Serialize the signed transaction and compute local txid
            let raw_tx = signed_tx.ser();
            let local_txid = signed_tx.hashed().hash().to_hex_be();
            // Update batch state to "signed" with local txid for tracking
            db.update_payout_batch_state(
                batch_id,
                "signed",
                Some(&format!("rawtx:{}", &local_txid)),
                None,
                None,
            )?;

            // Debug: Log pre-submission state
            debug!(
                batch_id,
                local_txid,
                raw_tx_hex_len = raw_tx.as_ref().len() * 2,
                "payout transaction signed, ready for submission"
            );

            // Submit the signed transaction to bitcoind via JSON-RPC
            let submitted_txid = match bitcoind
                .cmd_json("sendrawtransaction", &[raw_tx.hex().into()])
                .await
            {
                Ok(txid_json) => txid_json
                    .as_str()
                    .ok_or_else(|| anyhow!("sendrawtransaction returned non-string txid"))?
                    .to_string(),
                Err(err) => {
                    // Schedule retry in 60 seconds on submission failure
                    let retry_at =
                        (chrono::Utc::now() + chrono::Duration::seconds(60)).to_rfc3339();
                    db.schedule_batch_retry(batch_id, &err.to_string(), &retry_at)?;
                    error!(error = %err, batch_id, found_block_id, block_hash, "failed submitting payout tx via JSON-RPC");
                    continue;
                }
            };
            // Update batch state to "submitted" with the broadcast txid
            db.update_payout_batch_state(batch_id, "submitted", None, Some(&submitted_txid), None)?;
            // Mark the found block as having a payout submitted (prevents re-processing)
            db.mark_found_block_payout_submitted(found_block_id)?;

            // Reduce dust ledger for addresses that received accumulated dust
            // This completes the dust carry-forward cycle
            for (addr, dust_amount) in &dust_by_address {
                if *dust_amount > 0 {
                    if let Err(err) = db.reduce_dust_ledger(addr, *dust_amount) {
                        error!(error = %err, addr, "failed to reduce dust ledger after payout");
                    }
                }
            }

            // Log successful payout submission with summary information
            info!(
                batch_id,
                found_block_id,
                block_hash,
                outputs = plan.outputs.len(),
                tip_height,
                txid = %submitted_txid,
                "payout batch signed and submitted"
            );
            // Debug: Full payout summary for this block
            debug!(
                batch_id,
                found_block_id,
                block_hash,
                submitted_txid,
                coinbase_value = actual_coinbase_value,
                gross_reward_sat = plan.gross_reward_sat,
                fee_sat = plan.fee_sat,
                net_reward_sat = plan.net_reward_sat,
                num_outputs = plan.outputs.len(),
                num_dust = plan.dust.len(),
                num_shares = num_shares,
                num_unique_addresses = num_unique_addresses,
                total_work_units = total_work_units,
                tx_size_bytes,
                "complete payout summary for block"
            );

            // Check all pending batches for confirmation and mark as paid
            // This runs after each submission to keep confirmation status up-to-date
            if let Ok(submitted) = db.list_submitted_batches_pending_confirmation(100) {
                for (submitted_batch_id, submitted_found_block_id, txid) in submitted {
                    // Query bitcoind for transaction confirmations
                    let confirmed = bitcoind
                        .cmd_json("getrawtransaction", &[txid.clone().into(), true.into()])
                        .await
                        .ok()
                        .and_then(|v| v["confirmations"].as_i64())
                        .unwrap_or(0)
                        > 0;
                    if confirmed {
                        // Mark batch as confirmed and found block as paid
                        db.update_payout_batch_state(
                            submitted_batch_id,
                            "confirmed",
                            None,
                            Some(&txid),
                            None,
                        )?;
                        db.mark_found_block_paid(submitted_found_block_id)?;
                    }
                }
            }
        }

        // Renew lease after processing blocks to maintain exclusive access
        // This extends the lease duration so another instance doesn't take over
        if let Err(err) =
            scheduler_lease::renew_scheduler_lease(&db, instance_id, lease_duration_mins)
        {
            error!(error = %err, "failed to renew payout scheduler lease");
        }

        // Log summary if any blocks were processed this interval
        if processed_count > 0 {
            info!(
                processed_count,
                "completed payout processing for this interval"
            );
        }
    }
}

/// Parse a private key from hex or WIF format.
///
/// # Arguments
/// * `private_key` - Private key as a hex string or WIF (Wallet Import Format)
///
/// # Returns
/// * `Ok(SecKey)` - Parsed secret key
/// * `Err(...)` - If the key format is invalid
fn parse_hex_seckey(private_key: &str) -> Result<SecKey> {
    SecKey::from_hex_or_wif(private_key.trim())
        .map_err(|e| anyhow::anyhow!("invalid private key: {e}"))
}

/// Build and sign a payout transaction that spends the coinbase payout output.
///
/// Creates a transaction with:
/// - Input: coinbase vout[1] (pool payout: subsidy + TX fees - miner fund)
/// - Outputs: one output per miner (fixed amounts), pool fee receives the leftover
///
/// Uses P2PKH signing with the pool's private key. The sequence number is set
/// to 0xffff_fffe to enable Replace-By-Fee (RBF) if needed.
///
/// # Arguments
/// * `outputs` - List of (address, amount) pairs to pay to miners (excludes pool fee)
/// * `fee_script` - Pool's fee address script (receives leftover after network fee)
/// * `payout_script_bytes` - Pool's payout script (used for signing the input)
/// * `seckey` - Pool's private key for signing
/// * `prev_txid` - Transaction ID of the coinbase transaction
/// * `prev_value` - Value of the coinbase payout output in satoshis
///
/// # Returns
/// * `Ok(UnhashedTx)` - Signed payout transaction ready for serialization
/// * `Err(...)` - If signing fails, outputs exceed input, or script is not P2PKH
///
/// # Fee Handling
/// Network fee is automatically calculated at 2 sat/byte by TxBuilder::sign().
/// The pool fee output receives: prev_value - worker_outputs - network_fee.
fn build_and_sign_payout_tx(
    outputs: &[(String, i64)],
    fee_script: &Script,
    payout_script_bytes: &[u8],
    seckey: &SecKey,
    prev_txid: bitcoinsuite_core::Sha256d,
    prev_value: i64,
) -> Result<UnhashedTx> {
    // Initialize ECC context for secp256k1 signing
    let ecc = EccSecp256k1::default();
    // Derive public key from the secret key
    let pubkey = ecc.derive_pubkey(seckey);
    // Load the payout script for input signing validation
    let payout_script = Script::from_slice(payout_script_bytes);
    // Verify the payout script is P2PKH (only supported type for internal signing)
    if !matches!(
        payout_script.parse_variant(),
        bitcoinsuite_core::ScriptVariant::P2PKH(_)
    ) {
        anyhow::bail!("internal signing currently supports only P2PKH payout script")
    }

    // Build worker outputs (fixed amounts) + pool fee (leftover after network fee)
    let mut builder_outputs: Vec<TxBuilderOutput> = outputs
        .iter()
        .map(|(address, amount)| {
            let addr: LotusAddress = address.parse()?;
            Ok(TxBuilderOutput::Fixed(TxOutput {
                value: *amount,
                script: addr.script().clone(),
            }))
        })
        .collect::<Result<Vec<_>>>()?;
    // Pool fee receives whatever remains after worker payouts and network fee
    builder_outputs.push(TxBuilderOutput::Leftover(fee_script.clone()));

    // Build the transaction with one input (coinbase) and the outputs above
    let tx_builder = TxBuilder {
        version: 2,
        // Single input: coinbase vout[1] (pool payout output)
        inputs: vec![TxBuilderInput::new(
            TxInput {
                prev_out: OutPoint {
                    txid: prev_txid,
                    out_idx: 1, // Lotus coinbase: vout[1] is pool payout
                },
                script: Script::default(), // Empty for coinbase; will be replaced by signature
                sequence: SequenceNo::from_u32(0xffff_fffe), // RBF-enabled sequence
                sign_data: Some(SignData::new(vec![
                    SignField::OutputScript(payout_script),
                    SignField::Value(prev_value),
                ])),
            },
            // P2PKH signatory handles the signature generation
            Box::new(P2PKHSignatory {
                seckey: seckey.clone(),
                pubkey,
                sig_hash_type: SigHashType::ALL_BIP143, // BIP143 segwit-style sighash
            }),
        )],
        outputs: builder_outputs,
        lock_time: 0,
    };

    // Sign with 2 sat/byte fee rate (2000 sat/kB) and standard dust limit (546 sats)
    // TxBuilder automatically calculates network fee and sets pool fee to the leftover
    Ok(tx_builder.sign(&ecc, 2000, 546)?)
}
