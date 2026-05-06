use anyhow::{anyhow, Result};
use tracing::{error, info, warn};

use bitcoinsuite_bitcoind::rpc_client::{BitcoindRpcClient, BitcoindRpcClientConf};
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

/// Default gross reward in satoshis used for payout calculation.
/// This is the expected block reward (13 LTC = 1,300,000,000 satoshis).
const DEFAULT_GROSS_REWARD_SAT: i64 = 1_300_000_000;

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

    // Load the pool's payout script (used to identify coinbase outputs and for change)
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
        let tip_height = match adapter.get_mining_template(None, None).await {
            Ok(t) => t.height as i64,
            Err(err) => {
                error!(error = %err, "failed loading tip height for payout maturity sync");
                continue;
            }
        };
        // Sync confirmation counts for all found blocks in the database
        // Marks blocks as matured once they have enough confirmations
        if let Err(err) = db.sync_found_block_confirmations(
            tip_height,
            cfg.pool
                .pplns
                .min_confirmations
                .max(COINBASE_MATURITY_BLOCKS),
        ) {
            error!(error = %err, "failed syncing found block confirmations");
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
                info!(processed_count, "rate limit reached, will continue in next interval");
                break;
            }
            processed_count += 1;
            
            // Log first block normally, subsequent blocks as "catching up"
            if processed_count == 1 {
                info!(block_hash, "processing matured found block");
            } else {
                info!(block_hash, processed_count, "catching up: processing additional matured found block");
            }

            // Calculate target work units for PPLNS window (N multiplier)
            // This determines how many shares back the payout window extends
            let target_work_units = cfg.pool.pplns.n_multiplier.max(1.0);
            // Retrieve all weighted shares in the PPLNS window for this found block
            // Third parameter (200_000) is the maximum number of shares to retrieve
            let window_shares =
                db.list_weighted_shares_for_pplns_window(found_block_id, target_work_units, 200_000)?;
            // Convert database shares to WeightedShare structs for payout calculation
            let shares = window_shares
                .iter()
                .map(|s| WeightedShare {
                    payout_address: s.payout_address.clone(),
                    work_units: s.work_units,
                })
                .collect::<Vec<_>>();

            // Get fee configuration for payout calculation
            let fee_address = cfg.pool.fee.fee_address.as_deref();
            // Build initial payout plan using default gross reward (will be corrected below)
            let mut plan = build_pplns_payout_plan(
                DEFAULT_GROSS_REWARD_SAT,
                if cfg.pool.fee.enabled {
                    cfg.pool.fee.fee_bps
                } else {
                    0
                },
                fee_address,
                &shares,
                cfg.pool.pplns.min_payout_sat,
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
            // Coinbase structure: vout[0] = block reward to pool, vout[1] = payout output
            // We need vout[1] which contains the reward to be distributed to miners
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
            // Update payout plan with actual coinbase reward value (not the default)
            plan.gross_reward_sat = payout_output.value;
            // Recalculate fee based on actual gross reward
            plan.fee_sat = crate::payout::compute_fee(
                plan.gross_reward_sat,
                if cfg.pool.fee.enabled {
                    cfg.pool.fee.fee_bps
                } else {
                    0
                },
            );
            plan.net_reward_sat = plan.gross_reward_sat - plan.fee_sat;

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

            // Build and sign the payout transaction using the coinbase as input
            let signed_tx = match build_and_sign_payout_tx(
                &plan.outputs,
                &payout_script,
                &seckey,
                coinbase.tx.txid.clone(),
                payout_output.value,
            ) {
                Ok(tx) => tx,
                Err(err) => {
                    // Schedule retry in 60 seconds on signing failure
                    let retry_at = (chrono::Utc::now() + chrono::Duration::seconds(60)).to_rfc3339();
                    db.schedule_batch_retry(batch_id, &err.to_string(), &retry_at)?;
                    error!(error = %err, batch_id, found_block_id, block_hash, "failed signing payout tx");
                    continue;
                }
            };

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
                    let retry_at = (chrono::Utc::now() + chrono::Duration::seconds(60)).to_rfc3339();
                    db.schedule_batch_retry(batch_id, &err.to_string(), &retry_at)?;
                    error!(error = %err, batch_id, found_block_id, block_hash, "failed submitting payout tx via JSON-RPC");
                    continue;
                }
            };
            // Update batch state to "submitted" with the broadcast txid
            db.update_payout_batch_state(batch_id, "submitted", None, Some(&submitted_txid), None)?;
            // Mark the found block as having a payout submitted (prevents re-processing)
            db.mark_found_block_payout_submitted(found_block_id)?;

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
        if let Err(err) = scheduler_lease::renew_scheduler_lease(&db, instance_id, lease_duration_mins) {
            error!(error = %err, "failed to renew payout scheduler lease");
        }
        
        // Log summary if any blocks were processed this interval
        if processed_count > 0 {
            info!(processed_count, "completed payout processing for this interval");
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

/// Build and sign a payout transaction that spends the coinbase output.
/// 
/// Creates a transaction with:
/// - Input: coinbase vout[1] (the payout output from the found block)
/// - Outputs: one output per miner in the payout plan, plus optional change
/// 
/// Uses P2PKH signing with the pool's private key. The sequence number is set
/// to 0xffff_fffe to enable Replace-By-Fee (RBF) if needed.
/// 
/// # Arguments
/// * `outputs` - List of (address, amount) pairs to pay to miners
/// * `payout_script_bytes` - Pool's payout script (used for change output)
/// * `seckey` - Pool's private key for signing
/// * `prev_txid` - Transaction ID of the coinbase transaction
/// * `prev_value` - Value of the coinbase payout output in satoshis
/// 
/// # Returns
/// * `Ok(UnhashedTx)` - Signed payout transaction ready for serialization
/// * `Err(...)` - If signing fails, outputs exceed input, or script is not P2PKH
/// 
/// # Change Handling
/// If total outputs < prev_value, leftover satoshis are sent back to the pool
/// payout script as a change output (deterministic, no fee calculation).
fn build_and_sign_payout_tx(
    outputs: &[(String, i64)],
    payout_script_bytes: &[u8],
    seckey: &SecKey,
    prev_txid: bitcoinsuite_core::Sha256d,
    prev_value: i64,
) -> Result<UnhashedTx> {
    // Initialize ECC context for secp256k1 signing
    let ecc = EccSecp256k1::default();
    // Derive public key from the secret key
    let pubkey = ecc.derive_pubkey(seckey);
    // Load the payout script for validation and change output
    let payout_script = Script::from_slice(payout_script_bytes);
    // Verify the payout script is P2PKH (only supported type for internal signing)
    if !matches!(
        payout_script.parse_variant(),
        bitcoinsuite_core::ScriptVariant::P2PKH(_)
    ) {
        anyhow::bail!("internal signing currently supports only P2PKH payout script")
    }

    // Build the transaction with one input (coinbase) and multiple outputs (miners)
    let mut tx_builder = TxBuilder {
        version: 2,
        // Single input: coinbase vout[1] (payout output)
        inputs: vec![TxBuilderInput::new(
            TxInput {
                prev_out: OutPoint {
                    txid: prev_txid,
                    out_idx: 1, // coinbase output per consensus
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
        // One output per miner payout address
        outputs: outputs
            .iter()
            .map(|(address, amount)| {
                let addr: LotusAddress = address.parse()?;
                Ok(TxBuilderOutput::Fixed(TxOutput {
                    value: *amount,
                    script: addr.script().clone(),
                }))
            })
            .collect::<Result<Vec<_>>>()?,
        lock_time: 0,
    };

    // Calculate total output value to check against input value
    let total_out: i64 = outputs.iter().map(|(_, amount)| *amount).sum();
    // Sanity check: outputs cannot exceed the coinbase input value
    if total_out > prev_value {
        anyhow::bail!("payout outputs exceed available coinbase payout value")
    }
    // If there's leftover value, add a change output back to the pool
    // This handles satoshi rounding differences and ensures exact balance
    if total_out < prev_value {
        // Keep change deterministic by returning leftovers to pool payout script.
        tx_builder.outputs.push(TxBuilderOutput::Fixed(TxOutput {
            value: prev_value - total_out,
            script: Script::from_slice(payout_script_bytes),
        }));
    }

    // Sign the transaction using the ECC context and input index 0
    Ok(tx_builder.sign(&ecc, 0, 0)?)
}
