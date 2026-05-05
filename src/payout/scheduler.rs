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

const COINBASE_MATURITY_BLOCKS: u32 = 100;
const DEFAULT_GROSS_REWARD_SAT: i64 = 1_300_000_000;

pub async fn run_payout_scheduler(cfg: Config, db: AccountingDb) -> Result<()> {
    if cfg.pool.signing.mode != "internal" {
        warn!(mode = %cfg.pool.signing.mode, "payout scheduler disabled: only internal signer mode is currently supported");
        return Ok(());
    }

    let payout_script = cfg.resolve_pool_scripts()?.payout_script;
    let signing_key = cfg
        .pool
        .signing
        .private_key
        .as_deref()
        .ok_or_else(|| anyhow!("missing signing key"))?;
    let seckey = parse_hex_seckey(signing_key)?;

    // Create separate NNG and JSON-RPC clients, then compose them
    let nng_adapter = NngAdapter::connect(&cfg.nng_rpc_url)?;
    let json_rpc_client = JsonRpcClient::new(
        cfg.bitcoind_rpc.url.clone(),
        cfg.bitcoind_rpc.rpc_user.clone(),
        cfg.bitcoind_rpc.rpc_pass.clone(),
    );
    let adapter = BitcoindMiningAdapter::new(nng_adapter, json_rpc_client);
    let bitcoind = BitcoindRpcClient::new(BitcoindRpcClientConf {
        url: cfg.bitcoind_rpc.url.clone(),
        rpc_user: cfg.bitcoind_rpc.rpc_user.clone(),
        rpc_pass: cfg.bitcoind_rpc.rpc_pass.clone(),
    });
    let interval = std::time::Duration::from_secs(cfg.pool.pplns.payout_interval_secs.max(30));
    let instance_id = &cfg.pool.pplns.instance_id;
    let lease_duration_mins = scheduler_lease::DEFAULT_LEASE_DURATION_MINS;
    
    loop {
        tokio::time::sleep(interval).await;

        // Try to acquire scheduler lease
        match scheduler_lease::acquire_scheduler_lease(&db, instance_id, lease_duration_mins) {
            Ok(true) => {
                info!(instance_id = %instance_id, "acquired payout scheduler lease");
            }
            Ok(false) => {
                warn!(instance_id = %instance_id, "another instance holds the payout scheduler lease, skipping this interval");
                continue;
            }
            Err(err) => {
                error!(error = %err, "failed to acquire payout scheduler lease");
                continue;
            }
        }

        let tip_height = match adapter.get_mining_template(None, None).await {
            Ok(t) => t.height as i64,
            Err(err) => {
                error!(error = %err, "failed loading tip height for payout maturity sync");
                continue;
            }
        };
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
        let mut processed_count = 0;
        let max_blocks_per_interval = 10;
        
        while let Some((found_block_id, block_hash)) = db.take_next_matured_found_block()? {
            if processed_count >= max_blocks_per_interval {
                info!(processed_count, "rate limit reached, will continue in next interval");
                break;
            }
            processed_count += 1;
            
            if processed_count == 1 {
                info!(block_hash, "processing matured found block");
            } else {
                info!(block_hash, processed_count, "catching up: processing additional matured found block");
            }

            let target_work_units = cfg.pool.pplns.n_multiplier.max(1.0);
            let window_shares =
                db.list_weighted_shares_for_pplns_window(found_block_id, target_work_units, 200_000)?;
            let shares = window_shares
                .iter()
                .map(|s| WeightedShare {
                    payout_address: s.payout_address.clone(),
                    work_units: s.work_units,
                })
                .collect::<Vec<_>>();

            let fee_address = cfg.pool.fee.fee_address.as_deref();
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

            let block = match adapter.get_block_by_hash(&block_hash).await {
                Ok(b) => b,
                Err(err) => {
                    error!(error = %err, block_hash, "failed loading found block for payout tx construction");
                    continue;
                }
            };
            let Some(coinbase) = block.txs.first() else {
                error!(block_hash, "found block missing coinbase tx");
                continue;
            };
            let mut coinbase_raw = Bytes::from_slice(&coinbase.tx.raw);
            let parsed_coinbase = match Tx::deser(&mut coinbase_raw) {
                Ok(tx) => tx,
                Err(err) => {
                    error!(error = %err, block_hash, "failed decoding coinbase tx");
                    continue;
                }
            };
            let Some(payout_output) = parsed_coinbase.outputs().get(1) else {
                error!(block_hash, "coinbase tx missing payout output vout[1]");
                continue;
            };
            if payout_output.script.bytecode().as_ref() != payout_script.as_slice() {
                error!(
                    block_hash,
                    "coinbase payout script mismatch for matured found block"
                );
                continue;
            }
            plan.gross_reward_sat = payout_output.value;
            plan.fee_sat = crate::payout::compute_fee(
                plan.gross_reward_sat,
                if cfg.pool.fee.enabled {
                    cfg.pool.fee.fee_bps
                } else {
                    0
                },
            );
            plan.net_reward_sat = plan.gross_reward_sat - plan.fee_sat;

            let retry_key = format!("{}:{}", block_hash, plan.outputs.len());
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

            let signed_tx = match build_and_sign_payout_tx(
                &plan.outputs,
                &payout_script,
                &seckey,
                coinbase.tx.txid.clone(),
                payout_output.value,
            ) {
                Ok(tx) => tx,
                Err(err) => {
                    let retry_at = (chrono::Utc::now() + chrono::Duration::seconds(60)).to_rfc3339();
                    db.schedule_batch_retry(batch_id, &err.to_string(), &retry_at)?;
                    error!(error = %err, batch_id, found_block_id, block_hash, "failed signing payout tx");
                    continue;
                }
            };

            let raw_tx = signed_tx.ser();
            let local_txid = signed_tx.hashed().hash().to_hex_be();
            db.update_payout_batch_state(
                batch_id,
                "signed",
                Some(&format!("rawtx:{}", &local_txid)),
                None,
                None,
            )?;

            let submitted_txid = match bitcoind
                .cmd_json("sendrawtransaction", &[raw_tx.hex().into()])
                .await
            {
                Ok(txid_json) => txid_json
                    .as_str()
                    .ok_or_else(|| anyhow!("sendrawtransaction returned non-string txid"))?
                    .to_string(),
                Err(err) => {
                    let retry_at = (chrono::Utc::now() + chrono::Duration::seconds(60)).to_rfc3339();
                    db.schedule_batch_retry(batch_id, &err.to_string(), &retry_at)?;
                    error!(error = %err, batch_id, found_block_id, block_hash, "failed submitting payout tx via JSON-RPC");
                    continue;
                }
            };
            db.update_payout_batch_state(batch_id, "submitted", None, Some(&submitted_txid), None)?;
            db.mark_found_block_payout_submitted(found_block_id)?;

            info!(
                batch_id,
                found_block_id,
                block_hash,
                outputs = plan.outputs.len(),
                tip_height,
                txid = %submitted_txid,
                "payout batch signed and submitted"
            );

            if let Ok(submitted) = db.list_submitted_batches_pending_confirmation(100) {
                for (submitted_batch_id, submitted_found_block_id, txid) in submitted {
                    let confirmed = bitcoind
                        .cmd_json("getrawtransaction", &[txid.clone().into(), true.into()])
                        .await
                        .ok()
                        .and_then(|v| v["confirmations"].as_i64())
                        .unwrap_or(0)
                        > 0;
                    if confirmed {
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
        
        // Renew lease after processing blocks
        if let Err(err) = scheduler_lease::renew_scheduler_lease(&db, instance_id, lease_duration_mins) {
            error!(error = %err, "failed to renew payout scheduler lease");
        }
        
        if processed_count > 0 {
            info!(processed_count, "completed payout processing for this interval");
        }
    }
}

fn parse_hex_seckey(private_key: &str) -> Result<SecKey> {
    SecKey::from_hex_or_wif(private_key.trim())
        .map_err(|e| anyhow::anyhow!("invalid private key: {e}"))
}

fn build_and_sign_payout_tx(
    outputs: &[(String, i64)],
    payout_script_bytes: &[u8],
    seckey: &SecKey,
    prev_txid: bitcoinsuite_core::Sha256d,
    prev_value: i64,
) -> Result<UnhashedTx> {
    let ecc = EccSecp256k1::default();
    let pubkey = ecc.derive_pubkey(seckey);
    let payout_script = Script::from_slice(payout_script_bytes);
    if !matches!(
        payout_script.parse_variant(),
        bitcoinsuite_core::ScriptVariant::P2PKH(_)
    ) {
        anyhow::bail!("internal signing currently supports only P2PKH payout script")
    }

    let mut tx_builder = TxBuilder {
        version: 2,
        inputs: vec![TxBuilderInput::new(
            TxInput {
                prev_out: OutPoint {
                    txid: prev_txid,
                    out_idx: 1,
                },
                script: Script::default(),
                sequence: SequenceNo::from_u32(0xffff_fffe),
                sign_data: Some(SignData::new(vec![
                    SignField::OutputScript(payout_script),
                    SignField::Value(prev_value),
                ])),
            },
            Box::new(P2PKHSignatory {
                seckey: seckey.clone(),
                pubkey,
                sig_hash_type: SigHashType::ALL_BIP143,
            }),
        )],
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

    let total_out: i64 = outputs.iter().map(|(_, amount)| *amount).sum();
    if total_out > prev_value {
        anyhow::bail!("payout outputs exceed available coinbase payout value")
    }
    if total_out < prev_value {
        // Keep change deterministic by returning leftovers to pool payout script.
        tx_builder.outputs.push(TxBuilderOutput::Fixed(TxOutput {
            value: prev_value - total_out,
            script: Script::from_slice(payout_script_bytes),
        }));
    }

    Ok(tx_builder.sign(&ecc, 0, 0)?)
}
