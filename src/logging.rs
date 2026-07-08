// Domain-tagged tracing macros.
//
// Each macro wraps the corresponding tracing:: macro with a `target:` field
// matching a module area. These targets are finer-grained than the bounded
// contexts in docs/CONTEXT_MAP.md — some (validator, shutdown, main) don't
// map 1:1 to a context. This enables precise filtering via RUST_LOG.
//
// Domain → context mapping:
//   stratum     → Stratum Core (stratum_protocol)
//   validator   → Stratum Core sub-domain (share_processing/validator)
//   node_int    → Node Integration
//   accounting  → Accounting
//   payout      → Payout
//   http_api    → HTTP API
//   shutdown    → Infrastructure / cross-cutting
//   main        → Application entry point (main.rs)
//
// Filtering examples:
//   RUST_LOG=stratum=debug,accounting=info,node_int=warn
//   RUST_LOG=stratum=debug,validator=trace  # per-session validation logging

// --- stratum (stratum_protocol module) ---
#[macro_export]
macro_rules! stratum_info    { ($($arg:tt)+) => { tracing::info!(target: "stratum", $($arg)+) }; }
#[macro_export]
macro_rules! stratum_warn    { ($($arg:tt)+) => { tracing::warn!(target: "stratum", $($arg)+) }; }
#[macro_export]
macro_rules! stratum_error   { ($($arg:tt)+) => { tracing::error!(target: "stratum", $($arg)+) }; }
#[macro_export]
macro_rules! stratum_debug   { ($($arg:tt)+) => { tracing::debug!(target: "stratum", $($arg)+) }; }

// --- validator (share_processing/validator) ---
#[macro_export]
macro_rules! validator_info   { ($($arg:tt)+) => { tracing::info!(target: "validator", $($arg)+) }; }
#[macro_export]
macro_rules! validator_warn   { ($($arg:tt)+) => { tracing::warn!(target: "validator", $($arg)+) }; }
#[macro_export]
macro_rules! validator_error  { ($($arg:tt)+) => { tracing::error!(target: "validator", $($arg)+) }; }
#[macro_export]
macro_rules! validator_debug  { ($($arg:tt)+) => { tracing::debug!(target: "validator", $($arg)+) }; }

// --- node_int (node_integration module) ---
#[macro_export]
macro_rules! node_int_info    { ($($arg:tt)+) => { tracing::info!(target: "node_int", $($arg)+) }; }
#[macro_export]
macro_rules! node_int_warn    { ($($arg:tt)+) => { tracing::warn!(target: "node_int", $($arg)+) }; }
#[macro_export]
macro_rules! node_int_error   { ($($arg:tt)+) => { tracing::error!(target: "node_int", $($arg)+) }; }
#[macro_export]
macro_rules! node_int_debug   { ($($arg:tt)+) => { tracing::debug!(target: "node_int", $($arg)+) }; }

// --- accounting ---
#[macro_export]
macro_rules! accounting_info  { ($($arg:tt)+) => { tracing::info!(target: "accounting", $($arg)+) }; }
#[macro_export]
macro_rules! accounting_warn  { ($($arg:tt)+) => { tracing::warn!(target: "accounting", $($arg)+) }; }
#[macro_export]
macro_rules! accounting_error { ($($arg:tt)+) => { tracing::error!(target: "accounting", $($arg)+) }; }
#[macro_export]
macro_rules! accounting_debug { ($($arg:tt)+) => { tracing::debug!(target: "accounting", $($arg)+) }; }

// --- payout ---
#[macro_export]
macro_rules! payout_info   { ($($arg:tt)+) => { tracing::info!(target: "payout", $($arg)+) }; }
#[macro_export]
macro_rules! payout_warn   { ($($arg:tt)+) => { tracing::warn!(target: "payout", $($arg)+) }; }
#[macro_export]
macro_rules! payout_error  { ($($arg:tt)+) => { tracing::error!(target: "payout", $($arg)+) }; }
#[macro_export]
macro_rules! payout_debug  { ($($arg:tt)+) => { tracing::debug!(target: "payout", $($arg)+) }; }

// --- http_api ---
#[macro_export]
macro_rules! http_api_info  { ($($arg:tt)+) => { tracing::info!(target: "http_api", $($arg)+) }; }
#[macro_export]
macro_rules! http_api_warn  { ($($arg:tt)+) => { tracing::warn!(target: "http_api", $($arg)+) }; }
#[macro_export]
macro_rules! http_api_error { ($($arg:tt)+) => { tracing::error!(target: "http_api", $($arg)+) }; }
#[macro_export]
macro_rules! http_api_debug { ($($arg:tt)+) => { tracing::debug!(target: "http_api", $($arg)+) }; }

// --- shutdown ---
#[macro_export]
macro_rules! shutdown_info  { ($($arg:tt)+) => { tracing::info!(target: "shutdown", $($arg)+) }; }
#[macro_export]
macro_rules! shutdown_warn  { ($($arg:tt)+) => { tracing::warn!(target: "shutdown", $($arg)+) }; }
#[macro_export]
macro_rules! shutdown_error { ($($arg:tt)+) => { tracing::error!(target: "shutdown", $($arg)+) }; }
#[macro_export]
macro_rules! shutdown_debug { ($($arg:tt)+) => { tracing::debug!(target: "shutdown", $($arg)+) }; }

// --- main (application entry point) ---
#[macro_export]
macro_rules! main_info   { ($($arg:tt)+) => { tracing::info!(target: "main", $($arg)+) }; }
#[macro_export]
macro_rules! main_warn   { ($($arg:tt)+) => { tracing::warn!(target: "main", $($arg)+) }; }
#[macro_export]
macro_rules! main_error  { ($($arg:tt)+) => { tracing::error!(target: "main", $($arg)+) }; }
#[macro_export]
macro_rules! main_debug  { ($($arg:tt)+) => { tracing::debug!(target: "main", $($arg)+) }; }
