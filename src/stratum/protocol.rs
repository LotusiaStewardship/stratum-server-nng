use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Minimal JSON-RPC-ish Stratum V1 request shape.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct StratumRequest {
    pub id: Value,
    pub method: String,
    #[serde(default)]
    pub params: Value,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct StratumResponse {
    pub id: Value,
    pub result: Value,
    pub error: Value,
}

/// Optional/future methods scaffolding:
/// - mining.extranonce.subscribe
/// - mining.set_extranonce
/// - mining.suggest_difficulty
/// These are intentionally represented but not wired in this phase.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReservedMethod {
    ExtranonceSubscribe,
    SetExtranonce,
    SuggestDifficulty,
}
