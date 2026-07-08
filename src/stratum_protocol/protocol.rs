use serde::Serialize;
use serde_json::Value;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub enum Method {
    Subscribe,
    Authorize,
    Notify,
    SetDifficulty,
    Submit,
    Ping,
    ExtranonceSubscribe,
    SetExtranonce,
    SuggestDifficulty,
    Unknown(String),
}

impl Method {
    pub fn parse(s: &str) -> Self {
        match s {
            "mining.subscribe" => Self::Subscribe,
            "mining.authorize" => Self::Authorize,
            "mining.notify" => Self::Notify,
            "mining.set_difficulty" => Self::SetDifficulty,
            "mining.submit" => Self::Submit,
            "mining.ping" => Self::Ping,
            "mining.extranonce.subscribe" => Self::ExtranonceSubscribe,
            "mining.set_extranonce" => Self::SetExtranonce,
            "mining.suggest_difficulty" => Self::SuggestDifficulty,
            other => Self::Unknown(other.to_string()),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct StratumRequest {
    pub id: Value,
    pub method: Method,
    #[serde(default)]
    pub params: Value,
}

#[derive(Debug, Clone, Serialize)]
pub struct StratumResponse {
    pub id: Value,
    pub result: Value,
    pub error: Value,
}

impl StratumResponse {
    pub fn ok(id: Value, result: Value) -> Self {
        Self {
            id,
            result,
            error: Value::Null,
        }
    }

    pub fn err(id: Value, code: i64, message: &str) -> Self {
        Self {
            id,
            result: Value::Null,
            error: serde_json::json!([code, message, Value::Null]),
        }
    }

    /// Share rejected: result=false with error reason.
    pub fn rejected(id: Value, code: i64, message: &str) -> Self {
        Self {
            id,
            result: Value::Bool(false),
            error: serde_json::json!([code, message, Value::Null]),
        }
    }
}

#[derive(thiserror::Error, Debug)]
pub enum StratumError {
    #[error("line too long")]
    LineTooLong,
    #[error("invalid json: {0}")]
    InvalidJson(String),
    #[error("invalid request shape")]
    InvalidRequestShape,
}

/// Decode one JSON line with strict maximum length and request shape checks.
pub fn decode_request_line(line: &str, max_len: usize) -> Result<StratumRequest, StratumError> {
    if line.len() > max_len {
        return Err(StratumError::LineTooLong);
    }
    let value: Value =
        serde_json::from_str(line).map_err(|e| StratumError::InvalidJson(e.to_string()))?;
    let id = value
        .get("id")
        .cloned()
        .ok_or(StratumError::InvalidRequestShape)?;
    let method = value
        .get("method")
        .and_then(|v| v.as_str())
        .ok_or(StratumError::InvalidRequestShape)?;
    let params = value.get("params").cloned().unwrap_or(Value::Array(vec![]));
    if !params.is_array() {
        return Err(StratumError::InvalidRequestShape);
    }
    Ok(StratumRequest {
        id,
        method: Method::parse(method),
        params,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_decode_subscribe() {
        let req = decode_request_line(r#"{"id":1,"method":"mining.subscribe","params":[]}"#, 1024)
            .unwrap();
        assert_eq!(req.method, Method::Subscribe);
    }

    #[test]
    fn test_decode_authorize() {
        let req = decode_request_line(
            r#"{"id":2,"method":"mining.authorize","params":["lotus_16PSJNf1EDEfGvaYzaXJCJZrXH4pgiTo7kyW61iGi.rig","x"]}"#,
            1024,
        )
        .unwrap();
        assert_eq!(req.method, Method::Authorize);
    }

    #[test]
    fn test_decode_submit() {
        let req = decode_request_line(
            r#"{"id":3,"method":"mining.submit","params":["worker","job1","00112233","001122334455","0011223344556677"]}"#,
            1024,
        )
        .unwrap();
        assert_eq!(req.method, Method::Submit);
    }

    #[test]
    fn test_decode_invalid_json() {
        let result = decode_request_line(r#"{"id":1,"method":"mining.subscribe"#, 1024);
        assert!(matches!(result, Err(StratumError::InvalidJson(_))));
    }

    #[test]
    fn test_decode_line_too_long() {
        let line = "x".repeat(20);
        assert!(matches!(
            decode_request_line(&line, 8),
            Err(StratumError::LineTooLong)
        ));
    }

    #[test]
    fn test_decode_missing_id() {
        let result = decode_request_line(r#"{"method":"mining.subscribe","params":[]}"#, 1024);
        assert!(matches!(result, Err(StratumError::InvalidRequestShape)));
    }

    #[test]
    fn test_decode_missing_method() {
        let result = decode_request_line(r#"{"id":1,"params":[]}"#, 1024);
        assert!(matches!(result, Err(StratumError::InvalidRequestShape)));
    }

    #[test]
    fn test_decode_params_not_array() {
        let result = decode_request_line(
            r#"{"id":1,"method":"mining.subscribe","params":"bad"}"#,
            1024,
        );
        assert!(matches!(result, Err(StratumError::InvalidRequestShape)));
    }
}
