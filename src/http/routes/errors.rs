//! Error Handling - Custom error pages

use askama::Template;
use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
};
use crate::http::AppState;

/// Error page template
#[derive(Template)]
#[template(path = "http/error.html")]
pub struct ErrorPage {
    pub footer: FooterCtx,
    pub status_code: u16,
    pub title: String,
    pub message: String,
}

pub struct FooterCtx {
    pub pool_name: String,
    pub pool_fee_bps: String,
    pub contact: String,
}

/// Create error response with custom page
pub fn error_response(
    state: &AppState,
    status_code: StatusCode,
    title: &str,
    message: &str,
) -> Response {
    let fee_str = if state.pool_config.fee.enabled {
        state.pool_config.fee.fee_bps.to_string()
    } else {
        String::from("0")
    };

    let template = ErrorPage {
        footer: FooterCtx {
            pool_name: state.pool_config.name.clone(),
            pool_fee_bps: fee_str,
            contact: String::new(),
        },
        status_code: status_code.as_u16(),
        title: title.to_string(),
        message: message.to_string(),
    };

    match template.render() {
        Ok(html) => (status_code, axum::response::Html(html)).into_response(),
        Err(e) => {
            tracing::error!(error = %e, "failed to render error page");
            (status_code, format!("{}: {}", title, message)).into_response()
        }
    }
}
