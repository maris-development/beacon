//! DAP2 error responses.

use axum::http::{header, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};

use crate::headers::dap2_response;
use crate::names::quote;

/// A failure that the client gets as a DAP2 `Error { ... };` document.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum DapError {
    /// The URL or the constraint is not valid.
    #[error("{0}")]
    BadRequest(String),
    /// The request has no credentials, and anonymous access is off.
    #[error("authentication required")]
    Unauthorized,
    /// The table does not exist, or the caller cannot read it.
    #[error("{0}")]
    NotFound(String),
    /// The query failed before the first row. `/api/query` also uses 400 for this.
    #[error("{0}")]
    Query(String),
    /// A server fault.
    #[error("{0}")]
    Internal(String),
}

impl DapError {
    /// The HTTP status of the error.
    pub fn status(&self) -> StatusCode {
        match self {
            DapError::BadRequest(_) | DapError::Query(_) => StatusCode::BAD_REQUEST,
            DapError::Unauthorized => StatusCode::UNAUTHORIZED,
            DapError::NotFound(_) => StatusCode::NOT_FOUND,
            DapError::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    /// The DAP2 error document.
    pub fn body(&self) -> String {
        format!(
            "Error {{\n    code = {};\n    message = {};\n}};\n",
            self.status().as_u16(),
            quote(&self.to_string())
        )
    }
}

impl IntoResponse for DapError {
    fn into_response(self) -> Response {
        let mut response = dap2_response(
            "dods-error",
            "text/plain; charset=utf-8",
            self.body().into(),
        );
        *response.status_mut() = self.status();
        if self == DapError::Unauthorized {
            response.headers_mut().insert(
                header::WWW_AUTHENTICATE,
                HeaderValue::from_static("Basic realm=\"beacon\""),
            );
        }
        response
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn body_is_a_dap2_error_document() {
        let error = DapError::BadRequest("unknown column \"tmp\"".to_string());

        assert_eq!(
            error.body(),
            "Error {\n    code = 400;\n    message = \"unknown column \\\"tmp\\\"\";\n};\n"
        );
    }

    #[test]
    fn unauthorized_asks_for_basic_credentials() {
        let response = DapError::Unauthorized.into_response();

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(
            response.headers()[header::WWW_AUTHENTICATE],
            "Basic realm=\"beacon\""
        );
        assert_eq!(response.headers()["content-description"], "dods-error");
        assert_eq!(response.headers()["xdods-server"], "dods/3.7");
    }

    #[test]
    fn query_errors_use_400_like_the_query_api() {
        assert_eq!(
            DapError::Query("x".into()).status(),
            StatusCode::BAD_REQUEST
        );
        assert_eq!(
            DapError::NotFound("x".into()).status(),
            StatusCode::NOT_FOUND
        );
        assert_eq!(
            DapError::Internal("x".into()).status(),
            StatusCode::INTERNAL_SERVER_ERROR
        );
    }
}
