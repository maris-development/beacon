//! The protocol headers of DAP2 responses.

use axum::body::Body;
use axum::http::{header, HeaderMap, HeaderName, HeaderValue};
use axum::response::Response;

const CONTENT_DESCRIPTION: HeaderName = HeaderName::from_static("content-description");
const XDODS_SERVER: HeaderName = HeaderName::from_static("xdods-server");
const XOPENDAP_SERVER: HeaderName = HeaderName::from_static("xopendap-server");

/// A DAP2 response with the headers that DAP2 clients read.
pub fn dap2_response(
    description: &'static str,
    content_type: &'static str,
    body: Body,
) -> Response {
    let mut response = Response::new(body);
    let headers = response.headers_mut();
    headers.insert(header::CONTENT_TYPE, HeaderValue::from_static(content_type));
    headers.insert(CONTENT_DESCRIPTION, HeaderValue::from_static(description));
    insert_server_headers(headers);
    response
}

/// The two headers that name the DAP2 protocol version and the server.
pub fn insert_server_headers(headers: &mut HeaderMap) {
    // ERDDAP sends the same value. DAP2 clients read the protocol version from it.
    headers.insert(XDODS_SERVER, HeaderValue::from_static("dods/3.7"));
    headers.insert(
        XOPENDAP_SERVER,
        HeaderValue::from_static(concat!("beacon/", env!("CARGO_PKG_VERSION"))),
    );
}
