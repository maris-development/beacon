use axum::{http::HeaderMap, http::StatusCode};
use base64::{engine::general_purpose, Engine as _};

pub(crate) fn verify_basic_auth_header(headers: &HeaderMap) -> Result<(), StatusCode> {
    let auth_header = headers
        .get("Authorization")
        .ok_or(StatusCode::UNAUTHORIZED)?;

    let auth_str = auth_header.to_str().map_err(|_| StatusCode::UNAUTHORIZED)?;
    let (username, password) = parse_basic_auth_credentials(auth_str)?;

    if username == beacon_config::CONFIG.admin_username
        && password == beacon_config::CONFIG.admin_password
    {
        Ok(())
    } else {
        Err(StatusCode::UNAUTHORIZED)
    }
}

fn parse_basic_auth_credentials(auth_str: &str) -> Result<(String, String), StatusCode> {
    if !auth_str.starts_with("Basic ") {
        return Err(StatusCode::UNAUTHORIZED);
    }

    let credentials = general_purpose::STANDARD
        .decode(&auth_str[6..])
        .map_err(|_| StatusCode::UNAUTHORIZED)?;

    let credentials = String::from_utf8(credentials).map_err(|_| StatusCode::UNAUTHORIZED)?;

    let mut parts = credentials.splitn(2, ':');
    let username = parts.next().ok_or(StatusCode::UNAUTHORIZED)?;
    let password = parts.next().ok_or(StatusCode::UNAUTHORIZED)?;

    Ok((username.to_string(), password.to_string()))
}
