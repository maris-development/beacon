//! Shared authentication helpers used by both the HTTP and Flight SQL transports.

use base64::{engine::general_purpose, Engine as _};

/// Marker error for invalid or malformed authentication credentials
#[derive(Debug, Clone, Copy)]
pub(crate) struct AuthError;

/// Validates a complete HTTP basic authorization value against configured admin credentials
pub(crate) fn verify_basic_auth_value(auth_str: &str) -> Result<(), AuthError> {
    let (username, password) = parse_basic_auth_credentials(auth_str)?;

    if validate_basic_auth_credentials(&username, &password) {
        Ok(())
    } else {
        Err(AuthError)
    }
}

/// Compares username and password pairs against the configured admin credentials
pub(crate) fn validate_basic_auth_credentials(username: &str, password: &str) -> bool {
    username == beacon_config::CONFIG.admin.username
        && password == beacon_config::CONFIG.admin.password
}

/// Parses a `Basic ...` authorization header into username and password components
pub(crate) fn parse_basic_auth_credentials(auth_str: &str) -> Result<(String, String), AuthError> {
    if !auth_str.starts_with("Basic ") {
        return Err(AuthError);
    }

    let credentials = general_purpose::STANDARD
        .decode(&auth_str[6..])
        .map_err(|_| AuthError)?;

    let credentials = String::from_utf8(credentials).map_err(|_| AuthError)?;

    let mut parts = credentials.splitn(2, ':');
    let username = parts.next().ok_or(AuthError)?;
    let password = parts.next().ok_or(AuthError)?;

    Ok((username.to_string(), password.to_string()))
}

/// Extracts the bearer token from a `Bearer ...` authorization value.
pub(crate) fn parse_bearer_token(auth_str: &str) -> Result<&str, AuthError> {
    let token = auth_str.strip_prefix("Bearer ").ok_or(AuthError)?;
    if token.is_empty() {
        return Err(AuthError);
    }

    Ok(token)
}
