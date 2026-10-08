//! Shared authentication helpers used by both the HTTP and Flight SQL transports.
//!
//! Credential validation is delegated to the runtime's auth context; these helpers only parse the
//! wire formats (HTTP Basic, Bearer).

use base64::{
    alphabet,
    engine::{DecodePaddingMode, GeneralPurpose, GeneralPurposeConfig},
    Engine as _,
};

/// Standard base64 that accepts input with or without padding.
/// The arrow-go Flight client encodes `Basic` credentials without padding.
const BASIC_AUTH_BASE64: GeneralPurpose = GeneralPurpose::new(
    &alphabet::STANDARD,
    GeneralPurposeConfig::new().with_decode_padding_mode(DecodePaddingMode::Indifferent),
);

/// Marker error for invalid or malformed authentication credentials
#[derive(Debug, Clone, Copy)]
pub(crate) struct AuthError;

/// Parses a `Basic ...` authorization header into username and password components
pub(crate) fn parse_basic_auth_credentials(auth_str: &str) -> Result<(String, String), AuthError> {
    if !auth_str.starts_with("Basic ") {
        return Err(AuthError);
    }

    let credentials = BASIC_AUTH_BASE64
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

#[cfg(test)]
mod tests {
    use base64::engine::general_purpose;

    use super::*;

    fn parse(encoded: &str) -> Option<(String, String)> {
        parse_basic_auth_credentials(&format!("Basic {encoded}")).ok()
    }

    #[test]
    fn basic_auth_accepts_padded_and_unpadded_base64() {
        // Lengths 20, 21 and 22 give two, zero and one padding characters.
        for credentials in [
            "admin:securepassword",
            "admin:securepassword1",
            "admin:securepassword12",
        ] {
            let (user, pass) = credentials.split_once(':').unwrap();
            let expected = Some((user.to_string(), pass.to_string()));

            let padded = general_purpose::STANDARD.encode(credentials);
            let unpadded = general_purpose::STANDARD_NO_PAD.encode(credentials);

            assert_eq!(parse(&padded), expected, "padded {padded}");
            assert_eq!(parse(&unpadded), expected, "unpadded {unpadded}");
        }
    }

    #[test]
    fn basic_auth_rejects_malformed_input() {
        assert!(parse_basic_auth_credentials("Bearer abc").is_err());
        assert!(parse("not base64!").is_none());
        assert!(parse(&general_purpose::STANDARD.encode("no-colon")).is_none());
    }
}
