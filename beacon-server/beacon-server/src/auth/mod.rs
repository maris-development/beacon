//! Authentication and authorization of the server.
//!
//! The engine knows no users and no roles. The server owns them:
//!
//! - [`open`] builds the [`AuthContext`] over the auth store, a file next to `beacon.db`.
//! - [`RoleValidator`] gives the engine the access rules of one caller.
//! - [`AuthStatement`] holds the SQL statements that manage users and roles.
//! - The helpers below parse the wire formats (HTTP Basic, Bearer) for the HTTP and Flight
//!   SQL transports.

mod legacy;
mod statement;
mod system_tables;
mod validator;

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Context;
use base64::{
    alphabet,
    engine::{DecodePaddingMode, GeneralPurpose, GeneralPurposeConfig},
    Engine as _,
};
use beacon_auth::{
    AuthContext, BasicAuthProvider, FileAuthStore, InMemoryUserStore, RoleProvider, RoleStore,
    UserDirectory,
};
use beacon_core::runtime::Runtime;
use beacon_server_config::Config;

pub use statement::AuthStatement;
pub use validator::RoleValidator;

/// The file name of the auth store. The file is in the directory of `beacon.db`.
pub const AUTH_STORE_FILE: &str = "beacon-auth.json";

/// The path of the auth store for `config`.
pub fn auth_store_path(config: &Config) -> PathBuf {
    config.data.db_file.with_file_name(AUTH_STORE_FILE)
}

/// Builds the auth context of the server.
///
/// 1. Open the auth store. On the first start, import the users and roles that an earlier
///    version kept in `beacon.db`. Then drop those tables from `beacon.db`.
/// 2. Set the super-user of the configuration. Only this credential gives super-user rights.
/// 3. Load the users and roles into memory, where each login and each access check reads them.
/// 4. Create the anonymous user when anonymous access is on.
/// 5. Add the users and roles to the `system` schema of the engine.
///
/// # Arguments
///
/// * `config` - The server configuration, for the store path and the auth settings
/// * `runtime` - The engine, for the import and the `system` schema
///
/// # Errors
///
/// Returns an error when the auth store cannot be read or written, or when the import fails.
pub(crate) async fn open(config: &Config, runtime: &Runtime) -> anyhow::Result<Arc<AuthContext>> {
    let path = auth_store_path(config);
    let first_start = !path.exists();
    let store = Arc::new(FileAuthStore::open(path).await?);
    if first_start {
        legacy::import(runtime, &store)
            .await
            .context("failed to import the auth state from beacon.db")?;
    }
    // The store holds the state now, so `beacon.db` must hold no copy with password hashes.
    legacy::drop_tables(runtime)
        .await
        .context("failed to drop the internal auth tables from beacon.db")?;

    // `Arc::clone` shares the one store between the user copy and the role copy.
    let users = InMemoryUserStore::with_store(Arc::clone(&store) as Arc<dyn UserDirectory>);
    let provider = Arc::new(BasicAuthProvider::with_user_store(Arc::new(users)));
    let roles = RoleProvider::with_store(store as Arc<dyn RoleStore>);
    let mut auth = AuthContext::with_role_provider(provider, roles);
    auth.set_super_user(&config.admin.username, &config.admin.password);
    if config.auth.anonymous_enabled {
        auth.set_anonymous_user(crate::server::ANONYMOUS_USERNAME);
    }
    auth.hydrate().await?;

    // The anonymous user has no password and no role. An admin can give it read-only roles.
    if let Some(anonymous) = auth.anonymous_username() {
        if !auth.user_exists(anonymous).await {
            auth.create_user(anonymous, "").await?;
        }
    }

    let auth = Arc::new(auth);
    system_tables::register(runtime, &auth)?;
    Ok(auth)
}

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
