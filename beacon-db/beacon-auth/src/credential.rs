//! Wire-format-agnostic credential presented by a caller.
//!
//! The HTTP and Flight SQL transports parse their respective headers into a [`Credential`]; auth
//! providers then decide how to validate it. Local username/password providers handle
//! [`Credential::Basic`]; token providers (e.g. OIDC) handle [`Credential::Bearer`].

/// A credential extracted from a request, before authentication.
#[derive(Debug, Clone)]
pub enum Credential {
    /// A username/password pair (HTTP Basic, Flight `BasicAuth`).
    Basic { username: String, password: String },
    /// An opaque bearer token (e.g. an OIDC/OAuth2 JWT access token).
    Bearer(String),
}

impl Credential {
    /// Builds a basic credential from owned parts.
    pub fn basic(username: impl Into<String>, password: impl Into<String>) -> Self {
        Self::Basic {
            username: username.into(),
            password: password.into(),
        }
    }

    /// Builds a bearer credential from an owned token.
    pub fn bearer(token: impl Into<String>) -> Self {
        Self::Bearer(token.into())
    }
}
