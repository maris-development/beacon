//! Wire-format-agnostic credential presented by a caller.
//!
//! The HTTP and Flight SQL transports parse their respective headers into a [`Credential`]; auth
//! providers then decide how to validate it. Local username/password providers handle
//! [`Credential::Basic`]; token providers (e.g. OIDC) handle [`Credential::Bearer`]. The auth
//! context resolves [`Credential::Session`] itself.

/// A credential extracted from a request, before authentication.
#[derive(Debug, Clone)]
pub enum Credential {
    /// A username/password pair (HTTP Basic, Flight `BasicAuth`).
    Basic { username: String, password: String },
    /// An opaque bearer token (e.g. an OIDC/OAuth2 JWT access token).
    Bearer(String),
    /// A Beacon session token (`bst_...`), resolved by the auth context, never by a provider.
    Session(String),
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

    /// Builds a session credential from an owned token.
    pub fn session(token: impl Into<String>) -> Self {
        Self::Session(token.into())
    }

    /// Reads a `Bearer` value: a `bst_` token is a session, any other value is a provider token.
    pub fn from_bearer(token: &str) -> Self {
        if token.starts_with(crate::session::SESSION_TOKEN_PREFIX) {
            Self::session(token)
        } else {
            Self::bearer(token)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_prefixed_bearer_is_a_session() {
        match Credential::from_bearer("bst_abc") {
            Credential::Session(token) => assert_eq!(token, "bst_abc"),
            other => panic!("expected a session credential, got {other:?}"),
        }
    }

    #[test]
    fn any_other_bearer_stays_a_bearer() {
        match Credential::from_bearer("a.b.c") {
            Credential::Bearer(token) => assert_eq!(token, "a.b.c"),
            other => panic!("expected a bearer credential, got {other:?}"),
        }
    }
}
