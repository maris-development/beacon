//! Generic authentication and Beacon-owned authorization model.
//!
//! - [`AuthProvider`] is the pluggable authentication interface; it validates a [`Credential`] and
//!   returns the principal's identity and role names.
//! - [`BasicAuthProvider`] is the default in-memory username/password provider (argon2-hashed).
//! - [`AuthContext`] is the central object Beacon owns, combining a provider with the role model.
//!
//! Beacon owns authorization (the role/grant model); providers only answer "who is this and what
//! roles do they have", so an external identity provider can be slotted in without changing how
//! grants are expressed.

mod basic;
mod composite;
mod context;
mod credential;
mod oidc;
mod password;
mod provider;
mod role;

pub use basic::{BasicAuthProvider, InMemoryUserStore};
pub use composite::CompositeAuthProvider;
pub use context::{AuthContext, AuthIdentity, ANONYMOUS_USERNAME};
pub use credential::Credential;
pub use oidc::{OidcAuthProvider, OidcConfig};
pub use password::{hash_password, verify_password};
pub use provider::{AuthProvider, Authenticated, StoredUser, UserDirectory, UserRecord};
pub use role::{
    decode_target, encode_target, rule_kind, ConcreteTarget, Privilege, PrivilegeRule,
    PrivilegeTarget, Role, RoleProvider, RoleStore,
};
