//! Generic authentication and Beacon-owned authorization model.
//!
//! - [`AuthProvider`] is the pluggable authentication interface.
//! - [`BasicAuthProvider`] is the default in-memory username/password provider (argon2-hashed).
//! - [`AuthContext`] is the central object Beacon owns, combining a provider with the role model.

mod basic;
mod context;
mod password;
mod provider;
mod role;
mod sqlite;

pub use basic::{BasicAuthProvider, InMemoryUserStore};
pub use context::{AuthContext, AuthIdentity, ANONYMOUS_USERNAME};
pub use password::{hash_password, verify_password};
pub use provider::{AuthProvider, UserDirectory};
pub use role::{
    ConcreteTarget, Privilege, PrivilegeRule, PrivilegeTarget, Role, RoleProvider, RoleStore,
};
pub use sqlite::{SqliteAuthProvider, SqliteStore};
