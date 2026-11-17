//! The object stores of a Beacon server: which store a URL means, and how Beacon builds it.
//!
//! This crate is for Beacon only. It builds the cloud store clients, so no plugin and no
//! `beacon-ffi-*` crate depends on it.
//!
//! - [`registry`]: the registry that builds a remote store at the first lookup.
//! - [`secrets`]: the credentials of `CREATE SECRET`, by URL scope.
//! - [`big_page_list`]: a store wrapper that lists a prefix in parallel pages.
//! - [`location`]: the rule that maps a `LOCATION` text to a store and a path.

pub mod big_page_list;
pub mod location;
pub mod registry;
pub mod secrets;

pub use location::{Location, resolve_location};
pub use registry::LazyObjectStoreRegistry;
pub use secrets::{Secret, SecretStore, SecretType};
