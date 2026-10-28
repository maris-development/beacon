//! Croissant metadata for Beacon tables.
//!
//! Croissant is the MLCommons JSON-LD format that describes a dataset for ML tools.
//! This crate maps a table schema to Croissant types and holds the Croissant 1.1 context.
//! Its routes serve a catalog, one document per table and the table rows as Parquet. Every
//! request runs as the caller, so RBAC is the same as on `/api/query`.

pub mod context;
pub mod document;
pub mod http;
pub mod tables;
pub mod types;
pub mod url;

pub use context::{context, CONFORMS_TO};
pub use document::{catalog, data_url, dataset, dataset_url, CatalogEntry, DATA_FILE};
pub use http::{router, CroissantError};
pub use tables::{catalog_entries, readable_tables};
pub use types::{CroissantType, ServedColumn, ServedSchema};
pub use url::{base_url, CroissantSettings, LINK_HEADERS};
