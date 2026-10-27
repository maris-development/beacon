//! Croissant metadata for Beacon tables.
//!
//! Croissant is the MLCommons JSON-LD format that describes a dataset for ML tools.
//! This crate maps a table schema to Croissant types and holds the Croissant 1.1 context.

pub mod context;
pub mod types;

pub use context::{context, CONFORMS_TO};
pub use types::{CroissantType, ServedColumn, ServedSchema};
