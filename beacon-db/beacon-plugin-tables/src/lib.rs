//! Table types from plugins, in Beacon.
//!
//! A plugin exports a named `TableProviderFactory` (`beacon_plugin_sdk::table_type`).
//! This crate checks the names, keeps the installed types of a session, creates the tables
//! of `CREATE EXTERNAL TABLE ... STORED AS <name>`, and persists them.

mod definition;
mod filters;
mod install;
mod table;

pub use definition::PluginTableDefinition;
pub use install::{
    BUILT_IN_TABLE_TYPES, InstalledTableTypes, PluginTableType, check_table_types,
    import_table_types,
};
pub use table::{PluginTable, create_plugin_table};

#[cfg(test)]
mod tests;
