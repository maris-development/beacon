//! File formats from plugins, in Beacon.
//!
//! A plugin gives small calls for one file (`beacon_plugin_sdk::file_format`). This crate
//! does the work for many files: schema merge, type widening, column adaptation, filters
//! and the scan plan. It also installs the formats of a plugin in a session.
//!
//! It also writes `COPY ... TO` output through a plugin writer.
//!
//! Not supported yet: statistics, the crawler, a local output path for a writer
//! (`wants_local_file`), and JSON query output in a plugin format.

mod factory;
mod format;
mod install;
mod nd;
mod opener;
mod options;
mod sink;
mod source;
mod table_function;

pub use factory::PluginFileFormatFactory;
pub use format::PluginFileFormat;
pub use install::{
    InstalledFormats, check_formats, function_names, import_formats, install_formats,
    uninstall_formats,
};
pub use options::plugin_options;
pub use source::PluginFileSource;
pub use table_function::{PluginReadFunc, options_from_args};
