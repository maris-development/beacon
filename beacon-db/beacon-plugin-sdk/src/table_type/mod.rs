//! Table type plugins: a named DataFusion [`TableProviderFactory`] for
//! `CREATE EXTERNAL TABLE ... STORED AS <name>`.
//!
//! The factory crosses the boundary as a `datafusion-ffi` [`FFI_TableProviderFactory`].

mod runtime;

use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;

use datafusion::catalog::TableProviderFactory;
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::CreateExternalTable;
use datafusion_ffi::table_provider_factory::{
    FFI_TableProviderFactory, ForeignTableProviderFactory,
};
use datafusion_ffi::util::FFI_Result;
use stabby::string::String as SString;
use stabby::vec::Vec as SVec;

use crate::panic_safe::panic_message;
use crate::{ABI_VERSION, sdk_abi_version};
use runtime::{RuntimeFactory, plugin_runtime, plugin_task_ctx_provider};

/// A table type that a plugin exports: the `STORED AS` name and its factory.
#[derive(Debug, Clone)]
pub struct TableType {
    /// The `STORED AS` name. Use lower case `[a-z0-9_]+`.
    pub name: String,
    /// Makes the table of each `CREATE EXTERNAL TABLE`.
    pub factory: Arc<dyn TableProviderFactory>,
}

impl TableType {
    /// Makes a table type with the `STORED AS` name `name`.
    pub fn new(name: impl Into<String>, factory: Arc<dyn TableProviderFactory>) -> Self {
        Self {
            name: name.into(),
            factory,
        }
    }
}

/// A [`TableType`] as FFI data.
#[repr(C)]
pub struct FFI_PluginTableType {
    /// Returns [`crate::BEACON_PLUGIN_SDK_ABI`] of the exporter. It stays the first field.
    pub version: unsafe extern "C" fn() -> u64,
    /// The `STORED AS` name.
    pub name: SString,
    /// The factory. It runs inside the plugin runtime and turns panics into errors.
    pub factory: FFI_TableProviderFactory,
}

/// The name of the table types symbol, with a trailing NUL.
pub const TABLE_TYPES_SYMBOL: &[u8] = b"beacon_plugin_table_types\0";

/// The type of the [`TABLE_TYPES_SYMBOL`] function.
pub type TableTypesFn = extern "C" fn() -> FFI_Result<SVec<FFI_PluginTableType>>;

fn export_table_type(table_type: TableType) -> FFI_PluginTableType {
    let factory = Arc::new(RuntimeFactory(table_type.factory));
    FFI_PluginTableType {
        version: sdk_abi_version,
        name: SString::from(table_type.name.as_str()),
        factory: FFI_TableProviderFactory::new(
            factory,
            Some(plugin_runtime().handle().clone()),
            plugin_task_ctx_provider(),
            None,
        ),
    }
}

/// Exports the table types that `make` returns. A panic becomes an error.
pub fn export_table_types(make: fn() -> Vec<TableType>) -> FFI_Result<SVec<FFI_PluginTableType>> {
    let exported = catch_unwind(AssertUnwindSafe(|| {
        let mut out = SVec::new();
        for table_type in make() {
            out.push(export_table_type(table_type));
        }
        out
    }));
    match exported {
        Ok(types) => FFI_Result::Ok(types),
        Err(payload) => FFI_Result::Err(SString::from(
            format!(
                "plugin table types panicked: {}",
                panic_message(payload.as_ref())
            )
            .as_str(),
        )),
    }
}

/// Imports a table type: its name and its factory.
pub fn import_table_type(
    ffi: FFI_PluginTableType,
) -> Result<(String, Arc<dyn TableProviderFactory>)> {
    // SAFETY: `version` has the same place in every table version.
    let version = unsafe { (ffi.version)() };
    if version != ABI_VERSION {
        return Err(DataFusionError::Plan(format!(
            "table type FFI version {version} is not supported (want {ABI_VERSION})"
        )));
    }
    let name = ffi.name.as_str().to_string();
    Ok((name, Arc::new(ForeignTableProviderFactory(ffi.factory))))
}

/// Reads option `key` of `cmd`, or `format.<key>`: DataFusion prefixes a key without a `.`.
pub fn table_option<'a>(cmd: &'a CreateExternalTable, key: &str) -> Option<&'a str> {
    cmd.options
        .get(key)
        .or_else(|| cmd.options.get(&format!("format.{key}")))
        .map(String::as_str)
}

#[cfg(test)]
pub(crate) mod tests;
