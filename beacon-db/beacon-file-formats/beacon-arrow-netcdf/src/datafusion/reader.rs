use std::sync::Arc;

use beacon_nd_array::dataset::AnyDataset;
use object_store::{ObjectMeta, ObjectStore};

use crate::datafusion::object_meta_resolver::{DefaultNetCDFObjectResolver, NetCDFObjectResolver};

/// Which reader opens a NetCDF file.
///
/// The two readers produce the same dataset, so this only decides how the bytes
/// are fetched and decoded. See [`crate::oxcdf_reader`] for the trade-off.
///
/// This is the bare choice, small enough to key a schema by. [`FileAccess`] is
/// the choice plus whatever that reader needs to reach a file.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum ReaderBackend {
    /// `oxcdf`, pure Rust over `object_store`. The default.
    #[default]
    Oxcdf,
    /// netcdf-c, through its Rust bindings. The fallback.
    NetcdfC,
}

impl ReaderBackend {
    /// The name this backend answers to, in configuration and in a log line.
    pub fn as_str(&self) -> &'static str {
        match self {
            ReaderBackend::Oxcdf => "rust",
            ReaderBackend::NetcdfC => "netcdf-c",
        }
    }
}

impl std::fmt::Display for ReaderBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Parse a reader backend supplied through configuration.
///
/// `key` names the setting in the error, so one message serves the
/// `BEACON_NETCDF_BACKEND` variable, the `BEACON_HDF5_BACKEND` variable and the
/// `backend` option of one table. Both formats parse their backend here, so they
/// accept exactly the same spellings.
pub fn parse_backend(key: &str, value: &str) -> datafusion::error::Result<ReaderBackend> {
    match value.trim().to_ascii_lowercase().as_str() {
        "rust" | "oxcdf" => Ok(ReaderBackend::Oxcdf),
        "netcdf-c" | "netcdf_c" | "netcdfc" | "c" => Ok(ReaderBackend::NetcdfC),
        other => Err(datafusion::error::DataFusionError::Execution(format!(
            "invalid reader backend for '{key}': '{other}'. Use 'rust' or 'netcdf-c'"
        ))),
    }
}

/// The backend a table reads on: the `backend` option, else `default`.
///
/// `use_rust_reader` is the name this option carried in 2.0.0-rc.1. It still
/// works, so a table that pinned a reader keeps it, and `backend` wins when a
/// table names both.
///
/// netCDF and HDF5 both resolve their reader here, so a table option means the
/// same thing whichever format reads it.
pub fn backend_from_options(
    format_options: &std::collections::HashMap<String, String>,
    default: ReaderBackend,
) -> datafusion::error::Result<ReaderBackend> {
    if let Some(value) = format_options.get("backend") {
        return parse_backend("backend", value);
    }
    if let Some(value) = format_options.get("use_rust_reader") {
        return match value.trim().to_ascii_lowercase().as_str() {
            "true" | "1" | "yes" | "on" => Ok(ReaderBackend::Oxcdf),
            "false" | "0" | "no" | "off" => Ok(ReaderBackend::NetcdfC),
            other => Err(datafusion::error::DataFusionError::Execution(format!(
                "invalid boolean for option 'use_rust_reader': '{other}'"
            ))),
        };
    }
    Ok(default)
}

/// How a table reaches its files, and which reader opens them.
///
/// The two readers do not need the same things, so each variant carries exactly
/// what its reader needs and nothing else. That is the point of the type: a
/// format cannot hold a resolver the reader never consults, and it cannot pick
/// netcdf-c without one.
#[derive(Debug, Clone)]
pub enum FileAccess {
    /// netcdf-c opens a path itself, so it cannot work without a resolver to
    /// turn an object path into that path.
    NetcdfC {
        /// Builds the native path netcdf-c opens, from the table's root store.
        resolver: Arc<dyn NetCDFObjectResolver>,
    },
    /// `oxcdf` reads byte ranges through the scan's own object store. It needs
    /// nothing else, which is why this variant carries nothing.
    Oxcdf,
}

impl Default for FileAccess {
    /// netcdf-c with a resolver that cannot resolve anything.
    ///
    /// This is the "no location was supplied" state, which
    /// [`FileFormatFactory::create`](datafusion::datasource::file_format::FileFormatFactory::create)
    /// and [`FileFormatFactory::default`](datafusion::datasource::file_format::FileFormatFactory::default)
    /// produce. A scan on it fails per object, naming the path it could not
    /// resolve. See [`FileFormatFactoryExt::create_with_native_root`](beacon_datafusion_ext::format_ext::FileFormatFactoryExt::create_with_native_root)
    /// for the path that supplies one.
    fn default() -> Self {
        Self::NetcdfC {
            resolver: Arc::new(DefaultNetCDFObjectResolver),
        }
    }
}

impl FileAccess {
    /// netcdf-c, reading through `resolver`.
    pub fn netcdf_c(resolver: Arc<dyn NetCDFObjectResolver>) -> Self {
        Self::NetcdfC { resolver }
    }

    /// The reader this access uses.
    pub fn backend(&self) -> ReaderBackend {
        match self {
            FileAccess::NetcdfC { .. } => ReaderBackend::NetcdfC,
            FileAccess::Oxcdf => ReaderBackend::Oxcdf,
        }
    }

    /// Describe where one object's bytes come from.
    ///
    /// netcdf-c resolves the object to a native path. `oxcdf` takes the scan's
    /// store and the object path as they are, so it also reads s3, gs and az.
    /// The format, the statistics and the opener all come through here, so they
    /// can never disagree about a file.
    pub fn input_for(
        &self,
        store: &Arc<dyn ObjectStore>,
        object: &ObjectMeta,
    ) -> datafusion::error::Result<NetcdfInput> {
        match self {
            FileAccess::NetcdfC { resolver } => {
                let native_path = resolver.resolve(object).map_err(|e| {
                    datafusion::error::DataFusionError::Execution(format!(
                        "Failed to resolve object metadata (path) to NetCDF native path: {e}"
                    ))
                })?;
                Ok(NetcdfInput::NetcdfC(native_path))
            }
            FileAccess::Oxcdf => Ok(NetcdfInput::Oxcdf {
                store: store.clone(),
                path: object.location.clone(),
            }),
        }
    }
}

/// Where a reader gets the bytes of one NetCDF object.
///
/// The variant also names the reader: netcdf-c opens a path itself, and `oxcdf`
/// reads through the object store. Build one with [`FileAccess::input_for`].
#[derive(Debug, Clone)]
pub enum NetcdfInput {
    /// netcdf-c opens this native path directly. It is a local path, or an
    /// `http(s)` URL that carries the `#mode=bytes` suffix.
    NetcdfC(String),
    /// `oxcdf` range-reads the object through the store. No local copy is made,
    /// so this also covers s3, gs and az.
    Oxcdf {
        /// The store the object lives in.
        store: Arc<dyn ObjectStore>,
        /// The object, relative to that store.
        path: object_store::path::Path,
    },
}

impl NetcdfInput {
    /// The reader this input belongs to.
    pub fn backend(&self) -> ReaderBackend {
        match self {
            NetcdfInput::NetcdfC(_) => ReaderBackend::NetcdfC,
            NetcdfInput::Oxcdf { .. } => ReaderBackend::Oxcdf,
        }
    }

    /// The location to name in an error message.
    pub fn location(&self) -> String {
        match self {
            NetcdfInput::NetcdfC(path) => path.clone(),
            NetcdfInput::Oxcdf { path, .. } => path.to_string(),
        }
    }

    /// Open the dataset this input points at.
    pub async fn open(self) -> anyhow::Result<AnyDataset> {
        match self {
            NetcdfInput::NetcdfC(path) => crate::reader::open_dataset(path).await,
            NetcdfInput::Oxcdf { store, path } => {
                crate::oxcdf_reader::open_dataset(store, path).await
            }
        }
    }
}

/// Fetch the Arrow schema for a NetCDF object by opening the dataset and
/// converting its fields to an Arrow [`SchemaRef`].
///
/// When `read_dimensions` is provided the dataset is projected to only
/// include variables that belong to those dimensions before deriving the
/// Arrow schema. When it is absent, a broadcast-compatible default dimension
/// set is auto-selected (see [`beacon_nd_array::dataset::resolve_read_dimensions`])
/// so the schema matches
/// what `SELECT *` can actually return.
///
/// A repeated inference of the same file is answered by the schema cache of
/// [`beacon_datafusion_ext::format_ext`], above this function, so this one
/// always opens the file.
pub async fn fetch_schema(
    input: NetcdfInput,
    read_dimensions: Option<Vec<String>>,
) -> datafusion::error::Result<arrow::datatypes::SchemaRef> {
    let dataset = input.open().await.map_err(|e| {
        datafusion::error::DataFusionError::Execution(format!(
            "Failed to open NetCDF dataset for schema inference: {e}"
        ))
    })?;

    let dataset = if let Some(dims) = beacon_nd_array::dataset::resolve_read_dimensions(
        &dataset,
        read_dimensions,
        Some("read_netcdf"),
    ) {
        let proj = beacon_nd_array::projection::DatasetProjection {
            dimension_projection: Some(dims),
            index_projection: None,
        };
        dataset.project(&proj).map_err(|e| {
            datafusion::error::DataFusionError::Execution(format!(
                "Failed to project NetCDF dataset with dimensions: {e}"
            ))
        })?
    } else {
        dataset
    };

    let schema =
        beacon_nd_array::arrow::schema::any_dataset_to_arrow_schema(&dataset).map_err(|e| {
            datafusion::error::DataFusionError::Execution(format!(
                "Failed to derive Arrow schema from NetCDF dataset: {e}"
            ))
        })?;

    Ok(schema.into())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The Rust reader is the default. netcdf-c is reached by naming it.
    #[test]
    fn the_default_backend_is_the_rust_reader() {
        assert_eq!(ReaderBackend::default(), ReaderBackend::Oxcdf);
    }

    /// Every spelling a deployment may already hold, and both directions of the
    /// name a log line prints.
    #[test]
    fn a_backend_parses_from_either_name() {
        for value in ["rust", "RUST", " rust ", "oxcdf"] {
            assert_eq!(
                parse_backend("backend", value).unwrap(),
                ReaderBackend::Oxcdf,
                "{value}"
            );
        }
        for value in ["netcdf-c", "netcdf_c", "netcdfc", "C"] {
            assert_eq!(
                parse_backend("backend", value).unwrap(),
                ReaderBackend::NetcdfC,
                "{value}"
            );
        }
        assert_eq!(ReaderBackend::Oxcdf.as_str(), "rust");
        assert_eq!(ReaderBackend::NetcdfC.as_str(), "netcdf-c");
    }

    /// The error names the setting and the value, so an operator can find both.
    #[test]
    fn an_unknown_backend_names_the_setting_and_the_values() {
        let error = parse_backend("BEACON_NETCDF_BACKEND", "hdf5")
            .unwrap_err()
            .to_string();
        assert!(error.contains("BEACON_NETCDF_BACKEND"), "{error}");
        assert!(error.contains("hdf5"), "{error}");
        assert!(error.contains("rust"), "{error}");
        assert!(error.contains("netcdf-c"), "{error}");
    }
}
