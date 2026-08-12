//! [`ReadHdf5Func`]: the `read_hdf5` table function.
//!
//! Which reader runs is the format's decision, not this function's, so the
//! function asks the registered HDF5 factory. With netcdf-c — the default — it
//! hands the whole call to [`ReadNetCDFFunc`], which is what it has always
//! done. With the Rust reader it builds the format itself, because that reader
//! needs no native root and therefore accepts an s3, gs or az path.

use std::any::Any;
use std::collections::HashMap;
use std::sync::{Arc, Weak};

use arrow::datatypes::{DataType, Field};
use beacon_arrow_netcdf::datafusion::ReadNetCDFFunc;
use beacon_datafusion_ext::fast_object::FastObjectTable;
use beacon_common::table_function::BeaconTableFunctionImpl;
use beacon_datafusion_ext::listing_factory::ListingFactory;
use datafusion::{
    catalog::{TableFunctionImpl, TableProvider},
    common::plan_err,
    datasource::file_format::FileFormatFactory,
    prelude::{Expr, SessionContext},
    scalar::ScalarValue,
};

/// The `read_hdf5` table function.
pub struct ReadHdf5Func {
    /// The netCDF reader, for the default netcdf-c path. A NetCDF-4 file *is*
    /// HDF5, and netcdf-c's HDF5 dispatch opens plain HDF5 too, so there is
    /// nothing to re-implement there.
    inner: ReadNetCDFFunc,
    runtime_handle: tokio::runtime::Handle,
    session_ctx: Weak<SessionContext>,
}

impl ReadHdf5Func {
    pub fn new(runtime_handle: tokio::runtime::Handle, session_ctx: Weak<SessionContext>) -> Self {
        Self {
            // The inner reader carries the `read_hdf5` name so its own error messages read right.
            inner: ReadNetCDFFunc::with_name(
                "read_hdf5",
                runtime_handle.clone(),
                session_ctx.clone(),
            ),
            runtime_handle,
            session_ctx,
        }
    }
}

impl std::fmt::Debug for ReadHdf5Func {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ReadHdf5Func")
    }
}

impl BeaconTableFunctionImpl for ReadHdf5Func {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> String {
        "read_hdf5".to_string()
    }

    fn description(&self) -> Option<String> {
        Some(
            "Reads HDF5 files from specified glob paths. A NetCDF-4 file is HDF5, and plain HDF5 \
             whose datasets fit the array data model is read too."
                .to_string(),
        )
    }

    fn arguments(&self) -> Option<Vec<Field>> {
        Some(vec![
            Field::new(
                "glob_paths",
                DataType::List(Arc::new(Field::new("glob_path", DataType::Utf8, false))),
                false,
            ),
            Field::new(
                "dimensions",
                DataType::List(Arc::new(Field::new("dimension", DataType::Utf8, false))),
                true,
            ),
        ])
    }
}

impl TableFunctionImpl for ReadHdf5Func {
    fn call(&self, args: &[Expr]) -> datafusion::error::Result<Arc<dyn TableProvider>> {
        let session_ctx = self.session_ctx.upgrade().ok_or_else(|| {
            datafusion::common::plan_datafusion_err!("session context has been dropped")
        })?;
        let state = session_ctx.state();

        let factory = state.get_file_format_factory(crate::HDF5_FORMAT_NAME);
        let hdf5_factory = factory
            .as_ref()
            .and_then(|f| f.as_any().downcast_ref::<crate::Hdf5FormatFactory>());

        // Not registered, or registered on netcdf-c: this is the delegating
        // path this function has always taken.
        let Some(hdf5_factory) = hdf5_factory else {
            return self.inner.call(args);
        };
        if !hdf5_factory.config().use_rust_reader {
            return self.inner.call(args);
        }

        let listing_factory = state
            .config()
            .get_extension::<ListingFactory>()
            .ok_or_else(|| {
                datafusion::error::DataFusionError::Execution(
                    "read_hdf5: the ListingFactory is not registered on the session".to_string(),
                )
            })?;
        let glob_paths = beacon_common::table_function::parse_glob_paths_arg(args, "read_hdf5")?;
        let dimensions = parse_dimensions_arg(args)?;

        let listing_urls = glob_paths
            .iter()
            .map(|path| listing_factory.parse_listing_table_url(&state, path))
            .collect::<datafusion::error::Result<Vec<_>>>()?;
        if listing_urls.is_empty() {
            return plan_err!("read_hdf5: no valid glob paths provided");
        }

        // Build the file format from the factory registered on the session, so
        // the table function shares the runtime's configured format + reader
        // cache. Per-call settings (read dimensions) are passed as table
        // options. No native root: this reader reads through the object store,
        // so an s3, gs or az path works.
        let mut format_options: HashMap<String, String> = HashMap::new();
        if !dimensions.is_empty() {
            format_options.insert("read_dimensions".to_string(), dimensions.join(","));
        }
        let file_format = hdf5_factory.create(&state, &format_options)?;

        let fast_object_table = tokio::task::block_in_place(|| {
            self.runtime_handle.block_on(async {
                FastObjectTable::try_new(&session_ctx.state(), file_format, listing_urls).await
            })
        })?;

        Ok(Arc::new(fast_object_table))
    }
}

/// The optional second argument: the dimensions to read.
fn parse_dimensions_arg(args: &[Expr]) -> datafusion::error::Result<Vec<String>> {
    let Some(Expr::Literal(ScalarValue::List(values), _)) = args.get(1) else {
        return Ok(vec![]);
    };
    let Some(strings) = values
        .as_ref()
        .values()
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
    else {
        return plan_err!("read_hdf5 second argument must be a List<Utf8> of dimension names");
    };
    Ok(strings
        .iter()
        .filter_map(|value| value.map(|s| s.to_string()))
        .collect())
}
