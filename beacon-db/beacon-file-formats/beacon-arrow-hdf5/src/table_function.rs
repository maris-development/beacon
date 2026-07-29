//! [`ReadHdf5Func`]: the `read_hdf5` table function, delegating to the netCDF reader.

use std::any::Any;
use std::sync::{Arc, Weak};

use arrow::datatypes::Field;
use beacon_arrow_netcdf::datafusion::ReadNetCDFFunc;
use beacon_common::table_function::BeaconTableFunctionImpl;
use datafusion::{
    catalog::{TableFunctionImpl, TableProvider},
    prelude::{Expr, SessionContext},
};

/// The `read_hdf5` table function.
///
/// Reading is delegated to [`ReadNetCDFFunc`]: a NetCDF-4 file *is* HDF5, and netCDF-c's HDF5
/// dispatch opens plain HDF5 too, so there is nothing to re-implement — this only supplies the
/// `read_hdf5` name and HDF5-oriented help text.
pub struct ReadHdf5Func {
    inner: ReadNetCDFFunc,
}

impl ReadHdf5Func {
    pub fn new(runtime_handle: tokio::runtime::Handle, session_ctx: Weak<SessionContext>) -> Self {
        Self {
            // The inner reader carries the `read_hdf5` name so its own error messages read right.
            inner: ReadNetCDFFunc::with_name("read_hdf5", runtime_handle, session_ctx),
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
        self.inner.arguments()
    }
}

impl TableFunctionImpl for ReadHdf5Func {
    fn call(&self, args: &[Expr]) -> datafusion::error::Result<Arc<dyn TableProvider>> {
        self.inner.call(args)
    }
}
