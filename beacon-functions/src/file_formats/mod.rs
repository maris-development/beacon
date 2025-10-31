use std::sync::Arc;

use arrow::datatypes::{DataType, Field};
use beacon_formats::netcdf::object_resolver::{NetCDFObjectResolver, NetCDFSinkResolver};
use datafusion::{
    catalog::{TableFunction, TableFunctionImpl},
    execution::{object_store::ObjectStoreUrl, SessionState},
    logical_expr::{Documentation, Signature},
    prelude::SessionContext,
};

pub mod read_arrow;
pub mod read_csv;
pub mod read_netcdf;
pub mod read_parquet;
pub mod read_schema;
pub mod read_zarr;

pub fn register_table_functions(
    runtime_handle: tokio::runtime::Handle,
    session_ctx: Arc<SessionContext>,
    data_object_store_url: ObjectStoreUrl,
    data_object_store_prefix: object_store::path::Path,
    netcdf_object_resolver: Arc<NetCDFObjectResolver>,
    netcdf_sink_resolver: Arc<NetCDFSinkResolver>,
) -> Vec<Arc<dyn BeaconTableFunctionImpl>> {
    vec![
        Arc::new(read_parquet::ReadParquetFunc::new(
            runtime_handle.clone(),
            session_ctx.clone(),
            data_object_store_url.clone(),
            data_object_store_prefix.clone(),
        )),
        Arc::new(read_arrow::ReadArrowFunc::new(
            runtime_handle.clone(),
            session_ctx.clone(),
            data_object_store_url.clone(),
            data_object_store_prefix.clone(),
        )),
        Arc::new(read_csv::ReadCsvFunc::new(
            runtime_handle.clone(),
            session_ctx.clone(),
            data_object_store_url.clone(),
            data_object_store_prefix.clone(),
        )),
        Arc::new(read_zarr::ReadZarrFunc::new(
            runtime_handle.clone(),
            session_ctx.clone(),
            data_object_store_url.clone(),
            data_object_store_prefix.clone(),
        )),
        Arc::new(read_netcdf::ReadNetCDFFunc::new(
            runtime_handle.clone(),
            session_ctx.clone(),
            data_object_store_url.clone(),
            data_object_store_prefix.clone(),
            netcdf_object_resolver.clone(),
            netcdf_sink_resolver.clone(),
        )),
        Arc::new(read_schema::ReadSchemaFunc::new(
            runtime_handle,
            session_ctx,
            data_object_store_url,
            data_object_store_prefix,
            netcdf_object_resolver,
            netcdf_sink_resolver,
        )),
    ]
}

pub trait BeaconTableFunctionImpl: TableFunctionImpl + Send + Sync {
    fn name(&self) -> String;
    fn as_any(&self) -> &dyn std::any::Any;
    fn arguments(&self) -> Option<Vec<Field>> {
        None
    }
    fn description(&self) -> Option<String> {
        None
    }
    fn signature(&self) -> Signature {
        // Default field that accepts glob paths
        let mut all_datatypes = vec![];
        let options = self.arguments().unwrap_or_default();
        for option in options {
            all_datatypes.push(option.data_type().clone());
        }
        Signature::exact(
            all_datatypes,
            datafusion::logical_expr::Volatility::Immutable,
        )
    }
    fn documentation(&self) -> Option<Documentation> {
        None
    }
}
