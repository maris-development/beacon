use std::sync::Arc;

use arrow::datatypes::Field;
use beacon_datafusion_ext::format_ext::FileFormatFactoryExt;
use beacon_object_storage::DatasetsStore;
use datafusion::{
    catalog::TableFunctionImpl,
    execution::object_store::ObjectStoreUrl,
    logical_expr::{Documentation, Signature},
    prelude::SessionContext,
};

pub mod list_datasets;
pub mod read_arrow;
pub mod read_atlas;
pub mod read_bbf;
pub mod read_csv;
pub mod read_geoparquet;
pub mod read_netcdf;
pub mod read_odv_ascii;
pub mod read_parquet;
pub mod read_schema;
pub mod read_tiff;
pub mod read_zarr;

pub fn register_table_functions(
    runtime_handle: tokio::runtime::Handle,
    session_ctx: Arc<SessionContext>,
    data_object_store_url: ObjectStoreUrl,
    datasets_object_store: Arc<DatasetsStore>,
    file_formats: Vec<Arc<dyn FileFormatFactoryExt>>,
) -> Vec<Arc<dyn BeaconTableFunctionImpl>> {
    // Bridge: build the per-format config (owned by each format crate) from the
    // process-global config. A later change threads an owned config in instead.
    let netcdf_config = beacon_arrow_netcdf::datafusion::NetcdfConfig {
        use_reader_cache: beacon_config::CONFIG.netcdf.use_reader_cache,
        reader_cache_size: beacon_config::CONFIG.netcdf.reader_cache_size,
        enable_statistics: beacon_config::CONFIG.netcdf.enable_statistics,
    };
    let atlas_config = beacon_arrow_atlas::datafusion::AtlasConfig {
        use_reader_cache: beacon_config::CONFIG.atlas.use_reader_cache,
        reader_cache_size: beacon_config::CONFIG.atlas.reader_cache_size,
    };
    let bbf_config = beacon_arrow_bbf::datafusion::BbfConfig {
        split_streams_slice: beacon_config::CONFIG.runtime.bbf_split_streams_slice,
    };

    vec![
        Arc::new(read_parquet::ReadParquetFunc::new(
            runtime_handle.clone(),
            session_ctx.clone(),
            data_object_store_url.clone(),
        )),
        Arc::new(read_geoparquet::ReadGeoParquetFunc::new(
            runtime_handle.clone(),
            session_ctx.clone(),
            data_object_store_url.clone(),
        )),
        Arc::new(read_arrow::ReadArrowFunc::new(
            runtime_handle.clone(),
            session_ctx.clone(),
            data_object_store_url.clone(),
        )),
        Arc::new(read_csv::ReadCsvFunc::new(
            runtime_handle.clone(),
            session_ctx.clone(),
            data_object_store_url.clone(),
        )),
        Arc::new(read_zarr::ReadZarrFunc::new(
            runtime_handle.clone(),
            session_ctx.clone(),
            data_object_store_url.clone(),
        )),
        Arc::new(read_netcdf::ReadNetCDFFunc::new(
            runtime_handle.clone(),
            session_ctx.clone(),
            data_object_store_url.clone(),
            datasets_object_store.clone(),
            netcdf_config.clone(),
        )),
        Arc::new(read_tiff::ReadTiffFunc::new(
            runtime_handle.clone(),
            session_ctx.clone(),
            data_object_store_url.clone(),
            datasets_object_store.clone(),
        )),
        Arc::new(read_bbf::ReadBBFFunc::new(
            runtime_handle.clone(),
            session_ctx.clone(),
            data_object_store_url.clone(),
            bbf_config.clone(),
        )),
        Arc::new(read_schema::ReadSchemaFunc::new(
            runtime_handle.clone(),
            session_ctx.clone(),
            data_object_store_url.clone(),
            datasets_object_store.clone(),
        )),
        Arc::new(read_odv_ascii::ReadOdvAsciiFunc::new(
            runtime_handle.clone(),
            session_ctx.clone(),
            data_object_store_url.clone(),
        )),
        Arc::new(read_atlas::ReadAtlasFunc::new(
            runtime_handle.clone(),
            session_ctx.clone(),
            data_object_store_url.clone(),
            datasets_object_store,
            atlas_config,
        )),
        Arc::new(list_datasets::ListDatasetsFunc::new(
            runtime_handle,
            session_ctx,
            data_object_store_url,
            file_formats,
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
