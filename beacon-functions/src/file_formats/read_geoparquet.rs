use std::sync::Arc;

use arrow::datatypes::{DataType, Field};
use beacon_arrow_geoparquet::datafusion::{GeoParquetFormat, GeoParquetOptions};
use beacon_common::{listing_url::parse_listing_table_url, super_table::SuperListingTable};
use datafusion::{
    catalog::TableFunctionImpl, execution::object_store::ObjectStoreUrl, prelude::SessionContext,
};

use crate::file_formats::BeaconTableFunctionImpl;

pub struct ReadGeoParquetFunc {
    // Session Reference
    runtime_handle: tokio::runtime::Handle,
    session_ctx: Arc<SessionContext>,
    data_object_store_url: ObjectStoreUrl,
}

impl ReadGeoParquetFunc {
    pub fn new(
        runtime_handle: tokio::runtime::Handle,
        session_ctx: Arc<SessionContext>,
        data_object_store_url: ObjectStoreUrl,
    ) -> Self {
        Self {
            runtime_handle,
            session_ctx,
            data_object_store_url,
        }
    }
}

impl std::fmt::Debug for ReadGeoParquetFunc {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ReadGeoParquetFunc")
    }
}

impl BeaconTableFunctionImpl for ReadGeoParquetFunc {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn description(&self) -> Option<String> {
        Some(
            "Reads GeoParquet files from specified glob paths, decoding geometry columns to native GeoArrow."
                .to_string(),
        )
    }

    fn name(&self) -> String {
        "read_geoparquet".to_string()
    }

    fn arguments(&self) -> Option<Vec<arrow::datatypes::Field>> {
        Some(vec![Field::new(
            "glob_paths",
            DataType::List(Arc::new(Field::new("glob_path", DataType::Utf8, false))),
            false,
        )])
    }
}

impl TableFunctionImpl for ReadGeoParquetFunc {
    fn call(
        &self,
        args: &[datafusion::prelude::Expr],
    ) -> datafusion::error::Result<std::sync::Arc<dyn datafusion::catalog::TableProvider>> {
        let glob_paths = crate::file_formats::parse_glob_paths_arg(args, "read_geoparquet")?;

        tracing::debug!("read_geoparquet glob paths: {:?}", glob_paths);

        let mut listing_urls = vec![];
        for path in &glob_paths {
            tracing::debug!("read_geoparquet processing path: {}", path);
            listing_urls.push(parse_listing_table_url(&self.data_object_store_url, path)?);
        }

        // Reading does not use the lon/lat write options; defaults are fine.
        let file_format = GeoParquetFormat::new(GeoParquetOptions {
            longitude_column: None,
            latitude_column: None,
        });
        let super_listing_table = tokio::task::block_in_place(|| {
            self.runtime_handle.block_on(async move {
                SuperListingTable::new(
                    &self.session_ctx.state(),
                    Arc::new(file_format),
                    listing_urls,
                )
                .await
            })
        })?;

        Ok(Arc::new(super_listing_table))
    }
}
