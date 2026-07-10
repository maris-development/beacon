use std::sync::Arc;

use crate::datafusion::TiffFormat;
use arrow::datatypes::{DataType, Field};
use beacon_common::{listing_url::parse_listing_table_url, super_table::SuperListingTable};
use beacon_object_storage::DatasetsStore;
use datafusion::{
    catalog::TableFunctionImpl, execution::object_store::ObjectStoreUrl, prelude::SessionContext,
};

use beacon_common::table_function::BeaconTableFunctionImpl;

pub struct ReadTiffFunc {
    runtime_handle: tokio::runtime::Handle,
    session_ctx: Arc<SessionContext>,
    data_object_store_url: ObjectStoreUrl,
}

impl ReadTiffFunc {
    pub fn new(
        runtime_handle: tokio::runtime::Handle,
        session_ctx: Arc<SessionContext>,
        data_object_store_url: ObjectStoreUrl,
        _datasets_object_store: Arc<DatasetsStore>,
    ) -> Self {
        Self {
            runtime_handle,
            session_ctx,
            data_object_store_url,
        }
    }
}

impl std::fmt::Debug for ReadTiffFunc {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ReadTiffFunc")
    }
}

impl BeaconTableFunctionImpl for ReadTiffFunc {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn description(&self) -> Option<String> {
        Some("Reads TIFF files from specified glob paths.".to_string())
    }

    fn name(&self) -> String {
        "read_tiff".to_string()
    }

    fn arguments(&self) -> Option<Vec<arrow::datatypes::Field>> {
        Some(vec![Field::new(
            "glob_paths",
            DataType::List(Arc::new(Field::new("glob_path", DataType::Utf8, false))),
            false,
        )])
    }
}

impl TableFunctionImpl for ReadTiffFunc {
    fn call(
        &self,
        args: &[datafusion::prelude::Expr],
    ) -> datafusion::error::Result<std::sync::Arc<dyn datafusion::catalog::TableProvider>> {
        let glob_paths = beacon_common::table_function::parse_glob_paths_arg(args, "read_tiff")?;

        let mut listing_urls = vec![];
        for path in &glob_paths {
            listing_urls.push(parse_listing_table_url(&self.data_object_store_url, path)?);
        }

        let file_format = TiffFormat::new(Default::default());
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
