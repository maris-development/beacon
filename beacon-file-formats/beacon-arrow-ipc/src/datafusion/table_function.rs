use std::sync::Arc;

use arrow::datatypes::{DataType, Field};
use beacon_common::{listing_url::parse_listing_table_url, super_table::SuperListingTable};
use crate::datafusion::ArrowFormat;
use datafusion::{
    catalog::TableFunctionImpl, execution::object_store::ObjectStoreUrl, prelude::SessionContext,
};

use beacon_common::table_function::BeaconTableFunctionImpl;

pub struct ReadArrowFunc {
    // Session Reference
    runtime_handle: tokio::runtime::Handle,
    session_ctx: Arc<SessionContext>,
    data_object_store_url: ObjectStoreUrl,
}

impl ReadArrowFunc {
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

impl std::fmt::Debug for ReadArrowFunc {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ReadArrowFunc")
    }
}

impl BeaconTableFunctionImpl for ReadArrowFunc {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> String {
        "read_arrow".to_string()
    }

    fn description(&self) -> Option<String> {
        Some("Reads Arrow files from specified glob paths.".to_string())
    }

    fn arguments(&self) -> Option<Vec<arrow::datatypes::Field>> {
        Some(vec![Field::new(
            "glob_paths",
            DataType::List(Arc::new(Field::new("glob_path", DataType::Utf8, false))),
            false,
        )])
    }
}

impl TableFunctionImpl for ReadArrowFunc {
    fn call(
        &self,
        args: &[datafusion::prelude::Expr],
    ) -> datafusion::error::Result<std::sync::Arc<dyn datafusion::catalog::TableProvider>> {
        let glob_paths = beacon_common::table_function::parse_glob_paths_arg(args, "read_arrow")?;

        tracing::debug!("read_arrow glob paths: {:?}", glob_paths);

        let mut listing_urls = vec![];
        for path in &glob_paths {
            tracing::debug!("read_arrow processing path: {}", path);
            listing_urls.push(parse_listing_table_url(&self.data_object_store_url, path)?);
        }

        let file_format = ArrowFormat::default();
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
