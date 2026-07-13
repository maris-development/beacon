use std::sync::{Arc, Weak};

use arrow::datatypes::{DataType, Field};
use beacon_common::{listing_url::parse_listing_table_url, super_table::SuperListingTable};
use crate::datafusion::OdvFormat;
use datafusion::{
    catalog::TableFunctionImpl,
    execution::object_store::ObjectStoreUrl,
    prelude::{Expr, SessionContext},
};

use beacon_common::table_function::BeaconTableFunctionImpl;

pub struct ReadOdvAsciiFunc {
    runtime_handle: tokio::runtime::Handle,
    session_ctx: Weak<SessionContext>,
    data_object_store_url: ObjectStoreUrl,
}

impl ReadOdvAsciiFunc {
    pub fn new(
        runtime_handle: tokio::runtime::Handle,
        session_ctx: Weak<SessionContext>,
        data_object_store_url: ObjectStoreUrl,
    ) -> Self {
        Self {
            runtime_handle,
            session_ctx,
            data_object_store_url,
        }
    }
}

impl std::fmt::Debug for ReadOdvAsciiFunc {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ReadOdvAsciiFunc")
    }
}

impl BeaconTableFunctionImpl for ReadOdvAsciiFunc {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> String {
        "read_odv_ascii".to_string()
    }

    fn description(&self) -> Option<String> {
        Some("Reads ODV ASCII files from specified glob paths.".to_string())
    }

    fn arguments(&self) -> Option<Vec<Field>> {
        Some(vec![Field::new(
            "glob_paths",
            DataType::List(Arc::new(Field::new("glob_path", DataType::Utf8, false))),
            false,
        )])
    }
}

impl TableFunctionImpl for ReadOdvAsciiFunc {
    fn call(
        &self,
        args: &[Expr],
    ) -> datafusion::error::Result<Arc<dyn datafusion::catalog::TableProvider>> {
        let glob_paths = beacon_common::table_function::parse_glob_paths_arg(args, "read_odv_ascii")?;

        tracing::debug!("read_odv_ascii glob paths: {:?}", glob_paths);

        let mut listing_urls = vec![];
        for path in &glob_paths {
            tracing::debug!("read_odv_ascii processing path: {}", path);
            listing_urls.push(parse_listing_table_url(&self.data_object_store_url, path)?);
        }

        let file_format = OdvFormat::new();
        let session_ctx = self.session_ctx.upgrade().ok_or_else(|| {
            datafusion::common::plan_datafusion_err!("session context has been dropped")
        })?;
        let super_listing_table = tokio::task::block_in_place(|| {
            self.runtime_handle.block_on(async move {
                SuperListingTable::new(
                    &session_ctx.state(),
                    Arc::new(file_format),
                    listing_urls,
                )
                .await
            })
        })?;

        Ok(Arc::new(super_listing_table))
    }
}
