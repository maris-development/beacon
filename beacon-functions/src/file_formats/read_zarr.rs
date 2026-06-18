use std::{fmt::Debug, sync::Arc};

use arrow::{
    array::Array,
    datatypes::{DataType, Field},
};
use beacon_common::{listing_url::parse_listing_table_url, super_table::SuperListingTable};
use beacon_arrow_zarr::datafusion::ZarrFormat;
use datafusion::{
    catalog::TableFunctionImpl,
    common::plan_err,
    execution::object_store::ObjectStoreUrl,
    prelude::{Expr, SessionContext},
    scalar::ScalarValue,
};

use crate::file_formats::BeaconTableFunctionImpl;

pub struct ReadZarrFunc {
    // Session Reference
    runtime_handle: tokio::runtime::Handle,
    session_ctx: Arc<SessionContext>,
    data_object_store_url: ObjectStoreUrl,
}

impl ReadZarrFunc {
    pub fn new(
        runtime_handle: tokio::runtime::Handle,
        session: Arc<SessionContext>,
        data_object_store_url: ObjectStoreUrl,
    ) -> Self {
        Self {
            runtime_handle,
            session_ctx: session,
            data_object_store_url,
        }
    }
}

impl Debug for ReadZarrFunc {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ReadZarrFunc")
    }
}

impl BeaconTableFunctionImpl for ReadZarrFunc {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn description(&self) -> Option<String> {
        Some("Reads Zarr files from specified glob paths.".to_string())
    }

    fn name(&self) -> String {
        "read_zarr".to_string()
    }

    fn arguments(&self) -> Option<Vec<arrow::datatypes::Field>> {
        Some(vec![Field::new(
            "glob_paths",
            DataType::List(Arc::new(Field::new("glob_path", DataType::Utf8, false))),
            false,
        )])
    }
}

impl TableFunctionImpl for ReadZarrFunc {
    fn call(
        &self,
        args: &[datafusion::prelude::Expr],
    ) -> datafusion::error::Result<std::sync::Arc<dyn datafusion::catalog::TableProvider>> {
        let mut glob_paths: Vec<String> = vec![];
        if let Some(glob_path_arg) = args.first() {
            match glob_path_arg {
                Expr::Literal(ScalarValue::List(values), _) => {
                    let string_array = values.as_ref().values();
                    match string_array
                        .as_any()
                        .downcast_ref::<arrow::array::StringArray>()
                    {
                        Some(str_arr) => {
                            str_arr.iter().for_each(|opt_str| {
                                if let Some(s) = opt_str {
                                    glob_paths.push(s.to_string());
                                }
                            });
                        }
                        None => {
                            return plan_err!(
                                "read_zarr first argument must be a List<Utf8> of glob paths"
                            );
                        }
                    }
                }
                _ => {
                    return plan_err!(
                        "read_zarr first argument must be a List<Utf8> of glob paths"
                    );
                }
            }
        } else {
            return plan_err!("read_zarr requires at least 1 argument: glob_paths : List<Utf8>");
        }

        tracing::debug!("read_zarr glob paths: {:?}", glob_paths);

        let mut listing_urls = vec![];
        for path in &glob_paths {
            tracing::debug!("read_zarr processing path: {}", path);
            listing_urls.push(parse_listing_table_url(&self.data_object_store_url, path)?);
        }

        // Predicate pushdown is handled automatically by the shared engine, so
        // no manual statistics/column selection is needed.
        let file_format = ZarrFormat::default();
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
