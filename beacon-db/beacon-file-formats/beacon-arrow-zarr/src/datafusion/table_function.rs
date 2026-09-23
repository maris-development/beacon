use std::{
    fmt::Debug,
    sync::{Arc, Weak},
};

use crate::datafusion::ZarrFormat;
use arrow::datatypes::{DataType, Field};
use beacon_datafusion_ext::fast_object::FastObjectTable;
use beacon_datafusion_ext::listing_factory::ListingFactory;
use datafusion::{catalog::TableFunctionImpl, prelude::SessionContext};

use beacon_common::table_function::BeaconTableFunctionImpl;

pub struct ReadZarrFunc {
    // Session Reference
    runtime_handle: tokio::runtime::Handle,
    session_ctx: Weak<SessionContext>,
}

impl ReadZarrFunc {
    pub fn new(runtime_handle: tokio::runtime::Handle, session_ctx: Weak<SessionContext>) -> Self {
        Self {
            runtime_handle,
            session_ctx,
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
        Some(
            "Reads Zarr stores from specified glob paths. The optional second argument lists the \
             dimensions to read. The optional third argument, a boolean, skips a group that \
             does not fit that list instead of failing the query."
                .to_string(),
        )
    }

    fn name(&self) -> String {
        "read_zarr".to_string()
    }

    fn arguments(&self) -> Option<Vec<arrow::datatypes::Field>> {
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
            Field::new("skip_unbroadcastable", DataType::Boolean, true),
        ])
    }
}

impl TableFunctionImpl for ReadZarrFunc {
    fn call(
        &self,
        args: &[datafusion::prelude::Expr],
    ) -> datafusion::error::Result<std::sync::Arc<dyn datafusion::catalog::TableProvider>> {
        let session_ctx = self.session_ctx.upgrade().ok_or_else(|| {
            datafusion::common::plan_datafusion_err!("session context has been dropped")
        })?;
        let state = session_ctx.state();
        let listing_factory = state
            .config()
            .get_extension::<ListingFactory>()
            .ok_or_else(|| {
                datafusion::common::plan_datafusion_err!(
                    "ListingFactory extension not found in session state"
                )
            })?;
        let glob_paths = beacon_common::table_function::parse_glob_paths_arg(args, "read_zarr")?;

        // Optional second argument: an explicit list of dimensions to read.
        let dimensions =
            beacon_common::table_function::parse_dimensions_arg(args, 1, "read_zarr", "second")?;

        tracing::debug!("read_zarr glob paths: {:?}", glob_paths);

        let mut listing_urls = vec![];
        for path in &glob_paths {
            tracing::debug!("read_zarr processing path: {}", path);
            listing_urls.push(listing_factory.parse_listing_table_url(&state, path)?);
        }

        // Optional third argument: skip a group that does not fit the list.
        let skip_unbroadcastable = beacon_common::table_function::parse_bool_arg(
            args,
            2,
            "read_zarr",
            "third",
            "skip the groups that cannot broadcast",
        )?
        .unwrap_or(false);

        // Predicate pushdown is handled automatically by the shared engine, so
        // no manual statistics/column selection is needed.
        let read_dimensions = (!dimensions.is_empty()).then_some(dimensions);
        let file_format =
            ZarrFormat::new(read_dimensions).with_skip_unbroadcastable(skip_unbroadcastable);

        let fast_object_table = tokio::task::block_in_place(|| {
            self.runtime_handle.block_on(async move {
                FastObjectTable::try_new(&session_ctx.state(), Arc::new(file_format), listing_urls)
                    .await
            })
        })?;

        Ok(Arc::new(fast_object_table))
    }
}
