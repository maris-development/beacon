use std::sync::{Arc, Weak};

use crate::datafusion::TiffFormat;
use arrow::datatypes::{DataType, Field};
use beacon_datafusion_ext::fast_object::FastObjectTable;
use beacon_datafusion_ext::listing_factory::ListingFactory;
use datafusion::{
    catalog::TableFunctionImpl, common::plan_err, prelude::Expr, prelude::SessionContext,
    scalar::ScalarValue,
};

use beacon_common::table_function::BeaconTableFunctionImpl;

pub struct ReadTiffFunc {
    runtime_handle: tokio::runtime::Handle,
    session_ctx: Weak<SessionContext>,
}

impl ReadTiffFunc {
    pub fn new(runtime_handle: tokio::runtime::Handle, session_ctx: Weak<SessionContext>) -> Self {
        Self {
            runtime_handle,
            session_ctx,
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
        return plan_err!("read_tiff second argument must be a List<Utf8> of dimension names");
    };
    Ok(strings
        .iter()
        .filter_map(|value| value.map(|s| s.to_string()))
        .collect())
}

impl TableFunctionImpl for ReadTiffFunc {
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
        let glob_paths = beacon_common::table_function::parse_glob_paths_arg(args, "read_tiff")?;
        let dimensions = parse_dimensions_arg(args)?;

        let mut listing_urls = vec![];
        for path in &glob_paths {
            listing_urls.push(listing_factory.parse_listing_table_url(&state, path)?);
        }

        let file_format = TiffFormat::new(Default::default())
            .with_read_dimensions((!dimensions.is_empty()).then_some(dimensions));

        let fast_object_table = tokio::task::block_in_place(|| {
            self.runtime_handle.block_on(async move {
                FastObjectTable::try_new(&session_ctx.state(), Arc::new(file_format), listing_urls)
                    .await
            })
        })?;

        Ok(Arc::new(fast_object_table))
    }
}
