//! `read_atlas(paths)` and `read_atlas(paths, dimensions)`.

use std::collections::HashMap;
use std::sync::{Arc, Weak};

use arrow::datatypes::{DataType, Field};
use beacon_common::table_function::BeaconTableFunctionImpl;
use beacon_datafusion_ext::fast_object::FastObjectTable;
use beacon_datafusion_ext::listing_factory::ListingFactory;
use datafusion::{
    catalog::{TableFunctionImpl, TableProvider},
    common::{plan_datafusion_err, plan_err},
    error::Result,
    prelude::{Expr, SessionContext},
    scalar::ScalarValue,
};

use crate::datafusion::ATLAS_FORMAT;

/// Reads the Atlas collections that match one or more glob patterns.
pub struct ReadAtlasFunc {
    runtime_handle: tokio::runtime::Handle,
    session_ctx: Weak<SessionContext>,
}

impl ReadAtlasFunc {
    pub fn new(runtime_handle: tokio::runtime::Handle, session_ctx: Weak<SessionContext>) -> Self {
        Self {
            runtime_handle,
            session_ctx,
        }
    }
}

impl std::fmt::Debug for ReadAtlasFunc {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ReadAtlasFunc")
    }
}

impl BeaconTableFunctionImpl for ReadAtlasFunc {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn description(&self) -> Option<String> {
        Some(
            "Reads Atlas collections. Each path names a 'data.atlas' container file, exactly or \
             through a glob such as '**/data.atlas'. The optional second argument lists the \
             dimensions to read, and an array survives only when the list holds every one of its \
             own."
                .to_string(),
        )
    }

    fn name(&self) -> String {
        "read_atlas".to_string()
    }

    fn arguments(&self) -> Option<Vec<Field>> {
        Some(vec![
            Field::new(
                "glob_paths",
                DataType::List(Arc::new(Field::new("glob_path", DataType::Utf8, false))),
                false,
            ),
            Field::new(
                "dimensions",
                DataType::List(Arc::new(Field::new("dimension", DataType::Utf8, false))),
                false,
            ),
        ])
    }
}

impl TableFunctionImpl for ReadAtlasFunc {
    fn call(&self, args: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        let glob_paths = beacon_common::table_function::parse_glob_paths_arg(args, "read_atlas")?;

        let mut dimensions: Vec<String> = vec![];
        if let Some(argument) = args.get(1)
            && let Expr::Literal(ScalarValue::List(values), _) = argument
        {
            match values
                .as_ref()
                .values()
                .as_any()
                .downcast_ref::<arrow::array::StringArray>()
            {
                Some(names) => {
                    dimensions = names
                        .iter()
                        .filter_map(|name| name.map(str::to_string))
                        .collect();
                }
                None => {
                    return plan_err!(
                        "read_atlas second argument must be a List<Utf8> of dimension names"
                    );
                }
            }
        }

        tracing::debug!("read_atlas glob paths: {glob_paths:?}");

        let session_ctx = self
            .session_ctx
            .upgrade()
            .ok_or_else(|| plan_datafusion_err!("session context has been dropped"))?;
        let state = session_ctx.state();

        let listing_factory = state
            .config()
            .get_extension::<ListingFactory>()
            .ok_or_else(|| {
                plan_datafusion_err!("read_atlas: the listing factory is not registered")
            })?;
        let mut listing_urls = Vec::with_capacity(glob_paths.len());
        for path in &glob_paths {
            listing_urls.push(listing_factory.parse_listing_table_url(&state, path)?);
        }

        // Build the format from the factory registered on the session, so the
        // function shares the runtime's settings and its reader cache. The
        // per-call dimensions ride along as a table option.
        let mut format_options: HashMap<String, String> = HashMap::new();
        if !dimensions.is_empty() {
            format_options.insert("read_dimensions".to_string(), dimensions.join(","));
        }
        let factory = state.get_file_format_factory(ATLAS_FORMAT).ok_or_else(|| {
            plan_datafusion_err!("read_atlas: the atlas file format is not registered")
        })?;
        let file_format = factory.create(&state, &format_options)?;

        let table = tokio::task::block_in_place(|| {
            self.runtime_handle.block_on(async {
                FastObjectTable::try_new(&session_ctx.state(), file_format, listing_urls).await
            })
        })?;

        Ok(Arc::new(table))
    }
}
