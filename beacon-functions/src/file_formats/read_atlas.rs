use std::sync::Arc;

use arrow::datatypes::{DataType, Field};
use beacon_arrow_atlas::datafusion::{options::AtlasOptions, AtlasFormat};
use beacon_common::{listing_url::parse_listing_table_url, super_table::SuperListingTable};
use beacon_object_storage::DatasetsStore;
use datafusion::{
    catalog::TableFunctionImpl,
    common::plan_err,
    execution::object_store::ObjectStoreUrl,
    prelude::{Expr, SessionContext},
    scalar::ScalarValue,
};

use crate::file_formats::BeaconTableFunctionImpl;

pub struct ReadAtlasFunc {
    // Session Reference
    runtime_handle: tokio::runtime::Handle,
    session_ctx: Arc<SessionContext>,
    data_object_store_url: ObjectStoreUrl,
    datasets_object_store: Arc<DatasetsStore>,
}

impl ReadAtlasFunc {
    pub fn new(
        runtime_handle: tokio::runtime::Handle,
        session_ctx: Arc<SessionContext>,
        data_object_store_url: ObjectStoreUrl,
        datasets_object_store: Arc<DatasetsStore>,
    ) -> Self {
        Self {
            runtime_handle,
            session_ctx,
            data_object_store_url,
            datasets_object_store,
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
            "Reads atlas stores. Each path must point to an atlas.json marker file \
             (exact path or glob like **/atlas.json). Optional second arg filters \
             arrays to those matching the listed dimensions."
                .to_string(),
        )
    }

    fn name(&self) -> String {
        "read_atlas".to_string()
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
                false,
            ),
        ])
    }
}

impl TableFunctionImpl for ReadAtlasFunc {
    fn call(
        &self,
        args: &[datafusion::prelude::Expr],
    ) -> datafusion::error::Result<std::sync::Arc<dyn datafusion::catalog::TableProvider>> {
        let glob_paths = crate::file_formats::parse_glob_paths_arg(args, "read_atlas")?;

        let mut dimensions: Option<Vec<String>> = None;
        if let Some(dimensions_arg) = args.get(1) {
            if let Expr::Literal(ScalarValue::List(values), _) = dimensions_arg {
                let string_array = values.as_ref().values();
                match string_array
                    .as_any()
                    .downcast_ref::<arrow::array::StringArray>()
                {
                    Some(str_arr) => {
                        let dims: Vec<String> = str_arr
                            .iter()
                            .filter_map(|opt_str| opt_str.map(|s| s.to_string()))
                            .collect();
                        dimensions = Some(dims);
                    }
                    None => {
                        return plan_err!(
                            "read_atlas second argument must be a List<Utf8> of dimension names"
                        );
                    }
                }
            }
        }

        tracing::debug!("read_atlas glob paths: {:?}", glob_paths);

        let mut listing_urls = vec![];
        for path in &glob_paths {
            tracing::debug!("read_atlas processing path: {}", path);
            listing_urls.push(parse_listing_table_url(&self.data_object_store_url, path)?);
        }

        let atlas_options = AtlasOptions {
            read_dimensions: dimensions,
        };
        let file_format = AtlasFormat::new(self.datasets_object_store.clone(), atlas_options);
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
