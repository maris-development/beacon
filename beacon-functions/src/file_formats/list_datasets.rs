use std::sync::Arc;

use arrow::{
    array::StringArray,
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use beacon_common::listing_url::parse_listing_table_url;
use beacon_datafusion_ext::format_ext::{DatasetMetadata, FileFormatFactoryExt};
use datafusion::{
    catalog::{MemTable, TableFunctionImpl, TableProvider},
    error::DataFusionError,
    execution::object_store::ObjectStoreUrl,
    prelude::{Expr, SessionContext},
};
use futures::StreamExt;

use crate::file_formats::BeaconTableFunctionImpl;

pub struct ListDatasetsFunc {
    runtime_handle: tokio::runtime::Handle,
    session_ctx: Arc<SessionContext>,
    data_object_store_url: ObjectStoreUrl,
    file_formats: Vec<Arc<dyn FileFormatFactoryExt>>,
}

impl ListDatasetsFunc {
    pub fn new(
        runtime_handle: tokio::runtime::Handle,
        session_ctx: Arc<SessionContext>,
        data_object_store_url: ObjectStoreUrl,
        file_formats: Vec<Arc<dyn FileFormatFactoryExt>>,
    ) -> Self {
        Self {
            runtime_handle,
            session_ctx,
            data_object_store_url,
            file_formats,
        }
    }
}

impl std::fmt::Debug for ListDatasetsFunc {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ListDatasetsFunc")
    }
}

impl BeaconTableFunctionImpl for ListDatasetsFunc {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> String {
        "list_datasets".to_string()
    }

    fn description(&self) -> Option<String> {
        Some("Lists all datasets stored in beacon, returning file name and format.".to_string())
    }
}

impl TableFunctionImpl for ListDatasetsFunc {
    fn call(&self, _args: &[Expr]) -> datafusion::error::Result<Arc<dyn TableProvider>> {
        let file_formats = self.file_formats.clone();
        let session_ctx = self.session_ctx.clone();
        let data_object_store_url = self.data_object_store_url.clone();

        let datasets: Vec<DatasetMetadata> = tokio::task::block_in_place(|| {
            self.runtime_handle.block_on(async move {
                let state = session_ctx.state();
                let object_store = session_ctx
                    .runtime_env()
                    .object_store(data_object_store_url.clone())?;

                let listing_url = parse_listing_table_url(&data_object_store_url, "**/*")?;
                let mut objects = Vec::new();
                let mut entry_stream = listing_url
                    .list_all_files(&state, &object_store, "")
                    .await?;

                while let Some(entry) = entry_stream.next().await {
                    if let Ok(entry) = entry {
                        objects.push(entry);
                    }
                }

                let mut datasets = vec![];
                for file_format in file_formats.iter() {
                    let format_datasets = file_format.discover_datasets(&objects)?;
                    datasets.extend(format_datasets);
                }

                Ok::<Vec<DatasetMetadata>, DataFusionError>(datasets)
            })
        })?;

        let schema = Arc::new(Schema::new(vec![
            Field::new("file_name", DataType::Utf8, false),
            Field::new("file_format", DataType::Utf8, false),
        ]));

        let file_names: StringArray = datasets
            .iter()
            .map(|d| Some(d.file_path.as_str()))
            .collect();
        let file_formats: StringArray = datasets.iter().map(|d| Some(d.format.as_str())).collect();

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(file_names), Arc::new(file_formats)],
        )?;

        Ok(Arc::new(MemTable::try_new(schema, vec![vec![batch]])?))
    }
}
