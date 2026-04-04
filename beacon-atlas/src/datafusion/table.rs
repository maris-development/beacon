use std::{any::Any, sync::Arc};

use arrow::{
    array::StringArray,
    datatypes::{DataType, Field, Schema, SchemaRef},
    record_batch::RecordBatch,
};
use beacon_datafusion_ext::table_ext::{ExternalTableDefinition, TableDefinition};
use datafusion::{
    catalog::{Session, TableProvider},
    common::{Constraints, Statistics},
    datasource::{MemTable, TableType, listing::ListingTable},
    error::DataFusionError,
    execution::{SendableRecordBatchStream, object_store::ObjectStoreUrl},
    logical_expr::TableProviderFilterPushDown,
    physical_plan::{ExecutionPlan, stream::RecordBatchStreamAdapter},
    prelude::{Expr, SessionContext},
};
use futures::{StreamExt, stream::BoxStream};

use crate::{
    collection::AtlasCollection, consts::COLLECTION_METADATA_FILE,
    partition::ops::delete::DeleteDatasetsPartition, prelude::Dataset,
    schema::AtlasSuperTypingMode,
};

fn atlas_metadata_location(location: &str) -> String {
    let trimmed = location.trim_end_matches('/');
    if trimmed.ends_with(COLLECTION_METADATA_FILE) {
        trimmed.to_string()
    } else if trimmed.is_empty() {
        COLLECTION_METADATA_FILE.to_string()
    } else {
        format!("{trimmed}/{COLLECTION_METADATA_FILE}")
    }
}

fn atlas_collection_directory(location: &str) -> String {
    let trimmed = location.trim_end_matches('/');
    if let Some(without_metadata) = trimmed.strip_suffix(COLLECTION_METADATA_FILE) {
        without_metadata.trim_end_matches('/').to_string()
    } else {
        trimmed.to_string()
    }
}

fn ingestion_result_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("partition_name", DataType::Utf8, false),
        Field::new("dataset_name", DataType::Utf8, false),
        Field::new("status", DataType::Utf8, false),
        Field::new("message", DataType::Utf8, false),
    ]))
}

fn ingestion_result_batch(
    schema: Arc<Schema>,
    partition_name: String,
    dataset_name: String,
    status: String,
    message: String,
) -> datafusion::error::Result<RecordBatch> {
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![partition_name])),
            Arc::new(StringArray::from(vec![dataset_name])),
            Arc::new(StringArray::from(vec![status])),
            Arc::new(StringArray::from(vec![message])),
        ],
    )
    .map_err(DataFusionError::from)
}

#[derive(Debug, Clone)]
pub struct AtlasTable {
    name: String,
    inner: ListingTable,
}

impl AtlasTable {
    pub fn definition(&self) -> AtlasTableDefinition {
        let location = self.inner.table_paths()[0].to_string();
        let table_name = self.name.clone();

        AtlasTableDefinition {
            name: table_name,
            location,
        }
    }

    pub async fn from_definition(
        definition: AtlasTableDefinition,
        context: Arc<SessionContext>,
        data_store_url: &ObjectStoreUrl,
    ) -> anyhow::Result<Self> {
        let provider = definition.build_provider(context, data_store_url).await?;
        let inner = provider
            .as_any()
            .downcast_ref::<ListingTable>()
            .ok_or_else(|| anyhow::anyhow!("expected provider to be a ListingTable"))?
            .clone();

        Ok(Self {
            name: definition.name,
            inner,
        })
    }
}

#[async_trait::async_trait]
impl TableProvider for AtlasTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Get a reference to the schema for this table
    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }

    fn constraints(&self) -> Option<&Constraints> {
        self.inner.constraints()
    }

    /// Get the type of this table for metadata/catalog purposes.
    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        self.inner.scan(state, projection, filters, limit).await
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> datafusion::error::Result<Vec<TableProviderFilterPushDown>> {
        self.inner.supports_filters_pushdown(filters)
    }

    fn statistics(&self) -> Option<Statistics> {
        self.inner.statistics()
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct AtlasTableDefinition {
    pub name: String,
    pub location: String,
}

#[async_trait::async_trait]
#[typetag::serde(name = "atlas_table")]
impl TableDefinition for AtlasTableDefinition {
    async fn build_provider(
        &self,
        context: Arc<SessionContext>,
        data_store_url: &ObjectStoreUrl,
    ) -> anyhow::Result<Arc<dyn TableProvider>> {
        let definition = ExternalTableDefinition {
            name: self.name.clone(),
            location: atlas_metadata_location(&self.location),
            file_type: "atlas".to_string(),
            schema: Arc::new(Schema::empty()),
            definition: None,
            partition_cols: Vec::new(),
            options: Default::default(),
            if_not_exists: false,
        };

        definition.build_provider(context, data_store_url).await
    }
}

impl AtlasTableDefinition {
    pub async fn create(
        context: Arc<SessionContext>,
        data_store_url: &ObjectStoreUrl,
        name: String,
        location: String,
    ) -> anyhow::Result<Self> {
        // Creates the atlas collection at the specified location to ensure it exists.
        let object_store = context.runtime_env().object_store(data_store_url.clone())?;
        let collection_directory = atlas_collection_directory(&location);
        anyhow::ensure!(
            !collection_directory.is_empty(),
            "Atlas collection location cannot be empty"
        );
        let collection_path = object_store::path::Path::parse(collection_directory.as_str())?;

        AtlasCollection::create(
            object_store,
            collection_path,
            name.clone(),
            None,
            AtlasSuperTypingMode::General,
        )
        .await?;

        Ok(Self {
            name,
            location: atlas_metadata_location(&location),
        })
    }

    async fn open_atlas_collection(
        &self,
        context: Arc<SessionContext>,
        data_store_url: &ObjectStoreUrl,
    ) -> anyhow::Result<AtlasCollection> {
        let object_store = context.runtime_env().object_store(data_store_url.clone())?;
        let collection_directory = atlas_collection_directory(&self.location);
        let collection_path = object_store::path::Path::parse(collection_directory.as_str())?;

        AtlasCollection::open(object_store, collection_path).await
    }

    /// Inserts the datasets into a new atlas partition and returns a stream of record batches representing the results of the ingestion.
    /// The results look like: `partition_name | dataset_name | status | message`
    pub async fn ingest_into_partition(
        &self,
        context: Arc<SessionContext>,
        data_store_url: &ObjectStoreUrl,
        partition_name: &str,
        ingestion: BoxStream<'static, anyhow::Result<Dataset>>,
        fail_fast: bool,
    ) -> anyhow::Result<SendableRecordBatchStream> {
        let mut collection = self
            .open_atlas_collection(context.clone(), data_store_url)
            .await?;
        let writer = collection
            .create_partition_writer(partition_name, None)
            .await?;
        let io_cache = collection.io_cache();
        let result_schema = ingestion_result_schema();
        let stream_schema = result_schema.clone();
        let partition_name = partition_name.to_string();
        let initial_state: (BoxStream<'static, anyhow::Result<Dataset>>, Option<_>, bool) =
            (ingestion, Some(writer), false);

        let stream = futures::stream::unfold(
            initial_state,
            move |(mut ingestion, mut writer, mut stop_requested)| {
                let schema = stream_schema.clone();
                let partition_name = partition_name.clone();
                let io_cache = io_cache.clone();

                async move {
                    if stop_requested {
                        if let Some(partition_writer) = writer.take()
                            && let Err(error) = partition_writer.finish(io_cache).await
                        {
                            return Some((
                                Err(DataFusionError::Execution(format!(
                                    "failed to finalize partition: {error}"
                                ))),
                                (ingestion, writer, true),
                            ));
                        }
                        return None;
                    }

                    let Some(dataset) = ingestion.next().await else {
                        if let Some(partition_writer) = writer.take()
                            && let Err(error) = partition_writer.finish(io_cache).await
                        {
                            return Some((
                                Err(DataFusionError::Execution(format!(
                                    "failed to finalize partition: {error}"
                                ))),
                                (ingestion, writer, true),
                            ));
                        }
                        return None;
                    };

                    let dataset = match dataset {
                        Ok(dataset) => dataset,
                        Err(error) => {
                            stop_requested = fail_fast;
                            let batch = ingestion_result_batch(
                                schema,
                                partition_name,
                                "<unknown>".to_string(),
                                "error".to_string(),
                                format!("failed to read dataset from stream: {error}"),
                            );
                            return Some((batch, (ingestion, writer, stop_requested)));
                        }
                    };
                    let dataset_name = dataset.0.batch_name.clone();
                    let write_result = match writer.as_mut() {
                        Some(writer) => writer.write_dataset(dataset).await,
                        None => Err(anyhow::anyhow!("partition writer is not available")),
                    };

                    let (status, message) = match write_result {
                        Ok(()) => ("ok".to_string(), "inserted".to_string()),
                        Err(error) => {
                            stop_requested = fail_fast;
                            (
                                "error".to_string(),
                                format!("failed to ingest dataset: {error}"),
                            )
                        }
                    };

                    let batch = ingestion_result_batch(
                        schema,
                        partition_name,
                        dataset_name,
                        status,
                        message,
                    );
                    Some((batch, (ingestion, writer, stop_requested)))
                }
            },
        );

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            result_schema,
            stream,
        )))
    }

    pub async fn delete_datasets_from_partition(
        &self,
        context: Arc<SessionContext>,
        data_store_url: &ObjectStoreUrl,
        partition_name: &str,
        dataset_names: Vec<String>,
    ) -> anyhow::Result<SendableRecordBatchStream> {
        let mut collection = self
            .open_atlas_collection(context.clone(), data_store_url)
            .await?;
        let object_store = collection.object_store().clone();
        let mut partition = collection.get_partition(partition_name).await?;

        let result_schema = ingestion_result_schema();
        let partition_name_owned = partition_name.to_string();

        let mut result_partition_names = Vec::new();
        let mut result_dataset_names = Vec::new();
        let mut result_statuses = Vec::new();
        let mut result_messages = Vec::new();

        for dataset_name in dataset_names {
            let delete_result = DeleteDatasetsPartition::new(object_store.clone(), partition)
                .delete_dataset(dataset_name.clone())
                .execute()
                .await;

            match delete_result {
                Ok(updated_partition) => {
                    partition = updated_partition;
                    result_partition_names.push(partition_name_owned.clone());
                    result_dataset_names.push(dataset_name);
                    result_statuses.push("ok".to_string());
                    result_messages.push("deleted".to_string());
                }
                Err(error) => {
                    partition = collection.get_partition(partition_name).await?;
                    result_partition_names.push(partition_name_owned.clone());
                    result_dataset_names.push(dataset_name);
                    result_statuses.push("error".to_string());
                    result_messages.push(format!("failed to delete dataset: {error}"));
                }
            }
        }

        let batch = RecordBatch::try_new(
            result_schema.clone(),
            vec![
                Arc::new(StringArray::from(result_partition_names)),
                Arc::new(StringArray::from(result_dataset_names)),
                Arc::new(StringArray::from(result_statuses)),
                Arc::new(StringArray::from(result_messages)),
            ],
        )?;

        let table = MemTable::try_new(result_schema, vec![vec![batch]])?;
        let stream = context
            .read_table(Arc::new(table))?
            .execute_stream()
            .await?;
        Ok(stream)
    }
}

#[cfg(test)]
mod tests {
    use super::{atlas_collection_directory, atlas_metadata_location};

    #[test]
    fn metadata_location_appends_atlas_json_for_directory() {
        assert_eq!(
            atlas_metadata_location("collections/sensors"),
            "collections/sensors/atlas.json"
        );
    }

    #[test]
    fn metadata_location_preserves_existing_atlas_json() {
        assert_eq!(
            atlas_metadata_location("collections/sensors/atlas.json"),
            "collections/sensors/atlas.json"
        );
    }

    #[test]
    fn metadata_location_handles_trailing_slash() {
        assert_eq!(
            atlas_metadata_location("collections/sensors/"),
            "collections/sensors/atlas.json"
        );
    }

    #[test]
    fn collection_directory_strips_atlas_json_suffix() {
        assert_eq!(
            atlas_collection_directory("collections/sensors/atlas.json"),
            "collections/sensors"
        );
    }

    #[test]
    fn collection_directory_preserves_directory_location() {
        assert_eq!(
            atlas_collection_directory("collections/sensors/"),
            "collections/sensors"
        );
    }
}
