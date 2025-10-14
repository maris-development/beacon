use std::{collections::HashMap, sync::Arc};

use arrow::{
    array::RecordBatch,
    datatypes::{Schema, SchemaRef},
    error::ArrowError,
};
use beacon_arrow_zarr::reader::AsyncArrowZarrGroupReader;
use datafusion::{
    common::Statistics,
    datasource::{
        listing::PartitionedFile,
        physical_plan::{
            FileGroup, FileMeta, FileOpenFuture, FileOpener, FileScanConfig, FileSource,
        },
        schema_adapter::{DefaultSchemaAdapterFactory, SchemaAdapter, SchemaAdapterFactory},
    },
    physical_plan::metrics::ExecutionPlanMetricsSet,
};
use futures::{FutureExt, StreamExt};
use object_store::{ObjectMeta, ObjectStore};
use parking_lot::Mutex;
use zarrs::group::Group;
use zarrs_object_store::AsyncObjectStore;
use zarrs_storage::AsyncReadableListableStorageTraits;

use crate::zarr::{path_parent, stream_share::ZarrStreamShare};

pub async fn fetch_schema(
    object_store: Arc<dyn ObjectStore>,
    object_meta: &ObjectMeta,
) -> datafusion::error::Result<SchemaRef> {
    let zarr_store = Arc::new(AsyncObjectStore::new(object_store))
        as Arc<dyn AsyncReadableListableStorageTraits>;

    // The object meta reprensents the zarr.json file in the root of the zarr group. We need to open the group at the parent directory of this file.
    let parent_path = path_parent(&object_meta.location).ok_or_else(|| {
        datafusion::error::DataFusionError::Execution(format!(
            "Could not determine parent path of object: {}",
            object_meta.location
        ))
    })?;

    let group = Group::async_open(zarr_store.clone(), &format!("/{}", parent_path))
        .await
        .map_err(|e| datafusion::error::DataFusionError::Execution(e.to_string()))?;

    let reader = AsyncArrowZarrGroupReader::new(group)
        .await
        .map_err(datafusion::error::DataFusionError::Execution)?;

    let schema = reader.arrow_schema();

    Ok(schema)
}

#[derive(Default, Clone)]
pub struct ZarrSource {
    /// Optional schema adapter factory.
    schema_adapter_factory: Option<Arc<dyn SchemaAdapterFactory>>,
    /// Optional schema override.
    override_schema: Option<SchemaRef>,
    /// Optional column projection.
    projection: Option<Vec<usize>>,
    /// Execution plan metrics.
    execution_plan_metrics: ExecutionPlanMetricsSet,
    /// Projected statistics.
    projected_statistics: Option<Statistics>,
    /// Stream Partition Share
    stream_partition_shares: Arc<Mutex<HashMap<object_store::path::Path, Arc<ZarrStreamShare>>>>,
}

impl FileSource for ZarrSource {
    /// Creates a file opener for the given object store and scan config.
    fn create_file_opener(
        &self,
        object_store: Arc<dyn ObjectStore>,
        base_config: &FileScanConfig,
        _partition: usize,
    ) -> Arc<dyn FileOpener> {
        let table_schema = self
            .override_schema
            .clone()
            .unwrap_or_else(|| base_config.file_schema.clone());
        let projected_schema = base_config.projected_schema();
        let schema_adapter_factory = self
            .schema_adapter_factory
            .clone()
            .unwrap_or_else(|| Arc::new(DefaultSchemaAdapterFactory));
        let schema_adapter = schema_adapter_factory.create(projected_schema, table_schema);
        let arc_schema_adapter: Arc<dyn SchemaAdapter> = Arc::from(schema_adapter);

        Arc::new(ZarrFileOpener {
            zarr_object_store: Arc::new(AsyncObjectStore::new(object_store)),
            schema_adapter: arc_schema_adapter,
            stream_partition_shares: self.stream_partition_shares.clone(),
        })
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn with_batch_size(&self, _batch_size: usize) -> Arc<dyn FileSource> {
        Arc::new(self.clone())
    }

    fn with_schema(&self, schema: SchemaRef) -> Arc<dyn FileSource> {
        Arc::new(Self {
            override_schema: Some(schema),
            projection: self.projection.clone(),
            execution_plan_metrics: self.execution_plan_metrics.clone(),
            projected_statistics: self.projected_statistics.clone(),
            schema_adapter_factory: self.schema_adapter_factory.clone(),
            stream_partition_shares: self.stream_partition_shares.clone(),
        })
    }

    fn with_projection(&self, config: &FileScanConfig) -> Arc<dyn FileSource> {
        Arc::new(Self {
            override_schema: self.override_schema.clone(),
            projection: config.projection.clone(),
            execution_plan_metrics: self.execution_plan_metrics.clone(),
            projected_statistics: self.projected_statistics.clone(),
            schema_adapter_factory: self.schema_adapter_factory.clone(),
            stream_partition_shares: self.stream_partition_shares.clone(),
        })
    }

    fn with_statistics(&self, statistics: Statistics) -> Arc<dyn FileSource> {
        Arc::new(Self {
            override_schema: self.override_schema.clone(),
            projection: self.projection.clone(),
            execution_plan_metrics: self.execution_plan_metrics.clone(),
            projected_statistics: Some(statistics),
            schema_adapter_factory: self.schema_adapter_factory.clone(),
            stream_partition_shares: self.stream_partition_shares.clone(),
        })
    }

    fn metrics(&self) -> &ExecutionPlanMetricsSet {
        &self.execution_plan_metrics
    }

    fn statistics(&self) -> datafusion::error::Result<Statistics> {
        if let Some(statistics) = &self.projected_statistics {
            Ok(statistics.clone())
        } else if let Some(schema) = self.override_schema.as_ref() {
            Ok(Statistics::new_unknown(schema))
        } else {
            Err(datafusion::error::DataFusionError::Execution(
                "Schema must be set to compute statistics".to_string(),
            ))
        }
    }

    fn file_type(&self) -> &str {
        "zarr"
    }

    fn with_schema_adapter_factory(
        &self,
        factory: Arc<dyn SchemaAdapterFactory>,
    ) -> datafusion::error::Result<Arc<dyn FileSource>> {
        Ok(Arc::new(Self {
            override_schema: self.override_schema.clone(),
            projection: self.projection.clone(),
            execution_plan_metrics: self.execution_plan_metrics.clone(),
            projected_statistics: self.projected_statistics.clone(),
            schema_adapter_factory: Some(factory),
            stream_partition_shares: self.stream_partition_shares.clone(),
        }))
    }

    fn schema_adapter_factory(&self) -> Option<Arc<dyn SchemaAdapterFactory>> {
        self.schema_adapter_factory.clone()
    }

    fn repartitioned(
        &self,
        target_partitions: usize,
        repartition_file_min_size: usize,
        output_ordering: Option<datafusion::physical_expr::LexOrdering>,
        config: &FileScanConfig,
    ) -> datafusion::error::Result<Option<FileScanConfig>> {
        // Repartion by duplicating the file groups to reach the target number of partitions.
        let file_groups = config.file_groups.clone();

        if file_groups.len() >= target_partitions {
            // No need to repartition
            return Ok(None);
        }

        let repartitioned: Vec<FileGroup> = file_groups
            .iter()
            .cycle()
            .take(target_partitions)
            .cloned()
            .collect();

        Ok(config.clone().with_file_groups(repartitioned).into())
    }
}

pub struct ZarrFileOpener {
    zarr_object_store: Arc<AsyncObjectStore<Arc<dyn ObjectStore>>>,
    /// Schema adapter for mapping NetCDF schema to Arrow schema.
    schema_adapter: Arc<dyn SchemaAdapter>,
    /// Stream partition shares for the Zarr file.
    stream_partition_shares: Arc<Mutex<HashMap<object_store::path::Path, Arc<ZarrStreamShare>>>>,
}

impl FileOpener for ZarrFileOpener {
    fn open(
        &self,
        file_meta: FileMeta,
        _file: PartitionedFile,
    ) -> datafusion::error::Result<FileOpenFuture> {
        let zarr_object_store = self.zarr_object_store.clone();
        let adapter = self.schema_adapter.clone();
        let stream_partition_shares = self.stream_partition_shares.clone();
        let stream_partition_share = {
            let mut stream_partition_share_map = stream_partition_shares.lock();
            let object_path = file_meta.object_meta.location.clone();
            println!(
                "Getting or creating ZarrStreamShare for path: {}",
                object_path
            );
            stream_partition_share_map
                .entry(object_path)
                .or_insert_with(|| Arc::new(ZarrStreamShare::new()))
                .clone()
        };

        let fut = async move {
            let (stream, schema_mapper, file_schema) = stream_partition_share
                .get_or_try_init(|| async move {
                    tracing::debug!("Opening file: {:?}", file_meta.object_meta.location);

                    let parent_path =
                        path_parent(&file_meta.object_meta.location).ok_or_else(|| {
                            datafusion::error::DataFusionError::Execution(format!(
                                "Could not determine parent path of object: {}",
                                file_meta.object_meta.location
                            ))
                        })?;

                    // Open the zarr group
                    let group = Group::async_open(
                        zarr_object_store.clone() as Arc<dyn AsyncReadableListableStorageTraits>,
                        &format!("/{}", parent_path),
                    )
                    .await
                    .map_err(|e| datafusion::error::DataFusionError::Execution(e.to_string()))?;

                    let reader = AsyncArrowZarrGroupReader::new(group).await.map_err(|e| {
                        datafusion::error::DataFusionError::Execution(e.to_string())
                    })?;

                    let file_schema = reader.arrow_schema();

                    let (schema_mapper, projection) = adapter
                        .map_schema(&file_schema)
                        .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;

                    let stream_producer = reader
                        .into_parallel_stream_composer(Some(projection))
                        .map_err(|e| {
                            datafusion::error::DataFusionError::Execution(e.to_string())
                        })?;

                    Ok::<_, datafusion::error::DataFusionError>((
                        stream_producer,
                        schema_mapper,
                        file_schema,
                    ))
                })
                .await?
                .clone();
            let producer = stream.pollable_shared_stream();

            let stream_proxy = producer
                .map(
                    move |nd_batch| -> Result<arrow::array::RecordBatch, arrow::error::ArrowError> {
                        let schema_mapper = schema_mapper.clone();
                        let arrow_batch = nd_batch
                            .map_err(|e| {
                                tracing::error!(
                                    "Error reading NdRecordBatch from Zarr stream: {:?}",
                                    e
                                );
                                ArrowError::IoError(
                                    "Error reading NdRecordBatch from Zarr stream".to_string(),
                                    std::io::Error::other(e),
                                )
                            })?
                            .to_arrow_record_batch()
                            .unwrap_or_else(|e| {
                                tracing::error!(
                                    "Error converting NdRecordBatch to Arrow RecordBatch: {:?}",
                                    e
                                );
                                RecordBatch::new_empty(file_schema.clone())
                            });
                        let mapped_batch = schema_mapper
                            .map_batch(arrow_batch)
                            .map_err(|e| ArrowError::ExternalError(Box::new(e)))?;
                        Ok(mapped_batch)
                    },
                )
                .boxed();
            Ok(stream_proxy)
        };

        Ok(fut.boxed())
    }
}
