use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use arrow::{
    array::{BooleanArray, RecordBatch, new_null_array},
    datatypes::Schema,
    error::ArrowError,
};
use beacon_binary_format::{
    array::util::ZeroAccessor,
    index::pruning::CombinedColumnStatistics,
    object_store::ArrowBBFObjectReader,
    reader::async_reader::{AsyncBBFReader, AsyncPruningIndexReader},
};
use datafusion::{
    common::pruning::PruningStatistics,
    datasource::{
        listing::PartitionedFile,
        physical_plan::{FileMeta, FileOpenFuture, FileOpener},
        schema_adapter::SchemaAdapter,
    },
    physical_optimizer::pruning::PruningPredicate,
    prelude::Column,
};
use futures::{FutureExt, StreamExt};
use object_store::ObjectStore;
use parking_lot::Mutex;

use crate::bbf::{metrics::BBFGlobalMetrics, stream_share::StreamShare};

pub struct BBFOpener {
    schema_adapter: Arc<dyn SchemaAdapter>,
    pruning_predicate: Option<PruningPredicate>,
    object_store: Arc<dyn ObjectStore>,
    table_schema: Arc<Schema>,
    file_tracer: Arc<Mutex<Vec<String>>>,
    metrics: BBFGlobalMetrics,
    /// Stream Partition Share
    stream_partition_shares: Arc<Mutex<HashMap<object_store::path::Path, Arc<StreamShare>>>>,
}

impl FileOpener for BBFOpener {
    fn open(
        &self,
        file_meta: FileMeta,
        _file: PartitionedFile,
    ) -> datafusion::error::Result<FileOpenFuture> {
        let async_reader = ArrowBBFObjectReader::new(
            file_meta.object_meta.location.clone(),
            self.object_store.clone(),
        );
        let adapter = self.schema_adapter.clone();
        let pruning_predicate = self.pruning_predicate.clone();
        let table_schema = self.table_schema.clone();
        let file_tracer = self.file_tracer.clone();
        let stream_partition_shares = self.stream_partition_shares.clone();
        let stream_partition_share = {
            let mut stream_partition_share_map = stream_partition_shares.lock();
            let object_path = file_meta.object_meta.location.clone();
            stream_partition_share_map
                .entry(object_path)
                .or_insert_with(|| Arc::new(StreamShare::new()))
                .clone()
        };
        let metrics = self.metrics.clone();

        let fut = async move {
            let (stream, schema_mapper, file_schema) = stream_partition_share
                .get_or_try_init(|| async move {
                    tracing::debug!("Opening file: {:?}", file_meta.object_meta.location);

                    let reader = AsyncBBFReader::new(Arc::new(async_reader), 128)
                        .await
                        .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;

                    let file_schema = reader.arrow_schema();

                    let (schema_mapper, projection) = adapter
                        .map_schema(&file_schema)
                        .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
                    let mut selection: Option<BooleanArray> = None;
                    if let Some(pruning_predicate) = pruning_predicate {
                        selection =
                            BBFOpener::prune(&reader, &pruning_predicate, &table_schema).await?;
                    }

                    let keys = reader.physical_entries();
                    if let Some(selection) = &selection {
                        // Remove the keys where the selection is false
                        let entries_used: Vec<String> = selection
                            .iter()
                            .zip(keys.iter())
                            .filter_map(|(selected, key)| match selected {
                                Some(true) => Some(key.name.to_string()),
                                _ => None,
                            })
                            .collect();
                        file_tracer.lock().extend(entries_used);
                    } else {
                        let entries_used: Vec<String> =
                            keys.iter().map(|k| k.name.to_string()).collect();
                        file_tracer.lock().extend(entries_used);
                    }

                    let stream_producer = reader
                        .read(Some(projection), selection)
                        .await
                        .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;

                    Ok::<_, datafusion::error::DataFusionError>((
                        stream_producer,
                        schema_mapper,
                        Arc::new(file_schema),
                    ))
                })
                .await?
                .clone();
            let producer = stream.stream().await;

            let stream_proxy = producer
                .into_stream()
                .map(
                    move |nd_batch| -> Result<
                        arrow::array::RecordBatch,
                        datafusion::error::DataFusionError,
                    > {
                        let schema_mapper = schema_mapper.clone();
                        let arrow_batch = nd_batch.to_arrow_record_batch().unwrap_or_else(|e| {
                            tracing::error!(
                                "Error converting NdRecordBatch to Arrow RecordBatch: {:?}",
                                e
                            );
                            RecordBatch::new_empty(file_schema.clone())
                        });
                        let mapped_batch = schema_mapper
                            .map_batch(arrow_batch)
                            .map_err(|e| ArrowError::ExternalError(Box::new(e)))?;
                        metrics.add_rows(mapped_batch.num_rows());
                        Ok(mapped_batch)
                    },
                )
                .boxed();
            Ok(stream_proxy)
        };

        Ok(fut.boxed())
    }
}

impl BBFOpener {
    pub fn new(
        schema_adapter: Arc<dyn SchemaAdapter>,
        pruning_predicate: Option<PruningPredicate>,
        object_store: Arc<dyn ObjectStore>,
        table_schema: Arc<Schema>,
        file_tracer: Arc<Mutex<Vec<String>>>,
        stream_partition_shares: Arc<Mutex<HashMap<object_store::path::Path, Arc<StreamShare>>>>,
        metrics: BBFGlobalMetrics,
    ) -> Self {
        Self {
            schema_adapter,
            object_store,
            pruning_predicate,
            table_schema,
            file_tracer,
            stream_partition_shares,
            metrics,
        }
    }

    async fn prune(
        async_reader: &AsyncBBFReader,
        predicate: &PruningPredicate,
        table_schema: &Schema,
    ) -> datafusion::error::Result<Option<BooleanArray>> {
        let index = async_reader.pruning_index().await;

        if let Some(pruning_index) = index {
            let statistics = BBFPruningStatistics::new(
                predicate,
                pruning_index,
                &Arc::new(async_reader.arrow_schema()),
                table_schema,
            )
            .await;

            let prune_result = predicate.prune(&statistics)?;

            Ok(Some(BooleanArray::from(prune_result)))
        } else {
            Ok(None)
        }
    }
}

struct BBFPruningStatistics {
    file_columns: HashSet<String>,
    table_schema: HashMap<String, arrow::datatypes::DataType>,
    num_containers: usize,
    column_statistics: HashMap<String, ZeroAccessor<CombinedColumnStatistics>>,
}

impl BBFPruningStatistics {
    pub async fn new(
        predicate: &PruningPredicate,
        pruning_index_reader: AsyncPruningIndexReader,
        file_schema: &Schema,
        table_schema: &Schema,
    ) -> Self {
        // println!("Predicate: {:?}", predicate);
        let file_columns = file_schema
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();

        let table_schema = table_schema
            .fields()
            .iter()
            .map(|f| (f.name().clone(), f.data_type().clone()))
            .collect();

        let num_containers = pruning_index_reader.num_containers();
        let approx_expected_columns =
            datafusion::physical_expr::utils::collect_columns(predicate.orig_expr());
        // Load the combined statistics for each approx expected column
        let mut column_statistics = HashMap::new();

        for column in approx_expected_columns {
            let column_name = column.name().to_string();
            let statistics = pruning_index_reader.column(&column_name).await.unwrap();
            if let Some(statistics) = statistics {
                column_statistics.insert(column_name, statistics);
            }
        }

        Self {
            file_columns,
            num_containers,
            column_statistics,
            table_schema,
        }
    }
}

impl PruningStatistics for BBFPruningStatistics {
    fn min_values(&self, column: &Column) -> Option<arrow::array::ArrayRef> {
        // Check if the column exists in the file
        if !self.file_columns.contains(&column.name) {
            // Return Null Array as all the values are null
            let null_array = new_null_array(
                self.table_schema.get(&column.name).unwrap(),
                self.num_containers,
            );
            return Some(null_array);
        }

        if let Some(stats) = self.column_statistics.get(&column.name) {
            let min_values = stats.as_ref().min_value();
            let table_dtype = self.table_schema.get(&column.name).unwrap();
            if min_values.data_type() != table_dtype {
                // Cast if the data types are both numeric. This we can safely upcast
                if min_values.data_type().is_numeric() && table_dtype.is_numeric() {
                    let casted = arrow::compute::cast(&min_values, table_dtype).unwrap();
                    return Some(casted);
                } else {
                    return None;
                }
            }
            return Some(min_values);
        }

        None
    }

    fn max_values(&self, column: &Column) -> Option<arrow::array::ArrayRef> {
        // Check if the column exists in the file
        if !self.file_columns.contains(&column.name) {
            // Return Null Array as all the values are null
            let null_array = new_null_array(
                self.table_schema.get(&column.name).unwrap(),
                self.num_containers,
            );
            return Some(null_array);
        }

        if let Some(stats) = self.column_statistics.get(&column.name) {
            let max_values = stats.as_ref().max_value();
            let table_dtype = self.table_schema.get(&column.name).unwrap();
            if max_values.data_type() != table_dtype {
                // Cast if the data types are both numeric. This we can safely upcast
                if max_values.data_type().is_numeric() && table_dtype.is_numeric() {
                    let casted = arrow::compute::cast(&max_values, table_dtype).unwrap();
                    return Some(casted);
                } else {
                    return None;
                }
            }

            return Some(max_values);
        }

        None
    }

    fn num_containers(&self) -> usize {
        self.num_containers
    }

    fn null_counts(&self, column: &Column) -> Option<arrow::array::ArrayRef> {
        // Check if the column exists in the file
        if !self.file_columns.contains(&column.name) {
            // Return 1 so it equals the row counts as the column will only contains nulls
            return Some(Arc::new(arrow::array::UInt64Array::from(
                vec![1; self.num_containers],
            )));
        }

        if let Some(stats) = self.column_statistics.get(&column.name) {
            let null_counts = stats.as_ref().null_count();
            return Some(null_counts);
        }

        None
    }

    fn row_counts(&self, column: &Column) -> Option<arrow::array::ArrayRef> {
        if !self.file_columns.contains(&column.name) {
            // Return 1 so it equals the row counts as the column will only contains nulls
            return Some(Arc::new(arrow::array::UInt64Array::from(
                vec![1; self.num_containers],
            )));
        }

        if let Some(stats) = self.column_statistics.get(&column.name) {
            let row_counts = stats.as_ref().row_count();
            return Some(row_counts);
        }

        None
    }

    fn contained(
        &self,
        _column: &Column,
        _values: &std::collections::HashSet<datafusion::scalar::ScalarValue>,
    ) -> Option<BooleanArray> {
        None
    }
}
