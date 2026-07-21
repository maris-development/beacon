use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use arrow::{
    array::{BooleanArray, RecordBatch, new_null_array},
    datatypes::{Schema, SchemaRef},
    error::ArrowError,
};
use beacon_binary_format::{
    array::util::ZeroAccessor,
    index::pruning::CombinedColumnStatistics,
    object_store::ArrowBBFObjectReader,
    reader::async_reader::{AsyncBBFReader, AsyncPruningIndexReader},
};
use datafusion::physical_expr_adapter::BatchAdapterFactory;
use datafusion::{
    common::pruning::PruningStatistics,
    datasource::{
        listing::PartitionedFile,
        physical_plan::{FileOpenFuture, FileOpener},
    },
    physical_optimizer::pruning::PruningPredicate,
    prelude::Column,
};
use futures::{FutureExt, StreamExt};
use object_store::ObjectStore;
use parking_lot::Mutex;

use crate::datafusion::{metrics::BBFGlobalMetrics, stream_share::StreamShare};

pub struct BBFOpener {
    projected_schema: SchemaRef,
    pruning_predicate: Option<PruningPredicate>,
    object_store: Arc<dyn ObjectStore>,
    table_schema: Arc<Schema>,
    file_tracer: Arc<Mutex<Vec<String>>>,
    metrics: BBFGlobalMetrics,
    /// Stream Partition Share
    stream_partition_shares: Arc<Mutex<HashMap<object_store::path::Path, Arc<StreamShare>>>>,
    /// Whether to split each record batch into `split_batch_size`-row slices.
    split_streams_slice: bool,
    /// Row count per slice when `split_streams_slice` is set (the session batch
    /// size, propagated from the source).
    split_batch_size: usize,
}

impl FileOpener for BBFOpener {
    fn open(&self, file: PartitionedFile) -> datafusion::error::Result<FileOpenFuture> {
        let async_reader =
            ArrowBBFObjectReader::new(file.object_meta.location.clone(), self.object_store.clone());
        let projected_schema = self.projected_schema.clone();
        let pruning_predicate = self.pruning_predicate.clone();
        let table_schema = self.table_schema.clone();
        let file_tracer = self.file_tracer.clone();
        let stream_partition_shares = self.stream_partition_shares.clone();
        let stream_partition_share = {
            let mut stream_partition_share_map = stream_partition_shares.lock();
            let object_path = file.object_meta.location.clone();
            stream_partition_share_map
                .entry(object_path)
                .or_insert_with(|| Arc::new(StreamShare::new()))
                .clone()
        };
        let metrics = self.metrics.clone();
        let fut_projected_schema = projected_schema.clone();
        let split_streams_slice = self.split_streams_slice;
        let split_batch_size = self.split_batch_size;

        let fut = async move {
            let (stream, schema_mapper, file_schema) = stream_partition_share
                .get_or_try_init(|| async move {
                    tracing::debug!("Opening file: {:?}", file.object_meta.location);

                    let reader = AsyncBBFReader::new(Arc::new(async_reader), 128)
                        .await
                        .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;

                    let file_schema = reader.arrow_schema();

                    // Columns of this file that the query needs, in file order —
                    // used both to prune the read and as the source schema for
                    // the batch adapter.
                    let projection: Vec<usize> = file_schema
                        .fields()
                        .iter()
                        .enumerate()
                        .filter(|(_, f)| fut_projected_schema.index_of(f.name()).is_ok())
                        .map(|(i, _)| i)
                        .collect();
                    let source_schema: SchemaRef = Arc::new(file_schema.project(&projection)?);
                    let schema_mapper = Arc::new(
                        BatchAdapterFactory::new(fut_projected_schema.clone())
                            .make_adapter(&source_schema)?,
                    );
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
                        // let schema_mapper = schema_mapper.clone();
                        let arrow_batch = nd_batch.to_arrow_record_batch().unwrap_or_else(|e| {
                            tracing::error!(
                                "Error converting NdRecordBatch to Arrow RecordBatch: {:?}",
                                e
                            );
                            RecordBatch::new_empty(file_schema.clone())
                        });
                        let batch_schema = arrow_batch.schema();
                        // Map the batch schema to the table schema.
                        let schema_mapper = BatchAdapterFactory::new(projected_schema.clone())
                            .make_adapter(&batch_schema)?;
                        let mapped_batch = schema_mapper
                            .adapt_batch(&arrow_batch)
                            .map_err(|e| ArrowError::ExternalError(Box::new(e)))?;
                        metrics.add_rows(mapped_batch.num_rows());
                        Ok(mapped_batch)
                    },
                )
                .boxed();

            // When configured (per-runtime default or per-table option), split
            // each record batch into `split_batch_size`-row slices to avoid OOM.
            // This is especially helpful for large tables with wide rows.
            let stream_proxy = if split_streams_slice {
                stream_proxy
                    .flat_map(move |batch| {
                        if let Ok(batch) = batch {
                            let split_batches = split_record_batch(batch, split_batch_size);
                            let split_batches_res =
                                split_batches.into_iter().map(Ok).collect::<Vec<_>>();
                            futures::stream::iter(split_batches_res)
                        } else {
                            futures::stream::iter(vec![batch])
                        }
                    })
                    .boxed()
            } else {
                stream_proxy
            };

            Ok(stream_proxy)
        };

        Ok(fut.boxed())
    }
}

fn split_record_batch(batch: RecordBatch, chunk_size: usize) -> Vec<RecordBatch> {
    let num_rows = batch.num_rows();
    let mut batches = Vec::new();
    let mut start = 0;
    while start < num_rows {
        let end = std::cmp::min(start + chunk_size, num_rows);
        let sliced_batch = batch.slice(start, end - start);
        batches.push(sliced_batch);
        start = end;
    }
    batches
}

#[cfg(test)]
mod opener_tests {
    use crate::datafusion::source::BBFSource;
    use crate::datafusion::test_util::write_bbf_fixture;
    use datafusion::datasource::file_format::FileFormat;
    use datafusion::datasource::listing::PartitionedFile;
    use datafusion::datasource::physical_plan::{FileScanConfigBuilder, FileSource};
    use datafusion::datasource::table_schema::TableSchema;
    use datafusion::execution::object_store::ObjectStoreUrl;
    use datafusion::prelude::SessionContext;
    use futures::StreamExt;
    use object_store::ObjectStore;
    use std::sync::Arc;

    /// End-to-end read of a real BBF file through the source/opener pair: every
    /// entry's rows must arrive, mapped onto the inferred table schema (so columns
    /// missing from an entry come back as nulls), and the file tracer must record
    /// the entries that were touched.
    #[tokio::test]
    async fn opener_streams_all_entries_of_a_real_bbf_file() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (store, meta) = write_bbf_fixture(dir.path(), "scan.bbf").await;
        let object_store: Arc<dyn ObjectStore> = store;

        let ctx = SessionContext::new();
        let table_schema = crate::datafusion::BBFFormat::default()
            .infer_schema(&ctx.state(), &object_store, &[meta.clone()])
            .await
            .expect("schema");

        let source = BBFSource::new(TableSchema::from_file_schema(table_schema.clone()));
        let tracer = Arc::new(parking_lot::Mutex::new(Vec::new()));
        source.set_file_tracer(tracer.clone());

        let conf = FileScanConfigBuilder::new(
            ObjectStoreUrl::parse("file://").expect("url"),
            Arc::new(source.clone()) as Arc<dyn FileSource>,
        )
        .build();
        let opener = source
            .create_file_opener(object_store, &conf, 0)
            .expect("file opener");

        let batches: Vec<_> = opener
            .open(PartitionedFile::from(meta))
            .expect("open")
            .await
            .expect("stream")
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .expect("all batches should be ok");

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 5, "3 rows from entry_a + 2 rows from entry_b");
        for batch in &batches {
            assert_eq!(batch.schema(), table_schema, "batches must match the table");
        }

        let traced = tracer.lock().clone();
        assert!(
            traced.contains(&"entry_a".to_string()) && traced.contains(&"entry_b".to_string()),
            "file tracer should list both entries, got {traced:?}"
        );
    }
}

#[cfg(test)]
mod split_tests {
    use super::split_record_batch;
    use arrow::array::{Int32Array, RecordBatch};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn batch(rows: i32) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from((0..rows).collect::<Vec<_>>()))])
            .expect("valid batch")
    }

    fn values(batch: &RecordBatch) -> Vec<i32> {
        batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("int column")
            .values()
            .to_vec()
    }

    /// Slicing must partition the batch exactly: every row appears once, in order,
    /// with only the final slice allowed to be short. Losing or duplicating rows
    /// here would silently corrupt query results.
    #[test]
    fn split_record_batch_partitions_rows_without_loss() {
        let b = batch(10);
        let parts = split_record_batch(b, 4);
        assert_eq!(
            parts.iter().map(|p| p.num_rows()).collect::<Vec<_>>(),
            vec![4, 4, 2]
        );
        let flattened: Vec<i32> = parts.iter().flat_map(values).collect();
        assert_eq!(flattened, (0..10).collect::<Vec<i32>>());
    }

    /// When the chunk size divides the row count evenly there must be no trailing
    /// empty batch.
    #[test]
    fn split_record_batch_produces_no_empty_trailing_slice() {
        let parts = split_record_batch(batch(8), 4);
        assert_eq!(parts.len(), 2);
        assert!(parts.iter().all(|p| p.num_rows() == 4));
    }

    /// A chunk size at or above the row count must return the batch as-is, and an
    /// empty batch must yield nothing at all (rather than one empty slice).
    #[test]
    fn split_record_batch_handles_oversized_chunk_and_empty_input() {
        let parts = split_record_batch(batch(3), 100);
        assert_eq!(parts.len(), 1);
        assert_eq!(parts[0].num_rows(), 3);

        assert!(split_record_batch(batch(0), 4).is_empty());
    }

    /// The slices must keep the original schema so downstream operators can treat
    /// them interchangeably with the unsplit batch.
    #[test]
    fn split_record_batch_preserves_schema() {
        let b = batch(5);
        let schema = b.schema();
        for part in split_record_batch(b, 2) {
            assert_eq!(part.schema(), schema);
        }
    }
}

impl BBFOpener {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        projected_schema: SchemaRef,
        pruning_predicate: Option<PruningPredicate>,
        object_store: Arc<dyn ObjectStore>,
        table_schema: Arc<Schema>,
        file_tracer: Arc<Mutex<Vec<String>>>,
        stream_partition_shares: Arc<Mutex<HashMap<object_store::path::Path, Arc<StreamShare>>>>,
        metrics: BBFGlobalMetrics,
        split_streams_slice: bool,
        split_batch_size: usize,
    ) -> Self {
        Self {
            projected_schema,
            object_store,
            pruning_predicate,
            table_schema,
            file_tracer,
            stream_partition_shares,
            metrics,
            split_streams_slice,
            split_batch_size,
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
            .await?;

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
    ) -> datafusion::error::Result<Self> {
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
            let statistics = pruning_index_reader
                .column(&column_name)
                .await
                .map_err(|e| {
                    tracing::warn!(column = %column_name, error = %e, "failed to read BBF pruning index column");
                    datafusion::error::DataFusionError::External(Box::new(e))
                })?;
            if let Some(statistics) = statistics {
                column_statistics.insert(column_name, statistics);
            }
        }

        Ok(Self {
            file_columns,
            num_containers,
            column_statistics,
            table_schema,
        })
    }
}

impl PruningStatistics for BBFPruningStatistics {
    fn min_values(&self, column: &Column) -> Option<arrow::array::ArrayRef> {
        // Check if the column exists in the file
        if !self.file_columns.contains(&column.name) {
            // Return Null Array as all the values are null. If the column is not
            // in the table schema, there are no usable stats: return None so the
            // container is conservatively kept.
            let null_array = new_null_array(self.table_schema.get(&column.name)?, self.num_containers);
            return Some(null_array);
        }

        if let Some(stats) = self.column_statistics.get(&column.name) {
            let min_values = stats.as_ref().min_value();
            let table_dtype = self.table_schema.get(&column.name)?;
            if min_values.data_type() != table_dtype {
                // Cast if the data types are both numeric. This we can safely upcast.
                if min_values.data_type().is_numeric() && table_dtype.is_numeric() {
                    // A failed cast means no usable stats; return None rather than panicking.
                    return arrow::compute::cast(&min_values, table_dtype).ok();
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
            // Return Null Array as all the values are null. If the column is not
            // in the table schema, there are no usable stats: return None so the
            // container is conservatively kept.
            let null_array = new_null_array(self.table_schema.get(&column.name)?, self.num_containers);
            return Some(null_array);
        }

        if let Some(stats) = self.column_statistics.get(&column.name) {
            let max_values = stats.as_ref().max_value();
            let table_dtype = self.table_schema.get(&column.name)?;
            if max_values.data_type() != table_dtype {
                // Cast if the data types are both numeric. This we can safely upcast.
                if max_values.data_type().is_numeric() && table_dtype.is_numeric() {
                    // A failed cast means no usable stats; return None rather than panicking.
                    return arrow::compute::cast(&max_values, table_dtype).ok();
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
