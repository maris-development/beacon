use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use arrow::{
    array::{BooleanArray, RecordBatch, new_null_array},
    datatypes::{Schema, SchemaRef},
};
use beacon_binary_format::{
    array::util::ZeroAccessor,
    index::pruning::CombinedColumnStatistics,
    object_store::ArrowBBFObjectReader,
    reader::async_reader::{AsyncBBFReader, AsyncPruningIndexReader},
};
use beacon_datafusion_ext::nd::{
    Dimension, Dimensions, NdArrowArray, NdRecordBatch, encode_nd_record_batch, infer_target,
};
use datafusion::{
    common::pruning::PruningStatistics,
    datasource::{
        listing::PartitionedFile,
        physical_plan::{FileOpenFuture, FileOpener},
    },
    physical_optimizer::pruning::PruningPredicate,
    prelude::Column,
};
use futures::{FutureExt, StreamExt, stream::BoxStream};
use nd_arrow_array::dimensions::Dimensions as ReaderDimensions;
use object_store::ObjectStore;
use parking_lot::Mutex;
use tokio_util::sync::CancellationToken;

use crate::datafusion::{metrics::BBFGlobalMetrics, stream_share::StreamShare};

pub struct BBFOpener {
    /// The columns the scan reads. Only the names matter here: they select
    /// the file columns to read. The adapter above maps a file's columns onto
    /// the encoded scan schema by name.
    read_schema: SchemaRef,
    pruning_predicate: Option<PruningPredicate>,
    object_store: Arc<dyn ObjectStore>,
    table_schema: Arc<Schema>,
    file_tracer: Arc<Mutex<Vec<String>>>,
    metrics: BBFGlobalMetrics,
    /// Stream Partition Share
    stream_partition_shares: Arc<Mutex<HashMap<object_store::path::Path, Arc<StreamShare>>>>,
    /// Stops the open and ends the stream when it fires.
    cancellation_token: CancellationToken,
}

impl FileOpener for BBFOpener {
    fn open(&self, file: PartitionedFile) -> datafusion::error::Result<FileOpenFuture> {
        let async_reader =
            ArrowBBFObjectReader::new(file.object_meta.location.clone(), self.object_store.clone());
        let read_schema = self.read_schema.clone();
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
        let cancellation_token = self.cancellation_token.clone();

        let fut = async move {
            let producer = stream_partition_share
                .get_or_try_init(|| async move {
                    tracing::debug!("Opening file: {:?}", file.object_meta.location);

                    let reader = AsyncBBFReader::new(Arc::new(async_reader), 128)
                        .await
                        .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;

                    let file_schema = reader.arrow_schema();

                    // Columns of this file that the query needs, in file order.
                    // A column the file lacks is left to the adapter above.
                    let projection: Vec<usize> = file_schema
                        .fields()
                        .iter()
                        .enumerate()
                        .filter(|(_, f)| read_schema.index_of(f.name()).is_ok())
                        .map(|(i, _)| i)
                        .collect();
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

                    reader
                        .read(Some(projection), selection)
                        .await
                        .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))
                })
                .await?
                .clone();

            // One encoded row per entry. Each column keeps the name and type of
            // the file; the adapter above maps them onto the encoded scan schema.
            let stream_proxy = producer
                .stream()
                .await
                .into_stream()
                .map(move |entry| {
                    let (batch, rows) = encode_entry(&entry)?;
                    metrics.add_rows(rows);
                    Ok(batch)
                })
                .boxed();

            Ok::<BatchStream, datafusion::error::DataFusionError>(stream_proxy)
        };

        let fut = async move {
            let stream = cancellation_token
                .run_until_cancelled(fut)
                .await
                .ok_or_else(cancelled_error)??;
            Ok(end_on_cancel(stream, cancellation_token))
        };

        Ok(fut.boxed())
    }
}

type BatchStream = BoxStream<'static, datafusion::error::Result<RecordBatch>>;

fn cancelled_error() -> datafusion::error::DataFusionError {
    datafusion::error::DataFusionError::Execution("BBF scan cancelled".to_string())
}

/// Ends `stream` when `token` fires, then yields one error item. The error
/// tells the consumer that the result is partial.
fn end_on_cancel(stream: BatchStream, token: CancellationToken) -> BatchStream {
    let cancelled = token.clone().cancelled_owned();
    let trailer =
        futures::stream::once(async move { token.is_cancelled().then(|| Err(cancelled_error())) })
            .filter_map(futures::future::ready);
    stream.take_until(cancelled).chain(trailer).boxed()
}

/// One entry as one `beacon.nd`-encoded row, with the row count of its grid.
///
/// The reader names and sizes each dimension the way the nd pipeline does, so
/// the conversion moves no data. An entry whose columns disagree on a
/// dimension has no grid, and that is an error.
fn encode_entry(
    entry: &nd_arrow_array::batch::NdRecordBatch,
) -> datafusion::error::Result<(RecordBatch, usize)> {
    let columns = entry
        .arrays()
        .iter()
        .map(|array| {
            let dims = match array.dimensions() {
                ReaderDimensions::Scalar => Dimensions::scalar(),
                ReaderDimensions::MultiDimensional(dims) => Dimensions::try_new(
                    dims.iter()
                        .map(|dim| Dimension::new(dim.name.as_str(), dim.size))
                        .collect(),
                )?,
            };
            NdArrowArray::try_new(Arc::clone(array.as_arrow_array()), dims)
        })
        .collect::<datafusion::error::Result<Vec<_>>>()?;
    let target = infer_target(&columns)?;
    let nd = NdRecordBatch::try_new(entry.schema(), columns, target)?;
    Ok((encode_nd_record_batch(&nd)?, nd.num_rows()))
}

impl BBFOpener {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        read_schema: SchemaRef,
        pruning_predicate: Option<PruningPredicate>,
        object_store: Arc<dyn ObjectStore>,
        table_schema: Arc<Schema>,
        file_tracer: Arc<Mutex<Vec<String>>>,
        stream_partition_shares: Arc<Mutex<HashMap<object_store::path::Path, Arc<StreamShare>>>>,
        metrics: BBFGlobalMetrics,
        cancellation_token: CancellationToken,
    ) -> Self {
        Self {
            read_schema,
            object_store,
            pruning_predicate,
            table_schema,
            file_tracer,
            stream_partition_shares,
            metrics,
            cancellation_token,
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

#[cfg(test)]
mod opener_tests {
    use crate::datafusion::source::BBFSource;
    use crate::datafusion::test_util::write_bbf_fixture;
    use beacon_datafusion_ext::nd::{decode_nd_record_batch, encoded_schema};
    use datafusion::datasource::file_format::FileFormat;
    use datafusion::datasource::listing::PartitionedFile;
    use datafusion::datasource::physical_plan::{FileOpener, FileScanConfigBuilder, FileSource};
    use datafusion::datasource::table_schema::TableSchema;
    use datafusion::execution::object_store::ObjectStoreUrl;
    use datafusion::physical_expr::projection::ProjectionExprs;
    use datafusion::prelude::SessionContext;
    use futures::StreamExt;
    use object_store::ObjectStore;
    use std::sync::Arc;
    use tokio_util::sync::CancellationToken;

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
        let table_schema = crate::datafusion::BBFFormat
            .infer_schema(&ctx.state(), &object_store, std::slice::from_ref(&meta))
            .await
            .expect("schema");

        // The scan must name its columns, so read only `ints`.
        let ints = table_schema.index_of("ints").expect("ints column");
        let projection = ProjectionExprs::from_indices(&[ints], &table_schema);
        let expected_schema = Arc::new(table_schema.project(&[ints]).expect("project"));
        // The format hands the source the encoded schema. Do the same here.
        let encoded = Arc::new(encoded_schema(&table_schema));
        let source = BBFSource::new(TableSchema::from_file_schema(encoded))
            .with_projection(Some(projection));
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

        // One encoded row per entry. The grid of that row holds the entry's rows.
        assert_eq!(batches.len(), 2, "one encoded batch per entry");
        let expected_schema = Arc::new(encoded_schema(&expected_schema));
        let total_rows: usize = batches
            .iter()
            .map(|b| decode_nd_record_batch(b).expect("decode").num_rows())
            .sum();
        assert_eq!(total_rows, 5, "3 rows from entry_a + 2 rows from entry_b");
        for batch in &batches {
            assert_eq!(
                batch.schema(),
                expected_schema,
                "batches must carry the encoded projection"
            );
        }

        let traced = tracer.lock().clone();
        assert!(
            traced.contains(&"entry_a".to_string()) && traced.contains(&"entry_b".to_string()),
            "file tracer should list both entries, got {traced:?}"
        );
    }

    /// Opens the fixture through a source that carries `token`. Returns the
    /// opener and the file to open.
    async fn opener_with_token(
        dir: &std::path::Path,
        token: CancellationToken,
    ) -> (Arc<dyn FileOpener>, PartitionedFile) {
        let (store, meta) = write_bbf_fixture(dir, "cancel.bbf").await;
        let object_store: Arc<dyn ObjectStore> = store;

        let ctx = SessionContext::new();
        let table_schema = crate::datafusion::BBFFormat
            .infer_schema(&ctx.state(), &object_store, std::slice::from_ref(&meta))
            .await
            .expect("schema");
        let ints = table_schema.index_of("ints").expect("ints column");
        let projection = ProjectionExprs::from_indices(&[ints], &table_schema);
        let encoded = Arc::new(encoded_schema(&table_schema));
        let source = BBFSource::new(TableSchema::from_file_schema(encoded))
            .with_projection(Some(projection));
        source.set_cancellation_token(token);

        let conf = FileScanConfigBuilder::new(
            ObjectStoreUrl::parse("file://").expect("url"),
            Arc::new(source.clone()) as Arc<dyn FileSource>,
        )
        .build();
        let opener = source
            .create_file_opener(object_store, &conf, 0)
            .expect("file opener");
        (opener, PartitionedFile::from(meta))
    }

    /// A token that is already cancelled stops the open before any read.
    #[tokio::test]
    async fn open_fails_when_the_token_is_already_cancelled() {
        let dir = tempfile::tempdir().expect("tempdir");
        let token = CancellationToken::new();
        token.cancel();
        let (opener, file) = opener_with_token(dir.path(), token).await;

        let Err(err) = opener.open(file).expect("open").await else {
            panic!("a cancelled open must fail");
        };
        assert!(err.to_string().contains("cancelled"), "{err}");
    }

    /// A cancel while the stream runs ends the stream with one error item, so a
    /// consumer cannot take the partial result for a complete one.
    #[tokio::test]
    async fn stream_ends_with_an_error_after_cancel() {
        let dir = tempfile::tempdir().expect("tempdir");
        let token = CancellationToken::new();
        let (opener, file) = opener_with_token(dir.path(), token.clone()).await;

        let mut stream = opener.open(file).expect("open").await.expect("stream");
        stream
            .next()
            .await
            .expect("first batch")
            .expect("first batch is ok");

        token.cancel();
        let rest: Vec<_> = stream.collect().await;
        let last = rest.last().expect("the cancel error ends the stream");
        let err = last
            .as_ref()
            .expect_err("last item must be the cancel error");
        assert!(err.to_string().contains("cancelled"), "{err}");
        assert!(
            rest.iter().filter(|item| item.is_err()).count() == 1,
            "exactly one error item, got {rest:?}"
        );
    }

    /// Collects every batch the opener yields for `source` over `meta`.
    async fn collect_with(
        source: BBFSource,
        object_store: Arc<dyn ObjectStore>,
        meta: object_store::ObjectMeta,
    ) -> Vec<datafusion::error::Result<arrow::array::RecordBatch>> {
        let conf = FileScanConfigBuilder::new(
            ObjectStoreUrl::parse("file://").expect("url"),
            Arc::new(source.clone()) as Arc<dyn FileSource>,
        )
        .build();
        let opener = source
            .create_file_opener(object_store, &conf, 0)
            .expect("file opener");
        opener
            .open(PartitionedFile::from(meta))
            .expect("open")
            .await
            .expect("stream")
            .collect()
            .await
    }

    /// An entry whose columns disagree on a dimension cannot flatten. The
    /// stream reports that as an error instead of dropping the rows.
    #[tokio::test]
    async fn opener_reports_a_failed_flatten_as_an_error() {
        use arrow::array::{ArrayRef, Int32Array};
        use beacon_binary_format::array::dimensions::Dimensions;
        use beacon_binary_format::entry::{ArrayCollection, Column, Entry};
        use beacon_binary_format::writer::BBFWriter;

        let dir = tempfile::tempdir().expect("tempdir");
        let file_path = dir.path().join("ragged.bbf");
        {
            let mut writer =
                BBFWriter::new(&file_path, 1024 * 1024, None, true).expect("bbf writer");
            let three: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3]));
            let two: ArrayRef = Arc::new(Int32Array::from(vec![4, 5]));
            let collection = ArrayCollection::new(
                "ragged",
                Box::new(
                    vec![
                        Column::new("a", three, Dimensions::Multi(vec![("dim1", 3).into()])),
                        Column::new("b", two, Dimensions::Multi(vec![("dim1", 2).into()])),
                    ]
                    .into_iter(),
                ),
            );
            writer.append(Entry::new(collection), "entry");
            writer.finish().expect("finish bbf file");
        }
        let store = Arc::new(
            object_store::local::LocalFileSystem::new_with_prefix(dir.path()).expect("store"),
        );
        let meta = {
            use object_store::ObjectStoreExt;
            store
                .head(&object_store::path::Path::from("ragged.bbf"))
                .await
                .expect("head")
        };
        let object_store: Arc<dyn ObjectStore> = store;

        let ctx = SessionContext::new();
        let table_schema = crate::datafusion::BBFFormat
            .infer_schema(&ctx.state(), &object_store, std::slice::from_ref(&meta))
            .await
            .expect("schema");
        let a = table_schema.index_of("a").expect("a");
        let b = table_schema.index_of("b").expect("b");
        let encoded = Arc::new(encoded_schema(&table_schema));
        let source = BBFSource::new(TableSchema::from_file_schema(encoded))
            .with_projection(Some(ProjectionExprs::from_indices(&[a, b], &table_schema)));

        let items = collect_with(source, object_store, meta).await;
        assert!(
            items.iter().any(|item| item.is_err()),
            "a failed flatten must surface as an error, got {items:?}"
        );
    }
}
