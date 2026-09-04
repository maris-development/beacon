//! [`FileOpener`] that streams a single GeoParquet file as Arrow
//! [`RecordBatch`]es with geometry columns decoded to native GeoArrow.

use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use beacon_datafusion_ext::scan_adapt::batch_adapter_factory;
use datafusion::{
    datasource::{
        listing::{FileRange, PartitionedFile},
        physical_plan::{FileOpenFuture, FileOpener},
    },
    error::{DataFusionError, Result},
    physical_plan::metrics::{Count, ExecutionPlanMetricsSet, MetricBuilder},
};
use futures::{FutureExt, StreamExt, TryStreamExt, stream::BoxStream};
use geoparquet::reader::{GeoParquetReaderBuilder, GeoParquetRecordBatchStream};
use object_store::{ObjectMeta, ObjectStore};
use parquet::arrow::ProjectionMask;

use crate::datafusion::{bbox::QueryBox, reader};

/// What the bounding box pruning did, for `EXPLAIN ANALYZE`.
#[derive(Debug, Clone)]
pub(crate) struct BboxMetrics {
    /// Row groups the pruning looked at.
    considered: Count,
    /// Row groups it dropped, because their box missed the query box.
    pruned: Count,
    /// Files it dropped whole, every row group of them having been dropped.
    files_pruned: Count,
}

impl BboxMetrics {
    fn new(metrics: &ExecutionPlanMetricsSet) -> Self {
        Self {
            considered: MetricBuilder::new(metrics)
                .global_counter("geoparquet_row_groups_considered"),
            pruned: MetricBuilder::new(metrics).global_counter("geoparquet_row_groups_pruned"),
            files_pruned: MetricBuilder::new(metrics).global_counter("geoparquet_files_pruned"),
        }
    }

    fn record(&self, considered: usize, kept: usize) {
        self.considered.add(considered);
        self.pruned.add(considered - kept);
        if considered > 0 && kept == 0 {
            self.files_pruned.add(1);
        }
    }
}

/// Opens GeoParquet files and yields batches matching the read schema.
pub struct GeoParquetOpener {
    object_store: Arc<dyn ObjectStore>,
    /// The file columns the scan reads, in file order.
    read_schema: SchemaRef,
    batch_size: usize,
    /// The box a pushed-down spatial predicate. If present, the opener drops row groups whose own box misses it.
    query_box: Option<QueryBox>,
    bbox_metrics: BboxMetrics,
}

impl GeoParquetOpener {
    pub(crate) fn new(
        object_store: Arc<dyn ObjectStore>,
        read_schema: SchemaRef,
        batch_size: usize,
        query_box: Option<QueryBox>,
        metrics: &ExecutionPlanMetricsSet,
    ) -> Self {
        Self {
            object_store,
            read_schema,
            batch_size,
            query_box,
            bbox_metrics: BboxMetrics::new(metrics),
        }
    }

    async fn read_task(
        object: ObjectMeta,
        object_store: Arc<dyn ObjectStore>,
        range: Option<FileRange>,
        read_schema: SchemaRef,
        batch_size: usize,
        query_box: Option<QueryBox>,
        bbox_metrics: BboxMetrics,
    ) -> Result<BoxStream<'static, Result<RecordBatch>>> {
        let mut builder = reader::stream_builder(object_store, &object).await?;
        // Full GeoArrow schema of this file (geometry decoded to native types),
        // and the `geo` metadata it came from. A plain Parquet file has none.
        let (file_schema, geo_meta) = reader::geo_schema(&builder)?;

        // Select only the columns the scan asked for, so the reader never
        // decodes a column nobody reads. A column this file lacks is skipped
        // here and null-filled by the adapter below.
        let mut indices: Vec<usize> = read_schema
            .fields()
            .iter()
            .filter_map(|field| file_schema.index_of(field.name()).ok())
            .collect();
        indices.sort_unstable();
        let mask = ProjectionMask::roots(builder.parquet_schema(), indices.iter().copied());
        // The schema the reader emits: the file's own fields, narrowed the same
        // way. `GeoParquetRecordBatchStream` requires exactly this.
        let read_file_schema: SchemaRef = Arc::new(file_schema.project(&indices)?);

        // Which row groups this opener reads. Starts as all of them; the two
        // steps below only ever remove.
        let total = builder.metadata().num_row_groups();
        let mut keep: Vec<usize> = (0..total).collect();

        // DataFusion splits a large file into byte ranges, one per partition. A
        // range takes the row groups whose first page starts inside it, which is
        // the rule the plain Parquet reader uses too. Without this each
        // partition would read the whole file and the scan would return every
        // row once per partition.
        if let Some(range) = &range {
            keep.retain(|&index| {
                let column = builder.metadata().row_group(index).column(0);
                let offset = column
                    .dictionary_page_offset()
                    .unwrap_or_else(|| column.data_page_offset());
                range.contains(offset)
            });
        }

        // Drop the row groups whose own box misses the query box, before a byte
        // of data is read. Their boxes come from the file's `covering` metadata,
        // or are inferred from the coordinate columns of a native encoding.
        if let (Some(query_box), Some(geo_meta)) = (&query_box, &geo_meta) {
            match builder.intersecting_row_groups(query_box.rect, geo_meta, Some(&query_box.column))
            {
                Ok(hit) => {
                    let considered = keep.len();
                    keep.retain(|index| hit.contains(index));
                    bbox_metrics.record(considered, keep.len());
                }
                // A file that states no box for this column, or row groups with
                // no statistics. Reading it whole is slower and never wrong.
                Err(e) => tracing::debug!(
                    object = %object.location,
                    "GeoParquet bounding box pruning skipped: {e}"
                ),
            }
        }

        if keep.len() != total {
            builder = builder.with_row_groups(keep);
        }

        let parquet_stream = builder
            .with_projection(mask)
            .with_batch_size(batch_size)
            .build()
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        // `count(*)` selects no column. The GeoArrow wrapper cannot carry that:
        // it rebuilds every batch from its columns, and a batch with no column
        // states no row count. The plain Parquet stream keeps the count, and
        // with no column selected there is no geometry to decode anyway.
        let batches: BoxStream<'static, Result<RecordBatch>> = if indices.is_empty() {
            parquet_stream
                .map_err(|e| DataFusionError::External(Box::new(e)))
                .boxed()
        } else {
            // Wraps the raw Parquet stream to apply GeoArrow metadata and parse
            // geometry columns onto every emitted batch.
            GeoParquetRecordBatchStream::try_new(parquet_stream, read_file_schema.clone())
                .map_err(|e| DataFusionError::External(Box::new(e)))?
                .map_err(|e| DataFusionError::External(Box::new(e)))
                .boxed()
        };

        // Adapt the file's own fields onto the table's: cast a column whose type
        // this file states differently, and null-fill one it does not hold. The
        // column *selection* is already done, above and in the reader.
        let adapter = batch_adapter_factory(read_schema).make_adapter(&read_file_schema)?;

        let stream = batches
            .map(move |batch| {
                adapter.adapt_batch(&batch?).map_err(|e| {
                    DataFusionError::Execution(format!("Failed to adapt GeoParquet batch: {e}"))
                })
            })
            .boxed();

        Ok(stream)
    }
}

impl FileOpener for GeoParquetOpener {
    fn open(&self, file: PartitionedFile) -> Result<FileOpenFuture> {
        let fut = Self::read_task(
            file.object_meta,
            self.object_store.clone(),
            file.range,
            self.read_schema.clone(),
            self.batch_size,
            self.query_box.clone(),
            self.bbox_metrics.clone(),
        )
        .boxed();

        Ok(fut)
    }
}
