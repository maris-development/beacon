//! One partition's opener: a collection in, nd batches out.
//!
//! The opener reads through the [`AtlasReaderPool`]. The first partition to
//! reach a collection opens it and queues its datasets, and every partition
//! then streams the datasets it pops. What lives here is the column
//! resolution the read and the pruning share: a column view says where one
//! column of the scan comes from, for every dataset at once, and
//! [`under_fields`] puts one chunk of a dataset under the scan's fields.

use std::sync::Arc;

use arrow::{
    array::{ArrayRef, new_null_array},
    compute::cast,
    datatypes::{Field, FieldRef, Schema, SchemaRef},
};
use atlas::{ArrayFile, Atlas, Attr};
use beacon_datafusion_ext::nd::{Dimensions, NdArrowArray, NdRecordBatch};
use beacon_datafusion_ext::type_widening::is_type_conflict;
use beacon_nd_array::arrow::metrics::ReadMetrics;
use datafusion::{
    datasource::{
        listing::PartitionedFile,
        physical_plan::{FileOpenFuture, FileOpener},
    },
    error::{DataFusionError, Result},
    physical_plan::PhysicalExpr,
};
use futures::{FutureExt, StreamExt, TryStreamExt};
use indexmap::IndexMap;
use object_store::ObjectStore;

use crate::{
    datafusion::{metrics::AtlasScanMetrics, pool::AtlasReaderPool},
    store::AtlasReaderCache,
};

/// One partition's opener: a collection in, its batches out.
///
/// Every field is a handle or a clone, so the opener itself is cloned into the
/// stream it returns and outlives the call that made it.
#[derive(Clone)]
pub struct AtlasOpener {
    pub object_store: Arc<dyn ObjectStore>,
    pub cache: AtlasReaderCache,
    /// The scan's output schema, nd-encoded. Its field *names* are the columns
    /// to keep, and the encoding leaves names alone.
    pub projected_schema: SchemaRef,
    /// The same schema with the encoding unwrapped, which is what a predicate
    /// and the pruning engine are written against.
    pub logical_schema: SchemaRef,
    pub read_dimensions: Option<Vec<String>>,
    pub batch_size: usize,
    pub predicate: Option<Arc<dyn PhysicalExpr>>,
    pub read_metrics: ReadMetrics,
    pub scan_metrics: AtlasScanMetrics,
    /// The scan's pools, one per collection, shared by every partition.
    pub reader_pool: Arc<AtlasReaderPool>,
}

impl FileOpener for AtlasOpener {
    /// One collection in, one encoded batch per stored chunk of every dataset
    /// worth reading out.
    ///
    /// The collection is opened through the reader pool. The first partition
    /// to reach it opens it, prunes its datasets in one pass over the footer's
    /// statistics, and queues the survivors. A dataset the deletion mask hides
    /// is not queued, and neither is one the predicate rules out. Every
    /// partition then streams the datasets it pops off that queue, so the
    /// partitions that share a collection share its work.
    fn open(&self, file: PartitionedFile) -> Result<FileOpenFuture> {
        let store = self.object_store.clone();
        let cache = self.cache.clone();
        let projected_schema = self.projected_schema.clone();
        let logical_schema = self.logical_schema.clone();
        let predicate = self.predicate.clone();
        let scan_metrics = self.scan_metrics.clone();
        let pool = Arc::clone(&self.reader_pool);

        let fut = async move {
            let location = file.object_meta.location.clone();
            let stream = pool
                .try_open_into_pooled_stream(
                    Some(&cache),
                    store,
                    file.object_meta,
                    logical_schema,
                    projected_schema,
                    predicate,
                    scan_metrics,
                )
                .await
                .map_err(|e| {
                    DataFusionError::Execution(format!(
                        "Failed to open atlas collection '{location}': {e}"
                    ))
                })?;
            Ok(stream
                .map_err(|e| DataFusionError::External(e.into()))
                .boxed())
        };

        Ok(fut.boxed())
    }
}

/// Where each column of the scan comes from, for every dataset at once.
///
/// One segment open per array, and one attribute sweep per key. Each costs the
/// same however many datasets the collection holds, so a partition pays them
/// once and reads every dataset against the result. A column no dataset
/// declares gets `None`.
pub(crate) async fn column_views(
    atlas: &Atlas,
    logical_schema: &Schema,
) -> Result<IndexMap<FieldRef, Option<AtlasColumnView>>> {
    let mut views = IndexMap::with_capacity(logical_schema.fields().len());
    for field in logical_schema.fields() {
        let view = if let Some(key) = field.name().strip_prefix('.') {
            let map = atlas
                .attributes_by_dataset(None, key)
                .await
                .map_err(external)?;
            Some(AtlasColumnView::GlobalAttribute { map })
        } else if let Some((array, key)) = field.name().split_once('.') {
            let map = atlas
                .attributes_by_dataset(Some(array), key)
                .await
                .map_err(external)?;
            Some(AtlasColumnView::VariableAttribute {
                variable: array.to_string(),
                map,
            })
        } else {
            atlas
                .try_segment(field.name())
                .await
                .map_err(external)?
                .map(|segment| AtlasColumnView::Array {
                    segment: Arc::clone(segment),
                })
        };
        views.insert(Arc::clone(field), view);
    }
    Ok(views)
}

/// `nd` under `fields`: every field in order, on the same target grid.
///
/// A column comes out under the array's own type, and the table may declare a
/// wider one: that is a cast. A field the dataset lacks is a rank-0 null,
/// which broadcasts to an all-null column. The decoder makes the same of a
/// null struct row, so the scan sees one thing either way.
pub(crate) fn under_fields(nd: &NdRecordBatch, fields: &[FieldRef]) -> Result<NdRecordBatch> {
    let mut columns = Vec::with_capacity(fields.len());
    for field in fields {
        let column = match nd.schema().column_with_name(field.name()) {
            Some((index, _)) => {
                let column = nd.column(index);
                match as_field_type(Arc::clone(column.values()), field)? {
                    Some(values) => NdArrowArray::try_new(values, column.dims().clone())?,
                    None => null_scalar(field),
                }
            }
            None => null_scalar(field),
        };
        columns.push(column);
    }
    let schema = Arc::new(Schema::new(fields.to_vec()));
    NdRecordBatch::try_new(schema, columns, nd.target().clone())
}

/// A rank-0 null. It broadcasts to an all-null column of the target grid.
fn null_scalar(field: &Field) -> NdArrowArray {
    NdArrowArray::try_new(new_null_array(field.data_type(), 1), Dimensions::scalar())
        .expect("one element on no axis")
}

/// `values` in the type the table declares for `field`, or `None` for values
/// the table cannot hold.
///
/// A dataset may store a column narrower than the merged type, and the merge
/// widened it: that is a cast. A column the merge could not join is marked,
/// and a dataset of the other family then reads as null. That is what the mark
/// promises the scan.
fn as_field_type(values: ArrayRef, field: &Field) -> Result<Option<ArrayRef>> {
    if values.data_type() == field.data_type() {
        return Ok(Some(values));
    }
    match cast(&values, field.data_type()) {
        Ok(values) => Ok(Some(values)),
        Err(_) if is_type_conflict(field) => Ok(None),
        Err(error) => Err(error.into()),
    }
}

/// An atlas error, as the scan reports it.
fn external(error: impl std::error::Error + Send + Sync + 'static) -> DataFusionError {
    DataFusionError::External(Box::new(error))
}

/// Where one column of the scan comes from, for every dataset of a collection.
///
/// The scan reads through it, and pruning judges through it, so both see one
/// resolution of a column name.
pub(crate) enum AtlasColumnView {
    /// The variable's segment. It holds the array for every dataset that
    /// declares it, keyed by dataset name.
    Array { segment: Arc<ArrayFile> },
    /// A dataset-level attribute, its value per dataset.
    GlobalAttribute { map: IndexMap<String, Attr> },
    /// An attribute of one array, its value per dataset.
    VariableAttribute {
        variable: String,
        map: IndexMap<String, Attr>,
    },
}

#[cfg(test)]
mod tests {
    use arrow::array::{Array, AsArray, RecordBatch};
    use arrow::datatypes::{Float32Type, Float64Type, Int32Type, Int64Type};
    use beacon_datafusion_ext::type_widening::ArrowTypeWidening;

    use super::*;
    use crate::datafusion::view::AtlasView;
    use crate::{compat, test_support};
    use std::path::Path;

    use beacon_datafusion_ext::nd::{decode_nd_record_batch, encoded_schema};
    use datafusion::logical_expr::Operator;
    use datafusion::physical_expr::expressions::{BinaryExpr, Column as ColumnExpr, Literal};
    use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
    use datafusion::scalar::ScalarValue;
    use futures::TryStreamExt;

    /// The schema `infer_schema` derives for a fixture.
    async fn schema(dir: &Path) -> SchemaRef {
        let atlas = test_support::open(dir).await;
        Arc::new(
            compat::collection_arrow_schema(
                &atlas.footer().collection_schema(),
                &ArrowTypeWidening::default_extension(),
            )
            .unwrap(),
        )
    }

    /// Every chunk of `dataset`, read as the scan reads it: through the view,
    /// under the scan's fields. And the rows of all of them in chunk order.
    async fn read(dir: &Path, dataset: &str) -> (Vec<NdRecordBatch>, RecordBatch) {
        let schema = schema(dir).await;
        let (store, marker) = test_support::store_and_marker(dir);
        let view = AtlasView::new(None, store, marker, Arc::clone(&schema))
            .await
            .unwrap();
        let source = view.dataset(dataset).await.unwrap().unwrap();
        let mut chunks = Vec::new();
        for chunk in source.chunks() {
            let nd = source.poll_next(chunk).await.unwrap().unwrap();
            chunks.push(under_fields(&nd, schema.fields()).unwrap());
        }
        let batches: Vec<RecordBatch> = chunks.iter().map(|nd| nd.materialize().unwrap()).collect();
        let batch = arrow::compute::concat_batches(&schema, &batches).unwrap();
        (chunks, batch)
    }

    fn column<'a>(batch: &'a RecordBatch, name: &str) -> &'a ArrayRef {
        batch
            .column_by_name(name)
            .unwrap_or_else(|| panic!("no column {name}"))
    }

    /// Every column comes out on the dataset's grid. An attribute has no axis
    /// of its own, so it repeats on every row.
    #[tokio::test]
    async fn a_dataset_reads_every_column_on_its_own_grid() {
        let tmp = tempfile::tempdir().unwrap();
        test_support::two_datasets(tmp.path()).await;

        let (nd, batch) = read(tmp.path(), "winter").await;

        assert_eq!(nd.len(), 1, "an unchunked array is one chunk");
        assert_eq!(nd[0].target().shape(), vec![4]);
        assert_eq!(batch.num_rows(), 4);
        assert_eq!(
            column(&batch, "temperature")
                .as_primitive::<Float32Type>()
                .values()
                .to_vec(),
            vec![1.0, 2.0, 3.0, 4.0]
        );
        assert_eq!(
            column(&batch, "cycle")
                .as_primitive::<Int32Type>()
                .values()
                .to_vec(),
            vec![10, 20, 30, 40]
        );
        let season = column(&batch, ".season").as_string::<i32>();
        assert!(
            (0..4).all(|row| season.value(row) == "winter"),
            "a rank-0 attribute repeats on every row"
        );
        assert_eq!(
            column(&batch, ".year")
                .as_primitive::<Int64Type>()
                .values()
                .to_vec(),
            vec![2024; 4]
        );
        assert_eq!(
            column(&batch, "temperature.units")
                .as_string::<i32>()
                .value(3),
            "celsius"
        );
    }

    /// `summer` declares neither `cycle` nor `time`, sets no `year`, and has no
    /// `units` on `temperature`. Each is a column of nulls on summer's grid.
    #[tokio::test]
    async fn a_column_the_dataset_lacks_is_all_null() {
        let tmp = tempfile::tempdir().unwrap();
        test_support::two_datasets(tmp.path()).await;

        let (_, batch) = read(tmp.path(), "summer").await;

        assert_eq!(batch.num_rows(), 3);
        for missing in ["cycle", "time", ".year", "temperature.units"] {
            assert_eq!(column(&batch, missing).null_count(), 3, "{missing}");
        }
        assert_eq!(
            column(&batch, "temperature")
                .as_primitive::<Float32Type>()
                .values()
                .to_vec(),
            vec![20.0, 21.0, 22.0]
        );
        assert_eq!(
            column(&batch, ".season").as_string::<i32>().value(2),
            "summer"
        );
    }

    /// A 2-D array reads one stored chunk at a time, keeps both axes, and a
    /// cell nobody wrote reads as null.
    #[tokio::test]
    async fn a_fill_value_reads_as_null_on_a_two_dimensional_grid() {
        let tmp = tempfile::tempdir().unwrap();
        test_support::chunked_grid(tmp.path()).await;

        let (nd, batch) = read(tmp.path(), "grid").await;

        assert_eq!(nd.len(), 4, "a [4, 6] grid chunked [2, 3]");
        for chunk in &nd {
            assert_eq!(chunk.target().shape(), vec![2, 3]);
        }
        assert_eq!(batch.num_rows(), 24);
        let temperature = column(&batch, "temperature").as_primitive::<Float64Type>();
        assert_eq!(
            temperature.value(4),
            7.0,
            "row 1, column 1 of the grid: the fifth cell of the first chunk"
        );
        let mut cells = temperature.values().to_vec();
        cells.sort_by(|a, b| a.partial_cmp(b).unwrap());
        assert_eq!(cells, (0..24).map(f64::from).collect::<Vec<_>>());
        let sparse = column(&batch, "sparse");
        assert_eq!(sparse.null_count(), 12, "two of four rows were written");
        assert!(
            sparse.is_valid(0),
            "the first chunk lies in the written rows"
        );
        assert!(sparse.is_null(23), "the last chunk lies outside them");
    }

    /// `a` stores `value` as `Int16` and `b` as `Float32`. The table declares
    /// `Float64`, so each dataset casts up to it. `flag` is `a`'s alone.
    #[tokio::test]
    async fn a_narrower_dataset_casts_to_the_merged_type() {
        let tmp = tempfile::tempdir().unwrap();
        test_support::widening(tmp.path()).await;

        let (_, a) = read(tmp.path(), "a").await;
        let (_, b) = read(tmp.path(), "b").await;

        assert_eq!(
            column(&a, "value")
                .as_primitive::<Float64Type>()
                .values()
                .to_vec(),
            vec![1.0, 2.0]
        );
        assert_eq!(
            column(&b, "value")
                .as_primitive::<Float64Type>()
                .values()
                .to_vec(),
            vec![3.5, 4.5]
        );
        assert_eq!(
            column(&a, "flag")
                .as_primitive::<Int32Type>()
                .values()
                .to_vec(),
            vec![7, 8]
        );
        assert_eq!(column(&b, "flag").null_count(), 2);
    }

    // ── the opener ──────────────────────────────────────────────────────

    /// An opener over a fixture, built the way `AtlasSource` builds one.
    async fn opener(dir: &Path) -> (AtlasOpener, PartitionedFile) {
        let atlas = test_support::open(dir).await;
        let logical_schema = Arc::new(
            compat::collection_arrow_schema(
                &atlas.footer().collection_schema(),
                &ArrowTypeWidening::default_extension(),
            )
            .unwrap(),
        );
        let projected_schema = Arc::new(encoded_schema(&logical_schema));
        let (store, marker) = test_support::store_and_marker(dir);
        let metrics = ExecutionPlanMetricsSet::new();
        let opener = AtlasOpener {
            object_store: store,
            cache: AtlasReaderCache::new(4),
            projected_schema,
            logical_schema,
            read_dimensions: None,
            batch_size: 8192,
            predicate: None,
            read_metrics: ReadMetrics::new(&metrics, 0),
            scan_metrics: AtlasScanMetrics::new(&metrics, 0),
            reader_pool: Arc::new(AtlasReaderPool::new()),
        };
        (opener, PartitionedFile::from(marker))
    }

    async fn stream(opener: &AtlasOpener, file: PartitionedFile) -> Vec<RecordBatch> {
        opener
            .open(file)
            .unwrap()
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap()
    }

    /// One encoded batch per dataset, in write order, each on the scan's own
    /// schema.
    #[tokio::test]
    async fn the_opener_streams_one_encoded_batch_per_dataset() {
        let tmp = tempfile::tempdir().unwrap();
        test_support::two_datasets(tmp.path()).await;
        let (opener, file) = opener(tmp.path()).await;

        let batches = stream(&opener, file).await;

        assert_eq!(batches.len(), 2);
        for batch in &batches {
            assert_eq!(
                batch.schema(),
                opener.projected_schema,
                "the scan's schema, marks and all"
            );
        }
        let rows: Vec<usize> = batches
            .iter()
            .map(|batch| decode_nd_record_batch(batch).unwrap().num_rows())
            .collect();
        assert_eq!(rows, vec![4, 3], "winter, then summer");
        assert_eq!(opener.scan_metrics.datasets_scanned.value(), 2);
    }

    /// The deletion mask hides a dataset from the scan, though not from the
    /// schema.
    #[tokio::test]
    async fn a_deleted_dataset_is_not_streamed() {
        let tmp = tempfile::tempdir().unwrap();
        test_support::two_datasets(tmp.path()).await;
        test_support::open(tmp.path())
            .await
            .delete_dataset("winter")
            .await
            .unwrap();
        let (opener, file) = opener(tmp.path()).await;

        let batches = stream(&opener, file).await;

        assert_eq!(batches.len(), 1);
        let summer = decode_nd_record_batch(&batches[0])
            .unwrap()
            .materialize()
            .unwrap();
        assert_eq!(summer.num_rows(), 3);
        assert_eq!(
            column(&summer, "cycle").null_count(),
            3,
            "winter's column, summer's nulls"
        );
    }

    /// A predicate the statistics can judge skips the datasets it rules out
    /// before any of them is read.
    #[tokio::test]
    async fn a_predicate_prunes_datasets_before_the_read() {
        let tmp = tempfile::tempdir().unwrap();
        test_support::ranged(tmp.path(), 10).await;
        let (mut opener, file) = opener(tmp.path()).await;
        opener.predicate = Some(Arc::new(BinaryExpr::new(
            Arc::new(ColumnExpr::new("temperature", 0)),
            Operator::Gt,
            Arc::new(Literal::new(ScalarValue::Float32(Some(45.0)))),
        )));

        let batches = stream(&opener, file).await;

        assert_eq!(batches.len(), 5, "d5 to d9 reach past 45");
        assert_eq!(opener.scan_metrics.datasets_pruned.value(), 5);
        assert_eq!(opener.scan_metrics.datasets_scanned.value(), 5);
    }
}
