//! One partition's opener: a collection in, nd batches out.
//!
//! A column view says where one column of the scan comes from, for every
//! dataset at once: the variable's segment, or the attribute values keyed by
//! dataset. One dataset then reads as one [`NdRecordBatch`], each column on the
//! axes the dataset stores it on.

use std::sync::Arc;

use arrow::{
    array::{
        ArrayRef, BinaryArray, BooleanArray, Float32Array, Float64Array, Int8Array, Int16Array,
        Int32Array, Int64Array, PrimitiveArray, StringArray, UInt8Array, UInt16Array, UInt32Array,
        UInt64Array, new_null_array,
    },
    buffer::{NullBuffer, ScalarBuffer},
    compute::{cast, kernels::cmp::neq},
    datatypes::{
        ArrowPrimitiveType, Field, FieldRef, Float32Type, Float64Type, Int8Type, Int16Type,
        Int32Type, Int64Type, Schema, SchemaRef, TimestampNanosecondType, UInt8Type, UInt16Type,
        UInt32Type, UInt64Type,
    },
};
use atlas::{
    ArrayElement, ArrayFile, Atlas, Attr, DType, FillValue, TimestampNs, array_format::ArrayInfo,
};
use beacon_datafusion_ext::nd::{
    Dimension, Dimensions, NdArrowArray, NdRecordBatch, encode_nd_record_batch, infer_target,
};
use beacon_datafusion_ext::type_widening::is_type_conflict;
use beacon_nd_array::arrow::metrics::ReadMetrics;
use datafusion::{
    common::exec_err,
    datasource::{
        listing::PartitionedFile,
        physical_plan::{FileOpenFuture, FileOpener},
    },
    error::{DataFusionError, Result},
    physical_plan::PhysicalExpr,
};
use futures::{FutureExt, StreamExt};
use indexmap::IndexMap;
use object_store::ObjectStore;

use crate::{
    datafusion::{metrics::AtlasScanMetrics, pruning::prune_datasets},
    store::{AtlasReaderCache, get_or_open_atlas},
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
}

impl FileOpener for AtlasOpener {
    /// One collection in, one encoded batch per dataset worth reading out.
    ///
    /// The column views are built once per collection, and every dataset then
    /// reads against them. A dataset the deletion mask hides is not read, and
    /// neither is one the predicate rules out from the statistics in memory.
    fn open(&self, file: PartitionedFile) -> Result<FileOpenFuture> {
        let store = self.object_store.clone();
        let cache = self.cache.clone();
        let projected_schema = self.projected_schema.clone();
        let logical_schema = self.logical_schema.clone();
        let predicate = self.predicate.clone();
        let scan_metrics = self.scan_metrics.clone();

        let fut = async move {
            let open_timer = scan_metrics.open_time.timer();
            let atlas = get_or_open_atlas(Some(&cache), store, &file.object_meta)
                .await
                .map_err(|e| {
                    DataFusionError::Execution(format!(
                        "Failed to open atlas collection '{}': {e}",
                        file.object_meta.location
                    ))
                })?;
            let views = Arc::new(column_views(&atlas, &logical_schema).await?);
            drop(open_timer);

            let mut datasets = atlas.list_datasets();
            if let Some(predicate) = &predicate {
                let prune_timer = scan_metrics.prune_time.timer();
                let listed = datasets.len();
                datasets = prune_datasets(&views, datasets, predicate, &logical_schema).await;
                scan_metrics.datasets_pruned.add(listed - datasets.len());
                drop(prune_timer);
            }

            let stream = futures::stream::iter(datasets)
                .then(move |dataset| {
                    let views = Arc::clone(&views);
                    let schema = Arc::clone(&projected_schema);
                    let metrics = scan_metrics.clone();
                    async move {
                        let nd = fast_view_read_to_record_batch(&views, &dataset).await?;
                        metrics.datasets_scanned.add(1);
                        // The encoding names the columns and types the scan
                        // declared. The scan's schema carries each field's
                        // marks as well, so the batch takes that schema.
                        let batch = encode_nd_record_batch(&nd)?.with_schema(schema)?;
                        Ok::<_, DataFusionError>(batch)
                    }
                })
                .boxed();
            Ok(stream)
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

/// One dataset of the collection as an nd batch, one column per field.
///
/// A column comes out on the axes the dataset stores it on, and the target
/// grid is their union. A field the dataset lacks is a rank-0 null, which
/// broadcasts to an all-null column: an array no dataset declares, a segment
/// without this dataset's entry, or an attribute nobody set. The decoder makes
/// the same of a null struct row, so the scan sees one thing either way.
async fn fast_view_read_to_record_batch(
    views: &IndexMap<FieldRef, Option<AtlasColumnView>>,
    dataset: &str,
) -> Result<NdRecordBatch> {
    let mut columns = Vec::with_capacity(views.len());
    for (field, view) in views {
        let column = match view {
            None => null_scalar(field),
            Some(AtlasColumnView::Array { segment }) => match segment.array(dataset) {
                Some(info) => array_column(segment, dataset, info, field).await?,
                None => null_scalar(field),
            },
            Some(AtlasColumnView::GlobalAttribute { map })
            | Some(AtlasColumnView::VariableAttribute { map, .. }) => match map.get(dataset) {
                Some(attr) => attr_column(attr, field)?,
                None => null_scalar(field),
            },
        };
        columns.push(column);
    }
    let schema = Arc::new(Schema::new(views.keys().cloned().collect::<Vec<_>>()));
    let target = infer_target(&columns)?;
    NdRecordBatch::try_new(schema, columns, target)
}

/// A rank-0 null. It broadcasts to an all-null column of the target grid.
fn null_scalar(field: &Field) -> NdArrowArray {
    NdArrowArray::try_new(new_null_array(field.data_type(), 1), Dimensions::scalar())
        .expect("one element on no axis")
}

/// `dataset`'s entry of one variable, on the axes the segment records for it.
async fn array_column(
    segment: &ArrayFile,
    dataset: &str,
    info: &ArrayInfo,
    field: &Field,
) -> Result<NdArrowArray> {
    let dims = Dimensions::try_new(
        info.dimension_names
            .iter()
            .zip(&info.shape)
            .map(|(name, &size)| Dimension::new(name.as_str(), size as usize))
            .collect(),
    )?;
    let values = read_values(segment, dataset, info).await?;
    match as_field_type(values, field)? {
        Some(values) => NdArrowArray::try_new(values, dims),
        None => Ok(null_scalar(field)),
    }
}

/// One attribute value of `dataset`, on no axis.
fn attr_column(attr: &Attr, field: &Field) -> Result<NdArrowArray> {
    let values: ArrayRef = match attr {
        Attr::Bool(v) => Arc::new(BooleanArray::from(vec![*v])),
        Attr::Int8(v) => Arc::new(Int8Array::from(vec![*v])),
        Attr::Int16(v) => Arc::new(Int16Array::from(vec![*v])),
        Attr::Int32(v) => Arc::new(Int32Array::from(vec![*v])),
        Attr::Int64(v) => Arc::new(Int64Array::from(vec![*v])),
        Attr::UInt8(v) => Arc::new(UInt8Array::from(vec![*v])),
        Attr::UInt16(v) => Arc::new(UInt16Array::from(vec![*v])),
        Attr::UInt32(v) => Arc::new(UInt32Array::from(vec![*v])),
        Attr::UInt64(v) => Arc::new(UInt64Array::from(vec![*v])),
        Attr::Float32(v) => Arc::new(Float32Array::from(vec![*v])),
        Attr::Float64(v) => Arc::new(Float64Array::from(vec![*v])),
        Attr::String(v) => Arc::new(StringArray::from(vec![v.as_str()])),
        Attr::Binary(v) => Arc::new(BinaryArray::from(vec![v.as_slice()])),
        // A list has no rank-0 form, and the schema holds no list column. A
        // dataset that stores a list under a scalar column's key reads as null.
        _ => return Ok(null_scalar(field)),
    };
    match as_field_type(values, field)? {
        Some(values) => NdArrowArray::try_new(values, Dimensions::scalar()),
        None => Ok(null_scalar(field)),
    }
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

/// The flat values of `dataset`'s entry in `segment`, its fill read as null.
async fn read_values(segment: &ArrayFile, dataset: &str, info: &ArrayInfo) -> Result<ArrayRef> {
    let fill = info.fill_value.as_ref();
    match info.dtype {
        DType::Int8 => primitive_values::<Int8Type>(segment, dataset, fill).await,
        DType::Int16 => primitive_values::<Int16Type>(segment, dataset, fill).await,
        DType::Int32 => primitive_values::<Int32Type>(segment, dataset, fill).await,
        DType::Int64 => primitive_values::<Int64Type>(segment, dataset, fill).await,
        DType::UInt8 => primitive_values::<UInt8Type>(segment, dataset, fill).await,
        DType::UInt16 => primitive_values::<UInt16Type>(segment, dataset, fill).await,
        DType::UInt32 => primitive_values::<UInt32Type>(segment, dataset, fill).await,
        DType::UInt64 => primitive_values::<UInt64Type>(segment, dataset, fill).await,
        DType::Float32 => primitive_values::<Float32Type>(segment, dataset, fill).await,
        DType::Float64 => primitive_values::<Float64Type>(segment, dataset, fill).await,
        DType::TimestampNs => timestamp_values(segment, dataset, fill).await,
        DType::String => text_values(segment, dataset, fill).await,
        DType::Binary => binary_values(segment, dataset, fill).await,
        DType::Bool | DType::List { .. } | DType::FixedSizeList { .. } => exec_err!(
            "dataset '{dataset}' holds a {:?} array, which Beacon does not read",
            info.dtype
        ),
    }
}

/// The whole entry of `dataset` in `segment`, flat in row-major order.
async fn read_flat<T: ArrayElement>(segment: &ArrayFile, dataset: &str) -> Result<Vec<T>> {
    let values = segment
        .read_array::<T>(dataset, vec![], vec![])
        .await
        .map_err(external)?;
    Ok(values.into_owned().into_raw_vec_and_offset().0)
}

async fn primitive_values<A>(
    segment: &ArrayFile,
    dataset: &str,
    fill: Option<&FillValue>,
) -> Result<ArrayRef>
where
    A: ArrowPrimitiveType,
    A::Native: ArrayElement,
{
    let values = read_flat::<A::Native>(segment, dataset).await?;
    let fill = fill.map(|fill| <A::Native as ArrayElement>::fill_element(Some(fill)));
    mask_fill(
        PrimitiveArray::<A>::new(ScalarBuffer::from(values), None),
        fill,
    )
}

/// Both types are `#[repr(transparent)]` over `i64`. The rename is done
/// element by element all the same, on the type system rather than on layout.
async fn timestamp_values(
    segment: &ArrayFile,
    dataset: &str,
    fill: Option<&FillValue>,
) -> Result<ArrayRef> {
    let values: Vec<i64> = read_flat::<TimestampNs>(segment, dataset)
        .await?
        .into_iter()
        .map(|ts| ts.0)
        .collect();
    let fill = fill.map(|fill| <TimestampNs as ArrayElement>::fill_element(Some(fill)).0);
    mask_fill(
        PrimitiveArray::<TimestampNanosecondType>::from(values),
        fill,
    )
}

async fn text_values(
    segment: &ArrayFile,
    dataset: &str,
    fill: Option<&FillValue>,
) -> Result<ArrayRef> {
    let values = read_flat::<String>(segment, dataset).await?;
    let fill = fill.map(|fill| <String as ArrayElement>::fill_element(Some(fill)));
    Ok(Arc::new(StringArray::from_iter(values.into_iter().map(
        |value| (fill.as_ref() != Some(&value)).then_some(value),
    ))))
}

async fn binary_values(
    segment: &ArrayFile,
    dataset: &str,
    fill: Option<&FillValue>,
) -> Result<ArrayRef> {
    let values = read_flat::<Vec<u8>>(segment, dataset).await?;
    let fill = fill.map(|fill| <Vec<u8> as ArrayElement>::fill_element(Some(fill)));
    Ok(Arc::new(BinaryArray::from_iter(values.into_iter().map(
        |value| (fill.as_ref() != Some(&value)).then_some(value),
    ))))
}

/// `array`, with every element equal to `fill` read as null.
///
/// One vectorised compare gives the validity. A `NaN` fill masks nothing, as
/// `NaN` equals no value, and the same holds in every other nd format.
fn mask_fill<A: ArrowPrimitiveType>(
    array: PrimitiveArray<A>,
    fill: Option<A::Native>,
) -> Result<ArrayRef> {
    let Some(fill) = fill else {
        return Ok(Arc::new(array));
    };
    let fill = PrimitiveArray::<A>::new_scalar(fill);
    let kept = neq(&array, &fill)?;
    let nulls = NullBuffer::new(kept.values().clone());
    Ok(Arc::new(PrimitiveArray::<A>::new(
        array.values().clone(),
        Some(nulls),
    )))
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
    use crate::{compat, test_support};
    use std::path::Path;

    use beacon_datafusion_ext::nd::{decode_nd_record_batch, encoded_schema};
    use datafusion::logical_expr::Operator;
    use datafusion::physical_expr::expressions::{BinaryExpr, Column as ColumnExpr, Literal};
    use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
    use datafusion::scalar::ScalarValue;
    use futures::TryStreamExt;

    /// The column views of a fixture, over the schema `infer_schema` derives.
    async fn views(dir: &std::path::Path) -> IndexMap<FieldRef, Option<AtlasColumnView>> {
        let atlas = test_support::open(dir).await;
        let schema = compat::collection_arrow_schema(
            &atlas.footer().collection_schema(),
            &ArrowTypeWidening::default_extension(),
        )
        .unwrap();
        column_views(&atlas, &schema).await.unwrap()
    }

    async fn read(dir: &std::path::Path, dataset: &str) -> (NdRecordBatch, RecordBatch) {
        let views = views(dir).await;
        let nd = fast_view_read_to_record_batch(&views, dataset)
            .await
            .unwrap();
        let batch = nd.materialize().unwrap();
        (nd, batch)
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

        assert_eq!(nd.target().shape(), vec![4]);
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

    /// A 2-D array keeps both axes, and a cell nobody wrote reads as null.
    #[tokio::test]
    async fn a_fill_value_reads_as_null_on_a_two_dimensional_grid() {
        let tmp = tempfile::tempdir().unwrap();
        test_support::chunked_grid(tmp.path()).await;

        let (nd, batch) = read(tmp.path(), "grid").await;

        assert_eq!(nd.target().shape(), vec![4, 6]);
        assert_eq!(batch.num_rows(), 24);
        let temperature = column(&batch, "temperature").as_primitive::<Float64Type>();
        assert_eq!(temperature.value(7), 7.0, "row 1, column 1 of a 4x6 grid");
        let sparse = column(&batch, "sparse");
        assert_eq!(sparse.null_count(), 12, "two of four rows were written");
        assert!(sparse.is_valid(0));
        assert!(sparse.is_null(23));
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
