//! One collection, resolved against the table's schema.
//!
//! A column view says where one column of the scan comes from, for every
//! dataset at once: the variable's segment, or the attribute values keyed by
//! dataset. One segment open per array and one attribute sweep per key cost
//! the same however many datasets the collection holds, so a view pays them
//! once, and every dataset reads against the result. Pruning judges through
//! the same views, so the scan and the pruning see one resolution of a name.
//!
//! One dataset is then a lazy [`DatasetSource`] over those views. It reads one
//! stored chunk at a time as an [`NdRecordBatch`], each column on the axes the
//! dataset stores it on, and `under_fields` puts that chunk under the scan's
//! fields.

use std::sync::Arc;

use anyhow::Context as _;
use arrow::{
    array::{ArrayRef, new_null_array},
    compute::cast,
    datatypes::{Field, FieldRef, Schema, SchemaRef},
};
use atlas::{ArrayFile, Atlas, Attr};
use beacon_datafusion_ext::nd::{Dimensions, NdArrowArray, NdRecordBatch};
use beacon_datafusion_ext::type_widening::is_type_conflict;
use beacon_nd_array::{
    NdArrayD,
    dataset::{default::DefaultDataset, source::DatasetSource},
};
use datafusion::physical_plan::PhysicalExpr;
use indexmap::IndexMap;
use object_store::{ObjectMeta, ObjectStore};

use crate::{
    compat,
    datafusion::{metrics::AtlasScanMetrics, pruning::prune_datasets},
    store::{AtlasReaderCache, get_or_open_atlas},
};

/// An open collection and the resolution of every column of the table.
#[derive(Clone)]
pub struct AtlasView {
    atlas: Arc<Atlas>,
    table_schema: SchemaRef,
    column_views: Arc<IndexMap<FieldRef, Option<AtlasColumnView>>>,
}

impl AtlasView {
    /// Open the collection at `object_meta`, through `cache` when given, and
    /// resolve every column of `table_schema` against it.
    pub async fn new(
        cache: Option<&AtlasReaderCache>,
        store: Arc<dyn ObjectStore>,
        object_meta: ObjectMeta,
        table_schema: SchemaRef,
    ) -> anyhow::Result<Self> {
        let atlas = get_or_open_atlas(cache, store, &object_meta).await?;
        let views = column_views(&atlas, &table_schema)
            .await
            .with_context(|| format!("resolving the columns of '{}'", object_meta.location))?;

        Ok(Self {
            atlas,
            table_schema,
            column_views: Arc::new(views),
        })
    }

    /// The table schema the view resolves columns for, in field order.
    pub fn table_schema(&self) -> &SchemaRef {
        &self.table_schema
    }

    /// The datasets worth reading, in the collection's order.
    ///
    /// A dataset the deletion mask hides is not listed. With a predicate, one
    /// the statistics rule out is dropped too, and counted on `scan_metrics`.
    pub async fn list_datasets(
        &self,
        pruning_predicate: Option<Arc<dyn PhysicalExpr>>,
        scan_metrics: AtlasScanMetrics,
    ) -> anyhow::Result<Vec<String>> {
        let mut datasets = self.atlas.list_datasets();

        if let Some(predicate) = &pruning_predicate {
            let prune_timer = scan_metrics.prune_time.timer();
            let listed = datasets.len();
            datasets =
                prune_datasets(&self.column_views, datasets, predicate, &self.table_schema).await;
            scan_metrics.datasets_pruned.add(listed - datasets.len());
            drop(prune_timer);
        }

        Ok(datasets)
    }

    /// One dataset of the collection as a lazy nd dataset, under the table's
    /// fields.
    ///
    /// The dataset holds an array for every field it has: the variable's
    /// entry in its segment, read on demand through the atlas backend, or an
    /// attribute value on no axis. A field it lacks has no array, and reads
    /// as a rank-0 null. No array data is read here. The dataset's layout
    /// comes from the segments, and its chunk grid is the one the writer
    /// chose.
    pub async fn dataset(&self, dataset_name: &str) -> anyhow::Result<Arc<dyn DatasetSource>> {
        let mut arrays: IndexMap<String, Arc<dyn NdArrayD>> = IndexMap::new();
        for (field, view) in &*self.column_views {
            let array = match view {
                None => None,
                Some(AtlasColumnView::Array { segment }) => match segment.array(dataset_name) {
                    Some(info) => Some(
                        compat::array_to_nd_array(Arc::clone(segment), dataset_name, &info.dtype)
                            .with_context(|| {
                            format!(
                                "reading array '{}' of dataset '{dataset_name}'",
                                field.name()
                            )
                        })?,
                    ),
                    None => None,
                },
                Some(AtlasColumnView::GlobalAttribute { map })
                | Some(AtlasColumnView::VariableAttribute { map }) => {
                    // A list has no rank-0 form, and the schema holds no list
                    // column. A dataset that stores a list under a scalar
                    // column's key reads as null.
                    map.get(dataset_name)
                        .and_then(|attr| compat::attribute_to_nd_array(attr).ok())
                }
            };
            if let Some(array) = array {
                arrays.insert(field.name().clone(), array);
            }
        }
        let dataset = DefaultDataset::new(dataset_name.to_string(), arrays)
            .with_context(|| format!("laying out dataset '{dataset_name}'"))?;
        Ok(Arc::new(dataset))
    }
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
    VariableAttribute { map: IndexMap<String, Attr> },
}

/// Where each column of the scan comes from, for every dataset at once.
///
/// A column no dataset declares gets `None`.
pub(crate) async fn column_views(
    atlas: &Atlas,
    logical_schema: &Schema,
) -> anyhow::Result<IndexMap<FieldRef, Option<AtlasColumnView>>> {
    let mut views = IndexMap::with_capacity(logical_schema.fields().len());
    for field in logical_schema.fields() {
        let name = field.name();
        let view = if let Some(key) = name.strip_prefix('.') {
            let map = atlas
                .attributes_by_dataset(None, key)
                .await
                .with_context(|| format!("sweeping attribute '{key}' for column '{name}'"))?;
            Some(AtlasColumnView::GlobalAttribute { map })
        } else if let Some((array, key)) = name.split_once('.') {
            let map = atlas
                .attributes_by_dataset(Some(array), key)
                .await
                .with_context(|| format!("sweeping attribute '{key}' for column '{name}'"))?;
            Some(AtlasColumnView::VariableAttribute { map })
        } else {
            atlas
                .try_segment(name)
                .await
                .with_context(|| format!("opening the segment of column '{name}'"))?
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
pub(crate) fn under_fields(
    nd: &NdRecordBatch,
    fields: &[FieldRef],
) -> anyhow::Result<NdRecordBatch> {
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
    Ok(NdRecordBatch::try_new(
        schema,
        columns,
        nd.target().clone(),
    )?)
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
fn as_field_type(values: ArrayRef, field: &Field) -> anyhow::Result<Option<ArrayRef>> {
    if values.data_type() == field.data_type() {
        return Ok(Some(values));
    }
    match cast(&values, field.data_type()) {
        Ok(values) => Ok(Some(values)),
        Err(_) if is_type_conflict(field) => Ok(None),
        Err(error) => Err(error)
            .with_context(|| format!("casting column '{}' to {}", field.name(), field.data_type())),
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::{Array, AsArray, RecordBatch};
    use arrow::datatypes::{Float32Type, Float64Type, Int32Type, Int64Type};
    use beacon_datafusion_ext::type_widening::ArrowTypeWidening;
    use std::path::Path;

    use super::*;
    use crate::{compat, test_support};

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
        let source = view.dataset(dataset).await.unwrap();
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
}
