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
use beacon_datafusion_ext::type_widening::ArrowTypeWideningStrategy;
use beacon_nd_array::{
    NdArrayD,
    dataset::{
        AnyDataset, Dataset, default::DefaultDataset, resolve_read_dimensions,
        source::DatasetSource,
    },
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
///
/// The table projects at least one column: the scan refuses one that does
/// not, see `require_projection`. A dataset's grid is therefore always the
/// grid of the columns it reads.
#[derive(Clone)]
pub struct AtlasView {
    atlas: Arc<Atlas>,
    table_schema: SchemaRef,
    column_views: Arc<IndexMap<FieldRef, Option<AtlasColumnView>>>,
    /// The dimensions the scan reads, or `None` to pick a broadcast-compatible
    /// default per dataset. See [`AtlasView::dataset`].
    read_dimensions: Option<Vec<String>>,
    /// The rule that merged the table schema. It decides which casts read null.
    type_widening: Arc<dyn ArrowTypeWideningStrategy>,
}

impl AtlasView {
    /// Open the collection at `object_meta`, through `cache` when given, and
    /// resolve every column of `table_schema` against it. `type_widening` is
    /// the rule that merged that schema, and `read_dimensions` the axes the
    /// scan reads on.
    pub async fn new(
        cache: Option<&AtlasReaderCache>,
        store: Arc<dyn ObjectStore>,
        object_meta: ObjectMeta,
        table_schema: SchemaRef,
        read_dimensions: Option<Vec<String>>,
        type_widening: Arc<dyn ArrowTypeWideningStrategy>,
    ) -> anyhow::Result<Self> {
        let atlas = get_or_open_atlas(cache, store, &object_meta).await?;
        let views = column_views(&atlas, &table_schema)
            .await
            .with_context(|| format!("resolving the columns of '{}'", object_meta.location))?;

        Ok(Self {
            atlas,
            table_schema,
            column_views: Arc::new(views),
            read_dimensions,
            type_widening,
        })
    }

    /// The table schema the view resolves columns for, in field order.
    pub fn table_schema(&self) -> &SchemaRef {
        &self.table_schema
    }

    /// The rule that merged the table schema.
    pub fn type_widening(&self) -> &Arc<dyn ArrowTypeWideningStrategy> {
        &self.type_widening
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
    ///
    /// The arrays are then narrowed to the dimensions the scan reads, by the
    /// rule every nd format shares: an explicit list wins, and without one the
    /// dataset's broadcast-compatible default is taken. An array on any other
    /// axis is dropped, and its column reads as null on the grid that is kept.
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
        let arrays = on_read_dimensions(dataset_name, arrays, self.read_dimensions.clone()).await;
        let dataset = DefaultDataset::new(dataset_name.to_string(), arrays)
            .with_context(|| format!("laying out dataset '{dataset_name}'"))?;
        Ok(Arc::new(dataset))
    }
}

/// `arrays` narrowed to the dimensions the scan reads.
///
/// `read_dimensions` names them, or `None` leaves the choice to the dataset's
/// broadcast-compatible default, see [`resolve_read_dimensions`]. An array
/// survives when every one of its axes is in the set, so an attribute on no
/// axis always does. With no set to apply, every array survives.
async fn on_read_dimensions(
    dataset_name: &str,
    arrays: IndexMap<String, Arc<dyn NdArrayD>>,
    read_dimensions: Option<Vec<String>>,
) -> IndexMap<String, Arc<dyn NdArrayD>> {
    let dataset = AnyDataset::Regular(Dataset::new(dataset_name.to_string(), arrays).await);
    let dims = resolve_read_dimensions(&dataset, read_dimensions, None);
    let AnyDataset::Regular(dataset) = dataset else {
        unreachable!("built as a regular dataset above");
    };
    let Some(dims) = dims else {
        return dataset.arrays;
    };
    dataset
        .arrays
        .into_iter()
        .filter(|(_, array)| array.dimensions().iter().all(|dim| dims.contains(dim)))
        .collect()
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
/// null struct row, so the scan sees one thing either way. `type_widening` is
/// the rule that merged the table schema, and it decides which casts read null.
pub(crate) fn under_fields(
    nd: &NdRecordBatch,
    fields: &[FieldRef],
    type_widening: &dyn ArrowTypeWideningStrategy,
) -> anyhow::Result<NdRecordBatch> {
    let mut columns = Vec::with_capacity(fields.len());
    for field in fields {
        let column = match nd.schema().column_with_name(field.name()) {
            Some((index, _)) => {
                let column = nd.column(index);
                match as_field_type(Arc::clone(column.values()), field, type_widening)? {
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
/// widened it: that is a cast. A dataset of another family than the table
/// column reached the scan through `TypeConflict::KeepFirst` alone, and the
/// rule that merged the schema says so. Such a dataset reads as null.
fn as_field_type(
    values: ArrayRef,
    field: &Field,
    type_widening: &dyn ArrowTypeWideningStrategy,
) -> anyhow::Result<Option<ArrayRef>> {
    if values.data_type() == field.data_type() {
        return Ok(Some(values));
    }
    match cast(&values, field.data_type()) {
        Ok(values) => Ok(Some(values)),
        Err(_) if type_widening.casts_leniently(values.data_type(), field.data_type()) => Ok(None),
        Err(error) => Err(error)
            .with_context(|| format!("casting column '{}' to {}", field.name(), field.data_type())),
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::{Array, AsArray, RecordBatch};
    use arrow::datatypes::{Float32Type, Float64Type, Int32Type, Int64Type};
    use beacon_datafusion_ext::type_widening::{ArrowTypeWidening, DefaultArrowTypeWidening};
    use std::path::Path;

    use super::*;
    use crate::{compat, test_support};

    /// The strict default merge rule.
    fn strict() -> Arc<dyn ArrowTypeWideningStrategy> {
        Arc::new(DefaultArrowTypeWidening::new())
    }

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
        read_on(dir, dataset, None).await
    }

    /// [`read`], on the dimensions `read_dimensions` names.
    async fn read_on(
        dir: &Path,
        dataset: &str,
        read_dimensions: Option<Vec<String>>,
    ) -> (Vec<NdRecordBatch>, RecordBatch) {
        let schema = schema(dir).await;
        let (store, marker) = test_support::store_and_marker(dir);
        let view = AtlasView::new(
            None,
            store,
            marker,
            Arc::clone(&schema),
            read_dimensions,
            strict(),
        )
        .await
        .unwrap();
        let source = view.dataset(dataset).await.unwrap();
        let mut chunks = Vec::new();
        for chunk in source.chunks() {
            let nd = source.poll_next(chunk).await.unwrap().unwrap();
            chunks.push(under_fields(&nd, schema.fields(), strict().as_ref()).unwrap());
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

    /// `mixed` holds `temperature` on `obs` and `grid` on `lat` and `lon`. No
    /// one grid holds both, so the dimension list decides which is read, and
    /// the other reads as null on the grid that is kept.
    #[tokio::test]
    async fn the_dimension_list_drops_the_arrays_on_other_axes() {
        let tmp = tempfile::tempdir().unwrap();
        test_support::two_grids(tmp.path()).await;
        let dims = |names: &[&str]| Some(names.iter().map(|d| d.to_string()).collect());

        let (_, on_obs) = read_on(tmp.path(), "mixed", dims(&["obs"])).await;
        assert_eq!(on_obs.num_rows(), 4, "the `obs` axis");
        assert_eq!(
            column(&on_obs, "temperature")
                .as_primitive::<Float32Type>()
                .values()
                .to_vec(),
            vec![1.0, 2.0, 3.0, 4.0]
        );
        assert_eq!(column(&on_obs, "grid").null_count(), 4, "dropped, so null");

        let (_, on_grid) = read_on(tmp.path(), "mixed", dims(&["lat", "lon"])).await;
        assert_eq!(on_grid.num_rows(), 6, "the `lat` by `lon` grid");
        assert_eq!(column(&on_grid, "temperature").null_count(), 6);
        assert_eq!(
            column(&on_grid, "grid")
                .as_primitive::<Float64Type>()
                .values()
                .to_vec(),
            (0..6).map(f64::from).collect::<Vec<_>>()
        );
        assert_eq!(
            column(&on_grid, ".season").as_string::<i32>().value(5),
            "spring",
            "an attribute has no axis, so it survives any list"
        );
    }

    /// Without a list the dataset's broadcast-compatible default decides. The
    /// two grids tie on arrays kept, so the one with more cells wins.
    #[tokio::test]
    async fn without_a_list_the_default_grid_is_read() {
        let tmp = tempfile::tempdir().unwrap();
        test_support::two_grids(tmp.path()).await;

        let (_, batch) = read(tmp.path(), "mixed").await;

        assert_eq!(batch.num_rows(), 6, "six cells beat four");
        assert_eq!(column(&batch, "temperature").null_count(), 6);
        assert_eq!(column(&batch, "grid").null_count(), 0);
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
