//! One collection, resolved against the table's schema.
//!
//! A column view says where one column of the scan comes from, for every
//! dataset at once. Pruning and the scan read through the same views.

use std::sync::Arc;

use anyhow::Context as _;
use arrow::datatypes::{FieldRef, Schema};
use atlas::{ArrayFile, Atlas, Attr};
use beacon_nd_array::{
    NdArrayD,
    dataset::{Dataset, default::DefaultDataset, source::DatasetSource},
};
use indexmap::IndexMap;
use object_store::{ObjectMeta, ObjectStore};

use crate::{
    dataset,
    metrics::AtlasScanMetrics,
    open::{AtlasReaderCache, get_or_open_atlas},
    prune::prune_datasets,
    scan::ScanSpec,
};

/// An open collection and the resolution of every column of the scan.
/// A dataset's grid is the grid of the columns it reads.
#[derive(Clone)]
pub struct AtlasView {
    atlas: Arc<Atlas>,
    /// What the scan reads. The columns are resolved against its logical
    /// schema.
    spec: Arc<ScanSpec>,
    column_views: Arc<IndexMap<FieldRef, Option<AtlasColumnView>>>,
}

impl AtlasView {
    /// Open the collection at `object_meta`, through `cache` when given, and
    /// resolve every column of `spec` against it.
    pub async fn new(
        cache: Option<&AtlasReaderCache>,
        store: Arc<dyn ObjectStore>,
        object_meta: ObjectMeta,
        spec: Arc<ScanSpec>,
    ) -> anyhow::Result<Self> {
        let atlas = get_or_open_atlas(cache, store, &object_meta).await?;
        let views = column_views(&atlas, &spec.logical_schema)
            .await
            .with_context(|| format!("resolving the columns of '{}'", object_meta.location))?;

        Ok(Self {
            atlas,
            spec,
            column_views: Arc::new(views),
        })
    }

    /// What the scan reads.
    pub fn spec(&self) -> &Arc<ScanSpec> {
        &self.spec
    }

    /// The datasets worth reading, in the collection's order.
    ///
    /// A dataset the deletion mask hides is skipped; a predicate may drop more.
    pub async fn list_datasets(
        &self,
        scan_metrics: &AtlasScanMetrics,
    ) -> anyhow::Result<Vec<String>> {
        let mut datasets = self.atlas.list_datasets();

        if let Some(predicate) = &self.spec.predicate {
            let prune_timer = scan_metrics.prune_time.timer();
            let listed = datasets.len();
            datasets = prune_datasets(
                &self.column_views,
                datasets,
                predicate,
                &self.spec.logical_schema,
                &self.spec.cancel,
            )
            .await;
            scan_metrics.datasets_pruned.add(listed - datasets.len());
            drop(prune_timer);
        }

        Ok(datasets)
    }

    /// One dataset of the collection as a lazy nd dataset, under the table's
    /// fields. Arrays are narrowed to the read dimensions. Others read as null.
    pub async fn dataset(&self, dataset_name: &str) -> anyhow::Result<Arc<dyn DatasetSource>> {
        let mut arrays: IndexMap<String, Arc<dyn NdArrayD>> = IndexMap::new();
        for (field, view) in &*self.column_views {
            let array = match view {
                None => None,
                Some(AtlasColumnView::Array { segment }) => match segment.array(dataset_name) {
                    Some(info) => Some(
                        dataset::array_to_nd_array(Arc::clone(segment), dataset_name, &info.dtype)
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
                    // A list has no rank-0 form, so it reads as null here.
                    map.get(dataset_name)
                        .and_then(|attr| dataset::attribute_to_nd_array(attr).ok())
                }
            };
            if let Some(array) = array {
                arrays.insert(field.name().clone(), array);
            }
        }
        let arrays =
            on_read_dimensions(dataset_name, arrays, self.spec.read_dimensions.clone()).await?;
        let dataset = DefaultDataset::new(dataset_name.to_string(), arrays)
            .with_context(|| format!("laying out dataset '{dataset_name}'"))?;
        Ok(Arc::new(dataset))
    }
}

/// `arrays` narrowed to the dimensions `read_dimensions` names. An array
/// survives if all its axes are kept. Without a list, arrays on more than one
/// grid are refused: the query has to say which grid it flattens onto.
async fn on_read_dimensions(
    dataset_name: &str,
    arrays: IndexMap<String, Arc<dyn NdArrayD>>,
    read_dimensions: Option<Vec<String>>,
) -> anyhow::Result<IndexMap<String, Arc<dyn NdArrayD>>> {
    let dataset = Dataset::new(dataset_name.to_string(), arrays).await;
    let Some(dims) = read_dimensions else {
        if let Some(default) = dataset.default_broadcast_dimensions() {
            anyhow::bail!(
                "dataset '{dataset_name}' holds the columns read on more than one grid, and \
                 no one grid fits them all. Name fewer columns, or pass a dimension list: \
                 read_atlas(paths, dimensions), for example {default:?}"
            );
        }
        return Ok(dataset.arrays);
    };
    Ok(dataset
        .arrays
        .into_iter()
        .filter(|(_, array)| array.dimensions().iter().all(|dim| dims.contains(dim)))
        .collect())
}

/// Where one column of the scan comes from, for every dataset of a collection.
/// The scan and pruning both read through it.
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

#[cfg(test)]
mod tests {
    use arrow::array::{Array, ArrayRef, AsArray, RecordBatch};
    use arrow::datatypes::{Float32Type, Float64Type, Int32Type, Int64Type, SchemaRef};
    use beacon_datafusion_ext::nd::{
        decode_nd_record_batch, encode_nd_record_batch, encoded_schema,
    };
    use beacon_datafusion_ext::scan_adapt::BatchAdapter;
    use beacon_datafusion_ext::type_widening::{ArrowTypeWidening, DefaultArrowTypeWidening};
    use std::path::Path;

    use super::*;
    use crate::{schema, test_support};
    use beacon_datafusion_ext::nd::NdRecordBatch;
    use beacon_datafusion_ext::type_widening::ArrowTypeWideningStrategy;
    use tokio_util::sync::CancellationToken;

    /// The strict default merge rule.
    fn strict() -> Arc<dyn ArrowTypeWideningStrategy> {
        Arc::new(DefaultArrowTypeWidening::new())
    }

    /// The schema `infer_schema` derives for a fixture.
    async fn schema(dir: &Path) -> SchemaRef {
        let atlas = test_support::open(dir).await;
        Arc::new(
            schema::collection_arrow_schema(
                &atlas.footer().collection_schema(),
                &ArrowTypeWidening::default_extension(),
            )
            .unwrap(),
        )
    }

    /// Every chunk of `dataset`, read as the scan reads it: through the view,
    /// encoded, and adapted onto the scan's schema. And the rows of all of
    /// them in chunk order.
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
        let target = Arc::new(encoded_schema(&schema));
        let (store, marker) = test_support::store_and_marker(dir);
        let spec = ScanSpec::new(
            Arc::clone(&target),
            read_dimensions,
            None,
            CancellationToken::new(),
        )
        .unwrap();
        let view = AtlasView::new(None, store, marker, Arc::new(spec))
            .await
            .unwrap();
        let source = view.dataset(dataset).await.unwrap();
        let mut chunks = Vec::new();
        for chunk in source.chunks() {
            let nd = source.poll_next(chunk).await.unwrap().unwrap();
            let encoded = encode_nd_record_batch(&nd).unwrap();
            let adapted = BatchAdapter::try_new(Arc::clone(&target), &encoded.schema(), &*strict())
                .unwrap()
                .adapt(&encoded)
                .unwrap();
            chunks.push(decode_nd_record_batch(&adapted).unwrap());
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

    /// Without a list, two grids in one dataset are refused: the query has
    /// to say which grid it flattens onto. The error names a list that works.
    #[tokio::test]
    async fn without_a_list_two_grids_are_refused() {
        let tmp = tempfile::tempdir().unwrap();
        test_support::two_grids(tmp.path()).await;
        let schema = schema(tmp.path()).await;
        let (store, marker) = test_support::store_and_marker(tmp.path());
        let spec = ScanSpec::new(
            Arc::new(encoded_schema(&schema)),
            None,
            None,
            CancellationToken::new(),
        )
        .unwrap();
        let view = AtlasView::new(None, store, marker, Arc::new(spec))
            .await
            .unwrap();

        let error = view
            .dataset("mixed")
            .await
            .expect_err("two grids, no list")
            .to_string();

        assert!(error.contains("more than one grid"), "{error}");
        assert!(error.contains("dimension list"), "{error}");
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
