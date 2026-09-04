//! Turning an Atlas collection into what Beacon's engine reads: one dataset as
//! an [`AnyDataset`] of lazy columns, and a whole collection as one Arrow
//! schema.
//!
//! Nothing here reads array data. A dataset is built from the collection
//! footer, which the open already held, and its columns fetch their bytes when
//! the scan asks for them.

use std::collections::HashSet;
use std::sync::Arc;

use arrow::datatypes::{Schema, SchemaRef};
use atlas::{Atlas, Attr, DType, DatasetView};
use beacon_datafusion_ext::type_widening::{ArrowTypeWidening, LabeledSchema};
use beacon_nd_array::{
    NdArrayD,
    arrow::schema::any_dataset_to_arrow_schema,
    dataset::{AnyDataset, Dataset, resolve_read_dimensions},
    projection::DatasetProjection,
};
use indexmap::IndexMap;
use object_store::{ObjectMeta, ObjectStore};

use crate::compat;

/// Build one dataset as an [`AnyDataset`] of lazy columns.
///
/// `projected_names` is the column set the query wants:
///
/// - `None` — every array and attribute.
/// - `Some(names)` — only the columns named. A name the dataset does not hold
///   is ignored, so a projection may name columns from any dataset of the
///   collection.
///
/// The projection reaches the *build*, not just the result: an array outside it
/// gets no backend, and an attribute outside it is not even read out of the
/// footer. A column-subset query over a wide dataset therefore pays nothing for
/// the columns it did not ask for.
///
/// A value Beacon cannot surface — a `Bool` or list array, a list attribute —
/// is dropped with a `debug` log. A collection can hold a million datasets, so
/// a louder log would be a flood. Such an array is settled from the footer
/// alone, so its segment is never opened.
///
/// # What this reads
///
/// Names and element types come from the footer the open already held. A
/// layout and an attribute value live in the variable's own segment, so the
/// first dataset to want one opens it. A segment covers that variable across
/// the whole collection, so every later dataset reuses the same handle.
pub async fn dataset_from_view(
    view: Arc<DatasetView>,
    projected_names: Option<&[String]>,
) -> anyhow::Result<AnyDataset> {
    let included = |name: &str| projected_names.is_none_or(|names| names.iter().any(|n| n == name));
    // Whether any projected column could be an attribute of this array. It
    // saves building an attribute map the projection would throw away.
    let wants_attrs_of = |array: &str| {
        projected_names.is_none_or(|names| {
            names
                .iter()
                .any(|name| compat::is_attr_column_of(name, array))
        })
    };
    let wants_global_attrs =
        projected_names.is_none_or(|names| names.iter().any(|name| name.starts_with('.')));

    // The schema borrows the footer, so it is resolved before the first await
    // rather than held across one.
    let declared = declared_arrays(&view);
    let mut arrays: IndexMap<String, Arc<dyn NdArrayD>> = IndexMap::with_capacity(declared.len());

    for (array_name, dtype) in &declared {
        if included(array_name) {
            match compat::array_dtype_to_nd(dtype) {
                // The layout is in the variable's segment, so it is asked for
                // only once the dtype says the column can exist at all.
                Some(_) => {
                    let layout = view.array_layout(array_name).await.map_err(|e| {
                        anyhow::anyhow!(
                            "Failed to read the layout of atlas array '{array_name}' \
                             of dataset '{}': {e}",
                            view.name()
                        )
                    })?;
                    match compat::array_to_nd_array(Arc::clone(&view), array_name, dtype, &layout) {
                        Ok(nd) => {
                            arrays.insert(array_name.clone(), nd);
                        }
                        Err(e) => tracing::debug!(
                            dataset = %view.name(),
                            array = %array_name,
                            "atlas array left out of the dataset: {e}"
                        ),
                    }
                }
                None => tracing::debug!(
                    dataset = %view.name(),
                    array = %array_name,
                    "atlas array left out of the dataset: {} is no Beacon column",
                    compat::dtype_tag(dtype)
                ),
            }
        }

        if !wants_attrs_of(array_name) {
            continue;
        }
        for (key, value) in attributes_of(&view, Some(array_name)).await? {
            let column = compat::array_attr_column(array_name, &key);
            if !included(&column) {
                continue;
            }
            match compat::attribute_to_nd_array(&value) {
                Ok(nd) => {
                    arrays.insert(column, nd);
                }
                Err(e) => tracing::debug!(
                    dataset = %view.name(),
                    column = %column,
                    "atlas attribute left out of the dataset: {e}"
                ),
            }
        }
    }

    if wants_global_attrs {
        for (key, value) in attributes_of(&view, None).await? {
            let column = compat::global_attr_column(&key);
            if !included(&column) {
                continue;
            }
            match compat::attribute_to_nd_array(&value) {
                Ok(nd) => {
                    arrays.insert(column, nd);
                }
                Err(e) => tracing::debug!(
                    dataset = %view.name(),
                    column = %column,
                    "atlas attribute left out of the dataset: {e}"
                ),
            }
        }
    }

    arrays.sort_keys();

    let dataset = Dataset::new(view.name().to_string(), arrays).await;
    AnyDataset::try_from_dataset(dataset)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to wrap atlas dataset '{}': {e}", view.name()))
}

/// Every array the dataset declares, as owned name and element type.
///
/// The schema borrows the collection footer. Resolving it up front keeps that
/// borrow off the async path, where the same view is also cloned into a
/// backend.
fn declared_arrays(view: &DatasetView) -> Vec<(String, DType)> {
    view.schema()
        .iter()
        .map(|meta| (meta.name().to_string(), meta.dtype().clone()))
        .collect()
}

/// The attribute values of one scope: `Some(array)` for an array's own,
/// `None` for the dataset's.
///
/// Values live in a segment, not in the footer, so this reads one. An array
/// with no attribute costs nothing, because the schema settles it first.
async fn attributes_of(
    view: &DatasetView,
    array: Option<&str>,
) -> anyhow::Result<IndexMap<String, Attr>> {
    let scope = match array {
        Some(array) => view.array_attributes(array).await,
        None => view.attributes().await,
    };
    scope.map_err(|e| {
        let what = array.map_or_else(
            || "the dataset attributes".to_string(),
            |array| format!("the attributes of array '{array}'"),
        );
        anyhow::anyhow!(
            "Failed to read {what} of atlas dataset '{}': {e}",
            view.name()
        )
    })
}

/// Narrow `dataset` to `read_dimensions`, or to a broadcast-compatible default
/// when none are given.
///
/// Without this a `SELECT *` over a dataset whose arrays live on incompatible
/// dimension sets could not broadcast onto one grid. `log_label` names the
/// caller in the auto-selection log; pass `None` from per-dataset code, where
/// schema inference has already logged the choice.
pub fn project_read_dimensions(
    dataset: AnyDataset,
    read_dimensions: Option<Vec<String>>,
    log_label: Option<&str>,
) -> anyhow::Result<AnyDataset> {
    match resolve_read_dimensions(&dataset, read_dimensions, log_label) {
        Some(dims) => dataset
            .project(&DatasetProjection::new_with_dimension_projection(dims))
            .map_err(|e| anyhow::anyhow!("Failed to project the atlas dataset by dimension: {e}")),
        None => Ok(dataset),
    }
}

/// Build one dataset of the collection at `marker`, over `store`.
///
/// A convenience for a caller holding an object store. The scan opens the
/// collection once and calls [`dataset_from_view`] per dataset instead.
pub async fn open_dataset(
    store: Arc<dyn ObjectStore>,
    marker: &ObjectMeta,
    dataset: &str,
) -> anyhow::Result<AnyDataset> {
    let atlas = crate::store::get_or_open_atlas(None, store, marker).await?;
    let view = atlas
        .dataset(dataset)
        .map_err(|e| anyhow::anyhow!("Failed to open atlas dataset '{dataset}': {e}"))?;
    dataset_from_view(Arc::new(view), None).await
}

/// The Arrow schema of a whole collection: every live dataset, merged.
///
/// Atlas reconciles nothing. Two datasets may declare one array name with two
/// dtypes, so the collection's schema is the widening merge of its datasets'
/// schemas, under the rule the session carries.
///
/// # One schema per shape, not per dataset
///
/// Datasets that declare the same arrays share one interned schema in the
/// footer, and `atlas create` writes a fleet of files that way. Each dataset is
/// therefore reduced to a key over its interned schema and its attribute
/// namespace, and a schema is derived once per distinct key. A thousand
/// datasets of one shape cost one derivation.
///
/// # Cost
///
/// Linear in the dataset count, and every step is in memory: the footer keys
/// its datasets by name, so resolving one is a single hash lookup, and a key is
/// built from the footer and from segments that one open serves collection
/// wide. Only the distinct keys are derived into a schema. The result is cached
/// above this crate, so a table pays even that once rather than once per query.
pub async fn collection_schema(
    atlas: &Arc<Atlas>,
    read_dimensions: Option<&[String]>,
    label: &str,
    widening: &ArrowTypeWidening,
) -> anyhow::Result<SchemaRef> {
    let mut seen: HashSet<String> = HashSet::new();
    let mut schemas: Vec<LabeledSchema> = Vec::new();

    for name in atlas.list_datasets() {
        let view = atlas
            .dataset(&name)
            .map_err(|e| anyhow::anyhow!("Failed to open atlas dataset '{name}': {e}"))?;

        if !seen.insert(shape_key(&view).await?) {
            continue;
        }

        let dataset = dataset_from_view(Arc::new(view), None).await?;
        // The same narrowing the scan applies, so the schema states what a
        // query can actually return.
        let dataset =
            project_read_dimensions(dataset, read_dimensions.map(<[String]>::to_vec), None)?;
        let schema = any_dataset_to_arrow_schema(&dataset).map_err(|e| {
            anyhow::anyhow!("Failed to derive the Arrow schema of atlas dataset '{name}': {e}")
        })?;
        // The dataset names the schema, so a refused column names both sides.
        schemas.push(LabeledSchema::new(
            Arc::new(schema),
            format!("{label}#{name}"),
        ));
    }

    if schemas.is_empty() {
        // A collection with no live dataset has no column. That is legal, and
        // an empty schema is what every other reader answers with.
        return Ok(Arc::new(Schema::empty()));
    }

    widening
        .merge_schemas(&schemas)
        .map_err(|e| anyhow::anyhow!("Failed to merge the schemas of the atlas datasets: {e}"))
}

/// What makes two datasets produce the same columns and types.
///
/// Three things decide a dataset's Arrow schema, and the key holds all three:
///
/// - The arrays it declares, with their element types. Datasets that declare
///   the same ones share one interned schema in the footer, and `atlas create`
///   writes a fleet of files that way.
/// - Its attribute keys and their types, at both scopes. Those are named in the
///   interned schema too, so two datasets that differ only in an attribute's
///   *value* share a key. That is exactly the fleet case.
/// - Each array's dimension names, which the interned schema does **not** hold.
///   They pick the default grid, and a different grid keeps different columns,
///   so two datasets that agree on everything else can still differ here.
///
/// A shape is deliberately left out: an array of a different length is the same
/// column.
///
/// # Cost
///
/// The names and the types come from the footer. A dimension name comes from
/// its variable's segment, which one open serves for every dataset of the
/// collection, so the lookup is in memory after the first.
async fn shape_key(view: &DatasetView) -> anyhow::Result<String> {
    let mut key = String::new();

    for (array, dtype) in declared_arrays(view) {
        key.push('|');
        key.push_str(&array);
        key.push(':');
        key.push_str(&compat::dtype_tag(&dtype));

        // An array Beacon cannot read is no column, so its grid decides
        // nothing and its segment stays shut.
        if compat::array_dtype_to_nd(&dtype).is_some() {
            let layout = view.array_layout(&array).await.map_err(|e| {
                anyhow::anyhow!(
                    "Failed to read the layout of atlas array '{array}' of dataset '{}': {e}",
                    view.name()
                )
            })?;
            key.push('@');
            key.push_str(&layout.dimension_names().join(","));
        }
    }

    // Attribute keys and types are in the interned schema, so this reads
    // nothing. The values are not, and they do not belong in the key.
    fn push(key: &mut String, array: &str, attr: &str, dtype: &DType) {
        key.push('|');
        key.push_str(array);
        key.push('.');
        key.push_str(attr);
        key.push(':');
        key.push_str(&compat::dtype_tag(dtype));
    }
    let schema = view.schema();
    for meta in schema.iter() {
        let array = meta.name();
        for (attr, dtype) in meta.attribute_pairs() {
            push(&mut key, array, attr, dtype);
        }
    }
    for (attr, dtype) in schema.attribute_pairs() {
        push(&mut key, "", attr, dtype);
    }

    Ok(key)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support;
    use arrow::datatypes::DataType;
    use beacon_nd_array::{NdArray, datatypes::TimestampNanosecond};

    async fn view(dir: &std::path::Path, dataset: &str) -> Arc<DatasetView> {
        let atlas = test_support::open(dir).await;
        Arc::new(
            atlas
                .dataset(dataset)
                .expect("the dataset is in the collection"),
        )
    }

    fn widening() -> Arc<ArrowTypeWidening> {
        ArrowTypeWidening::default_extension()
    }

    fn names(dataset: &AnyDataset) -> Vec<String> {
        dataset.fields().keys().cloned().collect()
    }

    // ── one dataset ─────────────────────────────────────────────────────

    #[tokio::test]
    async fn a_dataset_holds_its_arrays_and_its_attributes() {
        let tmp = tempfile::tempdir().unwrap();
        test_support::two_datasets(tmp.path()).await;

        let dataset = dataset_from_view(view(tmp.path(), "winter").await, None)
            .await
            .unwrap();

        assert_eq!(dataset.name(), "winter");
        assert_eq!(
            names(&dataset),
            vec![
                ".season",
                ".year",
                "cycle",
                "temperature",
                "temperature.units",
                "time",
            ]
        );
    }

    /// A dataset attribute takes a leading dot, and an array attribute takes its
    /// array's name. That is what netCDF and Zarr do, and it keeps an attribute
    /// from colliding with an array of the same name.
    #[tokio::test]
    async fn attributes_are_named_the_way_every_nd_format_names_them() {
        let tmp = tempfile::tempdir().unwrap();
        test_support::two_datasets(tmp.path()).await;

        let dataset = dataset_from_view(view(tmp.path(), "winter").await, None)
            .await
            .unwrap();

        let season = dataset
            .get_array(".season")
            .expect("the dataset attribute is a column")
            .as_any()
            .downcast_ref::<NdArray<String>>()
            .expect("a string column");
        assert!(season.shape().is_empty(), "an attribute has no axis");
        assert_eq!(
            season.clone_into_raw_vec().await,
            vec!["winter".to_string()]
        );

        let units = dataset
            .get_array("temperature.units")
            .expect("the array attribute is a column")
            .as_any()
            .downcast_ref::<NdArray<String>>()
            .expect("a string column");
        assert_eq!(
            units.clone_into_raw_vec().await,
            vec!["celsius".to_string()]
        );
    }

    #[tokio::test]
    async fn every_column_keeps_the_type_the_footer_gave_it() {
        let tmp = tempfile::tempdir().unwrap();
        test_support::two_datasets(tmp.path()).await;

        let dataset = dataset_from_view(view(tmp.path(), "winter").await, None)
            .await
            .unwrap();
        let schema = any_dataset_to_arrow_schema(&dataset).unwrap();
        let field = |name: &str| schema.field_with_name(name).unwrap().data_type().clone();

        assert_eq!(field("temperature"), DataType::Float32);
        assert_eq!(field("cycle"), DataType::Int32);
        assert_eq!(
            field("time"),
            DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None)
        );
        assert_eq!(field(".year"), DataType::Int64);
        assert_eq!(field(".season"), DataType::Utf8);
    }

    #[tokio::test]
    async fn array_values_read_back() {
        let tmp = tempfile::tempdir().unwrap();
        test_support::two_datasets(tmp.path()).await;

        let dataset = dataset_from_view(view(tmp.path(), "winter").await, None)
            .await
            .unwrap();

        let temperature = dataset
            .get_array("temperature")
            .unwrap()
            .as_any()
            .downcast_ref::<NdArray<f32>>()
            .unwrap();
        assert_eq!(
            temperature.clone_into_raw_vec().await,
            vec![1.0, 2.0, 3.0, 4.0]
        );

        let time = dataset
            .get_array("time")
            .unwrap()
            .as_any()
            .downcast_ref::<NdArray<TimestampNanosecond>>()
            .unwrap();
        assert_eq!(
            time.clone_into_raw_vec().await[0],
            TimestampNanosecond(test_support::EPOCH_NANOS)
        );
    }

    /// The fill reaches the column, which is what lets the engine null an
    /// unwritten cell.
    #[tokio::test]
    async fn an_arrays_fill_value_reaches_its_column() {
        let tmp = tempfile::tempdir().unwrap();
        test_support::two_datasets(tmp.path()).await;

        let dataset = dataset_from_view(view(tmp.path(), "winter").await, None)
            .await
            .unwrap();

        let cycle = dataset
            .get_array("cycle")
            .unwrap()
            .as_any()
            .downcast_ref::<NdArray<i32>>()
            .unwrap();
        assert_eq!(cycle.fill_value().await, Some(-1));

        let temperature = dataset
            .get_array("temperature")
            .unwrap()
            .as_any()
            .downcast_ref::<NdArray<f32>>()
            .unwrap();
        assert_eq!(temperature.fill_value().await, None, "none was declared");
    }

    /// The chunk shape is the writer's, so a scan cuts the dataset on the grid
    /// the file stores rather than on one it invents.
    #[tokio::test]
    async fn a_column_reports_the_stored_chunk_shape() {
        let tmp = tempfile::tempdir().unwrap();
        test_support::chunked_grid(tmp.path()).await;

        let dataset = dataset_from_view(view(tmp.path(), "grid").await, None)
            .await
            .unwrap();
        let temperature = dataset.get_array("temperature").unwrap();
        assert_eq!(temperature.shape(), vec![4, 6]);
        assert_eq!(temperature.chunk_shape(), vec![2, 3]);
        assert_eq!(
            temperature.dimensions(),
            vec!["lat".to_string(), "lon".to_string()]
        );
    }

    // ── projection ──────────────────────────────────────────────────────

    #[tokio::test]
    async fn a_projection_builds_only_what_it_names() {
        let tmp = tempfile::tempdir().unwrap();
        test_support::two_datasets(tmp.path()).await;

        let wanted = vec!["temperature".to_string(), ".season".to_string()];
        let dataset = dataset_from_view(view(tmp.path(), "winter").await, Some(&wanted))
            .await
            .unwrap();

        assert_eq!(names(&dataset), vec![".season", "temperature"]);
    }

    #[tokio::test]
    async fn a_projection_may_name_a_column_this_dataset_lacks() {
        let tmp = tempfile::tempdir().unwrap();
        test_support::two_datasets(tmp.path()).await;

        // `cycle` is winter's alone; summer simply has none of it.
        let wanted = vec!["temperature".to_string(), "cycle".to_string()];
        let dataset = dataset_from_view(view(tmp.path(), "summer").await, Some(&wanted))
            .await
            .unwrap();
        assert_eq!(names(&dataset), vec!["temperature"]);
    }

    #[tokio::test]
    async fn an_empty_projection_builds_nothing() {
        let tmp = tempfile::tempdir().unwrap();
        test_support::two_datasets(tmp.path()).await;

        let dataset = dataset_from_view(view(tmp.path(), "winter").await, Some(&[]))
            .await
            .unwrap();
        assert!(names(&dataset).is_empty(), "COUNT(*) needs no column here");
    }

    // ── values Beacon cannot surface ────────────────────────────────────

    #[tokio::test]
    async fn a_list_attribute_is_dropped_and_the_rest_survives() {
        let tmp = tempfile::tempdir().unwrap();
        test_support::skips(tmp.path()).await;

        let dataset = dataset_from_view(view(tmp.path(), "s").await, None)
            .await
            .unwrap();
        let columns = names(&dataset);

        assert!(
            !columns.contains(&"value.range".to_string()),
            "a list attribute has no rank-0 form: {columns:?}"
        );
        assert!(
            !columns.contains(&".tags".to_string()),
            "nor does a list dataset attribute: {columns:?}"
        );
        assert_eq!(
            columns,
            vec![".title", "value", "value.units"],
            "everything else is kept"
        );
    }

    // ── ragged datasets ─────────────────────────────────────────────────

    /// The `{array}.{attr}` naming is what the engine's ragged detection reads,
    /// so a CF contiguous ragged collection is recognized without anything
    /// atlas-specific.
    #[tokio::test]
    async fn a_plain_dataset_is_regular() {
        let tmp = tempfile::tempdir().unwrap();
        test_support::two_datasets(tmp.path()).await;

        let dataset = dataset_from_view(view(tmp.path(), "winter").await, None)
            .await
            .unwrap();
        assert!(!dataset.is_ragged());
    }

    // ── the collection schema ───────────────────────────────────────────

    #[tokio::test]
    async fn a_collections_schema_is_the_union_of_its_datasets() {
        let tmp = tempfile::tempdir().unwrap();
        test_support::two_datasets(tmp.path()).await;
        let atlas = test_support::open(tmp.path()).await;

        let schema = collection_schema(&atlas, None, "c", &widening())
            .await
            .unwrap();
        let columns: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();

        for expected in [
            ".season",
            ".year",
            "cycle",
            "temperature",
            "temperature.units",
            "time",
        ] {
            assert!(
                columns.contains(&expected),
                "missing {expected}: {columns:?}"
            );
        }
    }

    /// Two datasets that give one array two numeric types merge to the type that
    /// holds both, by the rule of the session rather than one of atlas's own.
    ///
    /// `Int16` beside `Float32` gives `Float64`: a `Float32` holds no `Int32`,
    /// so a rule that kept it for a narrow integer would make the answer depend
    /// on which dataset the merge saw first. See issue #377.
    #[tokio::test]
    async fn a_shared_array_widens_to_a_type_that_holds_both() {
        let tmp = tempfile::tempdir().unwrap();
        test_support::widening(tmp.path()).await;
        let atlas = test_support::open(tmp.path()).await;

        let schema = collection_schema(&atlas, None, "c", &widening())
            .await
            .unwrap();
        assert_eq!(
            schema.field_with_name("value").unwrap().data_type(),
            &DataType::Float64,
            "Int16 and Float32 widen to Float64"
        );
        assert_eq!(
            schema.field_with_name("flag").unwrap().data_type(),
            &DataType::Int32,
            "a column only one dataset declares keeps its own type"
        );
    }

    /// A column two datasets type in two families is refused, and the error
    /// names both datasets.
    ///
    /// Atlas reconciles nothing, so a collection can hold this. Beacon settles
    /// it the way it settles two files of any other format, and the label makes
    /// the offender findable in a collection of a million datasets.
    #[tokio::test]
    async fn types_that_do_not_widen_are_refused_by_name() {
        let tmp = tempfile::tempdir().unwrap();
        test_support::incompatible(tmp.path()).await;
        let atlas = test_support::open(tmp.path()).await;

        let error = collection_schema(&atlas, None, "sensor", &widening())
            .await
            .expect_err("Utf8 and Int64 are two families")
            .to_string();

        assert!(error.contains("value"), "the column: {error}");
        assert!(
            error.contains("Utf8") && error.contains("Int64"),
            "both types: {error}"
        );
        assert!(
            error.contains("sensor#a") && error.contains("sensor#b"),
            "and both datasets: {error}"
        );
    }

    /// A deployment that reads such a collection anyway sets `keep_first`, and
    /// the column then takes the type of the first dataset in listing order.
    #[tokio::test]
    async fn keep_first_settles_a_conflict_with_the_first_datasets_type() {
        use beacon_datafusion_ext::type_widening::{DefaultArrowTypeWidening, TypeConflict};

        let tmp = tempfile::tempdir().unwrap();
        test_support::incompatible(tmp.path()).await;
        let atlas = test_support::open(tmp.path()).await;

        let keep_first = ArrowTypeWidening::new(Arc::new(DefaultArrowTypeWidening {
            on_conflict: TypeConflict::KeepFirst,
        }));
        let schema = collection_schema(&atlas, None, "c", &keep_first)
            .await
            .unwrap();

        // `a` is written first and states `value` as a string.
        assert_eq!(
            schema.field_with_name("value").unwrap().data_type(),
            &DataType::Utf8
        );
    }

    #[tokio::test]
    async fn a_collection_with_no_dataset_has_no_column() {
        let tmp = tempfile::tempdir().unwrap();
        test_support::empty(tmp.path()).await;
        let atlas = test_support::open(tmp.path()).await;

        let schema = collection_schema(&atlas, None, "c", &widening())
            .await
            .unwrap();
        assert!(schema.fields().is_empty());
    }

    /// Datasets that declare the same arrays share one interned schema, so the
    /// derivation runs once however many of them there are. The fleet fixture
    /// gives ten datasets one shape, and they differ only in an attribute
    /// value.
    #[tokio::test]
    async fn a_fleet_of_one_shape_derives_one_schema() {
        let tmp = tempfile::tempdir().unwrap();
        test_support::ranged(tmp.path(), 10).await;
        let atlas = test_support::open(tmp.path()).await;

        assert_eq!(atlas.interned_schemas(), 1, "the fixture shares its schema");

        let mut keys = HashSet::new();
        for name in atlas.list_datasets() {
            keys.insert(shape_key(&atlas.dataset(&name).unwrap()).await.unwrap());
        }
        assert_eq!(keys.len(), 1, "and every dataset reduces to one key");

        let schema = collection_schema(&atlas, None, "c", &widening())
            .await
            .unwrap();
        let columns: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(columns, vec![".platform", "temperature"]);
    }

    /// Two datasets that share arrays but not attribute *keys* produce two
    /// column sets, so the key has to separate them.
    #[tokio::test]
    async fn a_different_attribute_namespace_is_a_different_shape() {
        let tmp = tempfile::tempdir().unwrap();
        test_support::two_datasets(tmp.path()).await;
        let atlas = test_support::open(tmp.path()).await;

        let winter = shape_key(&atlas.dataset("winter").unwrap()).await.unwrap();
        let summer = shape_key(&atlas.dataset("summer").unwrap()).await.unwrap();
        assert_ne!(winter, summer);
    }

    // ── dimensions ──────────────────────────────────────────────────────

    #[tokio::test]
    async fn read_dimensions_narrow_the_schema_to_the_grid_they_name() {
        let tmp = tempfile::tempdir().unwrap();
        test_support::chunked_grid(tmp.path()).await;
        let atlas = test_support::open(tmp.path()).await;

        // Both arrays live on `lat` and `lon`; naming only `lat` leaves neither.
        let dims = ["lat".to_string()];
        let schema = collection_schema(&atlas, Some(&dims), "c", &widening())
            .await
            .unwrap();
        let columns: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert!(
            !columns.contains(&"temperature"),
            "a 2-D array does not fit a 1-D grid: {columns:?}"
        );
    }
}
