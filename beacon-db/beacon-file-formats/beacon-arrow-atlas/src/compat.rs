//! The mapping between an Atlas collection and Beacon's ND array model: column
//! names, element types, and the lazy arrays themselves.
//!
//! One mapping, in one place. The Arrow type of a column follows from its
//! [`NdArrayDataType`] through `beacon-nd-array`'s own conversion, so a schema
//! derived here and a batch produced by a scan can never disagree.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema};
use arrow::error::ArrowError;
use atlas::{ArrayLayout, Attr, CollectionSchema, DType, DatasetView, FillValue};
use beacon_datafusion_ext::type_widening::{ArrowTypeWidening, LabeledSchema};
use beacon_nd_array::{
    NdArray, NdArrayD, datatypes::NdArrayDataType, datatypes::TimestampNanosecond,
};

use crate::backend::{AtlasArrayBackend, AtlasElement, AttributeBackend};

// ─── Column names ────────────────────────────────────────────────────────────

/// The column a per-array attribute is surfaced under: `{array}.{attr}`.
pub fn array_attr_column(array: &str, attr: &str) -> String {
    format!("{array}.{attr}")
}

/// The column a dataset-level attribute is surfaced under: `.{attr}`.
///
/// The leading dot is what netCDF and Zarr use, and it keeps a dataset
/// attribute from colliding with an array of the same name.
pub fn global_attr_column(attr: &str) -> String {
    format!(".{attr}")
}

/// Whether `column` could name a per-array attribute of `array`.
///
/// Used to skip building an attribute map for an array whose attributes the
/// query does not project.
pub fn is_attr_column_of(column: &str, array: &str) -> bool {
    column.len() > array.len() + 1
        && column.starts_with(array)
        && column.as_bytes()[array.len()] == b'.'
}

// ─── Element types ───────────────────────────────────────────────────────────

/// The ND type of a scalar atlas dtype, or `None` for the list dtypes, which
/// have no rank-0 or column analogue in Beacon.
fn scalar_dtype_to_nd(dtype: &DType) -> Option<NdArrayDataType> {
    Some(match dtype {
        DType::Bool => NdArrayDataType::Bool,
        DType::Int8 => NdArrayDataType::I8,
        DType::Int16 => NdArrayDataType::I16,
        DType::Int32 => NdArrayDataType::I32,
        DType::Int64 => NdArrayDataType::I64,
        DType::UInt8 => NdArrayDataType::U8,
        DType::UInt16 => NdArrayDataType::U16,
        DType::UInt32 => NdArrayDataType::U32,
        DType::UInt64 => NdArrayDataType::U64,
        DType::Float32 => NdArrayDataType::F32,
        DType::Float64 => NdArrayDataType::F64,
        DType::String => NdArrayDataType::String,
        DType::Binary => NdArrayDataType::Binary,
        DType::TimestampNs => NdArrayDataType::Timestamp,
        DType::List { .. } | DType::FixedSizeList { .. } => return None,
    })
}

/// The ND type of an atlas **array** dtype, or `None` for one Beacon cannot
/// read as a column.
///
/// `Bool` is excluded, unlike an attribute: `array-format` implements no
/// element type for `bool`, so no reader can produce the values. Every list
/// dtype is excluded too.
pub fn array_dtype_to_nd(dtype: &DType) -> Option<NdArrayDataType> {
    match dtype {
        DType::Bool => None,
        other => scalar_dtype_to_nd(other),
    }
}

/// The ND type of an atlas **attribute** dtype, or `None` for a list-valued
/// one. A scalar `Bool` attribute *is* supported: its value comes from the
/// footer rather than from an array.
pub fn attr_dtype_to_nd(dtype: &DType) -> Option<NdArrayDataType> {
    scalar_dtype_to_nd(dtype)
}

/// The Arrow type of an atlas array dtype, or `None` when Beacon cannot read
/// it. Derived from [`array_dtype_to_nd`], so it always matches the scan.
pub fn array_dtype_to_arrow(dtype: &DType) -> Option<DataType> {
    array_dtype_to_nd(dtype).map(Into::into)
}

/// The Arrow type of an atlas attribute dtype, or `None` for a list.
pub fn attr_dtype_to_arrow(dtype: &DType) -> Option<DataType> {
    attr_dtype_to_nd(dtype).map(Into::into)
}

/// A stable tag for a dtype, for keys that group datasets by shape.
pub(crate) fn dtype_tag(dtype: &DType) -> String {
    format!("{dtype:?}")
}

// ─── Collection schema ───────────────────────────────────────────────────────

/// The Arrow schema of one collection, from its footer alone.
///
/// One nullable field per array, under the array's own name. A dataset
/// attribute becomes `.{attr}`, and an array attribute `{array}.{attr}`. A
/// name that two datasets type differently takes the type `widening` gives
/// the set, with the conflict mark it applies. A dtype Beacon cannot read is
/// dropped with a `debug` log.
///
/// Every dataset in the container counts, deleted ones too. A column only a
/// deleted dataset declares reads as null. `read_dimensions` does not narrow
/// this schema: the footer holds no dimension name.
///
/// The fields are sorted by name. Atlas permits an array named `.season` or
/// `temperature.units`, so one column name can come from two maps. Their types
/// then merge as one column.
pub fn collection_arrow_schema(
    schema: &CollectionSchema<'_>,
    widening: &ArrowTypeWidening,
) -> Result<Schema, ArrowError> {
    let mut columns: BTreeMap<String, Vec<DataType>> = BTreeMap::new();
    for (array, dtypes) in &schema.arrays {
        let types = readable_types(array, dtypes, array_dtype_to_arrow);
        columns.entry(array.to_string()).or_default().extend(types);
    }
    for (key, dtypes) in &schema.attributes {
        let column = global_attr_column(key);
        let types = readable_types(&column, dtypes, attr_dtype_to_arrow);
        columns.entry(column).or_default().extend(types);
    }
    for (array, attributes) in &schema.array_attributes {
        for (key, dtypes) in attributes {
            let column = array_attr_column(array, key);
            let types = readable_types(&column, dtypes, attr_dtype_to_arrow);
            columns.entry(column).or_default().extend(types);
        }
    }

    let mut fields = Vec::with_capacity(columns.len());
    for (name, types) in &columns {
        if types.is_empty() {
            continue;
        }
        fields.push(merge_types(widening, name, types)?);
    }
    Ok(Schema::new(fields))
}

/// The Arrow types of `dtypes` that Beacon can read as column `column`.
///
/// A dtype `to_arrow` refuses is logged at `debug` and dropped. A collection
/// can hold a million datasets, so a `warn` per skip would be a flood.
fn readable_types(
    column: &str,
    dtypes: &[&DType],
    to_arrow: fn(&DType) -> Option<DataType>,
) -> Vec<DataType> {
    dtypes
        .iter()
        .filter_map(|dtype| {
            let data_type = to_arrow(dtype);
            if data_type.is_none() {
                tracing::debug!(column, ?dtype, "no column for this atlas dtype, skipped");
            }
            data_type
        })
        .collect()
}

/// The field column `name` takes when its sources state `types`.
///
/// One nullable single-field schema per type, merged under the session rule.
/// That is [`ArrowTypeWidening::merge_schemas`] for one column: the same
/// widening, the same conflict setting, and the same conflict mark, which the
/// scan reads to cast a source the type cannot hold as null.
fn merge_types(
    widening: &ArrowTypeWidening,
    name: &str,
    types: &[DataType],
) -> Result<Field, ArrowError> {
    if let [only] = types {
        return Ok(Field::new(name, only.clone(), true));
    }
    let schemas: Vec<LabeledSchema> = types
        .iter()
        .map(|data_type| {
            let field = Field::new(name, data_type.clone(), true);
            LabeledSchema::unlabeled(Arc::new(Schema::new(vec![field])))
        })
        .collect();
    let merged = widening.merge_schemas(&schemas)?;
    Ok(merged.field(0).clone())
}

// ─── Lazy arrays ─────────────────────────────────────────────────────────────

/// Wrap one atlas array as a lazy [`NdArrayD`] over `view`.
///
/// No array data is read here. `dtype` comes from the collection footer, and
/// `layout` from the variable's segment, which one open serves for the whole
/// collection. The values themselves arrive when the engine asks the backend
/// for a subset.
///
/// The chunk shape is the one the writer chose. It is what lets the scan cut a
/// dataset on the grid the file actually stores, so one unit of work is one
/// stored chunk.
pub fn array_to_nd_array(
    view: Arc<DatasetView>,
    array_name: &str,
    dtype: &DType,
    layout: &ArrayLayout,
) -> anyhow::Result<Arc<dyn NdArrayD>> {
    let fill: Option<FillValue> = layout.fill_value().cloned();

    macro_rules! lazy {
        ($ty:ty) => {{
            let fill = fill
                .as_ref()
                .map(|value| <$ty as AtlasElement>::fill_element(Some(value)));
            let backend = AtlasArrayBackend::<$ty>::new(
                view,
                array_name.to_string(),
                layout.shape().to_vec(),
                layout
                    .dimension_names()
                    .into_iter()
                    .map(str::to_string)
                    .collect(),
                layout.chunk_shape().to_vec(),
                fill,
            );
            Ok(Arc::new(NdArray::new_with_backend(backend)?) as Arc<dyn NdArrayD>)
        }};
    }

    match dtype {
        DType::Int8 => lazy!(i8),
        DType::Int16 => lazy!(i16),
        DType::Int32 => lazy!(i32),
        DType::Int64 => lazy!(i64),
        DType::UInt8 => lazy!(u8),
        DType::UInt16 => lazy!(u16),
        DType::UInt32 => lazy!(u32),
        DType::UInt64 => lazy!(u64),
        DType::Float32 => lazy!(f32),
        DType::Float64 => lazy!(f64),
        DType::String => lazy!(String),
        DType::Binary => lazy!(Vec<u8>),
        DType::TimestampNs => lazy!(TimestampNanosecond),
        DType::Bool => Err(anyhow::anyhow!(
            "array '{array_name}' is Bool, which atlas stores no elements of"
        )),
        DType::FixedSizeList { .. } => Err(anyhow::anyhow!(
            "array '{array_name}' is a FixedSizeList, which Beacon does not model"
        )),
        DType::List { .. } => Err(anyhow::anyhow!(
            "array '{array_name}' is a List, which Beacon does not model"
        )),
    }
}

/// Wrap one scalar attribute value as a rank-0 [`NdArrayD`].
///
/// A rank-0 array broadcasts onto whatever grid the dataset's own arrays
/// define, so the value repeats across every row the dataset contributes.
/// A list-valued attribute has no such analogue and is refused.
pub fn attribute_to_nd_array(attr: &Attr) -> anyhow::Result<Arc<dyn NdArrayD>> {
    macro_rules! scalar {
        ($value:expr) => {
            Ok(
                Arc::new(NdArray::new_with_backend(AttributeBackend::new($value))?)
                    as Arc<dyn NdArrayD>,
            )
        };
    }

    match attr {
        Attr::Bool(v) => scalar!(*v),
        Attr::Int8(v) => scalar!(*v),
        Attr::Int16(v) => scalar!(*v),
        Attr::Int32(v) => scalar!(*v),
        Attr::Int64(v) => scalar!(*v),
        Attr::UInt8(v) => scalar!(*v),
        Attr::UInt16(v) => scalar!(*v),
        Attr::UInt32(v) => scalar!(*v),
        Attr::UInt64(v) => scalar!(*v),
        Attr::Float32(v) => scalar!(*v),
        Attr::Float64(v) => scalar!(*v),
        Attr::String(v) => scalar!(v.clone()),
        Attr::Binary(v) => scalar!(v.clone()),
        other => Err(anyhow::anyhow!(
            "attribute is a {} list, which has no rank-0 form in Beacon",
            dtype_tag(&other.dtype())
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use beacon_nd_array::NdArray;

    // ── column names ────────────────────────────────────────────────────

    #[test]
    fn an_attribute_takes_its_owners_name() {
        assert_eq!(array_attr_column("sst", "units"), "sst.units");
        assert_eq!(global_attr_column("Conventions"), ".Conventions");
    }

    #[test]
    fn an_attribute_column_is_recognized_by_its_array() {
        assert!(is_attr_column_of("sst.units", "sst"));
        assert!(
            !is_attr_column_of("sst", "sst"),
            "the array itself is not one"
        );
        assert!(
            !is_attr_column_of("sst_flag.units", "sst"),
            "a prefix is not a name"
        );
        assert!(!is_attr_column_of("sst.", "sst"), "an empty key is no key");
    }

    // ── element types ───────────────────────────────────────────────────

    #[test]
    fn every_readable_array_dtype_maps() {
        let cases = [
            (DType::Int8, NdArrayDataType::I8),
            (DType::Int16, NdArrayDataType::I16),
            (DType::Int32, NdArrayDataType::I32),
            (DType::Int64, NdArrayDataType::I64),
            (DType::UInt8, NdArrayDataType::U8),
            (DType::UInt16, NdArrayDataType::U16),
            (DType::UInt32, NdArrayDataType::U32),
            (DType::UInt64, NdArrayDataType::U64),
            (DType::Float32, NdArrayDataType::F32),
            (DType::Float64, NdArrayDataType::F64),
            (DType::String, NdArrayDataType::String),
            (DType::Binary, NdArrayDataType::Binary),
            (DType::TimestampNs, NdArrayDataType::Timestamp),
        ];
        for (dtype, expected) in cases {
            assert_eq!(array_dtype_to_nd(&dtype), Some(expected), "{dtype:?}");
        }
    }

    /// `array-format` implements no element type for `bool`, so a `Bool` array
    /// cannot be read even though the dtype exists. An attribute can.
    #[test]
    fn a_bool_array_is_refused_but_a_bool_attribute_is_not() {
        assert_eq!(array_dtype_to_nd(&DType::Bool), None);
        assert_eq!(attr_dtype_to_nd(&DType::Bool), Some(NdArrayDataType::Bool));
    }

    #[test]
    fn list_dtypes_have_no_column() {
        let list = DType::List {
            child: Box::new(DType::Int32),
        };
        let fixed = DType::FixedSizeList {
            child: Box::new(DType::Float32),
            size: 3,
        };
        for dtype in [list, fixed] {
            assert_eq!(array_dtype_to_nd(&dtype), None, "{dtype:?}");
            assert_eq!(attr_dtype_to_nd(&dtype), None, "{dtype:?}");
        }
    }

    /// The Arrow type follows the ND type, so a schema and a batch agree.
    #[test]
    fn the_arrow_type_follows_the_nd_type() {
        assert_eq!(
            array_dtype_to_arrow(&DType::Float64),
            Some(DataType::Float64)
        );
        assert_eq!(array_dtype_to_arrow(&DType::String), Some(DataType::Utf8));
        assert_eq!(
            array_dtype_to_arrow(&DType::TimestampNs),
            Some(DataType::Timestamp(
                arrow::datatypes::TimeUnit::Nanosecond,
                None
            ))
        );
        assert_eq!(array_dtype_to_arrow(&DType::Bool), None);
    }

    // ── attribute values ────────────────────────────────────────────────

    #[tokio::test]
    async fn a_scalar_attribute_is_a_rank_zero_column() {
        let nd = attribute_to_nd_array(&Attr::Int64(2024)).unwrap();
        assert_eq!(nd.datatype(), NdArrayDataType::I64);
        assert!(nd.shape().is_empty(), "an attribute has no axis");
        let typed = nd.as_any().downcast_ref::<NdArray<i64>>().unwrap();
        assert_eq!(typed.clone_into_raw_vec().await, vec![2024]);
    }

    #[tokio::test]
    async fn a_bool_attribute_is_a_column() {
        let nd = attribute_to_nd_array(&Attr::Bool(true)).unwrap();
        assert_eq!(nd.datatype(), NdArrayDataType::Bool);
    }

    #[test]
    fn a_list_attribute_is_refused_by_name() {
        let error = attribute_to_nd_array(&Attr::Int32List(vec![1, 2, 3]))
            .expect_err("a list has no rank-0 form")
            .to_string();
        assert!(error.contains("list"), "{error}");
    }

    // ── the collection schema ───────────────────────────────────────────

    use crate::test_support;
    use arrow::datatypes::TimeUnit;
    use beacon_datafusion_ext::type_widening::{DefaultArrowTypeWidening, is_type_conflict};

    fn widening() -> Arc<ArrowTypeWidening> {
        ArrowTypeWidening::default_extension()
    }

    fn names(schema: &Schema) -> Vec<&str> {
        schema.fields().iter().map(|f| f.name().as_str()).collect()
    }

    /// Every array and every attribute of every dataset is a column, in name
    /// order. The footer's maps have no order of their own.
    #[tokio::test]
    async fn a_collections_schema_is_the_union_of_its_datasets_in_name_order() {
        let tmp = tempfile::tempdir().unwrap();
        test_support::two_datasets(tmp.path()).await;
        let atlas = test_support::open(tmp.path()).await;

        let schema =
            collection_arrow_schema(&atlas.footer().collection_schema(), &widening()).unwrap();

        assert_eq!(
            names(&schema),
            vec![
                ".season",
                ".year",
                "cycle",
                "temperature",
                "temperature.units",
                "time"
            ]
        );
    }

    #[tokio::test]
    async fn every_column_keeps_the_type_the_footer_gave_it() {
        let tmp = tempfile::tempdir().unwrap();
        test_support::two_datasets(tmp.path()).await;
        let atlas = test_support::open(tmp.path()).await;

        let schema =
            collection_arrow_schema(&atlas.footer().collection_schema(), &widening()).unwrap();
        let field = |name: &str| schema.field_with_name(name).unwrap();

        assert_eq!(field("temperature").data_type(), &DataType::Float32);
        assert_eq!(field("cycle").data_type(), &DataType::Int32);
        assert_eq!(
            field("time").data_type(),
            &DataType::Timestamp(TimeUnit::Nanosecond, None)
        );
        assert_eq!(field(".year").data_type(), &DataType::Int64);
        assert_eq!(field(".season").data_type(), &DataType::Utf8);
        assert_eq!(field("temperature.units").data_type(), &DataType::Utf8);
        assert!(
            schema.fields().iter().all(|f| f.is_nullable()),
            "a dataset may lack any column, so every column is nullable"
        );
    }

    /// Two datasets that give one array two numeric types merge to the type
    /// that holds both, by the rule of the session rather than one of atlas's
    /// own. `Int16` beside `Float32` gives `Float64`. See issue #377.
    #[tokio::test]
    async fn a_shared_array_widens_to_a_type_that_holds_both() {
        let tmp = tempfile::tempdir().unwrap();
        test_support::widening(tmp.path()).await;
        let atlas = test_support::open(tmp.path()).await;

        let schema =
            collection_arrow_schema(&atlas.footer().collection_schema(), &widening()).unwrap();

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
    /// names the column and both types. The footer's type set names no
    /// dataset, so the error cannot.
    #[tokio::test]
    async fn types_that_do_not_widen_are_refused_by_name() {
        let tmp = tempfile::tempdir().unwrap();
        test_support::incompatible(tmp.path()).await;
        let atlas = test_support::open(tmp.path()).await;

        let error = collection_arrow_schema(&atlas.footer().collection_schema(), &widening())
            .expect_err("Utf8 and Int64 are two families")
            .to_string();

        assert!(error.contains("value"), "the column: {error}");
        assert!(
            error.contains("Utf8") && error.contains("Int64"),
            "both types: {error}"
        );
    }

    /// A deployment that reads such a collection anyway sets `keep_first`. The
    /// column then takes the type the footer states first, which is the type
    /// of the dataset written first, and carries the mark the scan reads.
    #[tokio::test]
    async fn keep_first_settles_a_conflict_with_the_first_type_and_marks_it() {
        let tmp = tempfile::tempdir().unwrap();
        test_support::incompatible(tmp.path()).await;
        let atlas = test_support::open(tmp.path()).await;
        let keep_first =
            ArrowTypeWidening::new(Arc::new(DefaultArrowTypeWidening::keeping_first_type()));

        let schema =
            collection_arrow_schema(&atlas.footer().collection_schema(), &keep_first).unwrap();
        let value = schema.field_with_name("value").unwrap();

        assert_eq!(value.data_type(), &DataType::Utf8, "dataset `a` came first");
        assert!(is_type_conflict(value), "the scan must cast `b` to null");
        assert_eq!(
            schema.field_with_name("only_a").unwrap().data_type(),
            &DataType::Int32
        );
    }

    #[tokio::test]
    async fn a_list_attribute_is_dropped_and_the_rest_survives() {
        let tmp = tempfile::tempdir().unwrap();
        test_support::skips(tmp.path()).await;
        let atlas = test_support::open(tmp.path()).await;

        let schema =
            collection_arrow_schema(&atlas.footer().collection_schema(), &widening()).unwrap();

        assert_eq!(names(&schema), vec![".title", "value", "value.units"]);
    }

    #[tokio::test]
    async fn an_empty_collection_has_no_column() {
        let tmp = tempfile::tempdir().unwrap();
        test_support::empty(tmp.path()).await;
        let atlas = test_support::open(tmp.path()).await;

        let schema =
            collection_arrow_schema(&atlas.footer().collection_schema(), &widening()).unwrap();

        assert!(schema.fields().is_empty(), "{:?}", names(&schema));
    }

    /// The footer reports every dataset the container holds. A deleted one
    /// still shapes the schema: its columns stay, and read as null.
    #[tokio::test]
    async fn a_deleted_dataset_still_shapes_the_schema() {
        let tmp = tempfile::tempdir().unwrap();
        test_support::two_datasets(tmp.path()).await;
        let atlas = test_support::open(tmp.path()).await;
        atlas.delete_dataset("winter").await.unwrap();
        let atlas = test_support::open(tmp.path()).await;

        let schema =
            collection_arrow_schema(&atlas.footer().collection_schema(), &widening()).unwrap();

        assert!(
            schema.field_with_name("cycle").is_ok(),
            "only `winter` declares `cycle`: {:?}",
            names(&schema)
        );
    }
}
