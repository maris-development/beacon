//! The mapping between an Atlas collection and Beacon's ND array model: column
//! names, element types, and the Arrow schema of a whole collection.
//!
//! One mapping, in one place, so a schema and a scanned batch never disagree.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema};
use arrow::error::ArrowError;
use atlas::{CollectionSchema, DType};
use beacon_datafusion_ext::type_widening::{ArrowTypeWidening, LabeledSchema};
use beacon_nd_array::datatypes::NdArrayDataType;

// ─── Column names ────────────────────────────────────────────────────────────

/// The column a per-array attribute is surfaced under: `{array}.{attr}`.
pub fn array_attr_column(array: &str, attr: &str) -> String {
    format!("{array}.{attr}")
}

/// The column a dataset-level attribute is surfaced under: `.{attr}`.
///
/// The leading dot follows netCDF and Zarr, and avoids colliding with an array name.
pub fn global_attr_column(attr: &str) -> String {
    format!(".{attr}")
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

/// The ND type of an atlas **array** dtype, or `None` for one Beacon cannot read as a column.
///
/// Excludes `Bool`: `array-format` has no element type for it, so no reader can produce values.
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
/// One nullable field per array and attribute, typed per `widening`. Includes deleted datasets.
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
/// Logs a refused dtype at `debug` and drops it, to avoid a flood of warnings.
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
/// Merges the types via [`ArrowTypeWidening::merge_schemas`], same conflict rule as the scan.
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

#[cfg(test)]
mod tests {
    use super::*;

    // ── column names ────────────────────────────────────────────────────

    #[test]
    fn an_attribute_takes_its_owners_name() {
        assert_eq!(array_attr_column("sst", "units"), "sst.units");
        assert_eq!(global_attr_column("Conventions"), ".Conventions");
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

    // ── the collection schema ───────────────────────────────────────────

    use crate::test_support;
    use arrow::datatypes::TimeUnit;
    use beacon_datafusion_ext::type_widening::DefaultArrowTypeWidening;

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
    /// of the dataset written first, and the other dataset reads null.
    #[tokio::test]
    async fn keep_first_settles_a_conflict_with_the_first_type() {
        let tmp = tempfile::tempdir().unwrap();
        test_support::incompatible(tmp.path()).await;
        let atlas = test_support::open(tmp.path()).await;
        let keep_first =
            ArrowTypeWidening::new(Arc::new(DefaultArrowTypeWidening::keeping_first_type()));

        let schema =
            collection_arrow_schema(&atlas.footer().collection_schema(), &keep_first).unwrap();
        let value = schema.field_with_name("value").unwrap();

        assert_eq!(value.data_type(), &DataType::Utf8, "dataset `a` came first");
        assert!(value.is_nullable(), "dataset `b` reads null");
        assert!(
            keep_first
                .strategy
                .casts_leniently(&DataType::Int64, &DataType::Utf8),
            "the scan casts `b` to null"
        );
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
