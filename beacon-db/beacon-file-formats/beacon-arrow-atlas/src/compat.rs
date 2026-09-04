//! The mapping between an Atlas collection and Beacon's ND array model: column
//! names, element types, and the lazy arrays themselves.
//!
//! One mapping, in one place. The Arrow type of a column follows from its
//! [`NdArrayDataType`] through `beacon-nd-array`'s own conversion, so a schema
//! derived here and a batch produced by a scan can never disagree.

use std::sync::Arc;

use arrow::datatypes::DataType;
use atlas::{ArrayLayout, Attr, DType, DatasetView, FillValue};
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
}
