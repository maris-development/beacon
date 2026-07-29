//! Conversion between atlas arrays/attributes and `beacon-nd-array` types.

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use atlas::{ArraySchema, Attr, DType, FillValue, MergedSchema};
use beacon_nd_array::{NdArray, NdArrayD, datatypes::TimestampNanosecond};

use crate::backend::{AtlasArrayBackend, AtlasReadable, AttributeBackend};

/// Arrow type for a scalar atlas [`DType`], or `None` for the list dtypes
/// (`List`, `FixedSizeList`) that have no rank-0 / column analogue in Beacon.
///
/// Kept in lock-step with the [`NdArrayType`](beacon_nd_array::datatypes::NdArrayType)
/// → Arrow mapping the read path produces, so a schema derived here and a batch
/// produced by the scan carry matching types.
fn scalar_dtype_to_arrow(dtype: &DType) -> Option<DataType> {
    Some(match dtype {
        DType::Bool => DataType::Boolean,
        DType::Int8 => DataType::Int8,
        DType::Int16 => DataType::Int16,
        DType::Int32 => DataType::Int32,
        DType::Int64 => DataType::Int64,
        DType::UInt8 => DataType::UInt8,
        DType::UInt16 => DataType::UInt16,
        DType::UInt32 => DataType::UInt32,
        DType::UInt64 => DataType::UInt64,
        DType::Float32 => DataType::Float32,
        DType::Float64 => DataType::Float64,
        DType::String => DataType::Utf8,
        DType::Binary => DataType::Binary,
        DType::TimestampNs => DataType::Timestamp(TimeUnit::Nanosecond, None),
        DType::List { .. } | DType::FixedSizeList { .. } => return None,
    })
}

/// Arrow type for an atlas **array** dtype, or `None` for dtypes Beacon can't
/// read as a column. `Bool` is excluded here (atlas's `ArrayElement` isn't
/// implemented for `bool`), matching [`array_to_nd_array`]'s rejection.
pub fn atlas_array_dtype_to_arrow(dtype: &DType) -> Option<DataType> {
    match dtype {
        DType::Bool => None,
        other => scalar_dtype_to_arrow(other),
    }
}

/// Arrow type for an atlas **attribute** dtype, or `None` for list-valued
/// attributes (no rank-0 analogue). Scalar `Bool` attributes *are* supported.
pub fn atlas_attr_dtype_to_arrow(dtype: &DType) -> Option<DataType> {
    scalar_dtype_to_arrow(dtype)
}

/// Build the Arrow schema for a whole atlas store from its collection-wide
/// [`MergedSchema`] — **no per-dataset iteration and no disk I/O**.
///
/// The merged schema already widens every array/attribute dtype across all
/// datasets (the same union `super_type_schema` would compute), so this is the
/// scale-friendly replacement for opening and typing each dataset in turn.
///
/// Columns mirror the reader's naming: arrays by name, per-array attributes as
/// `{array}.{attr}`, dataset-level attributes by their bare key. Fields are
/// sorted by name so the layout is stable (adaptation is by name, so order is
/// cosmetic). When `read_dimensions` is `Some`, only arrays whose dimensions are
/// a subset survive; rank-0 attributes (empty dimensions) always survive, so
/// per-array attributes are emitted regardless of their array's dimensionality —
/// matching what the scan produces after `resolve_read_dimensions`.
pub fn atlas_merged_schema_to_arrow(
    merged: &MergedSchema,
    read_dimensions: Option<&[String]>,
) -> Schema {
    let mut fields: Vec<Field> = Vec::new();

    for (name, arr) in &merged.arrays {
        let dims_ok = read_dimensions
            .map_or(true, |dims| arr.dimension_names.iter().all(|d| dims.contains(d)));
        if dims_ok
            && let Some(dt) = atlas_array_dtype_to_arrow(&arr.dtype.0)
        {
            fields.push(Field::new(name, dt, true));
        }
        for (attr, ty) in &arr.attributes {
            if let Some(dt) = atlas_attr_dtype_to_arrow(&ty.0) {
                fields.push(Field::new(format!("{name}.{attr}"), dt, true));
            }
        }
    }

    for (key, ty) in &merged.global_attributes {
        if let Some(dt) = atlas_attr_dtype_to_arrow(&ty.0) {
            fields.push(Field::new(key, dt, true));
        }
    }

    fields.sort_by(|a, b| a.name().cmp(b.name()));
    Schema::new(fields)
}

/// Convert an atlas array (described by its [`ArraySchema`]) into a lazy
/// [`NdArrayD`] backed by [`AtlasArrayBackend`].
///
/// `Bool`, `FixedSizeList` and `List` dtypes are rejected with an explicit
/// error — atlas's `ArrayElement` isn't implemented for `bool`, and Beacon's
/// ND array model has no analogue for list dtypes. Silently skipping them would
/// propagate dimension mismatches, so the caller (the reader) `warn!`-skips the
/// column instead.
///
/// `fill_value` comes from
/// [`DatasetView::array_fill_value`](atlas::DatasetView::array_fill_value) and
/// is converted to the per-dtype `T` via [`AtlasReadable::fill_element`].
pub fn array_to_nd_array(
    atlas: Arc<atlas::Atlas>,
    dataset_name: &str,
    array_name: &str,
    schema: &ArraySchema,
    fill_value: Option<FillValue>,
) -> anyhow::Result<Arc<dyn NdArrayD>> {
    let shape = schema.shape.clone();
    let dimensions = schema.dimension_names.clone();
    let chunk_shape = schema.chunk_shape.clone();

    macro_rules! mk {
        ($ty:ty) => {{
            let fill: Option<$ty> = fill_value
                .as_ref()
                .map(|fv| <$ty as AtlasReadable>::fill_element(Some(fv)));
            let backend = AtlasArrayBackend::<$ty>::new(
                atlas.clone(),
                dataset_name.to_string(),
                array_name.to_string(),
                shape.clone(),
                dimensions.clone(),
                chunk_shape.clone(),
                fill,
            );
            let nd = NdArray::new_with_backend(backend)?;
            Ok::<Arc<dyn NdArrayD>, anyhow::Error>(Arc::new(nd))
        }};
    }

    match &schema.dtype {
        DType::Bool => Err(anyhow::anyhow!(
            "Atlas array '{}' has dtype Bool which is not readable through Beacon \
             (atlas's ArrayElement does not implement bool)",
            array_name
        )),
        DType::Int8 => mk!(i8),
        DType::Int16 => mk!(i16),
        DType::Int32 => mk!(i32),
        DType::Int64 => mk!(i64),
        DType::UInt8 => mk!(u8),
        DType::UInt16 => mk!(u16),
        DType::UInt32 => mk!(u32),
        DType::UInt64 => mk!(u64),
        DType::Float32 => mk!(f32),
        DType::Float64 => mk!(f64),
        DType::String => mk!(String),
        DType::Binary => mk!(Vec<u8>),
        DType::TimestampNs => mk!(TimestampNanosecond),
        DType::FixedSizeList { .. } => Err(anyhow::anyhow!(
            "Atlas array '{}' has unsupported dtype FixedSizeList — Beacon does not model \
             fixed-size lists",
            array_name
        )),
        DType::List { .. } => Err(anyhow::anyhow!(
            "Atlas array '{}' has unsupported dtype List — Beacon does not model \
             variable-length lists",
            array_name
        )),
    }
}

/// Convert a scalar atlas attribute value into a rank-0 [`NdArrayD`].
///
/// List-valued attributes have no rank-0 scalar analogue in Beacon's ND array
/// model and are rejected with an error (the caller `warn!`-skips them).
pub fn attribute_to_nd_array(attr: &Attr) -> anyhow::Result<Arc<dyn NdArrayD>> {
    macro_rules! scalar {
        ($value:expr) => {
            Ok(Arc::new(NdArray::new_with_backend(AttributeBackend::new(
                $value,
            ))?) as Arc<dyn NdArrayD>)
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
        Attr::TimestampNanoseconds(v) => scalar!(TimestampNanosecond(*v)),
        Attr::BoolList(_)
        | Attr::Int8List(_)
        | Attr::Int16List(_)
        | Attr::Int32List(_)
        | Attr::Int64List(_)
        | Attr::UInt8List(_)
        | Attr::UInt16List(_)
        | Attr::UInt32List(_)
        | Attr::UInt64List(_)
        | Attr::Float32List(_)
        | Attr::Float64List(_)
        | Attr::StringList(_)
        | Attr::BinaryList(_) => Err(anyhow::anyhow!(
            "list-valued attributes are not representable as Beacon rank-0 arrays"
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use atlas::{Atlas, Codec, DType, StoreConfig};
    use beacon_nd_array::{NdArray, datatypes::NdArrayDataType};

    fn schema_with_dtype(dtype: DType) -> ArraySchema {
        ArraySchema {
            dtype,
            shape: vec![2],
            chunk_shape: vec![2],
            dimension_names: vec!["x".into()],
            codec: Codec::default(),
        }
    }

    async fn dummy_atlas() -> Arc<Atlas> {
        // The rejection branches return before touching the atlas handle, so we
        // need a value but never read from it.
        let tmp = tempfile::tempdir().expect("temp dir");
        let atlas = Atlas::create_path(tmp.path(), StoreConfig::default())
            .await
            .expect("create dummy atlas");
        std::mem::forget(tmp);
        Arc::new(atlas)
    }

    #[tokio::test]
    async fn array_to_nd_array_rejects_bool() {
        let atlas = dummy_atlas().await;
        let err = array_to_nd_array(atlas, "ds", "flag", &schema_with_dtype(DType::Bool), None)
            .expect_err("Bool should be rejected");
        let msg = format!("{err:#}");
        assert!(msg.contains("Bool"), "{msg}");
        assert!(msg.contains("flag"), "{msg}");
    }

    #[tokio::test]
    async fn array_to_nd_array_rejects_list() {
        let atlas = dummy_atlas().await;
        let err = array_to_nd_array(
            atlas,
            "ds",
            "events",
            &schema_with_dtype(DType::List {
                child: Box::new(DType::Int32),
            }),
            None,
        )
        .expect_err("List should be rejected");
        let msg = format!("{err:#}");
        assert!(msg.contains("List"), "{msg}");
        assert!(msg.contains("events"), "{msg}");
    }

    #[tokio::test]
    async fn attribute_bool_round_trips() {
        let nd = attribute_to_nd_array(&Attr::Bool(true)).expect("convert");
        assert_eq!(nd.datatype(), NdArrayDataType::Bool);
        assert!(nd.shape().is_empty());
        let typed = nd
            .as_any()
            .downcast_ref::<NdArray<bool>>()
            .expect("downcast");
        assert_eq!(typed.clone_into_raw_vec().await, vec![true]);
    }

    #[tokio::test]
    async fn attribute_int64_round_trips() {
        let nd = attribute_to_nd_array(&Attr::Int64(42)).expect("convert");
        assert_eq!(nd.datatype(), NdArrayDataType::I64);
        let typed = nd
            .as_any()
            .downcast_ref::<NdArray<i64>>()
            .expect("downcast");
        assert_eq!(typed.clone_into_raw_vec().await, vec![42i64]);
    }

    #[tokio::test]
    async fn attribute_string_round_trips() {
        let nd = attribute_to_nd_array(&Attr::String("winter".into())).expect("convert");
        assert_eq!(nd.datatype(), NdArrayDataType::String);
        let typed = nd
            .as_any()
            .downcast_ref::<NdArray<String>>()
            .expect("downcast");
        assert_eq!(typed.clone_into_raw_vec().await, vec!["winter".to_string()]);
    }

    #[tokio::test]
    async fn attribute_timestamp_round_trips() {
        let nanos = 1_700_000_000_000_000_000i64;
        let nd = attribute_to_nd_array(&Attr::TimestampNanoseconds(nanos)).expect("convert");
        assert_eq!(nd.datatype(), NdArrayDataType::Timestamp);
        let typed = nd
            .as_any()
            .downcast_ref::<NdArray<TimestampNanosecond>>()
            .expect("downcast");
        assert_eq!(
            typed.clone_into_raw_vec().await,
            vec![TimestampNanosecond(nanos)]
        );
    }

    #[tokio::test]
    async fn attribute_list_rejected() {
        let err = attribute_to_nd_array(&Attr::Int32List(vec![1, 2, 3]))
            .expect_err("list attribute should be rejected");
        assert!(format!("{err:#}").contains("list-valued"));
    }
}
