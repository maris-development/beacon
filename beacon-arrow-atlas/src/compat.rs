//! Compatibility helpers between atlas types and Beacon ND arrays.

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use atlas::{Attr, DType};
use beacon_nd_array::{NdArray, NdArrayD, datatypes::TimestampNanosecond};

use crate::backend::{AtlasArrayBackend, AtlasReadable, AttributeBackend};

/// Arrow data type for an atlas array dtype, or `None` for dtypes that
/// cannot be read through Beacon (`Bool`, `FixedSizeList`, `List`).
///
/// Kept in lock-step with [`array_to_nd_array`] so the two paths produce
/// matching field sets — every dtype that yields `Some(_)` here is one
/// that `array_to_nd_array` will build a backend for.
pub fn atlas_array_dtype_to_arrow(dtype: &DType) -> Option<DataType> {
    Some(match dtype {
        DType::Bool => return None,
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
        DType::FixedSizeList { .. } | DType::List { .. } => return None,
    })
}

/// Arrow data type for an atlas attribute value.
pub fn atlas_attr_to_arrow(attr: &Attr) -> DataType {
    match attr {
        Attr::Bool(_) => DataType::Boolean,
        Attr::Int64(_) => DataType::Int64,
        Attr::Float64(_) => DataType::Float64,
        Attr::String(_) => DataType::Utf8,
        Attr::TimestampNanoseconds(_) => DataType::Timestamp(TimeUnit::Nanosecond, None),
    }
}

/// Build the Arrow schema for an atlas dataset directly from its metadata
/// — no NdArray backends are constructed.
///
/// Fields are sorted alphabetically by name to match the post-`sort_keys`
/// ordering produced by [`crate::reader::dataset_from_atlas`], so the
/// indices returned by `SchemaAdapter::map_schema` line up with the
/// arrays/attributes a subsequent `dataset_from_atlas` call will include.
///
/// Arrays whose dtype is unsupported (see [`atlas_array_dtype_to_arrow`])
/// are silently skipped, mirroring the runtime behavior of
/// `dataset_from_atlas` which `warn!`-skips them.
///
/// When `read_dimensions` is `Some`, the result keeps only arrays whose
/// dimensions are a subset of the requested list (attributes are rank-0
/// and always survive). This mirrors
/// `Dataset::project_with_dimensions`: requested dimensions must exist
/// in the dataset, otherwise the function errors.
pub fn atlas_view_arrow_schema(
    view: &atlas::DatasetView,
    read_dimensions: Option<&[String]>,
) -> anyhow::Result<Schema> {
    let meta = view.meta();

    if let Some(requested) = read_dimensions {
        let available: std::collections::HashSet<&str> = meta
            .arrays
            .values()
            .flat_map(|s| s.dimension_names.iter().map(String::as_str))
            .collect();
        for dim in requested {
            if !available.contains(dim.as_str()) {
                anyhow::bail!(
                    "dimension '{dim}' not found in atlas dataset '{}'",
                    view.name()
                );
            }
        }
    }

    let mut fields: Vec<Field> = Vec::with_capacity(meta.arrays.len() + meta.attributes.len());

    for (name, schema) in &meta.arrays {
        let Some(dtype) = atlas_array_dtype_to_arrow(&schema.dtype) else {
            continue;
        };
        if let Some(requested) = read_dimensions {
            if !schema
                .dimension_names
                .iter()
                .all(|d| requested.iter().any(|r| r == d))
            {
                continue;
            }
        }
        fields.push(Field::new(name, dtype, true));
    }
    for (name, attr) in &meta.attributes {
        fields.push(Field::new(name, atlas_attr_to_arrow(attr), true));
    }

    fields.sort_by(|a, b| a.name().cmp(b.name()));
    Ok(Schema::new(fields))
}

/// Convert an atlas array (described by its [`atlas::ArraySchema`]) into a
/// lazy [`NdArrayD`] backed by [`AtlasArrayBackend`].
///
/// `FixedSizeList` and `List` dtypes are rejected with an explicit error —
/// they have no analogue in Beacon's ND array model and silently skipping
/// them would propagate dimension mismatches.
///
/// `fill_value` comes from [`atlas::DatasetView::array_fill_value`] and is
/// converted to the per-dtype `T` via [`AtlasReadable::fill_element`].
pub fn array_to_nd_array(
    atlas: Arc<atlas::Atlas>,
    dataset_name: &str,
    array_name: &str,
    schema: &atlas::ArraySchema,
    fill_value: Option<atlas::FillValue>,
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
            "Atlas array '{}' has dtype Bool which is not currently readable through Beacon \
             (atlas's ArrayElement does not yet impl bool)",
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
            "Atlas array '{}' has unsupported dtype FixedSizeList — Beacon does not currently model fixed-size lists",
            array_name
        )),
        DType::List { .. } => Err(anyhow::anyhow!(
            "Atlas array '{}' has unsupported dtype List — Beacon does not currently model variable-length lists",
            array_name
        )),
    }
}

/// Convert an atlas attribute value into a rank-0 ND array.
pub fn attribute_to_nd_array(_name: &str, attr: Attr) -> anyhow::Result<Arc<dyn NdArrayD>> {
    match attr {
        Attr::Bool(v) => Ok(Arc::new(NdArray::new_with_backend(AttributeBackend::new(v))?)),
        Attr::Int64(v) => Ok(Arc::new(NdArray::new_with_backend(AttributeBackend::new(v))?)),
        Attr::Float64(v) => Ok(Arc::new(NdArray::new_with_backend(AttributeBackend::new(v))?)),
        Attr::String(v) => Ok(Arc::new(NdArray::new_with_backend(AttributeBackend::new(v))?)),
        Attr::TimestampNanoseconds(v) => Ok(Arc::new(NdArray::new_with_backend(
            AttributeBackend::new(TimestampNanosecond(v)),
        )?)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use atlas::{ArraySchema, Codec, DType};
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

    // We can't actually open an atlas for the type-dispatch happy paths
    // without a real store; instead exercise the rejection branches and
    // the attribute conversion (which has no atlas dependency).

    #[test]
    fn array_to_nd_array_rejects_bool() {
        let dummy_atlas = dummy_atlas();
        let err = array_to_nd_array(
            dummy_atlas,
            "ds",
            "flag",
            &schema_with_dtype(DType::Bool),
            None,
        )
        .expect_err("Bool should be rejected");
        let msg = format!("{err:#}");
        assert!(msg.contains("Bool"), "{msg}");
        assert!(msg.contains("flag"), "{msg}");
    }

    #[test]
    fn array_to_nd_array_rejects_fixed_size_list() {
        let dummy_atlas = dummy_atlas();
        let err = array_to_nd_array(
            dummy_atlas,
            "ds",
            "vectors",
            &schema_with_dtype(DType::FixedSizeList {
                child: Box::new(DType::Float32),
                size: 4,
            }),
            None,
        )
        .expect_err("FixedSizeList should be rejected");
        let msg = format!("{err:#}");
        assert!(msg.contains("FixedSizeList"), "{msg}");
        assert!(msg.contains("vectors"), "{msg}");
    }

    #[test]
    fn array_to_nd_array_rejects_list() {
        let dummy_atlas = dummy_atlas();
        let err = array_to_nd_array(
            dummy_atlas,
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

    fn dummy_atlas() -> Arc<atlas::Atlas> {
        // The rejection branches return before touching the atlas handle,
        // so we need a value but never invoke it. Build one on a fresh
        // temp dir to keep the type system honest.
        let tmp = tempfile::tempdir().expect("temp dir");
        let rt = tokio::runtime::Runtime::new().expect("rt");
        let atlas = rt.block_on(async {
            atlas::Atlas::create_path(tmp.path(), atlas::StoreConfig::default())
                .await
                .expect("create dummy atlas")
        });
        // Leak the tempdir so it stays alive for the duration of the test;
        // the rejection paths never touch the filesystem so this is safe.
        std::mem::forget(tmp);
        Arc::new(atlas)
    }

    // ── attribute_to_nd_array ──────────────────────────────────────────

    #[tokio::test]
    async fn attribute_bool_round_trips() {
        let nd = attribute_to_nd_array("flag", Attr::Bool(true)).expect("convert");
        assert_eq!(nd.datatype(), NdArrayDataType::Bool);
        assert!(nd.shape().is_empty());
        let typed = nd.as_any().downcast_ref::<NdArray<bool>>().expect("downcast");
        assert_eq!(typed.clone_into_raw_vec().await, vec![true]);
    }

    #[tokio::test]
    async fn attribute_int64_round_trips() {
        let nd = attribute_to_nd_array("count", Attr::Int64(42)).expect("convert");
        assert_eq!(nd.datatype(), NdArrayDataType::I64);
        let typed = nd.as_any().downcast_ref::<NdArray<i64>>().expect("downcast");
        assert_eq!(typed.clone_into_raw_vec().await, vec![42i64]);
    }

    #[tokio::test]
    async fn attribute_float64_round_trips() {
        let nd = attribute_to_nd_array("scale", Attr::Float64(1.5)).expect("convert");
        assert_eq!(nd.datatype(), NdArrayDataType::F64);
        let typed = nd.as_any().downcast_ref::<NdArray<f64>>().expect("downcast");
        assert_eq!(typed.clone_into_raw_vec().await, vec![1.5f64]);
    }

    #[tokio::test]
    async fn attribute_string_round_trips() {
        let nd = attribute_to_nd_array("season", Attr::String("winter".into())).expect("convert");
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
        let nd = attribute_to_nd_array("ts", Attr::TimestampNanoseconds(nanos)).expect("convert");
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
}
