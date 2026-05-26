//! Array backend implementations used by the Atlas reader.

use std::sync::Arc;

use beacon_nd_array::{
    array::{backend::ArrayBackend, subset::ArraySubset},
    datatypes::{NdArrayType, TimestampNanosecond},
};
use ndarray::ArrayD;

/// Trait implemented for `T: NdArrayType` values that can be read from an
/// atlas `DatasetView` as a typed `ArrayD<T>`.
///
/// Atlas's `read_array::<E>` is generic over `array_format::ArrayElement`.
/// Most `NdArrayType` impls are also `ArrayElement` (numeric primitives,
/// `String`, `Vec<u8>`), but a few — notably
/// [`TimestampNanosecond`] — are layout-compatible newtypes that need a
/// thin element-wise conversion. This trait hides the difference behind
/// a single `read` entry point so [`AtlasArrayBackend`] stays generic.
#[async_trait::async_trait]
pub trait AtlasReadable: NdArrayType {
    async fn read(
        dataset: &atlas::DatasetView,
        array_name: &str,
        start: Vec<usize>,
        shape: Vec<usize>,
    ) -> anyhow::Result<ArrayD<Self>>;

    /// Convert an atlas `FillValue` into this type's per-element fill,
    /// using the same widening/sentinel rules `array_format` applies when
    /// materializing missing chunks.
    fn fill_element(fill: Option<&atlas::FillValue>) -> Self;
}

macro_rules! impl_atlas_readable_passthrough {
    ($ty:ty) => {
        #[async_trait::async_trait]
        impl AtlasReadable for $ty {
            async fn read(
                dataset: &atlas::DatasetView,
                array_name: &str,
                start: Vec<usize>,
                shape: Vec<usize>,
            ) -> anyhow::Result<ArrayD<Self>> {
                let arr = dataset
                    .read_array::<$ty>(array_name, start, shape)
                    .await
                    .map_err(|e| {
                        anyhow::anyhow!(
                            "Failed to read atlas array '{}': {}",
                            array_name,
                            e
                        )
                    })?
                    .ok_or_else(|| {
                        anyhow::anyhow!(
                            "Atlas array '{}' not found in dataset '{}'",
                            array_name,
                            dataset.name()
                        )
                    })?;
                Ok(arr.to_owned())
            }

            fn fill_element(fill: Option<&atlas::FillValue>) -> Self {
                <$ty as atlas::ArrayElement>::fill_element(fill)
            }
        }
    };
}

impl_atlas_readable_passthrough!(i8);
impl_atlas_readable_passthrough!(i16);
impl_atlas_readable_passthrough!(i32);
impl_atlas_readable_passthrough!(i64);
impl_atlas_readable_passthrough!(u8);
impl_atlas_readable_passthrough!(u16);
impl_atlas_readable_passthrough!(u32);
impl_atlas_readable_passthrough!(u64);
impl_atlas_readable_passthrough!(f32);
impl_atlas_readable_passthrough!(f64);
impl_atlas_readable_passthrough!(String);
impl_atlas_readable_passthrough!(Vec<u8>);

#[async_trait::async_trait]
impl AtlasReadable for TimestampNanosecond {
    async fn read(
        dataset: &atlas::DatasetView,
        array_name: &str,
        start: Vec<usize>,
        shape: Vec<usize>,
    ) -> anyhow::Result<ArrayD<Self>> {
        let arr = dataset
            .read_array::<atlas::TimestampNs>(array_name, start, shape)
            .await
            .map_err(|e| {
                anyhow::anyhow!(
                    "Failed to read atlas timestamp array '{}': {}",
                    array_name,
                    e
                )
            })?
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "Atlas array '{}' not found in dataset '{}'",
                    array_name,
                    dataset.name()
                )
            })?;
        // Map element-wise: TimestampNs(i64) -> TimestampNanosecond(i64).
        // Both are #[repr(transparent)] over i64.
        Ok(arr.to_owned().mapv(|ts| TimestampNanosecond(ts.0)))
    }

    fn fill_element(fill: Option<&atlas::FillValue>) -> Self {
        let ts = <atlas::TimestampNs as atlas::ArrayElement>::fill_element(fill);
        TimestampNanosecond(ts.0)
    }
}

/// Backend that reads atlas array data lazily.
pub struct AtlasArrayBackend<T: NdArrayType> {
    atlas: Arc<atlas::Atlas>,
    dataset_name: String,
    array_name: String,
    shape: Vec<usize>,
    dimensions: Vec<String>,
    chunk_shape: Vec<usize>,
    fill_value: Option<T>,
}

impl<T: NdArrayType> std::fmt::Debug for AtlasArrayBackend<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AtlasArrayBackend")
            .field("dataset_name", &self.dataset_name)
            .field("array_name", &self.array_name)
            .field("shape", &self.shape)
            .field("dimensions", &self.dimensions)
            .field("chunk_shape", &self.chunk_shape)
            .finish()
    }
}

impl<T: NdArrayType> AtlasArrayBackend<T> {
    pub fn new(
        atlas: Arc<atlas::Atlas>,
        dataset_name: String,
        array_name: String,
        shape: Vec<usize>,
        dimensions: Vec<String>,
        chunk_shape: Vec<usize>,
        fill_value: Option<T>,
    ) -> Self {
        Self {
            atlas,
            dataset_name,
            array_name,
            shape,
            dimensions,
            chunk_shape,
            fill_value,
        }
    }
}

#[async_trait::async_trait]
impl<T: NdArrayType + AtlasReadable> ArrayBackend<T> for AtlasArrayBackend<T> {
    fn len(&self) -> usize {
        self.shape.iter().product()
    }

    fn shape(&self) -> Vec<usize> {
        self.shape.clone()
    }

    fn dimensions(&self) -> Vec<String> {
        self.dimensions.clone()
    }

    fn chunk_shape(&self) -> Vec<usize> {
        self.chunk_shape.clone()
    }

    fn fill_value(&self) -> Option<T> {
        self.fill_value.clone()
    }

    async fn read_subset(&self, subset: ArraySubset) -> anyhow::Result<ArrayD<T>> {
        let view = self.atlas.open_dataset(&self.dataset_name).await.map_err(|e| {
            anyhow::anyhow!(
                "Failed to open atlas dataset '{}': {}",
                self.dataset_name,
                e
            )
        })?;
        T::read(&view, &self.array_name, subset.start, subset.shape).await
    }
}

/// Backend for scalar attribute values surfaced as rank-0 arrays.
#[derive(Debug)]
pub struct AttributeBackend<T: NdArrayType> {
    value: T,
}

impl<T: NdArrayType> AttributeBackend<T> {
    pub fn new(value: T) -> Self {
        Self { value }
    }
}

#[async_trait::async_trait]
impl<T: NdArrayType> ArrayBackend<T> for AttributeBackend<T> {
    fn len(&self) -> usize {
        1
    }

    fn shape(&self) -> Vec<usize> {
        vec![]
    }

    fn dimensions(&self) -> Vec<String> {
        vec![]
    }

    fn fill_value(&self) -> Option<T> {
        None
    }

    async fn read_subset(&self, _subset: ArraySubset) -> anyhow::Result<ArrayD<T>> {
        Ok(ndarray::arr0(self.value.clone()).into_dyn())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::reader::test_support::build_two_dataset_store;
    use atlas::Atlas;

    // ── AttributeBackend ───────────────────────────────────────────────

    #[tokio::test]
    async fn attribute_backend_is_rank_zero() {
        let backend = AttributeBackend::new("hello".to_string());
        assert_eq!(backend.len(), 1);
        assert!(ArrayBackend::<String>::shape(&backend).is_empty());
        assert!(ArrayBackend::<String>::dimensions(&backend).is_empty());
    }

    #[tokio::test]
    async fn attribute_backend_read_subset_returns_value() {
        let backend = AttributeBackend::new(42i32);
        let arr = backend
            .read_subset(ArraySubset {
                start: vec![],
                shape: vec![],
            })
            .await
            .expect("read");
        assert_eq!(arr.ndim(), 0);
        let raw = arr.into_raw_vec_and_offset().0;
        assert_eq!(raw, vec![42i32]);
    }

    // ── AtlasReadable::fill_element ────────────────────────────────────

    #[test]
    fn fill_element_passthrough_numeric() {
        use atlas::FillValue;
        assert_eq!(
            <i32 as AtlasReadable>::fill_element(Some(&FillValue::Int(-7))),
            -7i32
        );
        let nan = <f64 as AtlasReadable>::fill_element(Some(&FillValue::Float(f64::NAN)));
        assert!(nan.is_nan(), "NaN fill must round-trip as NaN");
        assert_eq!(<i32 as AtlasReadable>::fill_element(None), 0i32);
    }

    #[test]
    fn fill_element_timestamp_newtype_unwraps() {
        use atlas::FillValue;
        // The TimestampNanosecond impl goes through atlas::TimestampNs and
        // must end up wrapping the same i64.
        let ts = <TimestampNanosecond as AtlasReadable>::fill_element(Some(
            &FillValue::TimestampNs(123),
        ));
        assert_eq!(ts, TimestampNanosecond(123));
        let from_int =
            <TimestampNanosecond as AtlasReadable>::fill_element(Some(&FillValue::Int(456)));
        assert_eq!(from_int, TimestampNanosecond(456));
    }

    // ── AtlasArrayBackend ──────────────────────────────────────────────

    #[tokio::test]
    async fn atlas_array_backend_reports_metadata() {
        let backend = AtlasArrayBackend::<f32>::new(
            Arc::new(dummy_atlas().await),
            "winter".into(),
            "temperature".into(),
            vec![4],
            vec!["obs".into()],
            vec![4],
            Some(-1.0f32),
        );
        assert_eq!(<AtlasArrayBackend<f32> as ArrayBackend<f32>>::shape(&backend), vec![4]);
        assert_eq!(
            <AtlasArrayBackend<f32> as ArrayBackend<f32>>::dimensions(&backend),
            vec!["obs".to_string()]
        );
        assert_eq!(
            <AtlasArrayBackend<f32> as ArrayBackend<f32>>::chunk_shape(&backend),
            vec![4]
        );
        assert_eq!(
            <AtlasArrayBackend<f32> as ArrayBackend<f32>>::fill_value(&backend),
            Some(-1.0f32)
        );
        assert_eq!(backend.len(), 4);
    }

    #[tokio::test]
    async fn atlas_array_backend_read_subset_full_range() {
        let tmp = tempfile::tempdir().expect("temp dir");
        build_two_dataset_store(tmp.path()).await;
        let atlas = Atlas::open_path(tmp.path()).await.expect("open atlas");

        let backend = AtlasArrayBackend::<f32>::new(
            Arc::new(atlas),
            "winter".into(),
            "temperature".into(),
            vec![4],
            vec!["obs".into()],
            vec![4],
            None,
        );
        let arr = backend
            .read_subset(ArraySubset {
                start: vec![0],
                shape: vec![4],
            })
            .await
            .expect("read full");
        let raw = arr.into_raw_vec_and_offset().0;
        assert_eq!(raw, vec![1.0f32, 2.0, 3.0, 4.0]);
    }

    #[tokio::test]
    async fn atlas_array_backend_read_subset_partial_range() {
        let tmp = tempfile::tempdir().expect("temp dir");
        build_two_dataset_store(tmp.path()).await;
        let atlas = Atlas::open_path(tmp.path()).await.expect("open atlas");

        let backend = AtlasArrayBackend::<i32>::new(
            Arc::new(atlas),
            "winter".into(),
            "cycle".into(),
            vec![4],
            vec!["obs".into()],
            vec![4],
            None,
        );
        let arr = backend
            .read_subset(ArraySubset {
                start: vec![1],
                shape: vec![2],
            })
            .await
            .expect("read partial");
        let raw = arr.into_raw_vec_and_offset().0;
        assert_eq!(raw, vec![20i32, 30]);
    }

    /// Helper: build a fresh empty atlas store in a (leaked) temp dir for
    /// metadata-only tests that don't need the data to survive the test.
    async fn dummy_atlas() -> Atlas {
        let tmp = tempfile::tempdir().expect("temp dir");
        let atlas = Atlas::create_path(tmp.path(), atlas::StoreConfig::default())
            .await
            .expect("create dummy atlas");
        std::mem::forget(tmp);
        atlas
    }
}
