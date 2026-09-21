//! The lazy arrays one dataset reads through.
//!
//! [`AtlasArrayBackend`] reads one dataset's entry of a segment on demand.
//! [`AttributeBackend`] holds one attribute value as a rank-0 array.

use std::sync::Arc;

use atlas::{ArrayFile, Attr, DType, FillValue};
use beacon_nd_array::{
    NdArray, NdArrayD,
    array::{backend::ArrayBackend, subset::ArraySubset},
    datatypes::{NdArrayType, TimestampNanosecond},
};
use ndarray::ArrayD;

use crate::schema::dtype_tag;

/// A Beacon element type that can be read out of an atlas array.
/// Converts atlas's element types to Beacon's, including the `TimestampNanosecond` newtype.
#[async_trait::async_trait]
pub trait AtlasElement: NdArrayType {
    /// Read `shape` elements of `dataset`'s entry in `segment` from `start`.
    async fn read(
        segment: &ArrayFile,
        dataset: &str,
        start: Vec<usize>,
        shape: Vec<usize>,
    ) -> anyhow::Result<ArrayD<Self>>;

    /// This type's form of an array's fill value.
    ///
    /// The engine nulls elements equal to it; must match what a read returns for an unwritten cell.
    fn fill_element(fill: Option<&FillValue>) -> Self;
}

macro_rules! passthrough {
    ($ty:ty) => {
        #[async_trait::async_trait]
        impl AtlasElement for $ty {
            async fn read(
                segment: &ArrayFile,
                dataset: &str,
                start: Vec<usize>,
                shape: Vec<usize>,
            ) -> anyhow::Result<ArrayD<Self>> {
                let values = segment
                    .read_array::<$ty>(dataset, start, shape)
                    .await
                    .map_err(|e| {
                        anyhow::anyhow!(
                            "Failed to read dataset '{dataset}' from its atlas segment: {e}"
                        )
                    })?;
                Ok(values.into_owned())
            }

            fn fill_element(fill: Option<&FillValue>) -> Self {
                <$ty as atlas::ArrayElement>::fill_element(fill)
            }
        }
    };
}

passthrough!(i8);
passthrough!(i16);
passthrough!(i32);
passthrough!(i64);
passthrough!(u8);
passthrough!(u16);
passthrough!(u32);
passthrough!(u64);
passthrough!(f32);
passthrough!(f64);
passthrough!(String);
passthrough!(Vec<u8>);

/// Both types are `#[repr(transparent)]` over `i64`, so this converts by renaming.
/// Done element by element; a whole-array transmute would rely on layout, not types.
#[async_trait::async_trait]
impl AtlasElement for TimestampNanosecond {
    async fn read(
        segment: &ArrayFile,
        dataset: &str,
        start: Vec<usize>,
        shape: Vec<usize>,
    ) -> anyhow::Result<ArrayD<Self>> {
        let values = segment
            .read_array::<atlas::TimestampNs>(dataset, start, shape)
            .await
            .map_err(|e| {
                anyhow::anyhow!(
                    "Failed to read the timestamps of dataset '{dataset}' from its atlas segment: {e}"
                )
            })?;
        Ok(values.into_owned().mapv(|ts| TimestampNanosecond(ts.0)))
    }

    fn fill_element(fill: Option<&FillValue>) -> Self {
        TimestampNanosecond(<atlas::TimestampNs as atlas::ArrayElement>::fill_element(fill).0)
    }
}

/// Reads one dataset's entry of an atlas segment lazily, one region at a time.
/// Holds the segment itself, not a [`DatasetView`](atlas::DatasetView), to avoid re-resolving it per read.
pub struct AtlasArrayBackend<T: NdArrayType> {
    segment: Arc<ArrayFile>,
    dataset: String,
    shape: Vec<usize>,
    dimensions: Vec<String>,
    chunk_shape: Vec<usize>,
    fill_value: Option<T>,
}

impl<T: NdArrayType> std::fmt::Debug for AtlasArrayBackend<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AtlasArrayBackend")
            .field("dataset", &self.dataset)
            .field("shape", &self.shape)
            .field("dimensions", &self.dimensions)
            .field("chunk_shape", &self.chunk_shape)
            .finish_non_exhaustive()
    }
}

impl<T: NdArrayType + AtlasElement> AtlasArrayBackend<T> {
    /// The backend for `dataset`'s entry in `segment`.
    ///
    /// Reads the layout from the segment at no I/O cost. Refuses an unknown dataset by name.
    pub fn try_new(segment: Arc<ArrayFile>, dataset: String) -> anyhow::Result<Self> {
        let info = segment.array(&dataset).ok_or_else(|| {
            anyhow::anyhow!("dataset '{dataset}' has no entry in this atlas segment")
        })?;
        let shape = info.shape.iter().map(|&s| s as usize).collect();
        let dimensions = info.dimension_names.clone();
        let chunk_shape = info.chunk_shape.iter().map(|&s| s as usize).collect();
        let fill_value = info
            .fill_value
            .as_ref()
            .map(|fill| T::fill_element(Some(fill)));
        Ok(Self {
            segment,
            dataset,
            shape,
            dimensions,
            chunk_shape,
            fill_value,
        })
    }
}

#[async_trait::async_trait]
impl<T: NdArrayType + AtlasElement> ArrayBackend<T> for AtlasArrayBackend<T> {
    fn len(&self) -> usize {
        self.shape.iter().product()
    }

    fn shape(&self) -> Vec<usize> {
        self.shape.clone()
    }

    fn dimensions(&self) -> Vec<String> {
        self.dimensions.clone()
    }

    /// The chunk shape the writer chose.
    ///
    /// The scan cuts a dataset on this grid, so a read fetches only the chunks it needs.
    fn chunk_shape(&self) -> Vec<usize> {
        self.chunk_shape.clone()
    }

    fn fill_value(&self) -> Option<T> {
        self.fill_value.clone()
    }

    async fn read_subset(&self, subset: ArraySubset) -> anyhow::Result<ArrayD<T>> {
        T::read(&self.segment, &self.dataset, subset.start, subset.shape).await
    }
}

/// Holds one attribute value as a rank-0 array.
/// The value comes from the footer the open already read; nothing here touches the store.
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

/// Wrap one dataset's entry of an atlas segment as a lazy [`NdArrayD`].
///
/// No data is read here; values arrive when the engine asks for a subset.
pub fn array_to_nd_array(
    segment: Arc<ArrayFile>,
    dataset: &str,
    dtype: &DType,
) -> anyhow::Result<Arc<dyn NdArrayD>> {
    macro_rules! lazy {
        ($ty:ty) => {{
            let backend = AtlasArrayBackend::<$ty>::try_new(segment, dataset.to_string())?;
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
            "dataset '{dataset}' holds a Bool array, which atlas stores no elements of"
        )),
        DType::FixedSizeList { .. } => Err(anyhow::anyhow!(
            "dataset '{dataset}' holds a FixedSizeList array, which Beacon does not model"
        )),
        DType::List { .. } => Err(anyhow::anyhow!(
            "dataset '{dataset}' holds a List array, which Beacon does not model"
        )),
    }
}

/// Wrap one scalar attribute value as a rank-0 [`NdArrayD`].
///
/// Broadcasts across every row the dataset contributes. Refuses a list-valued attribute.
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
    use crate::test_support;
    use beacon_nd_array::datatypes::NdArrayDataType;

    /// The segment of one variable of a fixture collection.
    async fn segment(dir: &std::path::Path, array: &str) -> Arc<ArrayFile> {
        let atlas = test_support::open(dir).await;
        Arc::clone(atlas.segment(array).await.expect("segment"))
    }

    /// Shape, dimensions, chunking and fill all come from the segment.
    #[tokio::test]
    async fn the_backend_reports_what_the_segment_holds() {
        let tmp = tempfile::tempdir().unwrap();
        test_support::two_datasets(tmp.path()).await;

        let backend = AtlasArrayBackend::<i32>::try_new(
            segment(tmp.path(), "cycle").await,
            "winter".to_string(),
        )
        .unwrap();
        assert_eq!(ArrayBackend::<i32>::shape(&backend), vec![4]);
        assert_eq!(
            ArrayBackend::<i32>::dimensions(&backend),
            vec!["obs".to_string()]
        );
        assert_eq!(ArrayBackend::<i32>::chunk_shape(&backend), vec![4]);
        assert_eq!(ArrayBackend::<i32>::fill_value(&backend), Some(-1));
        assert_eq!(backend.len(), 4);
    }

    /// A segment holds an entry per dataset that declares the variable. A
    /// dataset that does not is refused by name, not read as empty.
    #[tokio::test]
    async fn a_dataset_the_segment_lacks_is_refused_by_name() {
        let tmp = tempfile::tempdir().unwrap();
        test_support::two_datasets(tmp.path()).await;

        let error = AtlasArrayBackend::<i32>::try_new(
            segment(tmp.path(), "cycle").await,
            "summer".to_string(),
        )
        .expect_err("only `winter` declares `cycle`")
        .to_string();
        assert!(error.contains("summer"), "{error}");
    }

    #[tokio::test]
    async fn a_full_read_returns_every_value() {
        let tmp = tempfile::tempdir().unwrap();
        test_support::two_datasets(tmp.path()).await;

        let backend = AtlasArrayBackend::<f32>::try_new(
            segment(tmp.path(), "temperature").await,
            "winter".to_string(),
        )
        .unwrap();
        let values = backend
            .read_subset(ArraySubset::new(vec![0], vec![4]))
            .await
            .unwrap();
        assert_eq!(values.into_raw_vec_and_offset().0, vec![1.0, 2.0, 3.0, 4.0]);
    }

    #[tokio::test]
    async fn a_window_returns_only_its_own_values() {
        let tmp = tempfile::tempdir().unwrap();
        test_support::two_datasets(tmp.path()).await;

        let backend = AtlasArrayBackend::<i32>::try_new(
            segment(tmp.path(), "cycle").await,
            "winter".to_string(),
        )
        .unwrap();
        let values = backend
            .read_subset(ArraySubset::new(vec![1], vec![2]))
            .await
            .unwrap();
        assert_eq!(values.into_raw_vec_and_offset().0, vec![20, 30]);
    }

    /// A window that spans two stored chunks assembles across them, and lands
    /// in row-major order.
    #[tokio::test]
    async fn a_window_across_chunks_is_assembled_in_order() {
        let tmp = tempfile::tempdir().unwrap();
        test_support::chunked_grid(tmp.path()).await;

        let backend = AtlasArrayBackend::<f64>::try_new(
            segment(tmp.path(), "temperature").await,
            "grid".to_string(),
        )
        .unwrap();
        assert_eq!(ArrayBackend::<f64>::chunk_shape(&backend), vec![2, 3]);
        // Window spans all four chunk columns and both chunk rows.
        let values = backend
            .read_subset(ArraySubset::new(vec![1, 2], vec![2, 2]))
            .await
            .unwrap();
        assert_eq!(values.shape(), &[2, 2]);
        assert_eq!(
            values.into_raw_vec_and_offset().0,
            vec![8.0, 9.0, 14.0, 15.0]
        );
    }

    /// A region nobody wrote reads back as the fill value, and costs no bytes.
    #[tokio::test]
    async fn an_unwritten_region_reads_as_the_fill() {
        let tmp = tempfile::tempdir().unwrap();
        test_support::chunked_grid(tmp.path()).await;

        let backend = AtlasArrayBackend::<f64>::try_new(
            segment(tmp.path(), "sparse").await,
            "grid".to_string(),
        )
        .unwrap();
        assert_eq!(ArrayBackend::<f64>::fill_value(&backend), Some(-999.0));
        let values = backend
            .read_subset(ArraySubset::new(vec![2, 0], vec![1, 3]))
            .await
            .unwrap();
        assert_eq!(values.into_raw_vec_and_offset().0, vec![-999.0; 3]);
    }

    #[tokio::test]
    async fn a_timestamp_array_reads_as_beacons_own_newtype() {
        let tmp = tempfile::tempdir().unwrap();
        test_support::two_datasets(tmp.path()).await;

        let backend = AtlasArrayBackend::<TimestampNanosecond>::try_new(
            segment(tmp.path(), "time").await,
            "winter".to_string(),
        )
        .unwrap();
        let values = backend
            .read_subset(ArraySubset::new(vec![0], vec![2]))
            .await
            .unwrap();
        assert_eq!(
            values.into_raw_vec_and_offset().0,
            vec![
                TimestampNanosecond(test_support::EPOCH_NANOS),
                TimestampNanosecond(test_support::EPOCH_NANOS + 86_400_000_000_000),
            ]
        );
    }

    #[tokio::test]
    async fn a_string_array_reads_its_values() {
        let tmp = tempfile::tempdir().unwrap();
        test_support::incompatible(tmp.path()).await;

        let backend = AtlasArrayBackend::<String>::try_new(
            segment(tmp.path(), "value").await,
            "a".to_string(),
        )
        .unwrap();
        let values = backend
            .read_subset(ArraySubset::new(vec![0], vec![2]))
            .await
            .unwrap();
        assert_eq!(
            values.into_raw_vec_and_offset().0,
            vec!["x".to_string(), "y".to_string()]
        );
    }

    #[test]
    fn a_fill_takes_the_form_array_format_returns() {
        assert_eq!(
            <i32 as AtlasElement>::fill_element(Some(&FillValue::Int(-7))),
            -7
        );
        assert!(<f64 as AtlasElement>::fill_element(Some(&FillValue::Float(f64::NAN))).is_nan());
        assert_eq!(<i32 as AtlasElement>::fill_element(None), 0);
        assert_eq!(
            <TimestampNanosecond as AtlasElement>::fill_element(Some(&FillValue::TimestampNs(
                i64::MIN
            ))),
            TimestampNanosecond(i64::MIN)
        );
    }

    #[tokio::test]
    async fn an_attribute_is_one_value_on_no_axis() {
        let backend = AttributeBackend::new("winter".to_string());
        assert_eq!(backend.len(), 1);
        assert!(ArrayBackend::<String>::shape(&backend).is_empty());
        assert!(ArrayBackend::<String>::dimensions(&backend).is_empty());

        let values = backend
            .read_subset(ArraySubset::new(vec![], vec![]))
            .await
            .unwrap();
        assert_eq!(values.ndim(), 0);
        assert_eq!(
            values.into_raw_vec_and_offset().0,
            vec!["winter".to_string()]
        );
    }

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
