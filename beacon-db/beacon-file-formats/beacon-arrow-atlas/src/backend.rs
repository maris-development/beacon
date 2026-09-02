//! The lazy array backends the Atlas reader hands to `beacon-nd-array`.
//!
//! [`AtlasArrayBackend`] reads a region of one atlas array on demand.
//! [`AttributeBackend`] holds one attribute value as a rank-0 array.

use std::sync::Arc;

use atlas::{DatasetView, FillValue};
use beacon_nd_array::{
    array::{backend::ArrayBackend, subset::ArraySubset},
    datatypes::{NdArrayType, TimestampNanosecond},
};
use ndarray::ArrayD;

/// A Beacon element type that can be read out of an atlas array.
///
/// Atlas reads through [`atlas::ArrayElement`], and Beacon's ND model through
/// [`NdArrayType`]. The two agree on the numeric types, `String` and
/// `Vec<u8>`, but Beacon's [`TimestampNanosecond`] is its own newtype over
/// `i64` and needs a conversion. This trait hides that difference behind one
/// entry point, so [`AtlasArrayBackend`] stays generic.
#[async_trait::async_trait]
pub trait AtlasElement: NdArrayType {
    /// Read `shape` elements of `array` from `start`.
    async fn read(
        view: &DatasetView,
        array: &str,
        start: Vec<usize>,
        shape: Vec<usize>,
    ) -> anyhow::Result<ArrayD<Self>>;

    /// This type's form of an array's fill value.
    ///
    /// The engine nulls every element equal to it, so it has to be the value
    /// the read actually returns for a cell nobody wrote. Deferring to
    /// `array-format`'s own conversion is what guarantees that.
    fn fill_element(fill: Option<&FillValue>) -> Self;
}

macro_rules! passthrough {
    ($ty:ty) => {
        #[async_trait::async_trait]
        impl AtlasElement for $ty {
            async fn read(
                view: &DatasetView,
                array: &str,
                start: Vec<usize>,
                shape: Vec<usize>,
            ) -> anyhow::Result<ArrayD<Self>> {
                let values = view
                    .read_array::<$ty>(array, start, shape)
                    .await
                    .map_err(|e| {
                        anyhow::anyhow!(
                            "Failed to read atlas array '{array}' of dataset '{}': {e}",
                            view.name()
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

/// Both types are `#[repr(transparent)]` over `i64`, so the conversion is a
/// rename. It is still done element by element, because the two are distinct
/// types and a transmute of a whole array would rest on layout rather than on
/// the type system.
#[async_trait::async_trait]
impl AtlasElement for TimestampNanosecond {
    async fn read(
        view: &DatasetView,
        array: &str,
        start: Vec<usize>,
        shape: Vec<usize>,
    ) -> anyhow::Result<ArrayD<Self>> {
        let values = view
            .read_array::<atlas::TimestampNs>(array, start, shape)
            .await
            .map_err(|e| {
                anyhow::anyhow!(
                    "Failed to read atlas timestamp array '{array}' of dataset '{}': {e}",
                    view.name()
                )
            })?;
        Ok(values.into_owned().mapv(|ts| TimestampNanosecond(ts.0)))
    }

    fn fill_element(fill: Option<&FillValue>) -> Self {
        TimestampNanosecond(<atlas::TimestampNs as atlas::ArrayElement>::fill_element(fill).0)
    }
}

/// Reads one atlas array lazily, one requested region at a time.
///
/// The backend holds the [`DatasetView`] rather than the collection and a
/// name. A view is resolved once, when the dataset is built; resolving it per
/// read would cost a linear scan of the collection footer every time.
pub struct AtlasArrayBackend<T: NdArrayType> {
    view: Arc<DatasetView>,
    array: String,
    shape: Vec<usize>,
    dimensions: Vec<String>,
    chunk_shape: Vec<usize>,
    fill_value: Option<T>,
}

impl<T: NdArrayType> std::fmt::Debug for AtlasArrayBackend<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AtlasArrayBackend")
            .field("dataset", &self.view.name())
            .field("array", &self.array)
            .field("shape", &self.shape)
            .field("dimensions", &self.dimensions)
            .field("chunk_shape", &self.chunk_shape)
            .finish_non_exhaustive()
    }
}

impl<T: NdArrayType> AtlasArrayBackend<T> {
    pub fn new(
        view: Arc<DatasetView>,
        array: String,
        shape: Vec<usize>,
        dimensions: Vec<String>,
        chunk_shape: Vec<usize>,
        fill_value: Option<T>,
    ) -> Self {
        Self {
            view,
            array,
            shape,
            dimensions,
            chunk_shape,
            fill_value,
        }
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
    /// The scan cuts a dataset on this grid, so one unit of work is one stored
    /// chunk and a read fetches no block it does not need.
    fn chunk_shape(&self) -> Vec<usize> {
        self.chunk_shape.clone()
    }

    fn fill_value(&self) -> Option<T> {
        self.fill_value.clone()
    }

    async fn read_subset(&self, subset: ArraySubset) -> anyhow::Result<ArrayD<T>> {
        T::read(&self.view, &self.array, subset.start, subset.shape).await
    }
}

/// Holds one attribute value as a rank-0 array.
///
/// The value came from the collection footer, which the open already read, so
/// nothing here touches the store.
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
    use crate::test_support;

    /// Open one dataset of a fixture collection.
    async fn view(dir: &std::path::Path, dataset: &str) -> Arc<DatasetView> {
        let atlas = atlas::Atlas::open_path(dir).await.expect("open");
        Arc::new(atlas.dataset(dataset).expect("dataset"))
    }

    // ── AtlasArrayBackend ───────────────────────────────────────────────

    #[tokio::test]
    async fn the_backend_reports_what_the_footer_holds() {
        let tmp = tempfile::tempdir().unwrap();
        test_support::two_datasets(tmp.path()).await;

        let backend = AtlasArrayBackend::<i32>::new(
            view(tmp.path(), "winter").await,
            "cycle".to_string(),
            vec![4],
            vec!["obs".to_string()],
            vec![4],
            Some(-1),
        );
        assert_eq!(ArrayBackend::<i32>::shape(&backend), vec![4]);
        assert_eq!(
            ArrayBackend::<i32>::dimensions(&backend),
            vec!["obs".to_string()]
        );
        assert_eq!(ArrayBackend::<i32>::chunk_shape(&backend), vec![4]);
        assert_eq!(ArrayBackend::<i32>::fill_value(&backend), Some(-1));
        assert_eq!(backend.len(), 4);
    }

    #[tokio::test]
    async fn a_full_read_returns_every_value() {
        let tmp = tempfile::tempdir().unwrap();
        test_support::two_datasets(tmp.path()).await;

        let backend = AtlasArrayBackend::<f32>::new(
            view(tmp.path(), "winter").await,
            "temperature".to_string(),
            vec![4],
            vec!["obs".to_string()],
            vec![4],
            None,
        );
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

        let backend = AtlasArrayBackend::<i32>::new(
            view(tmp.path(), "winter").await,
            "cycle".to_string(),
            vec![4],
            vec!["obs".to_string()],
            vec![4],
            None,
        );
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

        let backend = AtlasArrayBackend::<f64>::new(
            view(tmp.path(), "grid").await,
            "temperature".to_string(),
            vec![4, 6],
            vec!["lat".to_string(), "lon".to_string()],
            vec![2, 3],
            None,
        );
        // Rows 1..3, columns 2..4 of a 4x6 grid whose value is row * 6 + col.
        // That window straddles all four chunk columns and both chunk rows.
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

        let backend = AtlasArrayBackend::<f64>::new(
            view(tmp.path(), "grid").await,
            "sparse".to_string(),
            vec![4, 6],
            vec!["lat".to_string(), "lon".to_string()],
            vec![2, 3],
            Some(-999.0),
        );
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

        let backend = AtlasArrayBackend::<TimestampNanosecond>::new(
            view(tmp.path(), "winter").await,
            "time".to_string(),
            vec![4],
            vec!["obs".to_string()],
            vec![4],
            None,
        );
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

        let backend = AtlasArrayBackend::<String>::new(
            view(tmp.path(), "a").await,
            "value".to_string(),
            vec![2],
            vec!["obs".to_string()],
            vec![2],
            None,
        );
        let values = backend
            .read_subset(ArraySubset::new(vec![0], vec![2]))
            .await
            .unwrap();
        assert_eq!(
            values.into_raw_vec_and_offset().0,
            vec!["x".to_string(), "y".to_string()]
        );
    }

    // ── fill values ─────────────────────────────────────────────────────

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

    // ── AttributeBackend ────────────────────────────────────────────────

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
}
