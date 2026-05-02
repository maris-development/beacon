use std::marker::PhantomData;
use std::sync::Arc;

use anyhow::anyhow;
use array_format::{BinaryArray, FillValue, Reader, StorageLayout, StringArray};
use beacon_nd_array::datatypes::{NdArrayDataType, NdArrayType, TimestampNanosecond};
use beacon_nd_array::{
    NdArray, NdArrayD, array::backend::ArrayBackend, array::subset::ArraySubset,
};
use ndarray::{ArrayD, Axis, IxDyn, Slice};

// ─── AtlasArray ───────────────────────────────────────────────────────────────

/// A lazy array backend backed by an `array_format::Reader`.
///
/// Metadata (shape, dimensions, chunk_shape, fill_value) is resolved at
/// construction time from the footer — **no data I/O**. The actual array
/// bytes are only read when [`read_subset`](ArrayBackend::read_subset) is
/// called.
///
/// For **flat** arrays the full payload is read and then sliced locally.
/// For **chunked** arrays only the chunks that overlap the requested subset
/// are fetched, giving true partial-I/O benefits.
#[derive(Clone)]
pub struct AtlasArray<T: NdArrayType> {
    reader: Arc<Reader>,
    array_name: String,
    shape: Vec<usize>,
    chunk_shape: Vec<usize>,
    dimensions: Vec<String>,
    fill_value: Option<T>,
    _phantom: PhantomData<T>,
}

impl<T: NdArrayType> std::fmt::Debug for AtlasArray<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AtlasArray")
            .field("array_name", &self.array_name)
            .field("shape", &self.shape)
            .field("chunk_shape", &self.chunk_shape)
            .field("dimensions", &self.dimensions)
            .finish()
    }
}

impl<T: NdArrayType> AtlasArray<T> {
    /// Create a new lazy array from a shared reader and an array name.
    ///
    /// Looks up the array in the reader's footer to extract shape,
    /// dimensions, chunk shape, and fill value.
    ///
    /// # Errors
    ///
    /// Returns an error if the array is not found in the reader's footer.
    pub fn new(reader: Arc<Reader>, array_name: &str) -> anyhow::Result<Self>
    where
        T: FromFillValue,
    {
        let meta = reader
            .get_array(array_name)
            .map_err(|e| anyhow!("Array '{}' not found: {}", array_name, e))?;

        let shape: Vec<usize> = meta.layout.shape.iter().map(|&s| s as usize).collect();
        let mut dimensions = meta.layout.dimension_names.clone();

        // Attribute arrays may have a shape (e.g. [1]) but no named dimensions.
        // Pad with empty strings to satisfy the shape.len() == dimensions.len()
        // invariant required by NdArray.
        while dimensions.len() < shape.len() {
            dimensions.push(String::new());
        }

        let chunk_shape = match &meta.layout.storage {
            StorageLayout::Chunked { chunk_shape, .. } => {
                chunk_shape.iter().map(|&s| s as usize).collect()
            }
            StorageLayout::Flat { .. } => shape.clone(),
        };

        let fill_value = meta
            .fill_value
            .as_ref()
            .and_then(|fv| T::from_fill_value(fv));

        Ok(Self {
            reader,
            array_name: array_name.to_string(),
            shape,
            chunk_shape,
            dimensions,
            fill_value,
            _phantom: PhantomData,
        })
    }
}

#[async_trait::async_trait]
impl<T: NdArrayType + DecodeFromBytes + FromFillValue> ArrayBackend<T> for AtlasArray<T> {
    fn len(&self) -> usize {
        self.shape.iter().product()
    }

    fn shape(&self) -> Vec<usize> {
        self.shape.clone()
    }

    fn chunk_shape(&self) -> Vec<usize> {
        self.chunk_shape.clone()
    }

    fn dimensions(&self) -> Vec<String> {
        self.dimensions.clone()
    }

    fn fill_value(&self) -> Option<T> {
        self.fill_value.clone()
    }

    async fn read_subset(&self, subset: ArraySubset) -> anyhow::Result<ArrayD<T>> {
        self.validate_subset(&subset)?;

        let meta = self
            .reader
            .get_array(&self.array_name)
            .map_err(|e| anyhow!("Array '{}' not found: {}", self.array_name, e))?;

        match &meta.layout.storage {
            StorageLayout::Flat { .. } => self.read_flat_subset(&subset).await,
            StorageLayout::Chunked { chunk_shape, .. } => {
                let chunk_shape: Vec<usize> = chunk_shape.iter().map(|&s| s as usize).collect();
                self.read_chunked_subset(&subset, &chunk_shape).await
            }
        }
    }
}

impl<T: NdArrayType + DecodeFromBytes> AtlasArray<T> {
    /// Read a flat array, decode, and slice to the requested subset.
    async fn read_flat_subset(&self, subset: &ArraySubset) -> anyhow::Result<ArrayD<T>> {
        let bytes = self
            .reader
            .read_raw_bytes(&self.array_name)
            .await
            .map_err(|e| anyhow!("Failed to read '{}': {}", self.array_name, e))?;
        let values = T::decode_from_bytes(&bytes)?;
        let full = ArrayD::from_shape_vec(IxDyn(&self.shape), values)
            .map_err(|e| anyhow!("Shape mismatch for '{}': {}", self.array_name, e))?;
        Ok(slice_array(&full, subset))
    }

    /// Read only the chunks that overlap the requested subset, then assemble.
    async fn read_chunked_subset(
        &self,
        subset: &ArraySubset,
        chunk_shape: &[usize],
    ) -> anyhow::Result<ArrayD<T>> {
        let ndim = self.shape.len();

        // Determine which chunk coords overlap the subset
        let mut axis_ranges: Vec<std::ops::RangeInclusive<u32>> = Vec::with_capacity(ndim);
        for i in 0..ndim {
            let first = (subset.start[i] / chunk_shape[i]) as u32;
            let last = if subset.shape[i] == 0 {
                first
            } else {
                ((subset.start[i] + subset.shape[i] - 1) / chunk_shape[i]) as u32
            };
            axis_ranges.push(first..=last);
        }

        // Build the output array filled with fill_value (or T::default-ish zeros)
        let fill = self.fill_value.clone().unwrap_or_else(T::default_value);
        let mut output = ArrayD::from_elem(IxDyn(&subset.shape), fill);

        // Iterate over all overlapping chunk coordinates (cartesian product)
        let mut coord = vec![0u32; ndim];
        for i in 0..ndim {
            coord[i] = *axis_ranges[i].start();
        }

        loop {
            // Read this chunk
            let chunk_bytes = self
                .reader
                .read_chunk_raw(&self.array_name, &coord)
                .await
                .map_err(|e| {
                    anyhow!(
                        "Failed to read chunk {:?} of '{}': {}",
                        coord,
                        self.array_name,
                        e
                    )
                })?;
            let chunk_values = T::decode_from_bytes(&chunk_bytes)?;

            // Compute this chunk's global extent
            let mut chunk_global_start = vec![0usize; ndim];
            let mut chunk_actual_shape = vec![0usize; ndim];
            for i in 0..ndim {
                chunk_global_start[i] = coord[i] as usize * chunk_shape[i];
                chunk_actual_shape[i] = (chunk_shape[i]).min(self.shape[i] - chunk_global_start[i]);
            }

            let chunk_array = ArrayD::from_shape_vec(IxDyn(&chunk_actual_shape), chunk_values)
                .map_err(|e| anyhow!("Chunk shape mismatch for '{}': {}", self.array_name, e))?;

            // Compute intersection of chunk extent with the requested subset
            let mut src_start = vec![0usize; ndim];
            let mut dst_start = vec![0usize; ndim];
            let mut copy_shape = vec![0usize; ndim];
            for i in 0..ndim {
                let chunk_end = chunk_global_start[i] + chunk_actual_shape[i];
                let subset_end = subset.start[i] + subset.shape[i];
                let inter_start = chunk_global_start[i].max(subset.start[i]);
                let inter_end = chunk_end.min(subset_end);
                if inter_start >= inter_end {
                    // No overlap on this axis — skip entire chunk
                    copy_shape[i] = 0;
                } else {
                    src_start[i] = inter_start - chunk_global_start[i];
                    dst_start[i] = inter_start - subset.start[i];
                    copy_shape[i] = inter_end - inter_start;
                }
            }

            // Skip if any axis has zero overlap
            if copy_shape.iter().all(|&s| s > 0) {
                // Slice the chunk to the intersection region
                let src_slice = slice_array_region(&chunk_array, &src_start, &copy_shape);
                // Write into the output
                let mut dst_view = output.view_mut();
                for i in 0..ndim {
                    dst_view.slice_axis_inplace(
                        Axis(i),
                        Slice::from(dst_start[i]..dst_start[i] + copy_shape[i]),
                    );
                }
                dst_view.assign(&src_slice);
            }

            // Advance to next coord (odometer-style)
            let mut axis = ndim;
            loop {
                if axis == 0 {
                    return Ok(output);
                }
                axis -= 1;
                coord[axis] += 1;
                if coord[axis] <= *axis_ranges[axis].end() {
                    break;
                }
                coord[axis] = *axis_ranges[axis].start();
            }
        }
    }
}

// ─── Subset Slicing Helpers ───────────────────────────────────────────────────

/// Slice an ArrayD to an ArraySubset region.
fn slice_array<T: Clone>(array: &ArrayD<T>, subset: &ArraySubset) -> ArrayD<T> {
    let mut view = array.view();
    for i in 0..subset.start.len() {
        view.slice_axis_inplace(
            Axis(i),
            Slice::from(subset.start[i]..subset.start[i] + subset.shape[i]),
        );
    }
    view.to_owned()
}

/// Slice an ArrayD to a region given by start offsets and shape.
fn slice_array_region<T: Clone>(array: &ArrayD<T>, start: &[usize], shape: &[usize]) -> ArrayD<T> {
    let mut view = array.view();
    for i in 0..start.len() {
        view.slice_axis_inplace(Axis(i), Slice::from(start[i]..start[i] + shape[i]));
    }
    view.to_owned()
}

// ─── Decode Trait ─────────────────────────────────────────────────────────────

/// Decode raw bytes from an array-format file into a `Vec<T>`.
///
/// This is a sealed trait — only types that correspond to array-format's
/// supported data types implement it.
pub trait DecodeFromBytes: Sized {
    fn decode_from_bytes(bytes: &[u8]) -> anyhow::Result<Vec<Self>>;
    /// A zero-like default used to pre-fill output buffers for chunked reads.
    fn default_value() -> Self;
}

macro_rules! impl_decode_primitive {
    ($ty:ty, $default:expr) => {
        impl DecodeFromBytes for $ty {
            fn decode_from_bytes(bytes: &[u8]) -> anyhow::Result<Vec<Self>> {
                let elem = std::mem::size_of::<$ty>();
                if elem > 0 && bytes.len() % elem != 0 {
                    anyhow::bail!(
                        "byte length {} is not a multiple of element size {}",
                        bytes.len(),
                        elem
                    );
                }
                let len = if elem > 0 { bytes.len() / elem } else { 0 };
                let values: Vec<$ty> =
                    unsafe { std::slice::from_raw_parts(bytes.as_ptr() as *const $ty, len) }
                        .to_vec();
                Ok(values)
            }

            fn default_value() -> Self {
                $default
            }
        }
    };
}

impl_decode_primitive!(i8, 0);
impl_decode_primitive!(i16, 0);
impl_decode_primitive!(i32, 0);
impl_decode_primitive!(i64, 0);
impl_decode_primitive!(u8, 0);
impl_decode_primitive!(u16, 0);
impl_decode_primitive!(u32, 0);
impl_decode_primitive!(u64, 0);
impl_decode_primitive!(f32, 0.0);
impl_decode_primitive!(f64, 0.0);

impl DecodeFromBytes for bool {
    fn decode_from_bytes(bytes: &[u8]) -> anyhow::Result<Vec<Self>> {
        Ok(bytes.iter().map(|&b| b != 0).collect())
    }
    fn default_value() -> Self {
        false
    }
}

impl DecodeFromBytes for TimestampNanosecond {
    fn decode_from_bytes(bytes: &[u8]) -> anyhow::Result<Vec<Self>> {
        let i64s = i64::decode_from_bytes(bytes)?;
        Ok(i64s.into_iter().map(TimestampNanosecond).collect())
    }
    fn default_value() -> Self {
        TimestampNanosecond(0)
    }
}

impl DecodeFromBytes for String {
    fn decode_from_bytes(bytes: &[u8]) -> anyhow::Result<Vec<Self>> {
        let arr = StringArray::from_bytes(bytes.to_vec());
        let len = arr.len();
        Ok((0..len)
            .map(|i| arr.value(i).unwrap_or("").to_string())
            .collect())
    }
    fn default_value() -> Self {
        String::new()
    }
}

impl DecodeFromBytes for Vec<u8> {
    fn decode_from_bytes(bytes: &[u8]) -> anyhow::Result<Vec<Self>> {
        let arr = BinaryArray::from_bytes(bytes.to_vec());
        let len = arr.len();
        Ok((0..len)
            .map(|i| arr.value(i).unwrap_or(&[]).to_vec())
            .collect())
    }
    fn default_value() -> Self {
        Vec::new()
    }
}

// ─── FillValue Conversion ─────────────────────────────────────────────────────

/// Convert an `array_format::FillValue` to the concrete Rust type `T`.
pub trait FromFillValue: Sized {
    fn from_fill_value(fv: &FillValue) -> Option<Self>;
}

macro_rules! impl_fill_int {
    ($ty:ty) => {
        impl FromFillValue for $ty {
            fn from_fill_value(fv: &FillValue) -> Option<Self> {
                match fv {
                    FillValue::Int(v) => Some(*v as $ty),
                    FillValue::UInt(v) => Some(*v as $ty),
                    _ => None,
                }
            }
        }
    };
}

macro_rules! impl_fill_uint {
    ($ty:ty) => {
        impl FromFillValue for $ty {
            fn from_fill_value(fv: &FillValue) -> Option<Self> {
                match fv {
                    FillValue::UInt(v) => Some(*v as $ty),
                    FillValue::Int(v) => Some(*v as $ty),
                    _ => None,
                }
            }
        }
    };
}

macro_rules! impl_fill_float {
    ($ty:ty) => {
        impl FromFillValue for $ty {
            fn from_fill_value(fv: &FillValue) -> Option<Self> {
                match fv {
                    FillValue::Float(v) => Some(*v as $ty),
                    FillValue::Int(v) => Some(*v as $ty),
                    FillValue::UInt(v) => Some(*v as $ty),
                    _ => None,
                }
            }
        }
    };
}

impl_fill_int!(i8);
impl_fill_int!(i16);
impl_fill_int!(i32);
impl_fill_int!(i64);
impl_fill_uint!(u8);
impl_fill_uint!(u16);
impl_fill_uint!(u32);
impl_fill_uint!(u64);
impl_fill_float!(f32);
impl_fill_float!(f64);

impl FromFillValue for bool {
    fn from_fill_value(fv: &FillValue) -> Option<Self> {
        match fv {
            FillValue::Bool(b) => Some(*b),
            _ => None,
        }
    }
}

impl FromFillValue for TimestampNanosecond {
    fn from_fill_value(fv: &FillValue) -> Option<Self> {
        match fv {
            FillValue::Int(v) => Some(TimestampNanosecond(*v)),
            _ => None,
        }
    }
}

impl FromFillValue for String {
    fn from_fill_value(fv: &FillValue) -> Option<Self> {
        match fv {
            FillValue::String(s) => Some(s.clone()),
            _ => None,
        }
    }
}

impl FromFillValue for Vec<u8> {
    fn from_fill_value(_fv: &FillValue) -> Option<Self> {
        None // FillValue has no Binary variant
    }
}

// ─── Dynamic Constructor ──────────────────────────────────────────────────────

/// Create a lazy `Arc<dyn NdArrayD>` from a reader and array name.
///
/// The returned array does not read any data until [`NdArrayD`] methods that
/// access values (e.g. subset, broadcast, into_raw_vec) are called.
pub fn lazy_ndarray_dyn(
    reader: Arc<Reader>,
    array_name: &str,
    data_type: NdArrayDataType,
) -> anyhow::Result<Arc<dyn NdArrayD>> {
    macro_rules! make_lazy {
        ($ty:ty) => {{
            let backend = AtlasArray::<$ty>::new(Arc::clone(&reader), array_name)?;
            let nd = NdArray::new_with_backend(backend)?;
            Ok(Arc::new(nd) as Arc<dyn NdArrayD>)
        }};
    }

    match data_type {
        NdArrayDataType::Bool => make_lazy!(bool),
        NdArrayDataType::I8 => make_lazy!(i8),
        NdArrayDataType::I16 => make_lazy!(i16),
        NdArrayDataType::I32 => make_lazy!(i32),
        NdArrayDataType::I64 => make_lazy!(i64),
        NdArrayDataType::U8 => make_lazy!(u8),
        NdArrayDataType::U16 => make_lazy!(u16),
        NdArrayDataType::U32 => make_lazy!(u32),
        NdArrayDataType::U64 => make_lazy!(u64),
        NdArrayDataType::F32 => make_lazy!(f32),
        NdArrayDataType::F64 => make_lazy!(f64),
        NdArrayDataType::Timestamp => make_lazy!(TimestampNanosecond),
        NdArrayDataType::String => make_lazy!(String),
        NdArrayDataType::Binary => make_lazy!(Vec<u8>),
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use array_format::{NoCompression, PrimitiveArray, Writer, WriterConfig};
    use beacon_nd_array::array::backend::ArrayBackend;
    use bytes::Bytes;
    use futures::future::BoxFuture;
    use std::ops::Range;
    use std::sync::Mutex;

    // ── In-memory storage for direct arrf Writer/Reader tests ──────────

    #[derive(Clone, Default)]
    struct MemStorage(Arc<Mutex<Vec<u8>>>);

    impl array_format::Storage for MemStorage {
        fn read_range(&self, range: Range<u64>) -> BoxFuture<'_, array_format::Result<Bytes>> {
            let data = self.0.lock().unwrap();
            let bytes = Bytes::copy_from_slice(&data[range.start as usize..range.end as usize]);
            Box::pin(async move { Ok(bytes) })
        }

        fn write(&self, data: Bytes) -> BoxFuture<'_, array_format::Result<()>> {
            let mut buf = self.0.lock().unwrap();
            *buf = data.to_vec();
            Box::pin(async { Ok(()) })
        }

        fn size(&self) -> BoxFuture<'_, array_format::Result<u64>> {
            let len = self.0.lock().unwrap().len() as u64;
            Box::pin(async move { Ok(len) })
        }
    }

    fn config() -> WriterConfig<NoCompression> {
        WriterConfig {
            block_target_size: 64 * 1024,
            codec: NoCompression,
        }
    }

    // ── Flat array: f64 ────────────────────────────────────────────────

    #[tokio::test]
    async fn test_flat_f64_full_read() {
        let storage = MemStorage::default();
        let mut writer = Writer::new(storage.clone(), config());
        let values = vec![1.0_f64, 2.0, 3.0, 4.0, 5.0, 6.0];
        let data = PrimitiveArray::from_slice(&values);
        writer
            .write_array("arr", vec!["x".into(), "y".into()], vec![2, 3], None, &data)
            .unwrap();
        writer.flush().await.unwrap();

        let reader = Arc::new(Reader::open(storage, 1024 * 1024).await.unwrap());
        let backend = AtlasArray::<f64>::new(Arc::clone(&reader), "arr").unwrap();

        assert_eq!(backend.len(), 6);
        assert_eq!(backend.shape(), vec![2, 3]);
        assert_eq!(backend.chunk_shape(), vec![2, 3]);
        assert_eq!(backend.dimensions(), vec!["x", "y"]);
        assert_eq!(backend.fill_value(), None);

        let full = backend
            .read_subset(ArraySubset {
                start: vec![0, 0],
                shape: vec![2, 3],
            })
            .await
            .unwrap();
        assert_eq!(full.into_raw_vec_and_offset().0, values);
    }

    #[tokio::test]
    async fn test_flat_f64_subset() {
        let storage = MemStorage::default();
        let mut writer = Writer::new(storage.clone(), config());
        // 3×4 matrix: [[0,1,2,3],[4,5,6,7],[8,9,10,11]]
        let values: Vec<f64> = (0..12).map(|i| i as f64).collect();
        let data = PrimitiveArray::from_slice(&values);
        writer
            .write_array(
                "m",
                vec!["row".into(), "col".into()],
                vec![3, 4],
                None,
                &data,
            )
            .unwrap();
        writer.flush().await.unwrap();

        let reader = Arc::new(Reader::open(storage, 1024 * 1024).await.unwrap());
        let backend = AtlasArray::<f64>::new(Arc::clone(&reader), "m").unwrap();

        // Subset: rows 1..3, cols 1..3 → [[5,6],[9,10]]
        let sub = backend
            .read_subset(ArraySubset {
                start: vec![1, 1],
                shape: vec![2, 2],
            })
            .await
            .unwrap();
        assert_eq!(sub.shape(), &[2, 2]);
        assert_eq!(sub.into_raw_vec_and_offset().0, vec![5.0, 6.0, 9.0, 10.0]);
    }

    // ── Flat array: i32 ────────────────────────────────────────────────

    #[tokio::test]
    async fn test_flat_i32() {
        let storage = MemStorage::default();
        let mut writer = Writer::new(storage.clone(), config());
        let values = vec![10_i32, 20, 30];
        let data = PrimitiveArray::from_slice(&values);
        writer
            .write_array("ints", vec!["obs".into()], vec![3], None, &data)
            .unwrap();
        writer.flush().await.unwrap();

        let reader = Arc::new(Reader::open(storage, 1024 * 1024).await.unwrap());
        let backend = AtlasArray::<i32>::new(Arc::clone(&reader), "ints").unwrap();
        assert_eq!(backend.shape(), vec![3]);

        let full = backend
            .read_subset(ArraySubset {
                start: vec![0],
                shape: vec![3],
            })
            .await
            .unwrap();
        assert_eq!(full.into_raw_vec_and_offset().0, values);
    }

    // ── Flat array: String ─────────────────────────────────────────────

    #[tokio::test]
    async fn test_flat_string() {
        let storage = MemStorage::default();
        let mut writer = Writer::new(storage.clone(), config());
        let values = ["hello", "world", "foo"];
        let data = StringArray::from_slices(&values);
        writer
            .write_array("names", vec!["obs".into()], vec![3], None, &data)
            .unwrap();
        writer.flush().await.unwrap();

        let reader = Arc::new(Reader::open(storage, 1024 * 1024).await.unwrap());
        let backend = AtlasArray::<String>::new(Arc::clone(&reader), "names").unwrap();
        assert_eq!(backend.shape(), vec![3]);

        let full = backend
            .read_subset(ArraySubset {
                start: vec![0],
                shape: vec![3],
            })
            .await
            .unwrap();
        assert_eq!(
            full.into_raw_vec_and_offset().0,
            vec!["hello".to_string(), "world".to_string(), "foo".to_string()]
        );
    }

    // ── Flat array: bool ───────────────────────────────────────────────

    #[tokio::test]
    async fn test_flat_bool() {
        let storage = MemStorage::default();
        let mut writer = Writer::new(storage.clone(), config());
        // bool is stored as u8
        let bools: Vec<u8> = vec![1, 0, 1, 1];
        let data = PrimitiveArray::from_slice(&bools);
        writer
            .write_array("flags", vec!["obs".into()], vec![4], None, &data)
            .unwrap();
        writer.flush().await.unwrap();

        let reader = Arc::new(Reader::open(storage, 1024 * 1024).await.unwrap());
        let backend = AtlasArray::<bool>::new(Arc::clone(&reader), "flags").unwrap();

        let full = backend
            .read_subset(ArraySubset {
                start: vec![0],
                shape: vec![4],
            })
            .await
            .unwrap();
        assert_eq!(
            full.into_raw_vec_and_offset().0,
            vec![true, false, true, true]
        );
    }

    // ── Attribute array (no dimension names) ───────────────────────────

    #[tokio::test]
    async fn test_attribute_no_dimensions() {
        let storage = MemStorage::default();
        let mut writer = Writer::new(storage.clone(), config());
        let values = ["celsius"];
        let data = StringArray::from_slices(&values);
        // Empty dimensions, shape [1] — like an attribute
        writer
            .write_array("units", vec![], vec![1], None, &data)
            .unwrap();
        writer.flush().await.unwrap();

        let reader = Arc::new(Reader::open(storage, 1024 * 1024).await.unwrap());
        let backend = AtlasArray::<String>::new(Arc::clone(&reader), "units").unwrap();

        // Dimensions should be padded with empty strings
        assert_eq!(backend.shape(), vec![1]);
        assert_eq!(backend.dimensions(), vec![""]);

        let full = backend
            .read_subset(ArraySubset {
                start: vec![0],
                shape: vec![1],
            })
            .await
            .unwrap();
        assert_eq!(
            full.into_raw_vec_and_offset().0,
            vec!["celsius".to_string()]
        );
    }

    // ── Chunked array: 1D ──────────────────────────────────────────────

    #[tokio::test]
    async fn test_chunked_1d_full_read() {
        let storage = MemStorage::default();
        let mut writer = Writer::new(storage.clone(), config());

        // 6-element array, chunk size 2 → 3 chunks
        let dtype = array_format::DType::Float64;
        let mut cw = writer
            .begin_chunked_array("arr", dtype, vec!["x".into()], vec![6], vec![2], None)
            .unwrap();

        cw.write_array(vec![0], &PrimitiveArray::from_slice(&[1.0_f64, 2.0]))
            .unwrap();
        cw.write_array(vec![1], &PrimitiveArray::from_slice(&[3.0_f64, 4.0]))
            .unwrap();
        cw.write_array(vec![2], &PrimitiveArray::from_slice(&[5.0_f64, 6.0]))
            .unwrap();
        writer.flush().await.unwrap();

        let reader = Arc::new(Reader::open(storage, 1024 * 1024).await.unwrap());
        let backend = AtlasArray::<f64>::new(Arc::clone(&reader), "arr").unwrap();

        assert_eq!(backend.shape(), vec![6]);
        assert_eq!(backend.chunk_shape(), vec![2]);

        let full = backend
            .read_subset(ArraySubset {
                start: vec![0],
                shape: vec![6],
            })
            .await
            .unwrap();
        assert_eq!(
            full.into_raw_vec_and_offset().0,
            vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0]
        );
    }

    #[tokio::test]
    async fn test_chunked_1d_subset_spanning_chunks() {
        let storage = MemStorage::default();
        let mut writer = Writer::new(storage.clone(), config());

        let dtype = array_format::DType::Float64;
        let mut cw = writer
            .begin_chunked_array("arr", dtype, vec!["x".into()], vec![6], vec![2], None)
            .unwrap();

        cw.write_array(vec![0], &PrimitiveArray::from_slice(&[10.0_f64, 20.0]))
            .unwrap();
        cw.write_array(vec![1], &PrimitiveArray::from_slice(&[30.0_f64, 40.0]))
            .unwrap();
        cw.write_array(vec![2], &PrimitiveArray::from_slice(&[50.0_f64, 60.0]))
            .unwrap();
        writer.flush().await.unwrap();

        let reader = Arc::new(Reader::open(storage, 1024 * 1024).await.unwrap());
        let backend = AtlasArray::<f64>::new(Arc::clone(&reader), "arr").unwrap();

        // Subset [1..5] spans chunks 0, 1, 2
        let sub = backend
            .read_subset(ArraySubset {
                start: vec![1],
                shape: vec![4],
            })
            .await
            .unwrap();
        assert_eq!(
            sub.into_raw_vec_and_offset().0,
            vec![20.0, 30.0, 40.0, 50.0]
        );
    }

    // ── Chunked array: 2D ──────────────────────────────────────────────

    #[tokio::test]
    async fn test_chunked_2d_full_read() {
        let storage = MemStorage::default();
        let mut writer = Writer::new(storage.clone(), config());

        // 4×4 array, chunk shape 2×2 → 4 chunks
        let dtype = array_format::DType::Int32;
        let mut cw = writer
            .begin_chunked_array(
                "grid",
                dtype,
                vec!["row".into(), "col".into()],
                vec![4, 4],
                vec![2, 2],
                None,
            )
            .unwrap();

        // chunk (0,0): rows 0-1, cols 0-1
        cw.write_array(vec![0, 0], &PrimitiveArray::from_slice(&[0_i32, 1, 4, 5]))
            .unwrap();
        // chunk (0,1): rows 0-1, cols 2-3
        cw.write_array(vec![0, 1], &PrimitiveArray::from_slice(&[2_i32, 3, 6, 7]))
            .unwrap();
        // chunk (1,0): rows 2-3, cols 0-1
        cw.write_array(vec![1, 0], &PrimitiveArray::from_slice(&[8_i32, 9, 12, 13]))
            .unwrap();
        // chunk (1,1): rows 2-3, cols 2-3
        cw.write_array(
            vec![1, 1],
            &PrimitiveArray::from_slice(&[10_i32, 11, 14, 15]),
        )
        .unwrap();
        writer.flush().await.unwrap();

        let reader = Arc::new(Reader::open(storage, 1024 * 1024).await.unwrap());
        let backend = AtlasArray::<i32>::new(Arc::clone(&reader), "grid").unwrap();

        assert_eq!(backend.shape(), vec![4, 4]);
        assert_eq!(backend.chunk_shape(), vec![2, 2]);

        let full = backend
            .read_subset(ArraySubset {
                start: vec![0, 0],
                shape: vec![4, 4],
            })
            .await
            .unwrap();
        assert_eq!(
            full.into_raw_vec_and_offset().0,
            vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]
        );
    }

    #[tokio::test]
    async fn test_chunked_2d_subset() {
        let storage = MemStorage::default();
        let mut writer = Writer::new(storage.clone(), config());

        let dtype = array_format::DType::Int32;
        let mut cw = writer
            .begin_chunked_array(
                "grid",
                dtype,
                vec!["row".into(), "col".into()],
                vec![4, 4],
                vec![2, 2],
                None,
            )
            .unwrap();

        cw.write_array(vec![0, 0], &PrimitiveArray::from_slice(&[0_i32, 1, 4, 5]))
            .unwrap();
        cw.write_array(vec![0, 1], &PrimitiveArray::from_slice(&[2_i32, 3, 6, 7]))
            .unwrap();
        cw.write_array(vec![1, 0], &PrimitiveArray::from_slice(&[8_i32, 9, 12, 13]))
            .unwrap();
        cw.write_array(
            vec![1, 1],
            &PrimitiveArray::from_slice(&[10_i32, 11, 14, 15]),
        )
        .unwrap();
        writer.flush().await.unwrap();

        let reader = Arc::new(Reader::open(storage, 1024 * 1024).await.unwrap());
        let backend = AtlasArray::<i32>::new(Arc::clone(&reader), "grid").unwrap();

        // Subset: rows 1..3, cols 1..3 → [[5,6],[9,10]]
        let sub = backend
            .read_subset(ArraySubset {
                start: vec![1, 1],
                shape: vec![2, 2],
            })
            .await
            .unwrap();
        assert_eq!(sub.shape(), &[2, 2]);
        assert_eq!(sub.into_raw_vec_and_offset().0, vec![5, 6, 9, 10]);
    }

    // ── lazy_ndarray_dyn ───────────────────────────────────────────────

    #[tokio::test]
    async fn test_lazy_ndarray_dyn_f64() {
        let storage = MemStorage::default();
        let mut writer = Writer::new(storage.clone(), config());
        let values = vec![1.0_f64, 2.0, 3.0];
        let data = PrimitiveArray::from_slice(&values);
        writer
            .write_array("temps", vec!["obs".into()], vec![3], None, &data)
            .unwrap();
        writer.flush().await.unwrap();

        let reader = Arc::new(Reader::open(storage, 1024 * 1024).await.unwrap());
        let nd = lazy_ndarray_dyn(Arc::clone(&reader), "temps", NdArrayDataType::F64).unwrap();

        assert_eq!(nd.datatype(), NdArrayDataType::F64);
        assert_eq!(nd.shape(), vec![3]);
        assert_eq!(nd.dimensions(), vec!["obs"]);

        // Actually read data via downcast
        let typed = nd.as_any().downcast_ref::<NdArray<f64>>().unwrap();
        let vals = typed.clone_into_raw_vec().await;
        assert_eq!(vals, vec![1.0, 2.0, 3.0]);
    }

    #[tokio::test]
    async fn test_lazy_ndarray_dyn_string() {
        let storage = MemStorage::default();
        let mut writer = Writer::new(storage.clone(), config());
        let values = ["alpha", "beta"];
        let data = StringArray::from_slices(&values);
        writer
            .write_array("labels", vec!["obs".into()], vec![2], None, &data)
            .unwrap();
        writer.flush().await.unwrap();

        let reader = Arc::new(Reader::open(storage, 1024 * 1024).await.unwrap());
        let nd = lazy_ndarray_dyn(Arc::clone(&reader), "labels", NdArrayDataType::String).unwrap();

        assert_eq!(nd.datatype(), NdArrayDataType::String);
        let typed = nd.as_any().downcast_ref::<NdArray<String>>().unwrap();
        assert_eq!(
            typed.clone_into_raw_vec().await,
            vec!["alpha".to_string(), "beta".to_string()]
        );
    }

    // ── Error: non-existent array ──────────────────────────────────────

    #[tokio::test]
    async fn test_atlas_array_not_found() {
        let storage = MemStorage::default();
        let mut writer = Writer::new(storage.clone(), config());
        let data = PrimitiveArray::from_slice(&[1.0_f64]);
        writer
            .write_array("exists", vec!["x".into()], vec![1], None, &data)
            .unwrap();
        writer.flush().await.unwrap();

        let reader = Arc::new(Reader::open(storage, 1024 * 1024).await.unwrap());
        let result = AtlasArray::<f64>::new(Arc::clone(&reader), "nonexistent");
        assert!(result.is_err());
    }

    // ── Multiple arrays in one file ────────────────────────────────────

    #[tokio::test]
    async fn test_multiple_arrays_in_single_file() {
        let storage = MemStorage::default();
        let mut writer = Writer::new(storage.clone(), config());

        let d1 = PrimitiveArray::from_slice(&[1.0_f64, 2.0]);
        writer
            .write_array("ds1", vec!["obs".into()], vec![2], None, &d1)
            .unwrap();

        let d2 = PrimitiveArray::from_slice(&[10.0_f64, 20.0, 30.0]);
        writer
            .write_array("ds2", vec!["obs".into()], vec![3], None, &d2)
            .unwrap();
        writer.flush().await.unwrap();

        let reader = Arc::new(Reader::open(storage, 1024 * 1024).await.unwrap());

        let b1 = AtlasArray::<f64>::new(Arc::clone(&reader), "ds1").unwrap();
        assert_eq!(b1.shape(), vec![2]);
        let v1 = b1
            .read_subset(ArraySubset {
                start: vec![0],
                shape: vec![2],
            })
            .await
            .unwrap();
        assert_eq!(v1.into_raw_vec_and_offset().0, vec![1.0, 2.0]);

        let b2 = AtlasArray::<f64>::new(Arc::clone(&reader), "ds2").unwrap();
        assert_eq!(b2.shape(), vec![3]);
        let v2 = b2
            .read_subset(ArraySubset {
                start: vec![0],
                shape: vec![3],
            })
            .await
            .unwrap();
        assert_eq!(v2.into_raw_vec_and_offset().0, vec![10.0, 20.0, 30.0]);
    }

    // ── NdArray integration via new_with_backend ───────────────────────

    #[tokio::test]
    async fn test_ndarray_with_atlas_backend() {
        let storage = MemStorage::default();
        let mut writer = Writer::new(storage.clone(), config());
        let values = vec![100_i64, 200, 300, 400];
        let data = PrimitiveArray::from_slice(&values);
        writer
            .write_array("arr", vec!["x".into(), "y".into()], vec![2, 2], None, &data)
            .unwrap();
        writer.flush().await.unwrap();

        let reader = Arc::new(Reader::open(storage, 1024 * 1024).await.unwrap());
        let backend = AtlasArray::<i64>::new(Arc::clone(&reader), "arr").unwrap();
        let nd = NdArray::new_with_backend(backend).unwrap();

        assert_eq!(nd.shape(), vec![2, 2]);
        assert_eq!(nd.dimensions(), vec!["x", "y"]);

        // subset via NdArray
        let sub = nd
            .subset(ArraySubset {
                start: vec![0, 1],
                shape: vec![2, 1],
            })
            .await
            .unwrap();
        assert_eq!(sub.clone_into_raw_vec().await, vec![200, 400]);
    }
}
