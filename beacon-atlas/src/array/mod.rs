use std::sync::Arc;

use arrow::array::{ArrowPrimitiveType, PrimitiveArray};
use futures::StreamExt;

use crate::array::{
    data_type::PrimitiveArrayDataType,
    nd::NdArray,
    store::{ChunkStore, InMemoryChunkStore},
};

pub mod buffer;
pub mod data_type;
pub mod io_cache;
pub mod layout;
pub mod nd;
pub mod reader;
pub mod store;
pub mod writer;

pub use nd::try_from_arrow_ref;

/// A stream of chunked ND arrays with a shared element type and chunk shape.
#[derive(Debug, Clone)]
pub struct Array<S: ChunkStore + Send + Sync> {
    pub array_datatype: arrow::datatypes::DataType,
    pub array_shape: Vec<usize>,
    pub dimensions: Vec<String>,
    pub chunk_provider: S,
}

impl<S: ChunkStore + Send + Sync> Array<S> {
    pub async fn fetch(&self) -> anyhow::Result<Arc<dyn NdArray>> {
        //Fetch all chunks, concat using arrow
        let all_chunks_res = self.chunk_provider.chunks().collect::<Vec<_>>().await;
        let all_chunks = all_chunks_res
            .into_iter()
            .collect::<anyhow::Result<Vec<_>>>()?;

        // Concat the arrays
        let array = arrow::compute::concat(
            &all_chunks
                .iter()
                .map(|part| part.as_ref())
                .collect::<Vec<_>>(),
        )?;

        nd::try_from_arrow_ref(array, &self.array_shape, &self.dimensions).ok_or(anyhow::anyhow!(
            "Unable to convert stored array to nd-array using shared arrow buffer."
        ))
    }

    pub fn as_chunked_arrow_stream(
        &self,
    ) -> impl futures::Stream<Item = anyhow::Result<arrow::array::ArrayRef>> + '_ {
        self.chunk_provider.chunks()
    }
}

pub fn from_vec<T>(
    vec: Vec<T::Native>,
    dimensions: Vec<String>,
    shape: Vec<usize>,
) -> Array<Arc<dyn ChunkStore>>
where
    T: ArrowPrimitiveType,
    PrimitiveArray<T>: From<Vec<T::Native>>,
{
    let array = PrimitiveArray::<T>::from(vec);
    Array {
        array_datatype: T::DATA_TYPE,
        array_shape: shape,
        dimensions,
        chunk_provider: Arc::new(InMemoryChunkStore::new(vec![Arc::new(array)])),
    }
}

/// Creates an array from an `ndarray` value with named dimensions.
pub fn from_ndarray<T, S, D>(
    array: ndarray::ArrayBase<S, D>,
    dimensions: Vec<String>,
) -> anyhow::Result<Array<Arc<dyn ChunkStore>>>
where
    S: ndarray::Data<Elem = T>,
    D: ndarray::Dimension,
    Vec<T>: ArrayValues,
    T: Clone,
{
    let shape = array.shape().to_vec();
    if dimensions.len() != shape.len() {
        return Err(anyhow::anyhow!(
            "dimension name count ({}) does not match array rank ({})",
            dimensions.len(),
            shape.len()
        ));
    }

    let values = array.iter().cloned().collect::<Vec<T>>();
    Ok(from_vec_auto(values, dimensions, shape))
}

/// Converts owned vector values into an Arrow array with a known data type.
pub trait ArrayValues {
    fn to_array_ref(self) -> arrow::array::ArrayRef;
    fn data_type() -> arrow::datatypes::DataType;
    fn len(&self) -> usize;
}

macro_rules! impl_array_values_primitive {
    ($ty:ty, $array:ty, $dtype:expr) => {
        impl ArrayValues for Vec<$ty> {
            fn to_array_ref(self) -> arrow::array::ArrayRef {
                Arc::new(<$array>::from(self))
            }

            fn data_type() -> arrow::datatypes::DataType {
                $dtype
            }
            fn len(&self) -> usize {
                self.len()
            }
        }
    };
}

impl_array_values_primitive!(
    i8,
    arrow::array::Int8Array,
    arrow::datatypes::DataType::Int8
);
impl_array_values_primitive!(
    i16,
    arrow::array::Int16Array,
    arrow::datatypes::DataType::Int16
);
impl_array_values_primitive!(
    i32,
    arrow::array::Int32Array,
    arrow::datatypes::DataType::Int32
);
impl_array_values_primitive!(
    i64,
    arrow::array::Int64Array,
    arrow::datatypes::DataType::Int64
);
impl_array_values_primitive!(
    u8,
    arrow::array::UInt8Array,
    arrow::datatypes::DataType::UInt8
);
impl_array_values_primitive!(
    u16,
    arrow::array::UInt16Array,
    arrow::datatypes::DataType::UInt16
);
impl_array_values_primitive!(
    u32,
    arrow::array::UInt32Array,
    arrow::datatypes::DataType::UInt32
);
impl_array_values_primitive!(
    u64,
    arrow::array::UInt64Array,
    arrow::datatypes::DataType::UInt64
);
impl_array_values_primitive!(
    f32,
    arrow::array::Float32Array,
    arrow::datatypes::DataType::Float32
);
impl_array_values_primitive!(
    f64,
    arrow::array::Float64Array,
    arrow::datatypes::DataType::Float64
);

impl ArrayValues for Vec<bool> {
    fn to_array_ref(self) -> arrow::array::ArrayRef {
        Arc::new(arrow::array::BooleanArray::from(self))
    }

    fn data_type() -> arrow::datatypes::DataType {
        arrow::datatypes::DataType::Boolean
    }
    fn len(&self) -> usize {
        self.len()
    }
}

impl ArrayValues for Vec<String> {
    fn to_array_ref(self) -> arrow::array::ArrayRef {
        Arc::new(arrow::array::StringArray::from(self))
    }

    fn data_type() -> arrow::datatypes::DataType {
        arrow::datatypes::DataType::Utf8
    }
    fn len(&self) -> usize {
        self.len()
    }
}

impl<'a> ArrayValues for Vec<&'a str> {
    fn to_array_ref(self) -> arrow::array::ArrayRef {
        Arc::new(arrow::array::StringArray::from(self))
    }

    fn data_type() -> arrow::datatypes::DataType {
        arrow::datatypes::DataType::Utf8
    }
    fn len(&self) -> usize {
        self.len()
    }
}

impl ArrayValues for Vec<Vec<u8>> {
    fn to_array_ref(self) -> arrow::array::ArrayRef {
        let array = arrow::array::BinaryArray::from_iter_values(self.iter().map(|v| v.as_slice()));
        Arc::new(array)
    }

    fn data_type() -> arrow::datatypes::DataType {
        arrow::datatypes::DataType::Binary
    }
    fn len(&self) -> usize {
        self.len()
    }
}

impl ArrayValues for Vec<chrono::DateTime<chrono::Utc>> {
    fn to_array_ref(self) -> arrow::array::ArrayRef {
        let values = self
            .into_iter()
            .map(|value| value.timestamp_millis())
            .collect::<Vec<_>>();
        Arc::new(arrow::array::TimestampMillisecondArray::from(values))
    }

    fn data_type() -> arrow::datatypes::DataType {
        arrow::datatypes::DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None)
    }
    fn len(&self) -> usize {
        self.len()
    }
}

impl ArrayValues for Vec<chrono::NaiveDateTime> {
    fn to_array_ref(self) -> arrow::array::ArrayRef {
        let values = self
            .into_iter()
            .map(|value| value.and_utc().timestamp_millis())
            .collect::<Vec<_>>();
        Arc::new(arrow::array::TimestampMillisecondArray::from(values))
    }

    fn data_type() -> arrow::datatypes::DataType {
        arrow::datatypes::DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None)
    }
    fn len(&self) -> usize {
        self.len()
    }
}

/// Creates a single-chunk array from owned values with an inferred Arrow data type.
pub fn from_vec_auto<V>(
    vec: V,
    dimensions: Vec<String>,
    shape: Vec<usize>,
) -> Array<Arc<dyn ChunkStore>>
where
    V: ArrayValues,
{
    Array {
        array_datatype: V::data_type(),
        array_shape: shape,
        dimensions,
        chunk_provider: Arc::new(InMemoryChunkStore::new(vec![vec.to_array_ref()])),
    }
}

pub fn from_vec_string(
    vec: Vec<String>,
    dimensions: Vec<String>,
    shape: Vec<usize>,
) -> Array<Arc<dyn ChunkStore>> {
    from_vec_auto(vec, dimensions, shape)
}

pub fn from_vec_binary(
    vec: Vec<Vec<u8>>,
    dimensions: Vec<String>,
    shape: Vec<usize>,
) -> Array<Arc<dyn ChunkStore>> {
    from_vec_auto(vec, dimensions, shape)
}

#[cfg(test)]
mod tests {
    use super::Array;
    use super::from_ndarray;
    use super::from_vec_auto;
    use crate::array::data_type::I32Type;
    use crate::array::nd::AsNdArray;
    use crate::array::store::ChunkStore;
    use arrow::array::Array as ArrowArray;
    use arrow::array::{
        ArrayRef, BinaryArray, BooleanArray, Float32Array, Float64Array, Int8Array, Int16Array,
        Int32Array, Int64Array, StringArray, TimestampMillisecondArray, UInt8Array, UInt16Array,
        UInt32Array, UInt64Array,
    };
    use arrow::datatypes::{DataType, TimeUnit};
    use chrono::{NaiveDate, TimeZone};
    use futures::TryStreamExt;
    use futures::stream::{self, BoxStream, StreamExt};
    use ndarray::array;
    use std::sync::Arc;

    #[derive(Debug, Clone)]
    struct TestChunkStore {
        chunks: Vec<ArrayRef>,
    }

    impl TestChunkStore {
        fn new(chunks: Vec<ArrayRef>) -> Self {
            Self { chunks }
        }
    }

    #[async_trait::async_trait]
    impl ChunkStore for TestChunkStore {
        fn chunks(&self) -> BoxStream<'static, anyhow::Result<ArrayRef>> {
            let chunks = self.chunks.clone();
            stream::iter(chunks.into_iter().map(Ok)).boxed()
        }

        async fn fetch_chunk(
            &self,
            _start: usize,
            _len: usize,
        ) -> anyhow::Result<Option<ArrayRef>> {
            Ok(None)
        }
    }

    #[tokio::test]
    async fn fetch_concats_chunks_into_ndarray() -> anyhow::Result<()> {
        let chunks = vec![
            Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef,
            Arc::new(Int32Array::from(vec![3, 4, 5])) as ArrayRef,
        ];
        let array = Array {
            array_datatype: arrow::datatypes::DataType::Int32,
            array_shape: vec![5],
            dimensions: vec!["x".to_string()],
            chunk_provider: TestChunkStore::new(chunks),
        };

        let nd = array.fetch().await?;
        assert_eq!(nd.shape(), &[5]);
        assert_eq!(nd.dimensions(), vec!["x"]);

        let values = nd
            .as_primitive_nd::<I32Type>()
            .expect("expected i32 ndarray");
        let collected: Vec<i32> = values.iter().cloned().collect();
        assert_eq!(collected, vec![1, 2, 3, 4, 5]);

        Ok(())
    }

    #[tokio::test]
    async fn fetch_returns_error_when_no_chunks_available() {
        let array = Array {
            array_datatype: arrow::datatypes::DataType::Int32,
            array_shape: vec![0],
            dimensions: vec!["x".to_string()],
            chunk_provider: TestChunkStore::new(Vec::new()),
        };

        assert!(array.fetch().await.is_err());
    }

    #[tokio::test]
    async fn from_vec_auto_builds_timestamp_array() -> anyhow::Result<()> {
        let values = vec![
            chrono::Utc
                .with_ymd_and_hms(2024, 1, 1, 0, 0, 0)
                .single()
                .expect("valid timestamp"),
            chrono::Utc
                .with_ymd_and_hms(2024, 1, 1, 0, 0, 1)
                .single()
                .expect("valid timestamp"),
        ];

        let array = from_vec_auto(values.clone(), vec!["t".to_string()], vec![2]);
        assert_eq!(
            array.array_datatype,
            DataType::Timestamp(TimeUnit::Millisecond, None)
        );

        let chunks = array
            .chunk_provider
            .chunks()
            .try_collect::<Vec<_>>()
            .await?;
        let ts = chunks[0]
            .as_any()
            .downcast_ref::<arrow::array::TimestampMillisecondArray>()
            .expect("timestamp millisecond array");
        assert_eq!(ts.value(0), values[0].timestamp_millis());
        assert_eq!(ts.value(1), values[1].timestamp_millis());

        Ok(())
    }

    #[tokio::test]
    async fn from_vec_auto_builds_primitive_arrays() -> anyhow::Result<()> {
        let i8_values = vec![-1i8, 2i8];
        let array = from_vec_auto(i8_values.clone(), vec!["x".to_string()], vec![2]);
        assert_eq!(array.array_datatype, DataType::Int8);
        let chunk = array
            .chunk_provider
            .chunks()
            .try_collect::<Vec<_>>()
            .await?[0]
            .clone();
        let typed = chunk
            .as_any()
            .downcast_ref::<Int8Array>()
            .expect("int8 array");
        assert_eq!(
            (0..typed.len()).map(|i| typed.value(i)).collect::<Vec<_>>(),
            i8_values
        );

        let i16_values = vec![-3i16, 4i16];
        let array = from_vec_auto(i16_values.clone(), vec!["x".to_string()], vec![2]);
        assert_eq!(array.array_datatype, DataType::Int16);
        let chunk = array
            .chunk_provider
            .chunks()
            .try_collect::<Vec<_>>()
            .await?[0]
            .clone();
        let typed = chunk
            .as_any()
            .downcast_ref::<Int16Array>()
            .expect("int16 array");
        assert_eq!(
            (0..typed.len()).map(|i| typed.value(i)).collect::<Vec<_>>(),
            i16_values
        );

        let i32_values = vec![-5i32, 6i32];
        let array = from_vec_auto(i32_values.clone(), vec!["x".to_string()], vec![2]);
        assert_eq!(array.array_datatype, DataType::Int32);
        let chunk = array
            .chunk_provider
            .chunks()
            .try_collect::<Vec<_>>()
            .await?[0]
            .clone();
        let typed = chunk
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("int32 array");
        assert_eq!(
            (0..typed.len()).map(|i| typed.value(i)).collect::<Vec<_>>(),
            i32_values
        );

        let i64_values = vec![-7i64, 8i64];
        let array = from_vec_auto(i64_values.clone(), vec!["x".to_string()], vec![2]);
        assert_eq!(array.array_datatype, DataType::Int64);
        let chunk = array
            .chunk_provider
            .chunks()
            .try_collect::<Vec<_>>()
            .await?[0]
            .clone();
        let typed = chunk
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("int64 array");
        assert_eq!(
            (0..typed.len()).map(|i| typed.value(i)).collect::<Vec<_>>(),
            i64_values
        );

        let u8_values = vec![1u8, 2u8];
        let array = from_vec_auto(u8_values.clone(), vec!["x".to_string()], vec![2]);
        assert_eq!(array.array_datatype, DataType::UInt8);
        let chunk = array
            .chunk_provider
            .chunks()
            .try_collect::<Vec<_>>()
            .await?[0]
            .clone();
        let typed = chunk
            .as_any()
            .downcast_ref::<UInt8Array>()
            .expect("uint8 array");
        assert_eq!(
            (0..typed.len()).map(|i| typed.value(i)).collect::<Vec<_>>(),
            u8_values
        );

        let u16_values = vec![3u16, 4u16];
        let array = from_vec_auto(u16_values.clone(), vec!["x".to_string()], vec![2]);
        assert_eq!(array.array_datatype, DataType::UInt16);
        let chunk = array
            .chunk_provider
            .chunks()
            .try_collect::<Vec<_>>()
            .await?[0]
            .clone();
        let typed = chunk
            .as_any()
            .downcast_ref::<UInt16Array>()
            .expect("uint16 array");
        assert_eq!(
            (0..typed.len()).map(|i| typed.value(i)).collect::<Vec<_>>(),
            u16_values
        );

        let u32_values = vec![5u32, 6u32];
        let array = from_vec_auto(u32_values.clone(), vec!["x".to_string()], vec![2]);
        assert_eq!(array.array_datatype, DataType::UInt32);
        let chunk = array
            .chunk_provider
            .chunks()
            .try_collect::<Vec<_>>()
            .await?[0]
            .clone();
        let typed = chunk
            .as_any()
            .downcast_ref::<UInt32Array>()
            .expect("uint32 array");
        assert_eq!(
            (0..typed.len()).map(|i| typed.value(i)).collect::<Vec<_>>(),
            u32_values
        );

        let u64_values = vec![7u64, 8u64];
        let array = from_vec_auto(u64_values.clone(), vec!["x".to_string()], vec![2]);
        assert_eq!(array.array_datatype, DataType::UInt64);
        let chunk = array
            .chunk_provider
            .chunks()
            .try_collect::<Vec<_>>()
            .await?[0]
            .clone();
        let typed = chunk
            .as_any()
            .downcast_ref::<UInt64Array>()
            .expect("uint64 array");
        assert_eq!(
            (0..typed.len()).map(|i| typed.value(i)).collect::<Vec<_>>(),
            u64_values
        );

        let f32_values = vec![1.25f32, 2.5f32];
        let array = from_vec_auto(f32_values.clone(), vec!["x".to_string()], vec![2]);
        assert_eq!(array.array_datatype, DataType::Float32);
        let chunk = array
            .chunk_provider
            .chunks()
            .try_collect::<Vec<_>>()
            .await?[0]
            .clone();
        let typed = chunk
            .as_any()
            .downcast_ref::<Float32Array>()
            .expect("float32 array");
        assert_eq!(
            (0..typed.len()).map(|i| typed.value(i)).collect::<Vec<_>>(),
            f32_values
        );

        let f64_values = vec![3.5f64, 4.75f64];
        let array = from_vec_auto(f64_values.clone(), vec!["x".to_string()], vec![2]);
        assert_eq!(array.array_datatype, DataType::Float64);
        let chunk = array
            .chunk_provider
            .chunks()
            .try_collect::<Vec<_>>()
            .await?[0]
            .clone();
        let typed = chunk
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("float64 array");
        assert_eq!(
            (0..typed.len()).map(|i| typed.value(i)).collect::<Vec<_>>(),
            f64_values
        );

        Ok(())
    }

    #[tokio::test]
    async fn from_vec_auto_builds_boolean_and_string_arrays() -> anyhow::Result<()> {
        let bool_values = vec![true, false, true];
        let array = from_vec_auto(bool_values.clone(), vec!["x".to_string()], vec![3]);
        assert_eq!(array.array_datatype, DataType::Boolean);
        let chunk = array
            .chunk_provider
            .chunks()
            .try_collect::<Vec<_>>()
            .await?[0]
            .clone();
        let typed = chunk
            .as_any()
            .downcast_ref::<BooleanArray>()
            .expect("bool array");
        assert_eq!(
            (0..typed.len()).map(|i| typed.value(i)).collect::<Vec<_>>(),
            bool_values
        );

        let str_values = vec!["a", "b", "c"];
        let array = from_vec_auto(str_values.to_vec(), vec!["x".to_string()], vec![3]);
        assert_eq!(array.array_datatype, DataType::Utf8);
        let chunk = array
            .chunk_provider
            .chunks()
            .try_collect::<Vec<_>>()
            .await?[0]
            .clone();
        let typed = chunk
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("string array");
        assert_eq!(
            (0..typed.len())
                .map(|i| typed.value(i).to_string())
                .collect::<Vec<_>>(),
            str_values.iter().map(|v| v.to_string()).collect::<Vec<_>>()
        );

        let string_values = vec!["alpha".to_string(), "beta".to_string()];
        let array = from_vec_auto(string_values.clone(), vec!["x".to_string()], vec![2]);
        assert_eq!(array.array_datatype, DataType::Utf8);
        let chunk = array
            .chunk_provider
            .chunks()
            .try_collect::<Vec<_>>()
            .await?[0]
            .clone();
        let typed = chunk
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("string array");
        assert_eq!(
            (0..typed.len())
                .map(|i| typed.value(i).to_string())
                .collect::<Vec<_>>(),
            string_values
        );

        Ok(())
    }

    #[tokio::test]
    async fn from_vec_auto_builds_binary_array() -> anyhow::Result<()> {
        let values = vec![vec![0u8, 1u8], vec![2u8, 3u8, 4u8]];
        let array = from_vec_auto(values.clone(), vec!["x".to_string()], vec![2]);
        assert_eq!(array.array_datatype, DataType::Binary);
        let chunk = array
            .chunk_provider
            .chunks()
            .try_collect::<Vec<_>>()
            .await?[0]
            .clone();
        let typed = chunk
            .as_any()
            .downcast_ref::<BinaryArray>()
            .expect("binary array");
        let collected = (0..typed.len())
            .map(|i| typed.value(i).to_vec())
            .collect::<Vec<_>>();
        assert_eq!(collected, values);
        Ok(())
    }

    #[tokio::test]
    async fn from_vec_auto_builds_naive_timestamp_array() -> anyhow::Result<()> {
        let values = vec![
            NaiveDate::from_ymd_opt(2024, 1, 1)
                .unwrap()
                .and_hms_milli_opt(0, 0, 0, 0)
                .unwrap(),
            NaiveDate::from_ymd_opt(2024, 1, 1)
                .unwrap()
                .and_hms_milli_opt(0, 0, 0, 1)
                .unwrap(),
        ];

        let array = from_vec_auto(values.clone(), vec!["t".to_string()], vec![2]);
        assert_eq!(
            array.array_datatype,
            DataType::Timestamp(TimeUnit::Millisecond, None)
        );

        let chunk = array
            .chunk_provider
            .chunks()
            .try_collect::<Vec<_>>()
            .await?[0]
            .clone();
        let ts = chunk
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .expect("timestamp millisecond array");
        assert_eq!(ts.value(0), values[0].and_utc().timestamp_millis());
        assert_eq!(ts.value(1), values[1].and_utc().timestamp_millis());

        Ok(())
    }

    #[tokio::test]
    async fn from_ndarray_builds_array() -> anyhow::Result<()> {
        let values = array![[1i32, 2i32], [3i32, 4i32]];
        let array = from_ndarray(values, vec!["x".to_string(), "y".to_string()])?;
        assert_eq!(array.array_shape, vec![2, 2]);
        assert_eq!(array.array_datatype, DataType::Int32);

        let chunk = array
            .chunk_provider
            .chunks()
            .try_collect::<Vec<_>>()
            .await?[0]
            .clone();
        let typed = chunk
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("int32 array");
        assert_eq!(
            (0..typed.len()).map(|i| typed.value(i)).collect::<Vec<_>>(),
            vec![1, 2, 3, 4]
        );

        Ok(())
    }

    #[tokio::test]
    async fn from_ndarray_rejects_dimension_mismatch() {
        let values = array![[1i32, 2i32], [3i32, 4i32]];
        let err = from_ndarray(values, vec!["x".to_string()]).expect_err("expected error");
        assert!(err.to_string().contains("dimension name count"));
    }
}
