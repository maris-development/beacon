use std::sync::Arc;

use arrow::datatypes::TimeUnit;
use beacon_nd_arrow::array::backend::ArrayBackend;
use beacon_nd_arrow::array::compat_typings::TimestampNanosecond;
use object_store::ObjectStore;

use crate::{
    array::{
        AtlasArrayBackend,
        compat::AtlasArrowCompat,
        io_cache::IoCache,
        layout::{ArrayLayout, ArrayLayouts},
    },
    arrow_object_store::ArrowObjectStoreReader,
};

/// Reads chunked ND Arrow arrays and their layout metadata from object storage.
pub struct ArrayReader<S: ObjectStore + Clone> {
    array_reader: Arc<ArrowObjectStoreReader<S>>,
    layouts: ArrayLayouts,
    array_datatype: arrow::datatypes::DataType,
    io_cache: Arc<IoCache>,
}

impl<S: ObjectStore + Clone> ArrayReader<S> {
    pub async fn new_with_cache(
        store: S,
        layout_path: object_store::path::Path,
        array_path: object_store::path::Path,
        io_cache: Arc<IoCache>,
    ) -> anyhow::Result<Self> {
        let array_reader = Arc::new(ArrowObjectStoreReader::new(store.clone(), array_path).await?);

        let array_field = array_reader.schema();
        let array_datatype = array_field.field(0).data_type().clone();

        Ok(Self {
            array_reader,
            layouts: ArrayLayouts::from_object(store.clone(), layout_path).await?,
            array_datatype,
            io_cache,
        })
    }

    /// Returns the loaded array layouts.
    pub fn layouts(&self) -> &ArrayLayouts {
        &self.layouts
    }

    /// Returns the array datatype stored in this reader.
    pub fn array_datatype(&self) -> &arrow::datatypes::DataType {
        &self.array_datatype
    }

    /// Returns the shared IO cache.
    pub fn io_cache(&self) -> &Arc<IoCache> {
        &self.io_cache
    }

    fn lazy_dataset_array<A: AtlasArrowCompat>(
        &self,
        layout: Arc<ArrayLayout>,
    ) -> anyhow::Result<Arc<dyn beacon_nd_arrow::array::NdArrowArray>> {
        let array = AtlasArrayBackend::<S, A>::new(
            self.array_reader.clone(),
            self.io_cache.clone(),
            layout,
        );

        array.into_dyn_array()
    }

    /// Returns a chunked array for a dataset index, if present.
    pub fn read_dataset_array(
        &self,
        dataset_index: u32,
    ) -> Option<anyhow::Result<Arc<dyn beacon_nd_arrow::array::NdArrowArray>>> {
        let layout = if let Some(layout) = self.layouts.find_dataset_array_layout(dataset_index) {
            layout.clone()
        } else {
            return None;
        };
        let layout = Arc::new(layout);

        match &self.array_datatype {
            arrow::datatypes::DataType::Boolean => Some(self.lazy_dataset_array::<bool>(layout)),
            arrow::datatypes::DataType::Int8 => Some(self.lazy_dataset_array::<i8>(layout)),
            arrow::datatypes::DataType::Int16 => Some(self.lazy_dataset_array::<i16>(layout)),
            arrow::datatypes::DataType::Int32 => Some(self.lazy_dataset_array::<i32>(layout)),
            arrow::datatypes::DataType::Int64 => Some(self.lazy_dataset_array::<i64>(layout)),
            arrow::datatypes::DataType::UInt8 => Some(self.lazy_dataset_array::<u8>(layout)),
            arrow::datatypes::DataType::UInt16 => Some(self.lazy_dataset_array::<u16>(layout)),
            arrow::datatypes::DataType::UInt32 => Some(self.lazy_dataset_array::<u32>(layout)),
            arrow::datatypes::DataType::UInt64 => Some(self.lazy_dataset_array::<u64>(layout)),
            arrow::datatypes::DataType::Float32 => Some(self.lazy_dataset_array::<f32>(layout)),
            arrow::datatypes::DataType::Float64 => Some(self.lazy_dataset_array::<f64>(layout)),
            arrow::datatypes::DataType::Timestamp(TimeUnit::Nanosecond, _) => {
                Some(self.lazy_dataset_array::<TimestampNanosecond>(layout))
            }
            arrow::datatypes::DataType::Utf8 => Some(self.lazy_dataset_array::<String>(layout)),
            _ => {
                return Some(Err(anyhow::anyhow!(
                    "unsupported array datatype: {:?}",
                    self.array_datatype
                )));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::ArrayReader;
    use crate::array::{io_cache::IoCache, writer::ArrayWriter};
    use arrow::array::{Array as ArrowArray, BooleanArray, Int32Array, TimestampNanosecondArray};
    use beacon_nd_arrow::array::compat_typings::TimestampNanosecond;
    use beacon_nd_arrow::array::{NdArrowArray, NdArrowArrayDispatch, subset::ArraySubset};
    use object_store::{ObjectStore, memory::InMemory, path::Path};
    use std::sync::Arc;

    async fn build_reader(
        array: Arc<dyn NdArrowArray>,
        chunk_size: usize,
    ) -> anyhow::Result<ArrayReader<Arc<dyn ObjectStore>>> {
        let store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
        let array_path = Path::from("test-array.arrow");
        let layout_path = Path::from("test-layout.arrow");

        let mut writer = ArrayWriter::new(
            store.clone(),
            array_path.clone(),
            layout_path.clone(),
            array.data_type(),
            chunk_size,
        )?;
        writer.append_array(7, array).await?;
        writer.finalize().await?;

        ArrayReader::new_with_cache(
            store,
            layout_path,
            array_path,
            Arc::new(IoCache::new(1024 * 1024)),
        )
        .await
    }

    #[tokio::test]
    async fn read_dataset_array_returns_lazy_boolean_array() -> anyhow::Result<()> {
        let source = Arc::new(NdArrowArrayDispatch::new_in_mem(
            vec![true, false, true],
            vec![3],
            vec!["x".to_string()],
            None,
        )?) as Arc<dyn NdArrowArray>;
        let reader = build_reader(source, 2).await?;

        let array = reader
            .read_dataset_array(7)
            .transpose()?
            .expect("dataset array exists");
        let materialized = array.as_arrow_array_ref().await?;
        let values = materialized
            .as_any()
            .downcast_ref::<BooleanArray>()
            .expect("boolean array");

        assert_eq!(values.len(), 3);
        assert!(values.value(0));
        assert!(!values.value(1));
        assert!(values.value(2));

        Ok(())
    }

    #[tokio::test]
    async fn read_dataset_array_returns_lazy_timestamp_array() -> anyhow::Result<()> {
        let source = Arc::new(NdArrowArrayDispatch::new_in_mem(
            vec![TimestampNanosecond(100), TimestampNanosecond(200)],
            vec![2],
            vec!["time".to_string()],
            None,
        )?) as Arc<dyn NdArrowArray>;
        let reader = build_reader(source, 2).await?;

        let array = reader
            .read_dataset_array(7)
            .transpose()?
            .expect("dataset array exists");
        let materialized = array.as_arrow_array_ref().await?;
        let values = materialized
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .expect("timestamp nanosecond array");

        assert_eq!(values.len(), 2);
        assert_eq!(values.value(0), 100);
        assert_eq!(values.value(1), 200);

        Ok(())
    }

    #[tokio::test]
    async fn read_dataset_array_subset_spans_chunk_boundaries() -> anyhow::Result<()> {
        let source = Arc::new(NdArrowArrayDispatch::new_in_mem(
            (0..12).collect::<Vec<i32>>(),
            vec![3, 4],
            vec!["x".to_string(), "y".to_string()],
            None,
        )?) as Arc<dyn NdArrowArray>;
        let reader = build_reader(source, 5).await?;

        let array = reader
            .read_dataset_array(7)
            .transpose()?
            .expect("dataset array exists");
        let subset = array
            .subset(ArraySubset {
                start: vec![1, 1],
                shape: vec![2, 2],
            })
            .await?;
        let materialized = subset.as_arrow_array_ref().await?;
        let values = materialized
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("int32 array");

        assert_eq!(subset.shape(), vec![2, 2]);
        assert_eq!(subset.dimensions(), vec!["x".to_string(), "y".to_string()]);
        assert_eq!(values.len(), 4);
        assert_eq!(values.values(), &[5, 6, 9, 10]);

        Ok(())
    }
}
