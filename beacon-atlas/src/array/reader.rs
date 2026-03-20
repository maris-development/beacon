use std::sync::Arc;

use arrow::{
    array::{ArrayRef, UInt32Array},
    datatypes::{DataType, Field, Schema, TimeUnit},
    record_batch::RecordBatch,
};
use beacon_nd_arrow::array::backend::ArrayBackend;
use beacon_nd_arrow::array::compat_typings::TimestampNanosecond;
use object_store::ObjectStore;

use crate::{
    array::{
        AtlasArrayBackend,
        compat::AtlasArrowCompat,
        io_cache::IoCache,
        layout::{ArrayLayout, ArrayLayouts},
        statistics::StatisticsReader,
    },
    arrow_object_store::ArrowObjectStoreReader,
};

/// Reads chunked ND Arrow arrays and their layout metadata from object storage.
pub struct ArrayReader<S: ObjectStore + Clone> {
    store: S,
    array_reader: Arc<ArrowObjectStoreReader<S>>,
    layouts: ArrayLayouts,
    array_datatype: arrow::datatypes::DataType,
    io_cache: Arc<IoCache>,
    statistics_reader: tokio::sync::OnceCell<Option<StatisticsReader>>,
    statistics_path: Option<object_store::path::Path>,
}

impl<S: ObjectStore + Clone> std::fmt::Debug for ArrayReader<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ArrayReader")
            .field("array_datatype", &self.array_datatype)
            .finish()
    }
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
            store: store.clone(),
            array_reader,
            layouts: ArrayLayouts::from_object(store.clone(), layout_path).await?,
            array_datatype,
            io_cache,
            statistics_reader: tokio::sync::OnceCell::new(),
            statistics_path: None,
        })
    }

    pub async fn new_with_cache_and_statistics(
        store: S,
        layout_path: object_store::path::Path,
        array_path: object_store::path::Path,
        statistics_path: object_store::path::Path,
        io_cache: Arc<IoCache>,
    ) -> anyhow::Result<Self> {
        let mut reader =
            Self::new_with_cache(store.clone(), layout_path, array_path, io_cache).await?;
        reader.statistics_path = Some(statistics_path.clone());

        Ok(reader)
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

    /// Returns statistics for all datasets in this column, if available.
    ///
    /// The resulting batch includes a `dataset_index` column appended from layout metadata.
    pub async fn read_statistics_batch(&self) -> anyhow::Result<Option<RecordBatch>> {
        let Some(statistics_path) = &self.statistics_path else {
            return Ok(None);
        };
        let store = self.store.clone();

        let Some(statistics_reader) = self
            .statistics_reader
            .get_or_try_init(|| async move {
                match store.head(statistics_path).await {
                    Ok(_) => {
                        Some(StatisticsReader::new(store.clone(), statistics_path.clone()).await)
                            .transpose()
                    }
                    Err(object_store::Error::NotFound { .. }) => Ok(None),
                    Err(err) => Err(anyhow::anyhow!(err)),
                }
            })
            .await?
        else {
            return Ok(None);
        };

        let statistics_batch = statistics_reader.batch().clone();
        if statistics_batch
            .schema()
            .field_with_name("dataset_index")
            .is_ok()
        {
            return Ok(Some(statistics_batch));
        }

        let dataset_indices = self.layouts.dataset_indices();
        anyhow::ensure!(
            statistics_batch.num_rows() == dataset_indices.len(),
            "statistics row count ({}) does not match layout row count ({})",
            statistics_batch.num_rows(),
            dataset_indices.len()
        );

        let dataset_index_array = Arc::new(UInt32Array::from(dataset_indices)) as ArrayRef;
        let mut columns = statistics_batch.columns().to_vec();
        columns.push(dataset_index_array);

        let mut fields: Vec<Field> = statistics_batch
            .schema()
            .fields()
            .iter()
            .map(|field| field.as_ref().clone())
            .collect();
        fields.push(Field::new("dataset_index", DataType::UInt32, false));

        let schema = Arc::new(Schema::new(fields));
        Ok(Some(RecordBatch::try_new(schema, columns)?))
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
            DataType::Boolean => Some(self.lazy_dataset_array::<bool>(layout)),
            DataType::Int8 => Some(self.lazy_dataset_array::<i8>(layout)),
            DataType::Int16 => Some(self.lazy_dataset_array::<i16>(layout)),
            DataType::Int32 => Some(self.lazy_dataset_array::<i32>(layout)),
            DataType::Int64 => Some(self.lazy_dataset_array::<i64>(layout)),
            DataType::UInt8 => Some(self.lazy_dataset_array::<u8>(layout)),
            DataType::UInt16 => Some(self.lazy_dataset_array::<u16>(layout)),
            DataType::UInt32 => Some(self.lazy_dataset_array::<u32>(layout)),
            DataType::UInt64 => Some(self.lazy_dataset_array::<u64>(layout)),
            DataType::Float32 => Some(self.lazy_dataset_array::<f32>(layout)),
            DataType::Float64 => Some(self.lazy_dataset_array::<f64>(layout)),
            DataType::Timestamp(TimeUnit::Nanosecond, _) => {
                Some(self.lazy_dataset_array::<TimestampNanosecond>(layout))
            }
            DataType::Utf8 => Some(self.lazy_dataset_array::<String>(layout)),
            _ => Some(Err(anyhow::anyhow!(
                "unsupported array datatype: {:?}",
                self.array_datatype
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::ArrayReader;
    use crate::array::{io_cache::IoCache, writer::ArrayWriter};
    use crate::consts;
    use arrow::array::{
        Array as ArrowArray, BooleanArray, Int32Array, TimestampNanosecondArray, UInt32Array,
    };
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
        let statistics_path = Path::from("test-statistics.arrow");

        let mut writer = ArrayWriter::new(
            store.clone(),
            array_path.clone(),
            layout_path.clone(),
            statistics_path,
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

    async fn build_reader_with_statistics(
        datasets: Vec<(u32, Arc<dyn NdArrowArray>)>,
        chunk_size: usize,
    ) -> anyhow::Result<ArrayReader<Arc<dyn ObjectStore>>> {
        let store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
        let array_path = Path::from("test-array.arrow");
        let layout_path = Path::from("test-layout.arrow");
        let statistics_path = Path::from(consts::STATISTICS_FILE_NAME);

        let data_type = datasets
            .first()
            .ok_or_else(|| anyhow::anyhow!("at least one dataset is required"))?
            .1
            .data_type();

        let mut writer = ArrayWriter::new(
            store.clone(),
            array_path.clone(),
            layout_path.clone(),
            statistics_path.clone(),
            data_type,
            chunk_size,
        )?;

        for (dataset_index, dataset) in datasets {
            writer.append_array(dataset_index, dataset).await?;
        }
        writer.finalize().await?;

        ArrayReader::new_with_cache_and_statistics(
            store,
            layout_path,
            array_path,
            statistics_path,
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

    #[tokio::test]
    async fn read_statistics_batch_appends_dataset_index_column() -> anyhow::Result<()> {
        let first = Arc::new(NdArrowArrayDispatch::new_in_mem(
            vec![1_i32, 9, 3],
            vec![3],
            vec!["x".to_string()],
            None,
        )?) as Arc<dyn NdArrowArray>;
        let second = Arc::new(NdArrowArrayDispatch::new_in_mem(
            vec![10_i32, 11],
            vec![2],
            vec!["x".to_string()],
            None,
        )?) as Arc<dyn NdArrowArray>;

        let reader = build_reader_with_statistics(vec![(7, first), (15, second)], 8).await?;
        let statistics = reader
            .read_statistics_batch()
            .await?
            .expect("statistics batch should be present");

        assert_eq!(statistics.num_rows(), 2);
        assert!(statistics.schema().field_with_name("dataset_index").is_ok());

        let dataset_index = statistics
            .column(statistics.schema().index_of("dataset_index")?)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .expect("dataset_index must be u32");
        assert_eq!(dataset_index.values(), &[7, 15]);

        Ok(())
    }

    #[tokio::test]
    async fn read_statistics_batch_returns_none_when_statistics_file_missing() -> anyhow::Result<()>
    {
        let store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
        let array_path = Path::from("test-array.arrow");
        let layout_path = Path::from("test-layout.arrow");
        let written_statistics_path = Path::from(consts::STATISTICS_FILE_NAME);

        let source = Arc::new(NdArrowArrayDispatch::new_in_mem(
            vec![1_i32, 2, 3],
            vec![3],
            vec!["x".to_string()],
            None,
        )?) as Arc<dyn NdArrowArray>;

        let mut writer = ArrayWriter::new(
            store.clone(),
            array_path.clone(),
            layout_path.clone(),
            written_statistics_path,
            source.data_type(),
            8,
        )?;
        writer.append_array(3, source).await?;
        writer.finalize().await?;

        let reader = ArrayReader::new_with_cache_and_statistics(
            store,
            layout_path,
            array_path,
            Path::from("missing-statistics.arrow"),
            Arc::new(IoCache::new(1024 * 1024)),
        )
        .await?;

        assert!(reader.read_statistics_batch().await?.is_none());

        Ok(())
    }
}
