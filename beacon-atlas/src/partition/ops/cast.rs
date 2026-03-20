use std::sync::Arc;

use arrow::{
    compute::{CastOptions, cast_with_options},
    datatypes::{DataType, TimeUnit},
};
use beacon_nd_arrow::array::{
    NdArrowArray, NdArrowArrayDispatch, compat_typings::TimestampNanosecond,
};
use object_store::ObjectStore;

use crate::{
    array::{compat::AtlasArrowCompat, io_cache::IoCache},
    column::{ColumnReader, ColumnWriter},
    consts::{ENTRIES_COLUMN_NAME, PARTITION_METADATA_FILE, arrow_chunk_size_by_type},
    partition::{Partition, column_name_to_path, load_partition},
};

pub struct CastPartition<S: ObjectStore + Clone> {
    object_store: S,
    partition: Partition<S>,
    pending_casts: Vec<(String, DataType)>,
}

impl<S: ObjectStore + Clone> CastPartition<S> {
    pub fn new(object_store: S, partition: Partition<S>) -> Self {
        Self {
            object_store,
            partition,
            pending_casts: Vec::new(),
        }
    }

    pub fn cast_column(
        mut self,
        column_name: impl Into<String>,
        target_data_type: DataType,
    ) -> Self {
        self.pending_casts
            .push((column_name.into(), target_data_type));
        self
    }

    pub async fn execute(self) -> anyhow::Result<Partition<S>> {
        if self.pending_casts.is_empty() {
            return Ok(self.partition);
        }

        let partition_directory = self.partition.directory().clone();
        let mut metadata = self.partition.metadata().clone();
        let io_cache = Arc::new(IoCache::new(256 * 1024 * 1024));

        let cast_options = CastOptions {
            safe: true,
            format_options: Default::default(),
        };

        for (column_name, target_data_type) in self.pending_casts {
            anyhow::ensure!(
                column_name != ENTRIES_COLUMN_NAME,
                "column '{}' is reserved and cannot be cast",
                ENTRIES_COLUMN_NAME
            );

            let source_column = metadata
                .schema
                .columns
                .iter()
                .find(|column| column.name == column_name)
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "column '{}' not found in partition '{}'",
                        column_name,
                        self.partition.name()
                    )
                })?;

            if source_column.data_type == target_data_type {
                continue;
            }

            cast_column_data(
                self.object_store.clone(),
                partition_directory.clone(),
                self.partition.dataset_indexes(),
                &column_name,
                &target_data_type,
                io_cache.clone(),
                &cast_options,
            )
            .await?;

            let column = metadata
                .schema
                .columns
                .iter_mut()
                .find(|column| column.name == column_name)
                .expect("column already validated to exist");
            column.data_type = target_data_type;
        }

        self.object_store
            .put(
                &partition_directory.child(PARTITION_METADATA_FILE),
                serde_json::to_vec(&metadata)?.into(),
            )
            .await?;

        load_partition(
            self.object_store,
            partition_directory,
            self.partition.io_cache,
        )
        .await
    }
}

async fn cast_column_data<S: ObjectStore + Clone>(
    object_store: S,
    partition_directory: object_store::path::Path,
    dataset_indexes: &[u32],
    column_name: &str,
    target_data_type: &DataType,
    io_cache: Arc<IoCache>,
    cast_options: &CastOptions<'_>,
) -> anyhow::Result<()> {
    let column_path = column_name_to_path(partition_directory, column_name);
    let reader = ColumnReader::new(object_store.clone(), column_path.clone(), io_cache).await?;
    let mut writer = ColumnWriter::new(
        object_store,
        column_path,
        target_data_type.clone(),
        arrow_chunk_size_by_type(target_data_type),
    )?;

    for dataset_index in dataset_indexes.iter().copied() {
        let Some(array) = reader.read_column_array(dataset_index).transpose()? else {
            continue;
        };

        let shape = array.shape();
        let dimensions = array.dimensions();
        let arrow_array = array.as_arrow_array_ref().await?;
        let casted = cast_with_options(arrow_array.as_ref(), target_data_type, cast_options)?;
        let casted_array = nd_array_from_arrow(casted, shape, dimensions)?;

        writer
            .write_column_array(dataset_index, casted_array)
            .await?;
    }

    writer.finalize().await?;

    Ok(())
}

fn nd_array_from_arrow(
    array: Arc<dyn arrow::array::Array>,
    shape: Vec<usize>,
    dimensions: Vec<String>,
) -> anyhow::Result<Arc<dyn NdArrowArray>> {
    match array.data_type() {
        DataType::Boolean => {
            let values = <bool as AtlasArrowCompat>::from_arrow_array(array.as_ref())?;
            Ok(Arc::new(NdArrowArrayDispatch::new_in_mem(
                values, shape, dimensions, None,
            )?))
        }
        DataType::Int8 => {
            let values = <i8 as AtlasArrowCompat>::from_arrow_array(array.as_ref())?;
            Ok(Arc::new(NdArrowArrayDispatch::new_in_mem(
                values, shape, dimensions, None,
            )?))
        }
        DataType::Int16 => {
            let values = <i16 as AtlasArrowCompat>::from_arrow_array(array.as_ref())?;
            Ok(Arc::new(NdArrowArrayDispatch::new_in_mem(
                values, shape, dimensions, None,
            )?))
        }
        DataType::Int32 => {
            let values = <i32 as AtlasArrowCompat>::from_arrow_array(array.as_ref())?;
            Ok(Arc::new(NdArrowArrayDispatch::new_in_mem(
                values, shape, dimensions, None,
            )?))
        }
        DataType::Int64 => {
            let values = <i64 as AtlasArrowCompat>::from_arrow_array(array.as_ref())?;
            Ok(Arc::new(NdArrowArrayDispatch::new_in_mem(
                values, shape, dimensions, None,
            )?))
        }
        DataType::UInt8 => {
            let values = <u8 as AtlasArrowCompat>::from_arrow_array(array.as_ref())?;
            Ok(Arc::new(NdArrowArrayDispatch::new_in_mem(
                values, shape, dimensions, None,
            )?))
        }
        DataType::UInt16 => {
            let values = <u16 as AtlasArrowCompat>::from_arrow_array(array.as_ref())?;
            Ok(Arc::new(NdArrowArrayDispatch::new_in_mem(
                values, shape, dimensions, None,
            )?))
        }
        DataType::UInt32 => {
            let values = <u32 as AtlasArrowCompat>::from_arrow_array(array.as_ref())?;
            Ok(Arc::new(NdArrowArrayDispatch::new_in_mem(
                values, shape, dimensions, None,
            )?))
        }
        DataType::UInt64 => {
            let values = <u64 as AtlasArrowCompat>::from_arrow_array(array.as_ref())?;
            Ok(Arc::new(NdArrowArrayDispatch::new_in_mem(
                values, shape, dimensions, None,
            )?))
        }
        DataType::Float32 => {
            let values = <f32 as AtlasArrowCompat>::from_arrow_array(array.as_ref())?;
            Ok(Arc::new(NdArrowArrayDispatch::new_in_mem(
                values, shape, dimensions, None,
            )?))
        }
        DataType::Float64 => {
            let values = <f64 as AtlasArrowCompat>::from_arrow_array(array.as_ref())?;
            Ok(Arc::new(NdArrowArrayDispatch::new_in_mem(
                values, shape, dimensions, None,
            )?))
        }
        DataType::Utf8 | DataType::LargeUtf8 => {
            let values = <String as AtlasArrowCompat>::from_arrow_array(array.as_ref())?;
            Ok(Arc::new(NdArrowArrayDispatch::new_in_mem(
                values, shape, dimensions, None,
            )?))
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            let values =
                <TimestampNanosecond as AtlasArrowCompat>::from_arrow_array(array.as_ref())?;
            Ok(Arc::new(NdArrowArrayDispatch::new_in_mem(
                values, shape, dimensions, None,
            )?))
        }
        data_type => {
            anyhow::bail!("unsupported cast target datatype for partition cast: {data_type:?}")
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::Float64Array;
    use futures::stream;
    use object_store::{ObjectStore, memory::InMemory, path::Path};

    use super::CastPartition;
    use crate::{
        array::io_cache::IoCache,
        column::Column,
        partition::{
            load_partition,
            ops::{read::ReaderBuilder, write::PartitionWriter},
        },
    };

    #[tokio::test]
    async fn cast_partition_updates_schema_and_values() -> anyhow::Result<()> {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let io_cache = Arc::new(IoCache::new(256 * 1024 * 1024));
        let partition_path = Path::from("collections/example/partitions/part-00000");
        let mut writer =
            PartitionWriter::new(store.clone(), partition_path.clone(), "part-00000", None)?;

        writer
            .write_dataset_columns(
                "dataset-0",
                stream::iter(vec![Column::new_from_vec(
                    "temperature".to_string(),
                    vec![10i32],
                    vec![1],
                    vec!["x".to_string()],
                    None,
                )?]),
            )
            .await?;
        writer.finish().await?;

        let partition = load_partition(store.clone(), partition_path.clone(), io_cache).await?;
        let partition = CastPartition::new(store.clone(), partition)
            .cast_column("temperature", arrow::datatypes::DataType::Float64)
            .execute()
            .await?;

        let temp_col = partition
            .schema()
            .columns
            .iter()
            .find(|column| column.name == "temperature")
            .expect("temperature column exists");
        assert_eq!(temp_col.data_type, arrow::datatypes::DataType::Float64);

        let dataset = ReaderBuilder::new(store, partition).dataset(0).await?;
        let temp_values = dataset.0.arrays[1].as_arrow_array_ref().await?;
        let temp_values = temp_values.as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(temp_values.value(0), 10.0);

        Ok(())
    }

    #[tokio::test]
    async fn cast_partition_preserves_missing_arrays() -> anyhow::Result<()> {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let io_cache = Arc::new(IoCache::new(256 * 1024 * 1024));
        let partition_path = Path::from("collections/example/partitions/part-00000");
        let mut writer =
            PartitionWriter::new(store.clone(), partition_path.clone(), "part-00000", None)?;

        writer
            .write_dataset_columns(
                "dataset-0",
                stream::iter(vec![Column::new_from_vec(
                    "temperature".to_string(),
                    vec![10i32],
                    vec![1],
                    vec!["x".to_string()],
                    None,
                )?]),
            )
            .await?;
        writer
            .write_dataset_columns(
                "dataset-1",
                stream::iter(vec![Column::new_from_vec(
                    "salinity".to_string(),
                    vec![35i32],
                    vec![1],
                    vec!["x".to_string()],
                    None,
                )?]),
            )
            .await?;
        writer.finish().await?;

        let partition = load_partition(store.clone(), partition_path, io_cache).await?;
        let partition = CastPartition::new(store.clone(), partition)
            .cast_column("temperature", arrow::datatypes::DataType::Float64)
            .execute()
            .await?;

        let dataset_0 = ReaderBuilder::new(store.clone(), partition.clone())
            .dataset(0)
            .await?;
        let temp_values = dataset_0.0.arrays[1].as_arrow_array_ref().await?;
        let temp_values = temp_values.as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(temp_values.value(0), 10.0);

        let dataset_1 = ReaderBuilder::new(store, partition).dataset(1).await?;
        assert_eq!(dataset_1.0.schema.fields().len(), 2);
        assert_eq!(dataset_1.0.schema.field(0).name(), "__entry_key");
        assert_eq!(dataset_1.0.schema.field(1).name(), "salinity");

        Ok(())
    }

    #[tokio::test]
    async fn cast_partition_casts_multiple_columns() -> anyhow::Result<()> {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let io_cache = Arc::new(IoCache::new(256 * 1024 * 1024));
        let partition_path = Path::from("collections/example/partitions/part-00000");
        let mut writer =
            PartitionWriter::new(store.clone(), partition_path.clone(), "part-00000", None)?;

        writer
            .write_dataset_columns(
                "dataset-0",
                stream::iter(vec![
                    Column::new_from_vec(
                        "temperature".to_string(),
                        vec![10i32],
                        vec![1],
                        vec!["x".to_string()],
                        None,
                    )?,
                    Column::new_from_vec(
                        "salinity".to_string(),
                        vec![35i32],
                        vec![1],
                        vec!["x".to_string()],
                        None,
                    )?,
                ]),
            )
            .await?;
        writer.finish().await?;

        let partition = load_partition(store.clone(), partition_path.clone(), io_cache).await?;
        let partition = CastPartition::new(store.clone(), partition)
            .cast_column("temperature", arrow::datatypes::DataType::Float64)
            .cast_column("salinity", arrow::datatypes::DataType::Float64)
            .execute()
            .await?;

        let temperature = partition
            .schema()
            .columns
            .iter()
            .find(|column| column.name == "temperature")
            .expect("temperature column exists");
        let salinity = partition
            .schema()
            .columns
            .iter()
            .find(|column| column.name == "salinity")
            .expect("salinity column exists");
        assert_eq!(temperature.data_type, arrow::datatypes::DataType::Float64);
        assert_eq!(salinity.data_type, arrow::datatypes::DataType::Float64);

        Ok(())
    }

    #[tokio::test]
    async fn cast_partition_errors_for_missing_column() -> anyhow::Result<()> {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let io_cache = Arc::new(IoCache::new(256 * 1024 * 1024));
        let partition_path = Path::from("collections/example/partitions/part-00000");
        let mut writer =
            PartitionWriter::new(store.clone(), partition_path.clone(), "part-00000", None)?;

        writer
            .write_dataset_columns(
                "dataset-0",
                stream::iter(vec![Column::new_from_vec(
                    "temperature".to_string(),
                    vec![10i32],
                    vec![1],
                    vec!["x".to_string()],
                    None,
                )?]),
            )
            .await?;
        writer.finish().await?;

        let partition =
            load_partition(store.clone(), partition_path.clone(), io_cache.clone()).await?;
        let error = CastPartition::new(store.clone(), partition)
            .cast_column("pressure", arrow::datatypes::DataType::Float64)
            .execute()
            .await
            .expect_err("missing column should return an error");
        assert!(error.to_string().contains("column 'pressure' not found"));

        let partition = load_partition(store, partition_path, io_cache).await?;
        let temp_col = partition
            .schema()
            .columns
            .iter()
            .find(|column| column.name == "temperature")
            .expect("temperature column exists");
        assert_eq!(temp_col.data_type, arrow::datatypes::DataType::Int32);

        Ok(())
    }

    #[tokio::test]
    async fn cast_partition_rejects_reserved_entry_column() -> anyhow::Result<()> {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let io_cache = Arc::new(IoCache::new(256 * 1024 * 1024));
        let partition_path = Path::from("collections/example/partitions/part-00000");
        let mut writer =
            PartitionWriter::new(store.clone(), partition_path.clone(), "part-00000", None)?;

        writer
            .write_dataset_columns(
                "dataset-0",
                stream::iter(vec![Column::new_from_vec(
                    "temperature".to_string(),
                    vec![10i32],
                    vec![1],
                    vec!["x".to_string()],
                    None,
                )?]),
            )
            .await?;
        writer.finish().await?;

        let partition = load_partition(store, partition_path, io_cache).await?;
        let error =
            CastPartition::new(Arc::new(InMemory::new()) as Arc<dyn ObjectStore>, partition)
                .cast_column("__entry_key", arrow::datatypes::DataType::Float64)
                .execute()
                .await
                .expect_err("reserved entry key should return an error");

        assert!(
            error
                .to_string()
                .contains("column '__entry_key' is reserved and cannot be cast")
        );

        Ok(())
    }
}
