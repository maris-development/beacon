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
    consts::{
        DEFAULT_IO_CACHE_BYTES, ENTRIES_COLUMN_NAME, PARTITION_METADATA_FILE,
        arrow_chunk_size_by_type,
    },
    partition::{Partition, column_name_to_path},
};

/// Cast one partition column to a new Arrow data type.
///
/// This operation rewrites the backing column arrays, updates in-memory schema
/// metadata, persists partition metadata to object storage, and invalidates the
/// cached column reader for the casted column so subsequent reads see the new
/// type.
pub async fn cast_partition_column<S: ObjectStore + Clone>(
    partition: &mut Partition<S>,
    column_name: &str,
    target_data_type: DataType,
    safe_cast: bool,
) -> anyhow::Result<()> {
    anyhow::ensure!(
        column_name != ENTRIES_COLUMN_NAME,
        "column '{}' is reserved and cannot be cast",
        ENTRIES_COLUMN_NAME
    );

    let source_column = partition
        .metadata
        .schema
        .columns
        .iter()
        .find(|column| column.name == column_name)
        .ok_or_else(|| {
            anyhow::anyhow!(
                "column '{}' not found in partition '{}'",
                column_name,
                partition.name()
            )
        })?;

    if source_column.data_type == target_data_type {
        return Ok(());
    }

    let cast_options = CastOptions {
        safe: safe_cast,
        ..Default::default()
    };
    cast_column_data(partition, column_name, &target_data_type, &cast_options).await?;

    let column = partition
        .metadata
        .schema
        .columns
        .iter_mut()
        .find(|column| column.name == column_name)
        .expect("column already validated to exist");
    column.data_type = target_data_type;

    partition
        .store
        .put(
            &partition.directory.child(PARTITION_METADATA_FILE),
            serde_json::to_vec(&partition.metadata)?.into(),
        )
        .await?;

    // Reset caches so follow-up reads cannot observe stale pre-cast buffers.
    partition.io_cache = Arc::new(IoCache::new(DEFAULT_IO_CACHE_BYTES));
    let mut cache = partition.reader_cache.lock().await;
    cache.clear();

    Ok(())
}

/// Rewrite one column's persisted arrays by casting each dataset array.
async fn cast_column_data<S: ObjectStore + Clone>(
    partition: &mut Partition<S>,
    column_name: &str,
    target_data_type: &DataType,
    cast_options: &CastOptions<'_>,
) -> anyhow::Result<()> {
    let column_path = column_name_to_path(partition.directory.clone(), column_name);
    let source_io_cache = Arc::new(IoCache::new(DEFAULT_IO_CACHE_BYTES));
    let reader = ColumnReader::new(
        partition.store.clone(),
        column_path.clone(),
        source_io_cache,
    )
    .await?;
    let mut writer = ColumnWriter::new(
        partition.store.clone(),
        column_path,
        target_data_type.clone(),
        arrow_chunk_size_by_type(target_data_type),
    )?;

    for dataset_index in reader.column_array_dataset_indexes().iter().copied() {
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

/// Build an ND array from a casted Arrow array while preserving shape metadata.
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
    use arrow::datatypes::DataType;
    use futures::stream;
    use object_store::{ObjectStore, memory::InMemory, path::Path};

    use super::cast_partition_column;
    use crate::{
        array::io_cache::IoCache,
        column::Column,
        consts::DEFAULT_IO_CACHE_BYTES,
        partition::{
            load_partition,
            ops::{read::ReaderBuilder, write::PartitionWriter},
        },
    };

    fn test_io_cache() -> Arc<IoCache> {
        Arc::new(IoCache::new(DEFAULT_IO_CACHE_BYTES))
    }

    #[tokio::test]
    async fn cast_partition_updates_schema_and_values() -> anyhow::Result<()> {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let io_cache = test_io_cache();
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
        writer.finish(io_cache.clone()).await?;

        let mut partition =
            load_partition(store.clone(), partition_path.clone(), io_cache.clone()).await?;
        cast_partition_column(&mut partition, "temperature", DataType::Float64, true).await?;

        let temp_col = partition
            .schema()
            .columns
            .iter()
            .find(|column| column.name == "temperature")
            .expect("temperature column exists");
        assert_eq!(temp_col.data_type, DataType::Float64);

        // Reload to verify metadata was persisted to object store.
        let reloaded = load_partition(store.clone(), partition_path, io_cache).await?;
        let reloaded_temp_col = reloaded
            .schema()
            .columns
            .iter()
            .find(|column| column.name == "temperature")
            .expect("temperature column exists");
        assert_eq!(reloaded_temp_col.data_type, DataType::Float64);

        let dataset = ReaderBuilder::new(store, partition).dataset(0).await?;
        let temp_values = dataset.0.arrays[1].as_arrow_array_ref().await?;
        let temp_values = temp_values.as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(temp_values.value(0), 10.0);

        Ok(())
    }

    #[tokio::test]
    async fn cast_partition_preserves_missing_arrays() -> anyhow::Result<()> {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let io_cache = test_io_cache();
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
        writer.finish(io_cache.clone()).await?;

        let mut partition = load_partition(store.clone(), partition_path, io_cache).await?;
        cast_partition_column(&mut partition, "temperature", DataType::Float64, true).await?;

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
        let io_cache = test_io_cache();
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
        writer.finish(io_cache.clone()).await?;

        let mut partition = load_partition(store.clone(), partition_path, io_cache).await?;
        cast_partition_column(&mut partition, "temperature", DataType::Float64, true).await?;
        cast_partition_column(&mut partition, "salinity", DataType::Float64, true).await?;

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
        assert_eq!(temperature.data_type, DataType::Float64);
        assert_eq!(salinity.data_type, DataType::Float64);

        Ok(())
    }

    #[tokio::test]
    async fn cast_partition_errors_for_missing_column() -> anyhow::Result<()> {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let io_cache = test_io_cache();
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
        writer.finish(io_cache.clone()).await?;

        let mut partition =
            load_partition(store.clone(), partition_path.clone(), io_cache.clone()).await?;
        let error = cast_partition_column(&mut partition, "pressure", DataType::Float64, true)
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
        assert_eq!(temp_col.data_type, DataType::Int32);

        Ok(())
    }

    #[tokio::test]
    async fn cast_partition_rejects_reserved_entry_column() -> anyhow::Result<()> {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let io_cache = test_io_cache();
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
        writer.finish(io_cache.clone()).await?;

        let mut partition = load_partition(store, partition_path, io_cache).await?;
        let error = cast_partition_column(&mut partition, "__entry_key", DataType::Float64, true)
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
