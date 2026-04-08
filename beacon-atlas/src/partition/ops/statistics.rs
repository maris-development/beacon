use std::collections::HashMap;
use std::sync::Arc;

use arrow::{
    array::{ArrayRef, AsArray, Int32Array, UInt32Array, new_null_array},
    compute::take,
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use object_store::ObjectStore;

use crate::partition::Partition;

pub async fn partition_statistics<S: ObjectStore + Clone>(
    _object_store: S,
    partition: &Partition<S>,
    columns: &[String],
) -> PartitionStatistics {
    PartitionStatistics::new(partition, columns).await
}

pub struct PartitionStatistics {
    pub(crate) fetched_statistics: HashMap<String, RecordBatch>,
    pub(crate) num_containers: usize,
}

impl PartitionStatistics {
    pub async fn new<S: ObjectStore + Clone>(partition: &Partition<S>, columns: &[String]) -> Self {
        // Fetch the statistics for the requested columns and cache them in memory for later alignment and projection.
        let mut fetched_statistics = HashMap::new();
        let expected_dataset_indexes = partition.undeleted_dataset_indexes();
        for column_name in columns {
            if let Ok(reader) = partition.column_reader(column_name).await {
                let aligned_batch = match reader.read_column_statistics().await {
                    Ok(Some(stats_batch)) => align_statistics_batch(
                        stats_batch,
                        &expected_dataset_indexes,
                        reader.column_data_type(),
                    )
                    .ok(),
                    Ok(None) | Err(_) => {
                        build_synthetic_batch(&expected_dataset_indexes, &reader.column_data_type())
                            .ok()
                    }
                };

                if let Some(batch) = aligned_batch {
                    fetched_statistics.insert(column_name.clone(), batch);
                }
            }
        }

        Self {
            fetched_statistics,
            num_containers: expected_dataset_indexes.len(),
        }
    }

    pub fn statistics_for_column(&self, column_name: &str) -> Option<RecordBatch> {
        self.fetched_statistics.get(column_name).cloned()
    }
}

fn align_statistics_batch(
    array_statistics: RecordBatch,
    expected_dataset_indexes: &[u32],
    column_data_type: DataType,
) -> anyhow::Result<RecordBatch> {
    // the array statistics batch contains a dataset_index row that indicated in which dataset the statistics are valid for. When datasets are missing, we need to emit null statistics for those datasets, which requires us to build a new batch with the expected dataset_index backbone and take values from the original batch where possible.
    let aligned_batch = build_synthetic_batch(expected_dataset_indexes, &column_data_type)?;

    if expected_dataset_indexes.is_empty() {
        return Ok(aligned_batch);
    }

    let dataset_index_column = array_statistics
        .column(array_statistics.schema().index_of("dataset_index")?)
        .as_primitive::<arrow::datatypes::UInt32Type>();

    let expected_positions = expected_dataset_indexes
        .iter()
        .enumerate()
        .map(|(position, dataset_index)| (*dataset_index, position))
        .collect::<HashMap<_, _>>();

    let mut source_row_by_dataset_index = vec![None; expected_dataset_indexes.len()];
    for source_row in 0..dataset_index_column.len() {
        let dataset_index = dataset_index_column.value(source_row);
        let Some(expected_position) = expected_positions.get(&dataset_index).copied() else {
            continue;
        };

        if source_row_by_dataset_index[expected_position].is_none() {
            source_row_by_dataset_index[expected_position] = Some(source_row as i32);
        }
    }

    let take_indices = Int32Array::from(source_row_by_dataset_index);

    let mut aligned_columns = aligned_batch.columns().to_vec();
    for field_name in ["min", "max", "null_count", "row_count"] {
        let source_index = array_statistics.schema().index_of(field_name)?;
        let target_index = aligned_batch.schema().index_of(field_name)?;
        let source_column = array_statistics.column(source_index);
        let aligned_column = take(source_column.as_ref(), &take_indices, None)?;
        aligned_columns[target_index] = aligned_column;
    }

    Ok(RecordBatch::try_new(
        aligned_batch.schema(),
        aligned_columns,
    )?)
}

fn build_synthetic_batch(
    dataset_indexes: &[u32],
    column_data_type: &DataType,
) -> anyhow::Result<RecordBatch> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("min", column_data_type.clone(), true),
        Field::new("max", column_data_type.clone(), true),
        Field::new("null_count", DataType::UInt64, true),
        Field::new("row_count", DataType::UInt64, true),
        Field::new("dataset_index", DataType::UInt32, false),
    ]));

    let min = new_null_array(column_data_type, dataset_indexes.len());
    let max = new_null_array(column_data_type, dataset_indexes.len());
    let null_count = new_null_array(&DataType::UInt64, dataset_indexes.len());
    let row_count = new_null_array(&DataType::UInt64, dataset_indexes.len());
    let dataset_index = Arc::new(UInt32Array::from(dataset_indexes.to_vec())) as ArrayRef;

    Ok(RecordBatch::try_new(
        schema,
        vec![min, max, null_count, row_count, dataset_index],
    )?)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Array, AsArray};
    use futures::stream;
    use object_store::{ObjectStore, memory::InMemory, path::Path};

    use crate::{
        array::io_cache::IoCache,
        column::Column,
        consts::{DEFAULT_IO_CACHE_BYTES, STATISTICS_FILE_NAME},
        partition::{
            column_name_to_path, load_partition,
            ops::{
                delete::DeleteDatasetsPartition, statistics::PartitionStatistics,
                write::PartitionWriter,
            },
        },
    };

    async fn build_sparse_partition(
        store: Arc<dyn ObjectStore>,
        partition_path: &Path,
        io_cache: Arc<IoCache>,
    ) -> anyhow::Result<crate::partition::Partition<Arc<dyn ObjectStore>>> {
        let mut writer =
            PartitionWriter::new(store.clone(), partition_path.clone(), "part-00000", None)?;

        writer
            .write_dataset_columns(
                "dataset-0",
                stream::iter(vec![Column::new_from_vec(
                    "temperature".to_string(),
                    vec![10.0f32],
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
                    vec![35.0f32],
                    vec![1],
                    vec!["x".to_string()],
                    None,
                )?]),
            )
            .await?;

        writer.finish(io_cache).await
    }

    #[tokio::test]
    async fn aligns_projection_statistics_with_null_rows_for_missing_datasets() -> anyhow::Result<()>
    {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let io_cache = Arc::new(IoCache::new(DEFAULT_IO_CACHE_BYTES));
        let partition_path = Path::from("collections/example/partitions/part-00000");
        let partition = build_sparse_partition(store.clone(), &partition_path, io_cache).await?;

        let aligned = PartitionStatistics::new(
            &partition,
            &["temperature".to_string(), "salinity".to_string()],
        )
        .await;

        let temperature_batch = aligned
            .statistics_for_column("temperature")
            .expect("temperature stats should exist");
        let salinity_batch = aligned
            .statistics_for_column("salinity")
            .expect("salinity stats should exist");

        assert_eq!(temperature_batch.num_rows(), 2);
        assert_eq!(salinity_batch.num_rows(), 2);

        let temp_dataset_indexes = temperature_batch
            .column(temperature_batch.schema().index_of("dataset_index")?)
            .as_primitive::<arrow::datatypes::UInt32Type>();
        assert_eq!(temp_dataset_indexes.values(), &[0, 1]);

        let temp_min = temperature_batch
            .column(temperature_batch.schema().index_of("min")?)
            .as_primitive::<arrow::datatypes::Float32Type>();
        let temp_row_count = temperature_batch
            .column(temperature_batch.schema().index_of("row_count")?)
            .as_primitive::<arrow::datatypes::UInt64Type>();

        assert_eq!(temp_min.value(0), 10.0);
        assert_eq!(temp_row_count.value(0), 1);
        assert!(temp_min.is_null(1));
        assert!(temp_row_count.is_null(1));

        let sal_dataset_indexes = salinity_batch
            .column(salinity_batch.schema().index_of("dataset_index")?)
            .as_primitive::<arrow::datatypes::UInt32Type>();
        assert_eq!(sal_dataset_indexes.values(), &[0, 1]);

        let sal_min = salinity_batch
            .column(salinity_batch.schema().index_of("min")?)
            .as_primitive::<arrow::datatypes::Float32Type>();
        let sal_row_count = salinity_batch
            .column(salinity_batch.schema().index_of("row_count")?)
            .as_primitive::<arrow::datatypes::UInt64Type>();

        assert!(sal_min.is_null(0));
        assert!(sal_row_count.is_null(0));
        assert_eq!(sal_min.value(1), 35.0);
        assert_eq!(sal_row_count.value(1), 1);

        Ok(())
    }

    #[tokio::test]
    async fn emits_synthetic_batch_when_column_statistics_file_is_missing() -> anyhow::Result<()> {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let io_cache = Arc::new(IoCache::new(DEFAULT_IO_CACHE_BYTES));
        let partition_path = Path::from("collections/example/partitions/part-00000");

        let partition =
            build_sparse_partition(store.clone(), &partition_path, io_cache.clone()).await?;
        let partition = Arc::new(
            DeleteDatasetsPartition::new(store.clone(), partition)
                .delete_dataset_index(1)
                .execute()
                .await?,
        );

        let stats_path = column_name_to_path(partition.directory().clone(), "temperature")
            .child(STATISTICS_FILE_NAME);
        store.delete(&stats_path).await?;

        let loaded = load_partition(store.clone(), partition_path, io_cache).await?;
        let aligned = PartitionStatistics::new(&loaded, &["temperature".to_string()]).await;
        let batch = aligned
            .statistics_for_column("temperature")
            .expect("synthetic temperature stats should be present");
        assert_eq!(batch.num_rows(), 1);

        let dataset_indexes = batch
            .column(batch.schema().index_of("dataset_index")?)
            .as_primitive::<arrow::datatypes::UInt32Type>();
        assert_eq!(dataset_indexes.values(), &[0]);

        let min = batch
            .column(batch.schema().index_of("min")?)
            .as_primitive::<arrow::datatypes::Float32Type>();
        let row_count = batch
            .column(batch.schema().index_of("row_count")?)
            .as_primitive::<arrow::datatypes::UInt64Type>();

        assert!(min.is_null(0));
        assert!(row_count.is_null(0));

        Ok(())
    }

    #[tokio::test]
    async fn aligned_batches_share_same_dataset_index_backbone() -> anyhow::Result<()> {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let io_cache = Arc::new(IoCache::new(DEFAULT_IO_CACHE_BYTES));
        let partition_path = Path::from("collections/example/partitions/part-00000");

        let partition =
            build_sparse_partition(store.clone(), &partition_path, io_cache.clone()).await?;
        let partition = Arc::new(
            DeleteDatasetsPartition::new(store.clone(), partition)
                .delete_dataset_index(0)
                .execute()
                .await?,
        );

        let aligned = PartitionStatistics::new(
            &partition,
            &["temperature".to_string(), "salinity".to_string()],
        )
        .await;

        let temperature_batch = aligned
            .statistics_for_column("temperature")
            .expect("temperature stats should exist");
        let salinity_batch = aligned
            .statistics_for_column("salinity")
            .expect("salinity stats should exist");

        let first_dataset_indexes = temperature_batch
            .column(temperature_batch.schema().index_of("dataset_index")?)
            .as_primitive::<arrow::datatypes::UInt32Type>();
        let second_dataset_indexes = salinity_batch
            .column(salinity_batch.schema().index_of("dataset_index")?)
            .as_primitive::<arrow::datatypes::UInt32Type>();

        assert_eq!(first_dataset_indexes.values(), &[1]);
        assert_eq!(second_dataset_indexes.values(), &[1]);

        Ok(())
    }
}
