use std::collections::HashMap;
use std::sync::Arc;

use arrow::{
    array::{ArrayRef, AsArray, BooleanArray, Int32Array, UInt32Array, new_null_array},
    compute::take,
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use object_store::ObjectStore;

use crate::partition::Partition;

pub async fn partition_statistics<S: ObjectStore + Clone>(
    object_store: S,
    partition: &Partition<S>,
    columns: &[String],
) -> PartitionStatistics {
    PartitionStatistics::new(partition, columns).await
}

pub struct PartitionStatistics {
    fetched_statistics: HashMap<String, RecordBatch>,
}

impl PartitionStatistics {
    pub async fn new<S: ObjectStore + Clone>(partition: &Partition<S>, columns: &[String]) -> Self {
        // Fetch the statistics for the requested columns and cache them in memory for later alignment and projection.
        let mut fetched_statistics = HashMap::new();
        for column_name in columns {
            if let Ok(reader) = partition.column_reader(column_name).await {
                if let Ok(Some(stats_batch)) = reader.read_column_statistics().await {
                    fetched_statistics.insert(column_name.clone(), stats_batch);
                }
            }
        }

        Self { fetched_statistics }
    }

    pub fn statistics_for_column(&self, column_name: &str) -> Option<RecordBatch> {
        self.fetched_statistics.get(column_name).cloned()
    }
}

fn align_statistics_batch(
    array_statistics: RecordBatch,
    expected_total_rows: usize,
    column_data_type: DataType,
) -> anyhow::Result<RecordBatch> {
    // the array statistics batch contains a dataset_index row that indicated in which dataset the statistics are valid for. When datasets are missing, we need to emit null statistics for those datasets, which requires us to build a new batch with the expected dataset_index backbone and take values from the original batch where possible.
    todo!()
}

fn build_synthetic_batch(
    total_rows: usize,
    column_data_type: &DataType,
) -> anyhow::Result<RecordBatch> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("min", column_data_type.clone(), true),
        Field::new("max", column_data_type.clone(), true),
        Field::new("null_count", DataType::UInt64, true),
        Field::new("row_count", DataType::UInt64, true),
        Field::new("dataset_index", DataType::UInt32, false),
    ]));

    let min = new_null_array(column_data_type, total_rows);
    let max = new_null_array(column_data_type, total_rows);
    let null_count = new_null_array(&DataType::UInt64, total_rows);
    let row_count = new_null_array(&DataType::UInt64, total_rows);
    let dataset_indexes = (0..total_rows as u32).collect::<Vec<_>>();
    let dataset_index = Arc::new(UInt32Array::from(dataset_indexes)) as ArrayRef;

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
        column::Column,
        consts::STATISTICS_FILE_NAME,
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

        writer.finish().await
    }

    #[tokio::test]
    async fn aligns_projection_statistics_with_null_rows_for_missing_datasets() -> anyhow::Result<()>
    {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let partition_path = Path::from("collections/example/partitions/part-00000");
        let partition = Arc::new(build_sparse_partition(store.clone(), &partition_path).await?);

        let aligned = PartitionStatistics::new(store, partition)
            .with_projection(vec![1, 2])
            .read_statistics_batches()
            .await?;

        assert_eq!(aligned.len(), 2);
        assert_eq!(aligned[0].num_rows(), 2);
        assert_eq!(aligned[1].num_rows(), 2);

        let temp_dataset_indexes = aligned[0]
            .column(aligned[0].schema().index_of("dataset_index")?)
            .as_primitive::<arrow::datatypes::UInt32Type>();
        assert_eq!(temp_dataset_indexes.values(), &[0, 1]);

        let temp_min = aligned[0]
            .column(aligned[0].schema().index_of("min")?)
            .as_primitive::<arrow::datatypes::Float32Type>();
        let temp_row_count = aligned[0]
            .column(aligned[0].schema().index_of("row_count")?)
            .as_primitive::<arrow::datatypes::UInt64Type>();

        assert_eq!(temp_min.value(0), 10.0);
        assert_eq!(temp_row_count.value(0), 1);
        assert!(temp_min.is_null(1));
        assert!(temp_row_count.is_null(1));

        let sal_dataset_indexes = aligned[1]
            .column(aligned[1].schema().index_of("dataset_index")?)
            .as_primitive::<arrow::datatypes::UInt32Type>();
        assert_eq!(sal_dataset_indexes.values(), &[0, 1]);

        let sal_min = aligned[1]
            .column(aligned[1].schema().index_of("min")?)
            .as_primitive::<arrow::datatypes::Float32Type>();
        let sal_row_count = aligned[1]
            .column(aligned[1].schema().index_of("row_count")?)
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
        let partition_path = Path::from("collections/example/partitions/part-00000");

        let partition = build_sparse_partition(store.clone(), &partition_path).await?;
        let partition = Arc::new(
            DeleteDatasetsPartition::new(store.clone(), partition)
                .delete_dataset_index(1)
                .execute()
                .await?,
        );

        let stats_path = column_name_to_path(partition.directory().clone(), "temperature")
            .child(STATISTICS_FILE_NAME);
        store.delete(&stats_path).await?;

        let loaded = Arc::new(load_partition(store.clone(), partition_path).await?);
        let aligned = PartitionStatistics::new(store, loaded)
            .with_projection(vec![1])
            .read_statistics_batches()
            .await?;

        assert_eq!(aligned.len(), 1);
        let batch = &aligned[0];
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
        let partition_path = Path::from("collections/example/partitions/part-00000");

        let partition = build_sparse_partition(store.clone(), &partition_path).await?;
        let partition = Arc::new(
            DeleteDatasetsPartition::new(store.clone(), partition)
                .delete_dataset_index(0)
                .execute()
                .await?,
        );

        let aligned = PartitionStatistics::new(store, partition)
            .with_projection(vec![1, 2])
            .read_statistics_batches()
            .await?;

        assert_eq!(aligned.len(), 2);

        let first_dataset_indexes = aligned[0]
            .column(aligned[0].schema().index_of("dataset_index")?)
            .as_primitive::<arrow::datatypes::UInt32Type>();
        let second_dataset_indexes = aligned[1]
            .column(aligned[1].schema().index_of("dataset_index")?)
            .as_primitive::<arrow::datatypes::UInt32Type>();

        assert_eq!(first_dataset_indexes.values(), &[1]);
        assert_eq!(second_dataset_indexes.values(), &[1]);

        Ok(())
    }
}
