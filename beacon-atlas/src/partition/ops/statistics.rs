use std::collections::HashMap;
use std::sync::Arc;

use arrow::{
    array::{ArrayRef, AsArray, Int32Array, UInt32Array, new_null_array},
    compute::take,
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use object_store::ObjectStore;

use crate::{
    array::io_cache::IoCache,
    consts::DEFAULT_IO_CACHE_BYTES,
    partition::{Partition, ops::read::init_column_reader},
};

pub struct PartitionStatisticsBuilder<S: ObjectStore + Clone> {
    object_store: S,
    partition: Arc<Partition>,
    io_cache: Option<Arc<IoCache>>,
    projection: Option<Vec<usize>>,
}

impl<S: ObjectStore + Clone> PartitionStatisticsBuilder<S> {
    pub fn new(object_store: S, partition: Arc<Partition>) -> Self {
        Self {
            object_store,
            partition,
            io_cache: None,
            projection: None,
        }
    }

    pub fn with_projection(mut self, projection: Vec<usize>) -> Self {
        self.projection = Some(projection);
        self
    }

    pub fn with_io_cache(mut self, io_cache: Arc<IoCache>) -> Self {
        self.io_cache = Some(io_cache);
        self
    }

    pub async fn read_statistics_batches(self) -> anyhow::Result<Vec<RecordBatch>> {
        let partition_schema = self.partition.schema();
        let selected_columns = match &self.projection {
            Some(projection) => projection.clone(),
            None => (0..partition_schema.columns.len()).collect(),
        };

        let io_cache = self
            .io_cache
            .unwrap_or_else(|| Arc::new(IoCache::new(DEFAULT_IO_CACHE_BYTES)));
        let target_dataset_indexes = self.partition.undeleted_dataset_indexes();

        let mut output = Vec::with_capacity(selected_columns.len());
        for &column_index in &selected_columns {
            let column = partition_schema
                .columns
                .get(column_index)
                .ok_or_else(|| anyhow::anyhow!("column index {} out of bounds", column_index))?;

            let reader = init_column_reader(
                self.object_store.clone(),
                self.partition.directory(),
                &column.name,
                io_cache.clone(),
            )
            .await?;

            let aligned = align_statistics_batch(
                reader.read_column_statistics()?,
                &target_dataset_indexes,
                column.data_type.clone(),
            )?;

            output.push(aligned);
        }

        Ok(output)
    }
}

fn align_statistics_batch(
    source: Option<RecordBatch>,
    target_dataset_indexes: &[u32],
    column_data_type: DataType,
) -> anyhow::Result<RecordBatch> {
    let Some(source) = source else {
        return build_synthetic_batch(target_dataset_indexes, &column_data_type);
    };

    let dataset_index_column = source.schema().index_of("dataset_index").map_err(|_| {
        anyhow::anyhow!("statistics batch is missing required 'dataset_index' column")
    })?;

    let dataset_index_array = source
        .column(dataset_index_column)
        .as_primitive::<arrow::datatypes::UInt32Type>();

    let mut dataset_index_to_row = HashMap::with_capacity(dataset_index_array.len());
    for (row, dataset_index) in dataset_index_array.values().iter().copied().enumerate() {
        if dataset_index_to_row.insert(dataset_index, row).is_some() {
            anyhow::bail!(
                "statistics batch contains duplicate dataset_index '{}'",
                dataset_index
            );
        }
    }

    let take_indices = Int32Array::from(
        target_dataset_indexes
            .iter()
            .map(|dataset_index| {
                dataset_index_to_row
                    .get(dataset_index)
                    .map(|row| *row as i32)
            })
            .collect::<Vec<_>>(),
    );

    let mut output_columns: Vec<ArrayRef> = Vec::with_capacity(source.num_columns());
    let mut output_fields: Vec<Field> = Vec::with_capacity(source.num_columns());

    for (column_index, field) in source.schema().fields().iter().enumerate() {
        if column_index == dataset_index_column {
            output_columns
                .push(Arc::new(UInt32Array::from(target_dataset_indexes.to_vec())) as ArrayRef);
            output_fields.push(Field::new(field.name(), DataType::UInt32, false));
            continue;
        }

        output_columns.push(take(
            source.column(column_index).as_ref(),
            &take_indices,
            None,
        )?);
        output_fields.push(Field::new(field.name(), field.data_type().clone(), true));
    }

    Ok(RecordBatch::try_new(
        Arc::new(Schema::new(output_fields)),
        output_columns,
    )?)
}

fn build_synthetic_batch(
    target_dataset_indexes: &[u32],
    column_data_type: &DataType,
) -> anyhow::Result<RecordBatch> {
    let row_count = target_dataset_indexes.len();

    let schema = Arc::new(Schema::new(vec![
        Field::new("min", column_data_type.clone(), true),
        Field::new("max", column_data_type.clone(), true),
        Field::new("null_count", DataType::UInt64, true),
        Field::new("row_count", DataType::UInt64, true),
        Field::new("dataset_index", DataType::UInt32, false),
    ]));

    let min = new_null_array(column_data_type, row_count);
    let max = new_null_array(column_data_type, row_count);
    let null_count = new_null_array(&DataType::UInt64, row_count);
    let row_count = new_null_array(&DataType::UInt64, row_count);
    let dataset_index = Arc::new(UInt32Array::from(target_dataset_indexes.to_vec())) as ArrayRef;

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
                delete::DeleteDatasetsPartition, statistics::PartitionStatisticsBuilder,
                write::PartitionWriter,
            },
        },
    };

    async fn build_sparse_partition(
        store: Arc<dyn ObjectStore>,
        partition_path: &Path,
    ) -> anyhow::Result<crate::partition::Partition> {
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

        let aligned = PartitionStatisticsBuilder::new(store, partition)
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
        let aligned = PartitionStatisticsBuilder::new(store, loaded)
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

        let aligned = PartitionStatisticsBuilder::new(store, partition)
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
