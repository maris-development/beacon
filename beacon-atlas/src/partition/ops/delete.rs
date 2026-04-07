use std::collections::{HashMap, HashSet};

use object_store::ObjectStore;

use crate::{
    partition::ops::write_partition_entries,
    partition::{Partition, load_partition},
};

pub struct DeleteDatasetsPartition<S: ObjectStore + Clone> {
    object_store: S,
    partition: Partition<S>,
    pending_dataset_names: Vec<String>,
    pending_dataset_indexes: Vec<u32>,
}

impl<S: ObjectStore + Clone> DeleteDatasetsPartition<S> {
    pub fn new(object_store: S, partition: Partition<S>) -> Self {
        Self {
            object_store,
            partition,
            pending_dataset_names: Vec::new(),
            pending_dataset_indexes: Vec::new(),
        }
    }

    pub fn delete_dataset(mut self, dataset_name: impl Into<String>) -> Self {
        self.pending_dataset_names.push(dataset_name.into());
        self
    }

    pub fn delete_dataset_index(mut self, dataset_index: u32) -> Self {
        self.pending_dataset_indexes.push(dataset_index);
        self
    }

    pub async fn execute(self) -> anyhow::Result<Partition<S>> {
        if self.pending_dataset_names.is_empty() && self.pending_dataset_indexes.is_empty() {
            return Ok(self.partition);
        }

        let partition_directory = self.partition.directory().clone();
        let entry_keys = self.partition.entry_keys().to_vec();
        let dataset_indexes = self.partition.dataset_indexes().to_vec();
        let mut deletion_flags = self.partition.deletion_flags().to_vec();
        let insert_timestamps = self.partition.insert_timestamps().to_vec();

        let mut dataset_index_to_position = HashMap::with_capacity(dataset_indexes.len());
        for (position, dataset_index) in dataset_indexes.iter().copied().enumerate() {
            dataset_index_to_position.insert(dataset_index, position);
        }

        let mut positions_to_delete = HashSet::new();

        for dataset_name in self.pending_dataset_names {
            let matches = entry_keys
                .iter()
                .enumerate()
                .filter_map(|(position, entry_key)| {
                    (entry_key == &dataset_name).then_some(position)
                })
                .collect::<Vec<_>>();

            anyhow::ensure!(
                !matches.is_empty(),
                "dataset '{}' not found in partition '{}'",
                dataset_name,
                self.partition.name()
            );

            positions_to_delete.extend(matches);
        }

        for dataset_index in self.pending_dataset_indexes {
            let position = dataset_index_to_position
                .get(&dataset_index)
                .copied()
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "dataset index '{}' not found in partition '{}'",
                        dataset_index,
                        self.partition.name()
                    )
                })?;
            positions_to_delete.insert(position);
        }

        for position in positions_to_delete {
            deletion_flags[position] = true;
        }

        write_partition_entries(
            &self.object_store,
            &partition_directory,
            &entry_keys,
            &dataset_indexes,
            &deletion_flags,
            &insert_timestamps,
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::StringArray;
    use futures::stream;
    use object_store::{ObjectStore, memory::InMemory, path::Path};

    use super::DeleteDatasetsPartition;
    use crate::{
        array::io_cache::IoCache,
        column::Column,
        consts::DEFAULT_IO_CACHE_BYTES,
        partition::{
            load_partition,
            ops::{stream_read::PartitionStreamReaderBuilder, write::PartitionWriter},
        },
    };

    async fn build_two_dataset_partition(
        store: Arc<dyn ObjectStore>,
        partition_path: &Path,
    ) -> anyhow::Result<crate::partition::Partition<Arc<dyn ObjectStore>>> {
        let io_cache = Arc::new(IoCache::new(DEFAULT_IO_CACHE_BYTES));
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
                    "temperature".to_string(),
                    vec![20i32],
                    vec![1],
                    vec!["x".to_string()],
                    None,
                )?]),
            )
            .await?;

        writer.finish(io_cache).await
    }

    #[tokio::test]
    async fn delete_dataset_by_name_updates_partition_entries() -> anyhow::Result<()> {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let partition_path = Path::from("collections/example/partitions/part-00000");
        let partition = build_two_dataset_partition(store.clone(), &partition_path).await?;
        let original_insert_timestamps = partition.insert_timestamps().to_vec();

        let partition = DeleteDatasetsPartition::new(store.clone(), partition)
            .delete_dataset("dataset-1")
            .execute()
            .await?;

        assert_eq!(partition.deletion_flags(), &[false, true]);
        assert_eq!(partition.logical_entries(), vec!["dataset-0"]);
        assert_eq!(partition.undeleted_dataset_indexes(), vec![0]);
        assert_eq!(partition.insert_timestamps(), original_insert_timestamps);

        let stream = PartitionStreamReaderBuilder::new(store, Arc::new(partition))
            .create_shareable_stream(1)
            .await?;
        let mut datasets = stream.stream_ref();
        let dataset = datasets.next().expect("expected undeleted dataset")?;
        let entry_values = dataset.0.arrays[0].as_arrow_array_ref().await?;
        let entry_values = entry_values.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(entry_values.value(0), "dataset-0");
        assert!(datasets.next().is_none());

        Ok(())
    }

    #[tokio::test]
    async fn delete_dataset_by_index_updates_partition_entries() -> anyhow::Result<()> {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let partition_path = Path::from("collections/example/partitions/part-00000");
        let partition = build_two_dataset_partition(store.clone(), &partition_path).await?;
        let original_insert_timestamps = partition.insert_timestamps().to_vec();

        let partition = DeleteDatasetsPartition::new(store, partition)
            .delete_dataset_index(0)
            .execute()
            .await?;

        assert_eq!(partition.deletion_flags(), &[true, false]);
        assert_eq!(partition.logical_entries(), vec!["dataset-1"]);
        assert_eq!(partition.undeleted_dataset_indexes(), vec![1]);
        assert_eq!(partition.insert_timestamps(), original_insert_timestamps);

        Ok(())
    }

    #[tokio::test]
    async fn delete_dataset_supports_mixed_selectors_and_is_idempotent() -> anyhow::Result<()> {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let partition_path = Path::from("collections/example/partitions/part-00000");
        let partition = build_two_dataset_partition(store.clone(), &partition_path).await?;

        let partition = DeleteDatasetsPartition::new(store.clone(), partition)
            .delete_dataset("dataset-0")
            .delete_dataset_index(0)
            .delete_dataset("dataset-0")
            .execute()
            .await?;

        let partition = DeleteDatasetsPartition::new(store, partition)
            .delete_dataset("dataset-0")
            .execute()
            .await?;

        assert_eq!(partition.deletion_flags(), &[true, false]);
        assert_eq!(partition.logical_entries(), vec!["dataset-1"]);

        Ok(())
    }

    #[tokio::test]
    async fn delete_dataset_errors_for_unknown_dataset_name() -> anyhow::Result<()> {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let partition_path = Path::from("collections/example/partitions/part-00000");
        let partition = build_two_dataset_partition(store.clone(), &partition_path).await?;

        let error = DeleteDatasetsPartition::new(store, partition)
            .delete_dataset("dataset-404")
            .execute()
            .await
            .expect_err("unknown dataset name should return an error");

        assert!(
            error
                .to_string()
                .contains("dataset 'dataset-404' not found")
        );

        Ok(())
    }

    #[tokio::test]
    async fn delete_dataset_errors_for_unknown_dataset_index() -> anyhow::Result<()> {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let partition_path = Path::from("collections/example/partitions/part-00000");
        let partition = build_two_dataset_partition(store.clone(), &partition_path).await?;

        let error = DeleteDatasetsPartition::new(store.clone(), partition)
            .delete_dataset_index(99)
            .execute()
            .await
            .expect_err("unknown dataset index should return an error");

        assert!(
            error
                .to_string()
                .contains("dataset index '99' not found in partition")
        );

        let partition = load_partition(
            store,
            partition_path,
            Arc::new(IoCache::new(DEFAULT_IO_CACHE_BYTES)),
        )
        .await?;
        assert_eq!(partition.deletion_flags(), &[false, false]);

        Ok(())
    }
}
