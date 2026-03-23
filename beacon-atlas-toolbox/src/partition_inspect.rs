use beacon_atlas::partition::PartitionMetadata;
use beacon_atlas::prelude::AtlasCollection;

pub(crate) struct PartitionInspection {
    pub(crate) metadata: PartitionMetadata,
    pub(crate) total_datasets: usize,
    pub(crate) loaded_count: usize,
    pub(crate) deleted_count: usize,
    pub(crate) loaded_dataset_names: Vec<String>,
    pub(crate) deleted_dataset_names: Vec<String>,
}

pub(crate) async fn inspect_partition<S: object_store::ObjectStore + Clone>(
    collection: &mut AtlasCollection<S>,
    partition_name: &str,
) -> anyhow::Result<PartitionInspection> {
    let partition = collection.get_partition(partition_name).await?;
    let metadata = partition.metadata().clone();

    let total_datasets = partition.dataset_indexes().len();
    let mut loaded_dataset_names = Vec::new();
    let mut deleted_dataset_names = Vec::new();

    for (dataset_name, deleted) in partition
        .entry_keys()
        .iter()
        .zip(partition.deletion_flags().iter())
    {
        if *deleted {
            deleted_dataset_names.push(dataset_name.clone());
        } else {
            loaded_dataset_names.push(dataset_name.clone());
        }
    }

    loaded_dataset_names.sort();
    deleted_dataset_names.sort();

    Ok(PartitionInspection {
        metadata,
        total_datasets,
        loaded_count: loaded_dataset_names.len(),
        deleted_count: deleted_dataset_names.len(),
        loaded_dataset_names,
        deleted_dataset_names,
    })
}
