use std::path::Path;

use arrow::array::AsArray;
use arrow::ipc::reader::FileReader;
use beacon_atlas::prelude::AtlasCollection;
use beacon_atlas::partition::PartitionMetadata;

const PARTITION_METADATA_FILE: &str = "atlas_partition.json";
const PARTITION_ENTRIES_FILE: &str = "entries.arrow";

pub(crate) struct PartitionInspection {
    pub(crate) metadata: PartitionMetadata,
    pub(crate) total_datasets: usize,
    pub(crate) loaded_count: usize,
    pub(crate) deleted_count: usize,
    pub(crate) loaded_dataset_names: Vec<String>,
    pub(crate) deleted_dataset_names: Vec<String>,
}

pub(crate) fn inspect_partition<S: object_store::ObjectStore + Clone>(
    collection: &AtlasCollection<S>,
    partition_name: &str,
) -> anyhow::Result<PartitionInspection> {
    let snapshot = collection.snapshot()?;
    anyhow::ensure!(
        snapshot
            .metadata()
            .partitions
            .iter()
            .any(|name| name == partition_name),
        "partition '{}' does not exist in collection '{}'",
        partition_name,
        snapshot.metadata().name
    );

    let collection_path = collection.collection_path().to_string();
    let collection_path = Path::new(&collection_path);
    let partition_path = collection_path.join("partitions").join(partition_name);

    let metadata_path = partition_path.join(PARTITION_METADATA_FILE);
    let metadata_bytes = std::fs::read(&metadata_path)?;
    let metadata: PartitionMetadata = serde_json::from_slice(&metadata_bytes)?;

    let entries_path = partition_path.join(PARTITION_ENTRIES_FILE);
    let entries = read_partition_entries(&entries_path)?;

    Ok(PartitionInspection {
        metadata,
        total_datasets: entries.total,
        loaded_count: entries.loaded.len(),
        deleted_count: entries.deleted.len(),
        loaded_dataset_names: entries.loaded,
        deleted_dataset_names: entries.deleted,
    })
}

struct PartitionEntries {
    total: usize,
    loaded: Vec<String>,
    deleted: Vec<String>,
}

fn read_partition_entries(entries_path: &Path) -> anyhow::Result<PartitionEntries> {
    let file = std::fs::File::open(entries_path)?;
    let reader = FileReader::try_new(file, None)?;

    let schema = reader.schema();
    let dataset_name_index = schema.index_of("dataset_name")?;
    let deletion_index = schema.index_of("deletion")?;

    let mut loaded = Vec::new();
    let mut deleted = Vec::new();
    let mut total = 0usize;

    for batch in reader {
        let batch = batch?;
        let dataset_names = batch.column(dataset_name_index).as_string::<i32>();
        let deletions = batch.column(deletion_index).as_boolean();

        for row in 0..batch.num_rows() {
            let dataset_name = dataset_names.value(row).to_string();
            let is_deleted = deletions.value(row);
            total += 1;

            if is_deleted {
                deleted.push(dataset_name);
            } else {
                loaded.push(dataset_name);
            }
        }
    }

    loaded.sort();
    deleted.sort();

    Ok(PartitionEntries {
        total,
        loaded,
        deleted,
    })
}
