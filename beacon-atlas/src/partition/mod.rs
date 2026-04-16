pub mod entries;
pub mod ops;
pub mod state;

use std::sync::Arc;

use object_store::ObjectStore;

use crate::{
    cache::Cache,
    column::ColumnReader,
    partition::{entries::PartitionEntry, state::PartitionState},
    schema::AtlasSchema,
};

#[derive(Debug, Clone)]
pub struct Partition<S: object_store::ObjectStore + Clone> {
    store: S,
    name: String,
    directory: object_store::path::Path,
    state: PartitionState,
    reader_cache: Arc<moka::future::Cache<String, Arc<ColumnReader<S>>>>,
    cache: Arc<Cache>,
}

impl<S: object_store::ObjectStore + Clone> Partition<S> {
    pub(crate) fn new(
        store: S,
        name: String,
        directory: object_store::path::Path,
        state: PartitionState,
        cache: Arc<Cache>,
    ) -> Self {
        let reader_cache = Arc::new(moka::future::Cache::new(state.schema().columns.len() as u64));
        Self {
            store,
            name,
            directory,
            state,
            reader_cache,
            cache,
        }
    }

    pub(crate) fn try_open(
        store: S,
        name: String,
        directory: object_store::path::Path,
        cache: Arc<Cache>,
    ) -> anyhow::Result<Self> {
        todo!()
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn directory(&self) -> &object_store::path::Path {
        &self.directory
    }

    pub fn schema(&self) -> &AtlasSchema {
        &self.state.schema
    }

    pub(crate) async fn column_reader(
        &self,
        column_name: &str,
    ) -> anyhow::Result<Arc<ColumnReader<S>>> {
        let store = self.store.clone();
        let partition_path = self.directory.clone();
        let cache = self.cache.clone();

        let reader_result = self
            .reader_cache
            .try_get_with(column_name.to_string(), async move {
                let reader = init_column_reader(store, &partition_path, column_name, cache).await?;
                Ok::<_, anyhow::Error>(reader)
            })
            .await
            .map_err(|e| anyhow::anyhow!(e))?;

        Ok(reader_result)
    }

    pub(crate) fn state(&self) -> &PartitionState {
        &self.state
    }

    pub fn entries(&self) -> Vec<PartitionEntry> {
        self.state.entries()
    }

    pub fn physical_entries(&self) -> &[PartitionEntry] {
        self.state.physical_entries()
    }
}

pub(crate) fn column_name_to_path(
    partition_directory: object_store::path::Path,
    column_name: &str,
) -> object_store::path::Path {
    if column_name.starts_with('.') {
        let reserved_name = column_name.trim_start_matches('.');
        partition_directory
            .child("columns")
            .child(format!("__{}", reserved_name))
    } else if column_name.contains('.') {
        let parts: Vec<&str> = column_name.split('.').collect();
        let mut path = partition_directory.child("columns");
        for part in parts {
            path = path.child(part);
        }
        path
    } else {
        partition_directory.child("columns").child(column_name)
    }
}

pub(crate) async fn init_column_reader<S: ObjectStore + Clone>(
    object_store: S,
    partition_path: &object_store::path::Path,
    column_name: &str,
    cache: Arc<Cache>,
) -> anyhow::Result<Arc<ColumnReader<S>>> {
    let column_reader = ColumnReader::new(
        object_store,
        column_name_to_path(partition_path.clone(), column_name),
        cache.vec_cache(),
    )
    .await?;
    Ok(Arc::new(column_reader))
}

// #[cfg(test)]
// mod tests {
//     use std::sync::Arc;

//     use arrow::datatypes::{DataType, Field, Schema};
//     use chrono::DateTime;
//     use object_store::{ObjectStore, memory::InMemory, path::Path};

//     use crate::{IPC_WRITE_OPTS, consts::ENTRIES_FILE, partition::load_partition_state};

//     #[tokio::test]
//     async fn load_partition_state_supports_legacy_entries_without_insert_timestamp()
//     -> anyhow::Result<()> {
//         let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
//         let partition_directory = Path::from("collections/example/partitions/part-legacy");

//         let schema = Arc::new(Schema::new(vec![
//             Field::new("dataset_name", DataType::Utf8, false),
//             Field::new("dataset_index", DataType::UInt32, false),
//             Field::new("deletion", DataType::Boolean, false),
//         ]));
//         let batch = arrow::record_batch::RecordBatch::try_new(
//             schema.clone(),
//             vec![
//                 Arc::new(arrow::array::StringArray::from(vec![
//                     "dataset-0",
//                     "dataset-1",
//                 ])) as Arc<dyn arrow::array::Array>,
//                 Arc::new(arrow::array::UInt32Array::from(vec![0, 1]))
//                     as Arc<dyn arrow::array::Array>,
//                 Arc::new(arrow::array::BooleanArray::from(vec![false, true]))
//                     as Arc<dyn arrow::array::Array>,
//             ],
//         )?;

//         let mut encoded = Vec::new();
//         let mut writer = arrow::ipc::writer::FileWriter::try_new_with_options(
//             &mut encoded,
//             &schema,
//             IPC_WRITE_OPTS.clone(),
//         )?;
//         writer.write(&batch)?;
//         writer.finish()?;

//         store
//             .put(&partition_directory.child(ENTRIES_FILE), encoded.into())
//             .await?;

//         let state =
//             load_partition_state(store, partition_directory, "part-legacy".to_string()).await?;

//         assert_eq!(
//             state.entry_keys(),
//             &["dataset-0".to_string(), "dataset-1".to_string()]
//         );
//         assert_eq!(state.dataset_indexes(), &[0, 1]);
//         assert_eq!(state.deletion_flags(), &[false, true]);
//         assert_eq!(
//             state.insert_timestamps(),
//             &[
//                 DateTime::from_timestamp_nanos(0),
//                 DateTime::from_timestamp_nanos(0)
//             ]
//         );

//         Ok(())
//     }
// }
