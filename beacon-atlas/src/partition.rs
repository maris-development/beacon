use std::{collections::HashMap, io::Cursor};

use arrow::ipc::writer::FileWriter;
use object_store::{ObjectStore, PutPayload};

use crate::{
    IPC_WRITE_OPTS, attribute::AttributeWriter, dataset::DatasetWriter, variable::VariableWriter,
};

/// Writes partition datasets, variables, and global attributes to object storage.
///
/// A partition owns a collection of dataset entries and persists a compact
/// `entries.arrow` index alongside a boolean `entries_mask.arrow`.
pub struct PartitionWriter<S: ObjectStore + Clone> {
    store: S,
    partition_path: object_store::path::Path,

    /// Names of datasets in this partition.
    entries: Vec<String>,

    /// Writers for each variable in the partition.
    variable_writers: HashMap<String, VariableWriter<S>>,
    /// Writers for each global attribute in the partition.
    global_attributes_writer: HashMap<String, AttributeWriter<S>>,
}

impl<S: ObjectStore + Clone> PartitionWriter<S> {
    /// Create a new partition writer at `collection_path/partition_name`.
    pub fn new(store: S, partition_name: &str, collection_path: &object_store::path::Path) -> Self {
        let partition_path = collection_path.child(partition_name);
        Self {
            entries: Vec::new(),
            partition_path,
            store,
            variable_writers: HashMap::new(),
            global_attributes_writer: HashMap::new(),
        }
    }

    /// Returns the full partition path in object storage.
    pub fn path(&self) -> &object_store::path::Path {
        &self.partition_path
    }

    /// Returns the backing object store.
    pub fn store(&self) -> &S {
        &self.store
    }

    /// Returns the dataset entries tracked for this partition.
    pub fn entries(&self) -> &Vec<String> {
        &self.entries
    }

    /// Append a dataset entry and return a dataset writer scoped to this partition.
    pub fn append_dataset(&mut self, dataset_name: &str) -> DatasetWriter<'_, S> {
        if !self.entries.iter().any(|name| name == dataset_name) {
            self.entries.push(dataset_name.to_string());
        }
        DatasetWriter::new(
            self.store.clone(),
            &self.partition_path.child(dataset_name),
            self,
        )
    }

    pub(crate) fn variable_writers_mut(&mut self) -> &mut HashMap<String, VariableWriter<S>> {
        &mut self.variable_writers
    }

    pub(crate) fn global_attributes_writer_mut(
        &mut self,
    ) -> &mut HashMap<String, AttributeWriter<S>> {
        &mut self.global_attributes_writer
    }

    /// Finalize all writers and persist partition entry index files.
    pub async fn finish(self) -> anyhow::Result<()> {
        for (_, writer) in self.variable_writers {
            writer.finish().await?;
        }
        for (_, writer) in self.global_attributes_writer {
            writer.finish().await?;
        }

        // Write the entries as: entries.arrow IPC file
        let entries_path = self.partition_path.child("entries.arrow");
        let entries_array = arrow::array::StringArray::from(self.entries);
        let entries_batch = arrow::record_batch::RecordBatch::try_new(
            arrow::datatypes::Schema::new(vec![arrow::datatypes::Field::new(
                "entry",
                arrow::datatypes::DataType::Utf8,
                false,
            )])
            .into(),
            vec![std::sync::Arc::new(entries_array)],
        )?;

        let buffer = Cursor::new(Vec::new());
        let mut writer = FileWriter::try_new_with_options(
            buffer,
            &entries_batch.schema(),
            IPC_WRITE_OPTS.clone(),
        )?;
        writer.write(&entries_batch)?;
        let buffer = writer.into_inner()?;

        self.store
            .put(
                &entries_path,
                PutPayload::from_bytes(buffer.into_inner().into()),
            )
            .await?;

        // Write an entries_mask.arrow IPC file with all true values
        let entries_mask_path = self.partition_path.child("entries_mask.arrow");
        let entries_mask_array =
            arrow::array::BooleanArray::from(vec![true; entries_batch.num_rows()]);
        let entries_mask_batch = arrow::record_batch::RecordBatch::try_new(
            arrow::datatypes::Schema::new(vec![arrow::datatypes::Field::new(
                "mask",
                arrow::datatypes::DataType::Boolean,
                false,
            )])
            .into(),
            vec![std::sync::Arc::new(entries_mask_array)],
        )?;

        let buffer = Cursor::new(Vec::new());
        let mut writer = FileWriter::try_new_with_options(
            buffer,
            &entries_mask_batch.schema(),
            IPC_WRITE_OPTS.clone(),
        )?;
        writer.write(&entries_mask_batch)?;
        let buffer = writer.into_inner()?;

        self.store
            .put(
                &entries_mask_path,
                PutPayload::from_bytes(buffer.into_inner().into()),
            )
            .await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::Array;
    use arrow::ipc::reader::FileReader;
    use object_store::{ObjectStore, memory::InMemory, path::Path};

    use super::*;

    #[tokio::test]
    async fn partition_finish_writes_entries_and_mask() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let collection = Path::from("collections/demo");
        let mut writer = PartitionWriter::new(store.clone(), "p0", &collection);

        writer.append_dataset("ds_a");
        writer.append_dataset("ds_b");

        writer.finish().await.unwrap();

        let entries_path = Path::from("collections/demo/p0/entries.arrow");
        let entries_bytes = store
            .get(&entries_path)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        let entries_reader =
            FileReader::try_new(std::io::Cursor::new(entries_bytes.to_vec()), None).unwrap();
        let entries_batch = entries_reader.into_iter().next().unwrap().unwrap();
        let entries_array = entries_batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        assert_eq!(entries_array.len(), 2);
        assert_eq!(entries_array.value(0), "ds_a");
        assert_eq!(entries_array.value(1), "ds_b");

        let mask_path = Path::from("collections/demo/p0/entries_mask.arrow");
        let mask_bytes = store.get(&mask_path).await.unwrap().bytes().await.unwrap();
        let mask_reader =
            FileReader::try_new(std::io::Cursor::new(mask_bytes.to_vec()), None).unwrap();
        let mask_batch = mask_reader.into_iter().next().unwrap().unwrap();
        let mask_array = mask_batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::BooleanArray>()
            .unwrap();
        assert_eq!(mask_array.len(), 2);
        assert!(mask_array.value(0));
        assert!(mask_array.value(1));
    }

    #[tokio::test]
    async fn partition_entries_deduplicate_dataset_names() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let collection = Path::from("collections/dedupe");
        let mut writer = PartitionWriter::new(store.clone(), "p1", &collection);

        writer.append_dataset("ds_a");
        writer.append_dataset("ds_a");
        writer.append_dataset("ds_b");

        writer.finish().await.unwrap();

        let entries_path = Path::from("collections/dedupe/p1/entries.arrow");
        let entries_bytes = store
            .get(&entries_path)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        let entries_reader =
            FileReader::try_new(std::io::Cursor::new(entries_bytes.to_vec()), None).unwrap();
        let entries_batch = entries_reader.into_iter().next().unwrap().unwrap();
        let entries_array = entries_batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        assert_eq!(entries_array.len(), 2);
        assert_eq!(entries_array.value(0), "ds_a");
        assert_eq!(entries_array.value(1), "ds_b");
    }
}
