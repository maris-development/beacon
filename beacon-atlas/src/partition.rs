use std::{collections::HashMap, io::Cursor};

use arrow::ipc::writer::FileWriter;
use object_store::{ObjectStore, PutPayload};

use crate::{
    IPC_WRITE_OPTS, attribute::AttributeWriter, dataset::DatasetWriter, variable::VariableWriter,
};

pub struct PartitionWriter<S: ObjectStore + Clone> {
    store: S,
    partition_path: object_store::path::Path,

    /// Names of datasets in this partition
    entries: Vec<String>,

    /// Writers for each variable in the partition
    variable_writers: HashMap<String, VariableWriter<S>>,
    /// Writers for each global attribute in the partition
    global_attributes_writer: HashMap<String, AttributeWriter<S>>,
}

impl<S: ObjectStore + Clone> PartitionWriter<S> {
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

    pub fn path(&self) -> &object_store::path::Path {
        &self.partition_path
    }

    pub fn store(&self) -> &S {
        &self.store
    }

    pub fn entries(&self) -> &Vec<String> {
        &self.entries
    }

    pub fn append_dataset(&mut self, dataset_name: &str) -> DatasetWriter<'_, S> {
        if !self.entries.contains(&dataset_name.to_string()) {
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
