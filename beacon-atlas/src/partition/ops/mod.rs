use std::sync::Arc;

use arrow::{array::RecordBatch, datatypes::Field};
use object_store::ObjectStore;

use crate::{IPC_WRITE_OPTS, consts::ENTRIES_FILE};

pub mod read;
pub mod stream_read;
pub mod write;

pub(crate) async fn write_partition_entries<S: ObjectStore + ?Sized>(
    object_store: &S,
    partition_directory: &object_store::path::Path,
    entry_keys: &[String],
    partition_dataset_indexes: &[u32],
    deletion_flags: &[bool],
) -> anyhow::Result<()> {
    anyhow::ensure!(
        entry_keys.len() == deletion_flags.len()
            && entry_keys.len() == partition_dataset_indexes.len(),
        "entries and deletion flags must have the same length"
    );

    let schema = Arc::new(arrow::datatypes::Schema::new(vec![
        Field::new("dataset_name", arrow::datatypes::DataType::Utf8, false),
        Field::new("dataset_index", arrow::datatypes::DataType::UInt32, false),
        Field::new("deletion", arrow::datatypes::DataType::Boolean, false),
    ]));
    let batch = arrow::record_batch::RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(arrow::array::StringArray::from(entry_keys.to_vec()))
                as Arc<dyn arrow::array::Array>,
            Arc::new(arrow::array::UInt32Array::from(
                partition_dataset_indexes.to_vec(),
            )) as Arc<dyn arrow::array::Array>,
            Arc::new(arrow::array::BooleanArray::from(deletion_flags.to_vec()))
                as Arc<dyn arrow::array::Array>,
        ],
    )?;

    let mut encoded = Vec::new();
    let mut writer = arrow::ipc::writer::FileWriter::try_new_with_options(
        &mut encoded,
        &schema,
        IPC_WRITE_OPTS.clone(),
    )?;
    writer.write(&batch)?;
    writer.finish()?;

    object_store
        .put(&partition_directory.child(ENTRIES_FILE), encoded.into())
        .await?;

    Ok(())
}

pub(super) async fn load_partition_entries<S: ObjectStore + ?Sized>(
    object_store: &S,
    partition_directory: &object_store::path::Path,
) -> anyhow::Result<RecordBatch> {
    let file_bytes = object_store
        .get(&partition_directory.child(ENTRIES_FILE))
        .await?
        .bytes()
        .await?;
    let cursor = std::io::Cursor::new(file_bytes);
    let file_reader = arrow::ipc::reader::FileReader::try_new(cursor, None)?;
    let schema = file_reader.schema();
    let batches = file_reader.collect::<Result<Vec<_>, arrow::error::ArrowError>>()?;
    let concatenated_batch = arrow::compute::concat_batches(&schema, &batches)?;
    Ok(concatenated_batch)
}
