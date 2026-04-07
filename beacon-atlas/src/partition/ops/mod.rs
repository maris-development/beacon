use std::sync::Arc;

use arrow::{array::RecordBatch, datatypes::Field};
use chrono::{DateTime, Utc};
use object_store::ObjectStore;

use crate::{IPC_WRITE_OPTS, consts::ENTRIES_FILE};

pub mod cast;
pub mod delete;
pub mod read;
pub mod statistics;
pub mod stream_read;
pub mod write;

/// Writes the partition `entries.arrow` file.
///
/// The input slices represent one logical table and must be aligned by row:
/// dataset name, dataset index, deletion flag, and insertion timestamp.
pub(crate) async fn write_partition_entries<S: ObjectStore + ?Sized>(
    object_store: &S,
    partition_directory: &object_store::path::Path,
    entry_keys: &[String],
    partition_dataset_indexes: &[u32],
    deletion_flags: &[bool],
    insert_timestamps: &[DateTime<Utc>],
) -> anyhow::Result<()> {
    anyhow::ensure!(
        entry_keys.len() == deletion_flags.len()
            && entry_keys.len() == partition_dataset_indexes.len()
            && entry_keys.len() == insert_timestamps.len(),
        "entries and deletion flags must have the same length"
    );

    let insert_timestamp_nanos = insert_timestamps
        .iter()
        .map(|timestamp| {
            timestamp
                .timestamp_nanos_opt()
                .ok_or_else(|| anyhow::anyhow!("insert timestamp is out of range for nanoseconds"))
        })
        .collect::<anyhow::Result<Vec<_>>>()?;

    // Persist entries as a compact Arrow IPC table.
    let schema = Arc::new(arrow::datatypes::Schema::new(vec![
        Field::new("dataset_name", arrow::datatypes::DataType::Utf8, false),
        Field::new("dataset_index", arrow::datatypes::DataType::UInt32, false),
        Field::new("deletion", arrow::datatypes::DataType::Boolean, false),
        Field::new(
            "insert_timestamp",
            arrow::datatypes::DataType::Timestamp(
                arrow::datatypes::TimeUnit::Nanosecond,
                Some("UTC".into()),
            ),
            false,
        ),
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
            Arc::new(
                arrow::array::TimestampNanosecondArray::from(insert_timestamp_nanos)
                    .with_timezone("UTC"),
            ) as Arc<dyn arrow::array::Array>,
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

/// Loads and concatenates all batches from the partition `entries.arrow` file.
///
/// Returns a single [`RecordBatch`] containing `dataset_name`, `dataset_index`,
/// `deletion`, and `insert_timestamp` columns.
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
    // Readers consume one batch at a time; combine for simpler downstream access.
    let concatenated_batch = arrow::compute::concat_batches(&schema, &batches)?;
    Ok(concatenated_batch)
}

#[cfg(test)]
mod tests {
    use arrow::array::AsArray;
    use chrono::DateTime;
    use object_store::{memory::InMemory, path::Path};

    use super::{load_partition_entries, write_partition_entries};

    #[tokio::test]
    async fn write_and_load_partition_entries_round_trip() -> anyhow::Result<()> {
        let store = InMemory::new();
        let partition_directory = Path::from("collections/example/partitions/part-00000");

        let entry_keys = vec!["dataset-0".to_string(), "dataset-1".to_string()];
        let dataset_indexes = vec![0, 1];
        let deletion_flags = vec![false, true];
        let insert_timestamps = vec![
            DateTime::from_timestamp_nanos(1_700_000_000_000_000_000),
            DateTime::from_timestamp_nanos(1_700_000_000_500_000_000),
        ];

        write_partition_entries(
            &store,
            &partition_directory,
            &entry_keys,
            &dataset_indexes,
            &deletion_flags,
            &insert_timestamps,
        )
        .await?;

        let loaded = load_partition_entries(&store, &partition_directory).await?;

        assert_eq!(loaded.num_columns(), 4);
        assert_eq!(loaded.num_rows(), 2);

        let loaded_entry_keys = loaded.column(0).as_string::<i32>();
        let loaded_indexes = loaded
            .column(1)
            .as_primitive::<arrow::datatypes::UInt32Type>();
        let loaded_deletions = loaded.column(2).as_boolean();
        let loaded_insert_timestamps = loaded
            .column(3)
            .as_primitive::<arrow::datatypes::TimestampNanosecondType>();

        assert_eq!(loaded_entry_keys.value(0), "dataset-0");
        assert_eq!(loaded_entry_keys.value(1), "dataset-1");
        assert_eq!(loaded_indexes.value(0), 0);
        assert_eq!(loaded_indexes.value(1), 1);
        assert!(!loaded_deletions.value(0));
        assert!(loaded_deletions.value(1));
        assert_eq!(
            loaded_insert_timestamps.value(0),
            insert_timestamps[0].timestamp_nanos_opt().unwrap()
        );
        assert_eq!(
            loaded_insert_timestamps.value(1),
            insert_timestamps[1].timestamp_nanos_opt().unwrap()
        );

        Ok(())
    }

    #[tokio::test]
    async fn write_partition_entries_rejects_mismatched_lengths() {
        let store = InMemory::new();
        let partition_directory = Path::from("collections/example/partitions/part-00000");

        let entry_keys = vec!["dataset-0".to_string(), "dataset-1".to_string()];
        let dataset_indexes = vec![0];
        let deletion_flags = vec![false, true];
        let insert_timestamps = vec![DateTime::from_timestamp_nanos(1_700_000_000_000_000_000)];

        let error = write_partition_entries(
            &store,
            &partition_directory,
            &entry_keys,
            &dataset_indexes,
            &deletion_flags,
            &insert_timestamps,
        )
        .await
        .expect_err("expected length validation error");

        assert!(
            error
                .to_string()
                .contains("entries and deletion flags must have the same length")
        );
    }
}
