// pub mod cast;
// pub mod delete;
pub mod read;
pub mod write;

// #[cfg(test)]
// mod tests {
//     use arrow::array::AsArray;
//     use chrono::DateTime;
//     use object_store::{memory::InMemory, path::Path};

//     use super::{load_partition_entries, write_partition_entries};

//     #[tokio::test]
//     async fn write_and_load_partition_entries_round_trip() -> anyhow::Result<()> {
//         let store = InMemory::new();
//         let partition_directory = Path::from("collections/example/partitions/part-00000");

//         let entry_keys = vec!["dataset-0".to_string(), "dataset-1".to_string()];
//         let dataset_indexes = vec![0, 1];
//         let deletion_flags = vec![false, true];
//         let insert_timestamps = vec![
//             DateTime::from_timestamp_nanos(1_700_000_000_000_000_000),
//             DateTime::from_timestamp_nanos(1_700_000_000_500_000_000),
//         ];

//         write_partition_entries(
//             &store,
//             &partition_directory,
//             &entry_keys,
//             &dataset_indexes,
//             &deletion_flags,
//             &insert_timestamps,
//         )
//         .await?;

//         let loaded = load_partition_entries(&store, &partition_directory).await?;

//         assert_eq!(loaded.num_columns(), 4);
//         assert_eq!(loaded.num_rows(), 2);

//         let loaded_entry_keys = loaded.column(0).as_string::<i32>();
//         let loaded_indexes = loaded
//             .column(1)
//             .as_primitive::<arrow::datatypes::UInt32Type>();
//         let loaded_deletions = loaded.column(2).as_boolean();
//         let loaded_insert_timestamps = loaded
//             .column(3)
//             .as_primitive::<arrow::datatypes::TimestampNanosecondType>();

//         assert_eq!(loaded_entry_keys.value(0), "dataset-0");
//         assert_eq!(loaded_entry_keys.value(1), "dataset-1");
//         assert_eq!(loaded_indexes.value(0), 0);
//         assert_eq!(loaded_indexes.value(1), 1);
//         assert!(!loaded_deletions.value(0));
//         assert!(loaded_deletions.value(1));
//         assert_eq!(
//             loaded_insert_timestamps.value(0),
//             insert_timestamps[0].timestamp_nanos_opt().unwrap()
//         );
//         assert_eq!(
//             loaded_insert_timestamps.value(1),
//             insert_timestamps[1].timestamp_nanos_opt().unwrap()
//         );

//         Ok(())
//     }

//     #[tokio::test]
//     async fn write_partition_entries_rejects_mismatched_lengths() {
//         let store = InMemory::new();
//         let partition_directory = Path::from("collections/example/partitions/part-00000");

//         let entry_keys = vec!["dataset-0".to_string(), "dataset-1".to_string()];
//         let dataset_indexes = vec![0];
//         let deletion_flags = vec![false, true];
//         let insert_timestamps = vec![DateTime::from_timestamp_nanos(1_700_000_000_000_000_000)];

//         let error = write_partition_entries(
//             &store,
//             &partition_directory,
//             &entry_keys,
//             &dataset_indexes,
//             &deletion_flags,
//             &insert_timestamps,
//         )
//         .await
//         .expect_err("expected length validation error");

//         assert!(
//             error
//                 .to_string()
//                 .contains("entries and deletion flags must have the same length")
//         );
//     }
// }
