use std::{path::Path, sync::Arc};

use arrow::{
    array::{Array, RecordBatch, StringArray},
    datatypes::{DataType, Field, Schema},
    util::display::FormatOptions,
};
use beacon_binary_format::{
    entry::{self, EntryKey},
    index::{
        ArchivedIndex, Index,
        pruning::{
            ArchivedCombinedColumnStatistics, CombinedColumnStatistics, PruningIndex,
            PruningIndexReader,
        },
    },
    reader::async_reader::{AsyncBBFReader, AsyncRangeRead},
};

pub async fn list_pruning_index<P: AsRef<Path>>(file_name: P, column: Option<String>) {
    let file = Arc::new(std::fs::File::open(file_name).unwrap());
    let async_range_reader = Arc::new(file) as Arc<dyn AsyncRangeRead>;
    let reader = AsyncBBFReader::new(async_range_reader.clone(), 32)
        .await
        .unwrap();

    let schema = reader.arrow_schema();
    let index_reader = reader
        .pruning_index()
        .await
        .expect("No pruning index found.");
    let entry_keys = reader.physical_entries();

    println!("Number of entry keys: {}", entry_keys.len());
    println!("Number of containers: {}", index_reader.num_containers());

    if let Some(column_name) = column {
        let stats = index_reader.column(&column_name).await.unwrap().unwrap();
        let batch = create_record_batch_pruning_view(stats.as_ref(), &entry_keys);

        println!(
            "Statistics for column '{}':\n{}",
            column_name,
            arrow::util::pretty::pretty_format_batches_with_options(
                &[batch],
                &FormatOptions::new().with_types_info(true)
            )
            .unwrap()
        );
    } else {
        // Go through all columns
        for field in schema.fields() {
            let stats = index_reader.column(field.name()).await.unwrap().unwrap();

            let batch = create_record_batch_pruning_view(stats.as_ref(), &entry_keys);

            println!(
                "Statistics for column '{}':\n{}",
                field.name(),
                arrow::util::pretty::pretty_format_batches_with_options(
                    &[batch],
                    &FormatOptions::new().with_types_info(true)
                )
                .unwrap()
            );
        }
    }
}

fn create_record_batch_pruning_view(
    statistics: &ArchivedCombinedColumnStatistics,
    entry_keys: &[EntryKey],
) -> RecordBatch {
    let entry_key_strings: Vec<String> =
        entry_keys.iter().map(|key| format!("{:?}", key)).collect();

    let columns = [
        ("min_value", statistics.min_value().clone()),
        ("max_value", statistics.max_value().clone()),
        ("null_count", statistics.null_count().clone()),
        ("row_count", statistics.row_count().clone()),
        ("entry_keys", Arc::new(StringArray::from(entry_key_strings))),
    ];

    let schema = Schema::new(
        columns
            .iter()
            .map(|(name, array)| Field::new(name.to_string(), array.data_type().clone(), true))
            .collect::<Vec<_>>(),
    );
    RecordBatch::try_new(
        Arc::new(schema),
        columns.iter().map(|(_, array)| array.clone()).collect(),
    )
    .unwrap()
}
