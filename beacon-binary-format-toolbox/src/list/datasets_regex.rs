use std::{path::Path, sync::Arc};

use arrow::{
    array::BooleanArray,
    datatypes::{Field, Schema},
};
use beacon_binary_format::reader::async_reader::{AsyncBBFReader, AsyncRangeRead};
use futures::StreamExt;

pub async fn list_datasets_regex<P: AsRef<Path>>(
    file_name: P,
    pattern: String,
    offset: Option<usize>,
    limit: Option<usize>,
    column: Option<String>,
) {
    let file = Arc::new(std::fs::File::open(file_name).unwrap());
    let async_range_reader = Arc::new(file) as Arc<dyn AsyncRangeRead>;
    let reader = AsyncBBFReader::new(async_range_reader, 32).await.unwrap();

    let schema = reader.arrow_schema();
    let entry_keys = reader.physical_entries();

    let regex = regex::Regex::new(&pattern).unwrap();

    for (i, entry_key) in entry_keys.iter().enumerate() {
        if regex.is_match(&entry_key.name) {
            println!("Found matching entry at index {}: {:?}", i, entry_key);
            list_dataset(i, entry_keys.len(), &reader, column.clone(), &schema).await;
        }
    }
}

async fn list_dataset(
    index: usize,
    num_entries: usize,
    reader: &AsyncBBFReader,
    column: Option<String>,
    file_schema: &Schema,
) {
    let column_indices_to_process = if let Some(column_name) = column {
        file_schema
            .index_of(&column_name)
            .map(|idx| vec![idx])
            .unwrap_or_else(|_| {
                eprintln!("Column '{}' not found in schema", column_name);
                vec![]
            })
    } else {
        (0..file_schema.fields().len()).collect::<Vec<_>>()
    };
    let mut boolean_array = vec![false; num_entries];
    boolean_array[index] = true;
    let selection_array = BooleanArray::from(boolean_array);

    for col_idx in column_indices_to_process {
        // Process each column index as needed
        let array = reader
            .read(Some(vec![col_idx]), Some(selection_array.clone()))
            .await
            .unwrap();

        let mut stream = Box::pin(array.stream().await.into_stream());

        while let Some(batch) = stream.next().await {
            // Process each batch as needed
            let array = &batch.arrays()[0];
            let dims = array.dimensions();
            let batch = batch.to_arrow_record_batch().unwrap();

            println!(
                "Column {}:
                Data Type: {:?}
                Dimensions: {:?}
                \n{}",
                file_schema.field(col_idx).name(),
                array.data_type(),
                dims,
                arrow::util::pretty::pretty_format_batches(&[batch]).unwrap()
            );
        }
    }
}
