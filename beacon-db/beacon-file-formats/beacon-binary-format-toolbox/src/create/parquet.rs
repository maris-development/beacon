use beacon_binary_format::entry::Entry;
use beacon_binary_format::entry::{ArrayCollection, Column};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::fs::File;

pub const FILE_EXTENSION: &str = "parquet";

pub fn read_entry(path: &str) -> Result<Entry, anyhow::Error> {
    // Open the Parquet file
    let file =
        File::open(path).map_err(|e| anyhow::anyhow!("Failed to open Parquet file: {}", e))?;

    // Create a Parquet reader
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|e| anyhow::anyhow!("Failed to create Parquet reader: {}", e))?;

    let reader = builder
        .with_batch_size(128_000)
        .build()
        .map_err(|e| anyhow::anyhow!("Failed to build Parquet record batch reader: {}", e))?;

    // Read all record batches
    let mut array_collections = vec![];
    for batch in reader {
        let batch = batch.map_err(|e| anyhow::anyhow!("Failed to read record batch: {}", e))?;
        let batch_schema = batch.schema();
        let mut columns = Vec::new();
        for i in 0..batch.num_columns() {
            let array = batch.column(i);
            let field = batch_schema.field(i);
            let name = field.name().to_string();

            // Convert arrow array to a Column
            let column = Column::try_from_arrow(name, array.clone())
                .map_err(|e| anyhow::anyhow!("Failed to convert arrow array to Column: {}", e))?;

            columns.push(column);
        }

        let array_collection =
            ArrayCollection::new("parquet_collection", Box::new(columns.into_iter()));
        array_collections.push(array_collection);
    }

    match array_collections.len() {
        0 => Err(anyhow::anyhow!(
            "No valid record batches found in the Parquet file."
        )),
        1 => {
            let entry = Entry::new(array_collections.into_iter().next().unwrap());
            Ok(entry)
        }
        _ => {
            let entry = Entry::new_chunked(Box::new(array_collections.into_iter()));
            Ok(entry)
        }
    }
}
