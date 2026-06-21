use arrow::csv::ReaderBuilder;
use arrow::record_batch::RecordBatch;
use beacon_binary_format::entry::Entry;
use beacon_binary_format::entry::{ArrayCollection, Column};
use std::fs::File;

pub const FILE_EXTENSION: &str = "csv";
pub fn read_entry(path: &str, delimiter: u8) -> Result<Entry, anyhow::Error> {
    // Open the CSV file
    let schema = arrow::csv::infer_schema_from_files(&[path.to_string()], delimiter, None, true)
        .map_err(|e| anyhow::anyhow!("Failed to infer schema from CSV file: {}", e))?;

    // Reset the file position
    let file = File::open(path).map_err(|e| anyhow::anyhow!("Failed to reopen CSV file: {}", e))?;

    // Create a CSV reader
    let reader = ReaderBuilder::new(schema.into())
        .with_batch_size(128_000)
        .build(file)
        .map_err(|e| anyhow::anyhow!("Failed to create CSV reader: {}", e))?;

    // Read all record batches
    let record_batches: Vec<RecordBatch> = reader
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| anyhow::anyhow!("Failed to read record batches: {}", e))?;

    let mut array_collections = vec![];
    for batch in record_batches {
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
            ArrayCollection::new("csv_collection", Box::new(columns.into_iter()));
        array_collections.push(array_collection);
    }

    match array_collections.len() {
        0 => Err(anyhow::anyhow!(
            "No valid record batches found in the CSV file."
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
