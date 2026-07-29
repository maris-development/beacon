use std::fs::File;

use arrow::{
    array::{Array, ArrayRef, AsArray, RecordBatch, RunArray, UInt32Array},
    datatypes::{Int32Type, RunEndIndexType},
    ipc::reader::FileReader,
};
use beacon_binary_format::entry::{ArrayCollection, Column, Entry};

pub const FILE_EXTENSION: &str = "arrow";

pub fn read_entry(path: &str) -> Result<Entry, anyhow::Error> {
    let file = File::open(path)
        .map_err(|e| anyhow::anyhow!("Failed to open Arrow file '{}': {}", path, e))?;

    let arrow_reader = FileReader::try_new_buffered(file, None)
        .map_err(|e| anyhow::anyhow!("Failed to create Arrow reader for '{}': {}", path, e))?;

    let iter = Box::new(arrow_reader.into_iter().map(|batch| {
        let batch = batch
            .map_err(|e| anyhow::anyhow!("Failed to read record batch: {}", e))
            .unwrap();
        // let batch = convert_record_batch(&batch);
        let batch_schema = batch.schema();
        let mut columns = Vec::new();
        for i in 0..batch.num_columns() {
            let array = batch.column(i);
            let field = batch_schema.field(i);
            let name = field.name().to_string();

            // Convert arrow array to a Column
            let column = Column::try_from_arrow(name, array.clone())
                .map_err(|e| anyhow::anyhow!("Failed to convert arrow array to Column: {}", e))
                .unwrap();

            columns.push(column);
        }

        ArrayCollection::new("arrow_batch", Box::new(columns.into_iter()))
    }));

    let entry = Entry::new_chunked(iter);

    Ok(entry)
}

fn convert_schema_run_arrays_to_flat(
    schema: &arrow::datatypes::Schema,
) -> arrow::datatypes::Schema {
    let fields: Vec<arrow::datatypes::Field> = schema
        .fields()
        .iter()
        .map(|field| {
            let data_type = match field.data_type() {
                arrow::datatypes::DataType::RunEndEncoded(_, run) => run.data_type().clone(),
                _ => field.data_type().clone(),
            };
            arrow::datatypes::Field::new(field.name(), data_type, field.is_nullable())
        })
        .collect();
    arrow::datatypes::Schema::new(fields)
}

fn convert_record_batch(record_batch: &RecordBatch) -> RecordBatch {
    let new_schema = convert_schema_run_arrays_to_flat(record_batch.schema().as_ref());
    let new_arrays: Vec<ArrayRef> = record_batch
        .columns()
        .iter()
        .map(|array| match array.as_run_opt::<Int32Type>() {
            Some(run) => flatten_run_array(run),
            None => array.clone(),
        })
        .collect();
    RecordBatch::try_new(std::sync::Arc::new(new_schema), new_arrays)
        .expect("Failed to create new RecordBatch")
}

fn flatten_run_array<R: RunEndIndexType>(run: &RunArray<R>) -> ArrayRef {
    // 1) logical indices [0, 1, 2, ..., len-1]
    let len = run.len();
    let logical: Vec<u32> = (0..(len as u32)).collect();

    // 2) map logical -> physical indices inside the values array
    let phys = run.get_physical_indices(&logical).expect("indices mapping");

    // 3) gather those physical indices from the values() array
    let idx = UInt32Array::from_iter_values(phys.into_iter().map(|i| i as u32));
    arrow::compute::take(run.values().as_ref(), &idx, None).expect("gather")
}
