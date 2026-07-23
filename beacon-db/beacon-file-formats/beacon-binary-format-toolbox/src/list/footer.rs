use std::{path::Path, sync::Arc};

use arrow::datatypes::{Field, Schema};
use beacon_binary_format::reader::async_reader::{AsyncBBFReader, AsyncRangeRead};

pub async fn list_footer<P: AsRef<Path>>(file_name: P) {
    let file = Arc::new(std::fs::File::open(file_name).unwrap());
    let async_range_reader = Arc::new(file) as Arc<dyn AsyncRangeRead>;
    let reader = AsyncBBFReader::new(async_range_reader, 32).await.unwrap();

    let schema = reader.arrow_schema();
    let entry_keys = reader.physical_entries();
    let entry_logical_deletes = reader.entries_logical_deletes();

    println!("BBF Version: {}", reader.version());
    println!("File Schema: {:#?}", SimpleSchema::from(&schema));
    println!("Number of Entries: {}", entry_keys.len());
    println!("Number of Logical Deletes: {}", entry_logical_deletes.len());

    // Loop through keys and deletes together to display the state of each key
    for (key, deleted) in entry_keys.iter().zip(entry_logical_deletes.iter()) {
        println!("Entry Key: {:?} (is deleted: {})", key, deleted);
    }
}

#[derive(Debug)]
struct SimpleSchema {
    columns: Vec<SimpleColumn>,
}

impl From<&Schema> for SimpleSchema {
    fn from(schema: &Schema) -> Self {
        SimpleSchema {
            columns: schema
                .fields()
                .iter()
                .map(|f| SimpleColumn::from(f.as_ref()))
                .collect(),
        }
    }
}

#[derive(Debug)]
struct SimpleColumn {
    name: String,
    data_type: String,
}

impl From<&Field> for SimpleColumn {
    fn from(field: &Field) -> Self {
        SimpleColumn {
            name: field.name().to_string(),
            data_type: field.data_type().to_string(),
        }
    }
}
