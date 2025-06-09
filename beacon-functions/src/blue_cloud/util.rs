use std::collections::HashMap;
use std::error::Error;
use std::fs::File;
use std::path::Path;

pub fn read_mappings<P: AsRef<Path>>(
    path: P,
    to_map_column: &str,
) -> Result<HashMap<String, String>, Box<dyn Error + Send + Sync>> {
    // Open the CSV file
    let file = File::open(path.as_ref())?;
    let mut rdr = csv::Reader::from_reader(file);

    let headers = rdr.headers()?.clone();

    // Find index of the column name
    let column_index = headers
        .iter()
        .position(|h| h == to_map_column)
        .ok_or("Column not found")?;

    let mut map: HashMap<String, String> = HashMap::new();

    for result in rdr.records() {
        let record = result?;
        let key = record.get(0).unwrap_or("").trim();
        let value = record.get(column_index).unwrap_or("").trim();

        if !value.is_empty() {
            map.insert(key.to_string(), value.to_string());
        }
    }

    // Print to verify
    Ok(map)
}

pub fn read_mappings_from_reader<R: std::io::Read>(
    reader: R,
    to_map_column: &str,
) -> Result<HashMap<String, String>, Box<dyn Error + Send + Sync>> {
    let mut rdr = csv::Reader::from_reader(reader);

    let headers = rdr.headers()?.clone();

    // Find index of the column name
    let column_index = headers
        .iter()
        .position(|h| h == to_map_column)
        .ok_or("Column not found")?;

    let mut map: HashMap<String, String> = HashMap::new();

    for result in rdr.records() {
        let record = result?;
        let key = record.get(0).unwrap_or("").trim();
        let value = record.get(column_index).unwrap_or("").trim();

        if !value.is_empty() {
            map.insert(key.to_string(), value.to_string());
        }
    }

    Ok(map)
}
