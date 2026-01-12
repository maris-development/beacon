use beacon_arrow_netcdf::reader::*;
use beacon_binary_format::entry::{Column, Entry};

pub const FILE_EXTENSION: &str = "nc";

pub fn read_entry(path: &str, skip_column_on_error: bool) -> Result<Entry, anyhow::Error> {
    let reader = NetCDFArrowReader::new(path)?;

    let schema = reader.schema();

    let mut columns = vec![];
    for column in schema.fields() {
        match reader.read_column(column.name()) {
            Ok(array) => {
                let column = Column::from_nd_arrow(column.name(), array);
                columns.push(column);
            }
            Err(err) => {
                if skip_column_on_error {
                    eprintln!("Error reading column '{}': {}", column.name(), err);
                } else {
                    return Err(err.into());
                }
            }
        }
    }

    let array_collection = beacon_binary_format::entry::ArrayCollection::new(
        "netcdf_collection",
        Box::new(columns.into_iter()),
    );

    let entry = Entry::new(array_collection);

    Ok(entry)
}
