use beacon_arrow_netcdf::reader::*;
use beacon_binary_format::entry::{Column, Entry};
use nd_arrow_array::dimensions::{Dimension, Dimensions};

pub const FILE_EXTENSION: &str = "nc";

pub async fn read_entry(path: &str, skip_column_on_error: bool) -> Result<Entry, anyhow::Error> {
    let reader = NetCDFArrowReader::new(path)?;

    let schema = reader.schema();

    let mut columns = vec![];
    for column in schema.fields() {
        match reader.read_column(column.name()) {
            Ok(array) => {
                // Transform the array into nd_arrow_array
                let dimensions = array.dimensions();
                let dimensions_sizes = array.shape();
                let arrow_array = array.as_arrow_array_ref().await.unwrap();
                let nd_dimensions: Vec<nd_arrow_array::dimensions::Dimension> = dimensions
                    .iter()
                    .zip(dimensions_sizes.iter())
                    .map(|(name, size)| Dimension {
                        name: name.clone(),
                        size: *size,
                    })
                    .collect();

                let dims = Dimensions::new(nd_dimensions);

                let nd_arrow_array = nd_arrow_array::NdArrowArray::new(arrow_array, dims).unwrap();

                let column = Column::from_nd_arrow(column.name(), nd_arrow_array);
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
