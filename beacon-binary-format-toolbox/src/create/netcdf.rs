use beacon_arrow_netcdf::reader::open_dataset;
use beacon_binary_format::array::dimensions::{Dimension, Dimensions};
use beacon_binary_format::entry::{ArrayCollection, Column, Entry};
use beacon_nd_array::arrow::array::ndarray_to_arrow_array;

pub const FILE_EXTENSION: &str = "nc";

pub async fn read_entry(path: &str, skip_column_on_error: bool) -> Result<Entry, anyhow::Error> {
    let dataset = open_dataset(path).await?;

    let mut columns = vec![];
    for (name, _data_type) in dataset.fields() {
        let array = match dataset.get_array(&name) {
            Some(array) => array,
            None => {
                if skip_column_on_error {
                    eprintln!("Column '{}' not found in dataset", name);
                    continue;
                } else {
                    return Err(anyhow::anyhow!("Column '{}' not found in dataset", name));
                }
            }
        };

        // Materialize the lazy ND array into an Arrow array.
        let arrow_array = match ndarray_to_arrow_array(array.as_ref()).await {
            Ok(arrow_array) => arrow_array,
            Err(err) => {
                if skip_column_on_error {
                    eprintln!("Error reading column '{}': {}", name, err);
                    continue;
                } else {
                    return Err(err);
                }
            }
        };

        // Preserve the variable's named dimensions / shape so multi-dimensional
        // variables are not flattened to a 1D column.
        let dim_names = array.dimensions();
        let shape = array.shape();
        let dimensions = if dim_names.is_empty() {
            Dimensions::Scalar
        } else {
            Dimensions::Multi(
                dim_names
                    .into_iter()
                    .zip(shape)
                    .map(Dimension::from)
                    .collect(),
            )
        };

        let column = Column::new(name, arrow_array, dimensions);
        columns.push(column);
    }

    let array_collection = ArrayCollection::new("netcdf_collection", Box::new(columns.into_iter()));

    let entry = Entry::new(array_collection);

    Ok(entry)
}
