use std::collections::{HashMap, HashSet};

use indexmap::IndexMap;
use zarrs::{array::Array, group::Group};
use zarrs_storage::AsyncReadableListableStorageTraits;

use crate::attributes::AttributeValue;

pub struct ArrowGroupReader {
    group: Group<dyn AsyncReadableListableStorageTraits>,
    schema: arrow::datatypes::SchemaRef,
    arrays: IndexMap<String, Array<dyn AsyncReadableListableStorageTraits>>,
    attributes: IndexMap<String, AttributeValue>,
}

impl ArrowGroupReader {
    pub async fn new(group: Group<dyn AsyncReadableListableStorageTraits>) -> Self {
        let mut fields = Vec::new();

        let mut attributes = IndexMap::new();
        let mut arrays = IndexMap::new();
        for (attr_name, attr_value) in group.attributes() {
            if let Some(attr_value) = AttributeValue::from_json_value(attr_value) {
                let arrow_data_type = attr_value.arrow_data_type();
                fields.push(arrow::datatypes::Field::new(
                    format!(".{}", attr_name),
                    arrow_data_type,
                    true,
                ));
                attributes.insert(format!(".{}", attr_name), attr_value);
            }
        }

        let group_path = group.path();

        for array in group.async_child_arrays().await.unwrap() {
            let array_json_attributes = array.attributes();
            let array_node_path = array.path().clone();
            let array_name = array_node_path
                .as_str()
                .strip_prefix(group_path.as_str())
                .unwrap()
                .to_string();

            // Strip the / from the start of the array name if present
            let array_name = array_name.strip_prefix('/').unwrap_or(&array_name);

            for (attr_name, attr_value) in array_json_attributes {
                if let Some(attr_value) = AttributeValue::from_json_value(attr_value) {
                    fields.push(arrow::datatypes::Field::new(
                        format!("{}.{}", array_name, attr_name),
                        attr_value.arrow_data_type(),
                        true,
                    ));
                    attributes.insert(format!("{}.{}", array_name, attr_name), attr_value);
                }
            }

            let zarr_data_type = array.data_type();
            if let Ok(arrow_data_type) =
                crate::data_types::try_zarrs_dtype_to_arrow(zarr_data_type.clone())
            {
                fields.push(arrow::datatypes::Field::new(
                    array_name,
                    arrow_data_type,
                    true,
                ));
                arrays.insert(array_name.to_string(), array);
            } else {
                // Unsupported data type, skip adding field
                tracing::warn!(
                    "Skipping array {} with unsupported data type {:?}",
                    array_name,
                    zarr_data_type
                );
            }
        }

        Self {
            schema: std::sync::Arc::new(arrow::datatypes::Schema::new(fields)),
            group,
            arrays,
            attributes,
        }
    }

    pub fn arrow_schema(&self) -> arrow::datatypes::SchemaRef {
        self.schema.clone()
    }

    pub fn arrays(&self) -> &IndexMap<String, Array<dyn AsyncReadableListableStorageTraits>> {
        &self.arrays
    }

    pub fn attributes(&self) -> &IndexMap<String, AttributeValue> {
        &self.attributes
    }
}

#[cfg(test)]
mod tests {

    use std::sync::Arc;

    use object_store::local::LocalFileSystem;
    use zarrs::{array::Array, array_subset::ArraySubset, group::Group};
    use zarrs_object_store::AsyncObjectStore;
    use zarrs_storage::AsyncReadableListableStorage;

    #[tokio::test]
    async fn test_name() {
        let local_fs = LocalFileSystem::new_with_prefix("./test_files").unwrap();

        let object_store = AsyncObjectStore::new(local_fs);

        let zarr_store: AsyncReadableListableStorage = Arc::new(object_store);

        let group = Group::async_open(zarr_store.clone(), "/gridded-example.zarr")
            .await
            .unwrap();

        println!("{:?}", group.attributes());

        println!("{:?}", group.async_child_array_paths().await.unwrap());
        println!("{:?}", group.async_child_group_paths().await.unwrap());

        let sst_arr = Array::async_open(zarr_store.clone(), "/gridded-example.zarr/analysed_sst")
            .await
            .unwrap();
        println!("SST: {:?}", sst_arr.chunk_grid());

        let lat_arr = Array::async_open(zarr_store.clone(), "/gridded-example.zarr/lat")
            .await
            .unwrap();
        println!("Lat: {:?}", lat_arr.chunk_grid());
        println!("Lat data type: {:?}", lat_arr.data_type());

        let arr = lat_arr
            .async_retrieve_array_subset_ndarray::<f32>(&ArraySubset::new_with_ranges(&[(0..10)]))
            .await
            .unwrap();

        println!("Lat arr: {:?}", arr);

        let time_arr = Array::async_open(zarr_store.clone(), "/gridded-example.zarr/time")
            .await
            .unwrap();

        println!("Time: {:?}", time_arr.chunk_grid());
        println!("Time shape: {:?}", time_arr.chunk_grid_shape());
        println!("Time attrs: {:?}", time_arr.attributes());
        println!("Time data type: {:?}", time_arr.data_type());
    }

    #[tokio::test]
    async fn test_arrow_schema() {
        let local_fs = LocalFileSystem::new_with_prefix("./test_files").unwrap();

        let object_store = AsyncObjectStore::new(local_fs);

        let zarr_store: AsyncReadableListableStorage = Arc::new(object_store);

        let group = Group::async_open(zarr_store.clone(), "/gridded-example.zarr")
            .await
            .unwrap();

        let reader = super::ArrowGroupReader::new(group).await;

        let schema = reader.arrow_schema();
        for field in schema.fields() {
            println!("Field: {:?} : {:?}", field.name(), field.data_type());
        }
    }
}
