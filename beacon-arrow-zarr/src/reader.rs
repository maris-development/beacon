use std::collections::HashMap;

use zarrs::group::{self, Group};
use zarrs_storage::AsyncReadableListableStorageTraits;

use crate::attributes::AttributeValue;

pub struct ArrowGroupReader {
    group: Group<dyn AsyncReadableListableStorageTraits>,
    global_attributes: HashMap<String, AttributeValue>,
    array_attributes: HashMap<String, HashMap<String, AttributeValue>>,
}

impl ArrowGroupReader {
    pub async fn new(group: Group<dyn AsyncReadableListableStorageTraits>) -> Self {
        group.attributes();

        Self { group }
    }
}

#[cfg(test)]
mod tests {

    use std::sync::Arc;

    use object_store::local::LocalFileSystem;
    use zarrs::{array::Array, array_subset::ArraySubset, group::Group};
    use zarrs_object_store::AsyncObjectStore;
    use zarrs_storage::{AsyncListableStorageTraits, AsyncReadableListableStorage};

    use super::*;

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
        println!("Time attrs: {:?}", time_arr.attributes());
        println!("Time data type: {:?}", time_arr.data_type());
    }
}
