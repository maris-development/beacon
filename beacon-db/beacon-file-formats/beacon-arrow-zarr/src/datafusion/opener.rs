//! [`NdFileOpener`] for zarr groups.

use std::sync::Arc;

use beacon_nd_array::{dataset_v2::Dataset, nd_file_opener::NdFileOpener};
use datafusion::datasource::listing::PartitionedFile;
use futures::{FutureExt, future::BoxFuture};
use object_store::ObjectStore;

use crate::{
    reader::open_dataset,
    util::{ZarrPath, ZarrStorage},
};

/// Opens one leaf zarr group, named by its `zarr.json`, as a [`Dataset`].
#[derive(Debug, Clone, Default)]
pub struct ZarrOpener {
    storage: Option<ZarrStorage>, // Replaces the store of the scan, for Icechunk
}

impl ZarrOpener {
    /// Create an opener that opens groups in `storage`, or in the store of the scan when `None`.
    pub fn new(storage: Option<ZarrStorage>) -> Self {
        Self { storage }
    }
}

impl NdFileOpener for ZarrOpener {
    fn open(
        &self,
        store: Arc<dyn ObjectStore>,
        file: PartitionedFile,
    ) -> BoxFuture<'static, anyhow::Result<Dataset>> {
        let storage = self
            .storage
            .clone()
            .unwrap_or_else(|| ZarrStorage::from_object_store(store));
        async move {
            let path =
                ZarrPath::new_from_object_meta(file.object_meta).map_err(anyhow::Error::msg)?;
            open_dataset(storage.inner(), &path.as_zarr_path()).await
        }
        .boxed()
    }
}

#[cfg(test)]
mod tests {
    use object_store::{ObjectStoreExt, local::LocalFileSystem, path::Path};

    use super::*;

    #[tokio::test]
    async fn a_group_opens_with_its_arrays() {
        let store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix("./test_files").unwrap());
        let meta = store
            .head(&Path::from("gridded-example.zarr/zarr.json"))
            .await
            .unwrap();

        let dataset = ZarrOpener::default()
            .open(store, PartitionedFile::from(meta))
            .await
            .unwrap();

        assert!(dataset.arrays.contains_key("analysed_sst"));
    }
}
