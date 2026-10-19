use std::sync::Arc;

use beacon_nd_array::{dataset_v2::Dataset, nd_file_opener::NdFileOpener};
use datafusion::datasource::listing::PartitionedFile;
use futures::{prelude::future::BoxFuture, FutureExt};
use object_store::ObjectStore;

use crate::oxcdf_reader::open_dataset_v2;

#[derive(Debug, Clone)]
pub struct OxcdfOpener;

impl NdFileOpener for OxcdfOpener {
    fn open(
        &self,
        store: Arc<dyn ObjectStore>,
        file: PartitionedFile,
    ) -> BoxFuture<'static, anyhow::Result<Dataset>> {
        async move { open_dataset_v2(store, file.path().clone()).await }.boxed()
    }
}
