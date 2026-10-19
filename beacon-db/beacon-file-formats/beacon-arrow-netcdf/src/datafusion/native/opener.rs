use std::sync::Arc;

use beacon_nd_array::{dataset_v2::Dataset, nd_file_opener::NdFileOpener};
use datafusion::datasource::listing::PartitionedFile;
use futures::{prelude::future::BoxFuture, FutureExt};
use object_store::ObjectStore;

#[derive(Debug, Clone)]
pub struct NativeOpener {}

impl NdFileOpener for NativeOpener {
    fn open(
        &self,
        _store: Arc<dyn ObjectStore>,
        _file: PartitionedFile,
    ) -> BoxFuture<'static, anyhow::Result<Dataset>> {
        // ToDo: Implement the logic to open the file using native c-reader using the provided ObjectStore and PartitionedFile.
        async move { Err(anyhow::anyhow!("Not implemented yet")) }.boxed()
    }
}
