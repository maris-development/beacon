use crate::dataset_v2::Dataset;
use datafusion::datasource::listing::PartitionedFile;
use futures::future::BoxFuture;
use std::fmt::Debug;

pub trait NdFileOpener: Send + Sync + Debug {
    fn open(&self, file: PartitionedFile) -> BoxFuture<'static, anyhow::Result<Dataset>>;
}
