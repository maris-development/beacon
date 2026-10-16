use crate::dataset_v2::Dataset;
use datafusion::datasource::listing::PartitionedFile;
use futures::future::BoxFuture;

pub trait NdFileOpener {
    fn open(&self, file: PartitionedFile) -> BoxFuture<'static, anyhow::Result<Dataset>>;
}
