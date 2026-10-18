use crate::dataset_v2::Dataset;
use datafusion::datasource::listing::PartitionedFile;
use futures::future::BoxFuture;
use object_store::ObjectStore;
use std::{fmt::Debug, sync::Arc};

/// Reads one file of an nd format into a [`Dataset`].
pub trait NdFileOpener: Send + Sync + Debug {
    /// Open `file` from `store`.
    ///
    /// # Errors
    ///
    /// Returns the error of the read, or of a file that the format cannot read.
    fn open(
        &self,
        store: Arc<dyn ObjectStore>,
        file: PartitionedFile,
    ) -> BoxFuture<'static, anyhow::Result<Dataset>>;
}
