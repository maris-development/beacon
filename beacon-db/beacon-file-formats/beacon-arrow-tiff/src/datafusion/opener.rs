//! [`NdFileOpener`] for TIFF/GeoTIFF files.

use std::sync::Arc;

use beacon_nd_array::{dataset_v2::Dataset, nd_file_opener::NdFileOpener};
use datafusion::datasource::listing::PartitionedFile;
use futures::{FutureExt, future::BoxFuture};
use object_store::ObjectStore;

/// Opens one TIFF/GeoTIFF file as a [`Dataset`].
#[derive(Debug, Clone, Default)]
pub struct TiffOpener;

impl NdFileOpener for TiffOpener {
    fn open(
        &self,
        store: Arc<dyn ObjectStore>,
        file: PartitionedFile,
    ) -> BoxFuture<'static, anyhow::Result<Dataset>> {
        async move { crate::reader::open_dataset(store, file.object_meta.location).await }.boxed()
    }
}
