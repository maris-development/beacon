//! Open one TIFF file on the ND framework.

use std::sync::Arc;

use async_trait::async_trait;
use beacon_nd_dataset::{Error, NdDataset, Result};
use beacon_nd_scan::NdFileOpener;
use object_store::{ObjectMeta, ObjectStore};

use super::dataset::dataset_of;
use crate::reader::{ObjectStoreAsyncReader, read_image_ifd};

/// Opens a TIFF file. It reads the IFDs only.
#[derive(Debug, Default)]
pub struct TiffOpener;

#[async_trait]
impl NdFileOpener for TiffOpener {
    fn format_name(&self) -> &'static str {
        "tiff"
    }

    async fn open(
        &self,
        store: Arc<dyn ObjectStore>,
        file: &ObjectMeta,
    ) -> Result<Arc<dyn NdDataset>> {
        let name = file.location.to_string();
        tracing::debug!(path = %name, "opening TIFF dataset");
        let reader = ObjectStoreAsyncReader::new(store, file.location.clone());
        let (ifd, endianness) = read_image_ifd(&reader).await.map_err(Error::format)?;
        Ok(Arc::new(dataset_of(&name, &ifd, endianness, reader)?))
    }
}
