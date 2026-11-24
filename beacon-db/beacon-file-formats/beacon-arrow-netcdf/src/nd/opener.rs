//! Open one NetCDF file on the ND framework.

use std::sync::Arc;

use async_trait::async_trait;
use beacon_nd_dataset::{attribute_columns, Error, NdDataset, Result};
use beacon_nd_scan::NdFileOpener;
use object_store::{ObjectMeta, ObjectStore};
use oxcdf::AsyncNetcdfFile;

use super::dataset::dataset_of;
use crate::oxcdf_reader::open_file;

/// Opens a NetCDF file. It reads the header only.
#[derive(Debug, Default)]
pub struct NetcdfOpener;

#[async_trait]
impl NdFileOpener for NetcdfOpener {
    fn format_name(&self) -> &'static str {
        "netcdf"
    }

    async fn open(
        &self,
        store: Arc<dyn ObjectStore>,
        file: &ObjectMeta,
    ) -> Result<Arc<dyn NdDataset>> {
        let name = file.location.to_string();
        let netcdf = Arc::new(open_file(store, file).await.map_err(Error::format)?);
        if is_ragged(&netcdf) {
            return Err(Error::Unsupported {
                reason: format!(
                    "'{name}' is a ragged dataset; the NetCDF reader does not support ragged datasets yet"
                ),
                dataset: name,
            });
        }
        let dataset = dataset_of(&name, &netcdf)?;
        Ok(attribute_columns(Arc::new(dataset)))
    }
}

/// True when a root variable has a `sample_dimension` attribute.
fn is_ragged(file: &AsyncNetcdfFile) -> bool {
    file.root()
        .variables
        .iter()
        .any(|variable| variable.attribute("sample_dimension").is_some())
}
