//! Open one HDF5 file on the ND framework.

use std::sync::Arc;

use async_trait::async_trait;
use beacon_arrow_netcdf::dimensions::PhonyDimensions;
use beacon_arrow_netcdf::oxcdf_reader::open_file;
use beacon_nd_dataset::{attribute_columns, Error, NdDataset, Result};
use beacon_nd_scan::NdFileOpener;
use object_store::{ObjectMeta, ObjectStore};
use oxcdf::AsyncNetcdfFile;

use super::dataset::dataset_of;
use crate::conventions::optodas;
use crate::{Hdf5Convention, ReadOptions};

/// Opens an HDF5 file. It reads the metadata only.
#[derive(Debug, Clone, Copy, Default)]
pub struct Hdf5Opener {
    read: ReadOptions,
}

impl Hdf5Opener {
    /// An opener that reads with the table settings `read`.
    pub fn new(read: ReadOptions) -> Self {
        Self { read }
    }
}

#[async_trait]
impl NdFileOpener for Hdf5Opener {
    fn format_name(&self) -> &'static str {
        "hdf5"
    }

    async fn open(
        &self,
        store: Arc<dyn ObjectStore>,
        file: &ObjectMeta,
    ) -> Result<Arc<dyn NdDataset>> {
        let name = file.location.to_string();
        let hdf5 = Arc::new(open_file(store, file).await.map_err(Error::format)?);
        if is_ragged(&hdf5) {
            return Err(Error::Unsupported {
                reason: format!(
                    "'{name}' is a ragged dataset; the HDF5 reader does not support ragged datasets yet"
                ),
                dataset: name,
            });
        }

        // One name per length, so a dataset of one group broadcasts against another.
        let mut phony = PhonyDimensions::of_file(&hdf5);
        if !self.read.unify_phony_dimensions {
            phony = phony.without_renames();
        }
        phony.log_merges(&name);

        // The convention names its axes before any variable is built.
        let convention = match self.read.convention {
            Hdf5Convention::None => None,
            Hdf5Convention::OptoDas => optodas::detect(&hdf5, &phony)
                .await
                .map_err(Error::format)?,
        };
        let (extra, scaled) = match &convention {
            Some(convention) => {
                phony = phony.rename(convention.axis_names());
                (
                    convention.coordinates()?,
                    convention.scale().map(|scale| (optodas::PAYLOAD, scale)),
                )
            }
            None => (Vec::new(), None),
        };

        let dataset = dataset_of(&name, &hdf5, &phony, extra, scaled)?;
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
