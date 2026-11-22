//! Open one NetCDF file for a read.
//!
//! The schema inference, the scan and the statistics all open a file here, so
//! they agree on which files they read and on the error a file gives.

use std::sync::Arc;

use beacon_nd_array::dataset::{project_read_dimensions_or_skip, AnyDataset, Dataset};
use datafusion::error::{DataFusionError, Result};
use object_store::{ObjectMeta, ObjectStore};

/// Which files a read passes over in place of an error.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct SkipFiles {
    /// Skip a file that does not fit `read_dimensions`.
    pub unbroadcastable: bool,
    /// Skip a file the reader cannot open.
    pub invalid: bool,
}

/// Open `object` and project it to `read_dimensions`.
///
/// `None` when `skip` passes over the file. A ragged file is an error.
pub(crate) async fn open_for_read(
    store: &Arc<dyn ObjectStore>,
    object: &ObjectMeta,
    read_dimensions: Option<Vec<String>>,
    skip: SkipFiles,
) -> Result<Option<Dataset>> {
    let dataset = match crate::oxcdf_reader::open_object(store.clone(), object).await {
        Ok(dataset) => dataset,
        Err(e) if skip.invalid => {
            tracing::warn!(file = %object.location, error = %e, "skipping an unreadable NetCDF file");
            return Ok(None);
        }
        Err(e) => return Err(DataFusionError::Execution(format!("failed to open: {e}"))),
    };
    let Some(dataset) = project_read_dimensions_or_skip(
        dataset,
        read_dimensions,
        skip.unbroadcastable,
        Some("netcdf"),
    )
    .map_err(|e| DataFusionError::Execution(e.to_string()))?
    else {
        return Ok(None);
    };
    regular(dataset).map(Some)
}

/// Open `object` with all its arrays. A ragged file is an error.
pub(crate) async fn open_whole(
    store: &Arc<dyn ObjectStore>,
    object: &ObjectMeta,
) -> Result<Dataset> {
    let dataset = crate::oxcdf_reader::open_object(store.clone(), object)
        .await
        .map_err(|e| DataFusionError::Execution(format!("failed to open: {e}")))?;
    regular(dataset)
}

/// The regular dataset, or the error for a ragged one.
pub(crate) fn regular(dataset: AnyDataset) -> Result<Dataset> {
    match dataset {
        AnyDataset::Regular(dataset) => Ok(dataset),
        ragged @ AnyDataset::Ragged { .. } => Err(DataFusionError::NotImplemented(format!(
            "'{}' is a ragged dataset; the NetCDF reader does not support ragged datasets yet",
            ragged.name()
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datafusion::test_util::{local_object, test_file, GRIDDED_FILE, WOD_FILE};

    #[tokio::test]
    async fn a_ragged_file_is_refused() {
        let (store, object) = local_object(&test_file(WOD_FILE)).await;

        let error = open_whole(&store, &object).await.unwrap_err();

        assert!(
            matches!(error, DataFusionError::NotImplemented(_)),
            "{error}"
        );
        assert!(error.to_string().contains("ragged"), "{error}");
    }

    #[tokio::test]
    async fn a_regular_file_opens() {
        let (store, object) = local_object(&test_file(GRIDDED_FILE)).await;

        assert!(open_whole(&store, &object).await.is_ok());
    }
}
