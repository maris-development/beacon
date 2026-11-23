//! The open path of one file of an ND format.

use std::sync::Arc;

use async_trait::async_trait;
use beacon_nd_dataset::{Error, GridSelection, NdDataset, select_grid};
use datafusion::error::Result;
use object_store::{ObjectMeta, ObjectStore};

use crate::error::to_datafusion;

/// Opens one file of an ND format.
#[async_trait]
pub trait NdFileOpener: Send + Sync + std::fmt::Debug + 'static {
    /// The name for errors and EXPLAIN, for example "netcdf".
    fn format_name(&self) -> &'static str;
    /// Open one file. Read the metadata only.
    async fn open(
        &self,
        store: Arc<dyn ObjectStore>,
        file: &ObjectMeta,
    ) -> beacon_nd_dataset::Result<Arc<dyn NdDataset>>;
}

/// The table options that every ND format has.
#[derive(Debug, Clone)]
pub struct NdScanOptions {
    /// The grid of each file.
    pub grid: GridSelection,
    /// Skip a file that does not fit a given grid.
    pub skip_unbroadcastable: bool,
    /// Skip a file that the format cannot open.
    pub skip_invalid_files: bool,
}

impl Default for NdScanOptions {
    /// The grid of each file, and no skips.
    fn default() -> Self {
        Self {
            grid: GridSelection::PerFile,
            skip_unbroadcastable: false,
            skip_invalid_files: false,
        }
    }
}

/// Open one file and apply the grid. `Ok(None)` when an option skips the file.
///
/// An unsupported file is a `NotImplemented` error. Only a given grid with
/// `skip_unbroadcastable` skips it.
pub async fn open_for_read(
    opener: &dyn NdFileOpener,
    store: Arc<dyn ObjectStore>,
    file: &ObjectMeta,
    options: &NdScanOptions,
) -> Result<Option<Arc<dyn NdDataset>>> {
    let dataset = match opener.open(store, file).await {
        Ok(dataset) => dataset,
        Err(error @ Error::Unsupported { .. }) => {
            if options.skip_unbroadcastable && matches!(options.grid, GridSelection::Given(_)) {
                tracing::warn!(file = %file.location, %error, "skip an unsupported file");
                return Ok(None);
            }
            return Err(to_datafusion(error));
        }
        Err(error) if options.skip_invalid_files => {
            tracing::warn!(file = %file.location, %error, "skip a file that does not open");
            return Ok(None);
        }
        Err(error) => return Err(to_datafusion(error)),
    };
    let dataset = match select_grid(dataset, &options.grid) {
        Ok(dataset) => dataset,
        Err(error @ Error::OffGrid { .. }) if options.skip_unbroadcastable => {
            tracing::warn!(file = %file.location, %error, "skip a file off the grid");
            return Ok(None);
        }
        Err(error) => return Err(to_datafusion(error)),
    };
    // Release builds skip this check of the contract.
    #[cfg(debug_assertions)]
    beacon_nd_dataset::validate(dataset.as_ref()).map_err(to_datafusion)?;
    Ok(Some(dataset))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use beacon_nd_dataset::{GridSelection, NdDataset};
    use datafusion::error::{DataFusionError, Result};
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use object_store::{ObjectMeta, ObjectStore};

    use super::{NdScanOptions, open_for_read};
    use crate::testing::{Entry, MemoryOpener, grid_dataset};

    fn meta() -> ObjectMeta {
        ObjectMeta {
            location: Path::from("a.nd"),
            last_modified: Default::default(),
            size: 1000,
            e_tag: None,
            version: None,
        }
    }

    fn given(names: &[&str]) -> GridSelection {
        GridSelection::Given(names.iter().map(|name| name.to_string()).collect())
    }

    async fn open(entry: Entry, options: NdScanOptions) -> Result<Option<Arc<dyn NdDataset>>> {
        let opener = MemoryOpener::default().with("a.nd", entry);
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let result = open_for_read(&opener, store, &meta(), &options).await;
        assert_eq!(opener.opens(), 1);
        result
    }

    fn names(dataset: &dyn NdDataset) -> Vec<String> {
        dataset
            .variable_names()
            .iter()
            .map(|name| name.to_string())
            .collect()
    }

    fn with_grid(grid: GridSelection) -> NdScanOptions {
        NdScanOptions {
            grid,
            ..NdScanOptions::default()
        }
    }

    fn skip_invalid(grid: GridSelection) -> NdScanOptions {
        NdScanOptions {
            skip_invalid_files: true,
            ..with_grid(grid)
        }
    }

    fn skip_unbroadcastable(grid: GridSelection) -> NdScanOptions {
        NdScanOptions {
            skip_unbroadcastable: true,
            ..with_grid(grid)
        }
    }

    #[tokio::test]
    async fn a_good_file_opens_on_its_default_grid() {
        let dataset = open(
            Entry::Dataset(grid_dataset("a.nd")),
            NdScanOptions::default(),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(names(dataset.as_ref()), ["temp", "lat", "lon"]);
    }

    #[tokio::test]
    async fn a_given_grid_keeps_the_variables_on_it() {
        let dataset = open(
            Entry::Dataset(grid_dataset("a.nd")),
            with_grid(given(&["lat"])),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(names(dataset.as_ref()), ["lat"]);
    }

    #[tokio::test]
    async fn an_invalid_file_is_an_error_with_its_cause() {
        let error = open(Entry::Invalid("bad header"), NdScanOptions::default())
            .await
            .unwrap_err();
        assert!(error.to_string().contains("bad header"), "{error}");
    }

    #[tokio::test]
    async fn an_invalid_file_is_skipped_with_the_option() {
        let result = open(
            Entry::Invalid("bad header"),
            skip_invalid(GridSelection::PerFile),
        )
        .await;
        assert!(matches!(result, Ok(None)), "{result:?}");
    }

    #[tokio::test]
    async fn a_file_off_the_grid_is_an_error() {
        let error = open(
            Entry::Dataset(grid_dataset("a.nd")),
            with_grid(given(&["depth"])),
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("depth"), "{error}");
    }

    #[tokio::test]
    async fn a_file_off_the_grid_is_skipped_with_the_option() {
        let result = open(
            Entry::Dataset(grid_dataset("a.nd")),
            skip_unbroadcastable(given(&["depth"])),
        )
        .await;
        assert!(matches!(result, Ok(None)), "{result:?}");
    }

    #[tokio::test]
    async fn the_invalid_skip_does_not_skip_a_file_off_the_grid() {
        let result = open(
            Entry::Dataset(grid_dataset("a.nd")),
            skip_invalid(given(&["depth"])),
        )
        .await;
        let error = result.expect_err("the file off the grid fails");
        assert!(error.to_string().contains("depth"), "{error}");
    }

    #[tokio::test]
    async fn an_unsupported_file_is_not_implemented() {
        let result = open(Entry::Unsupported("ragged"), NdScanOptions::default()).await;
        assert!(
            matches!(result, Err(DataFusionError::NotImplemented(_))),
            "{result:?}"
        );
    }

    #[tokio::test]
    async fn the_invalid_skip_does_not_skip_an_unsupported_file() {
        let result = open(
            Entry::Unsupported("ragged"),
            skip_invalid(GridSelection::PerFile),
        )
        .await;
        assert!(
            matches!(result, Err(DataFusionError::NotImplemented(_))),
            "{result:?}"
        );
    }

    #[tokio::test]
    async fn an_unsupported_file_is_skipped_with_a_given_grid_and_the_skip() {
        let result = open(
            Entry::Unsupported("ragged"),
            skip_unbroadcastable(given(&["lat"])),
        )
        .await;
        assert!(matches!(result, Ok(None)), "{result:?}");
    }

    #[tokio::test]
    async fn an_unsupported_file_is_not_skipped_on_the_default_grid() {
        let result = open(
            Entry::Unsupported("ragged"),
            skip_unbroadcastable(GridSelection::PerFile),
        )
        .await;
        assert!(
            matches!(result, Err(DataFusionError::NotImplemented(_))),
            "{result:?}"
        );
    }
}
