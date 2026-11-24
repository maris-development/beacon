//! Open one Zarr leaf group for the ND scan.

use std::sync::Arc;

use async_trait::async_trait;
use beacon_nd_dataset::{Error, GridSelection, NdDataset, Result, attribute_columns};
use beacon_nd_scan::{NdFileOpener, NdScanOptions, NdSource};
use datafusion::datasource::table_schema::TableSchema;
use object_store::{ObjectMeta, ObjectStore};

use crate::nd::open_group;
use crate::util::{ZarrPath, ZarrStorage};

/// Opens the leaf group of a `zarr.json` object. It reads the metadata only.
#[derive(Debug)]
pub struct ZarrOpener {
    /// The storage to open groups over, in place of the session store.
    storage: Option<ZarrStorage>,
}

impl ZarrOpener {
    /// An opener over `storage`, or over the session store when `storage` is `None`.
    pub fn new(storage: Option<ZarrStorage>) -> Self {
        Self { storage }
    }
}

#[async_trait]
impl NdFileOpener for ZarrOpener {
    fn format_name(&self) -> &'static str {
        "zarr"
    }

    async fn open(
        &self,
        store: Arc<dyn ObjectStore>,
        file: &ObjectMeta,
    ) -> Result<Arc<dyn NdDataset>> {
        let storage = match &self.storage {
            Some(storage) => storage.clone(),
            None => ZarrStorage::from_object_store(store),
        };
        let group_path = ZarrPath::new_from_object_meta(file.clone())
            .map_err(Error::format)?
            .as_zarr_path();
        let dataset = open_group(storage.inner(), &group_path).await?;
        Ok(attribute_columns(Arc::new(dataset)))
    }
}

/// The scan options of a Zarr table.
pub fn scan_options(
    read_dimensions: Option<Vec<String>>,
    skip_unbroadcastable: bool,
) -> NdScanOptions {
    NdScanOptions {
        grid: match read_dimensions {
            Some(dimensions) => GridSelection::Given(dimensions),
            None => GridSelection::PerFile,
        },
        skip_unbroadcastable,
        skip_invalid_files: false,
        // The size of `zarr.json` says nothing about the data of the group.
        min_slice_size: Some(0),
    }
}

/// The file source of each Zarr scan. `table_schema` is the encoded schema.
pub fn zarr_source(
    table_schema: TableSchema,
    read_dimensions: Option<Vec<String>>,
    skip_unbroadcastable: bool,
    storage: Option<ZarrStorage>,
) -> NdSource {
    NdSource::new(
        Arc::new(ZarrOpener::new(storage)),
        scan_options(read_dimensions, skip_unbroadcastable),
        table_schema,
    )
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::datatypes::Schema;
    use beacon_nd_dataset::{Error, GridSelection};
    use beacon_nd_scan::{NdFileOpener, encoded_table_schema};
    use datafusion::datasource::physical_plan::FileSource;
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use object_store::{ObjectMeta, ObjectStore};
    use serde_json::json;

    use super::{ZarrOpener, scan_options, zarr_source};
    use crate::nd::testing::{TestStore, write_array, write_group};

    fn meta(path: &str) -> ObjectMeta {
        ObjectMeta {
            location: Path::from(path),
            last_modified: Default::default(),
            size: 0,
            e_tag: None,
            version: None,
        }
    }

    /// A store with the group "/s.zarr" and its array "a", which has a `long_name`.
    async fn group() -> TestStore {
        let store = TestStore::new();
        write_group(&store, "/s.zarr", json!({})).await;
        write_array(
            &store,
            "/s.zarr/a",
            &[2],
            &[2],
            &[Some("x")],
            json!({"long_name": "the a"}),
            vec![1.0f64, 2.0],
        )
        .await;
        store
    }

    #[tokio::test]
    async fn the_opener_reads_a_group_from_the_session_store() {
        let store = group().await;
        let dataset = ZarrOpener::new(None)
            .open(store.object_store(), &meta("s.zarr/zarr.json"))
            .await
            .unwrap();
        let names = dataset.variable_names();
        assert!(names.iter().any(|name| &**name == "a"), "{names:?}");
        assert!(
            names.iter().any(|name| &**name == "a.long_name"),
            "{names:?}"
        );
    }

    #[tokio::test]
    async fn the_storage_override_wins_over_the_session_store() {
        let store = group().await;
        let session: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let dataset = ZarrOpener::new(Some(store.storage()))
            .open(session, &meta("s.zarr/zarr.json"))
            .await;
        assert!(dataset.is_ok(), "{dataset:?}");
    }

    #[tokio::test]
    async fn an_object_that_is_not_zarr_json_is_an_error() {
        let store = group().await;
        let result = ZarrOpener::new(None)
            .open(store.object_store(), &meta("s.zarr/a/c/0"))
            .await;
        assert!(matches!(result, Err(Error::Format(_))), "{result:?}");
    }

    #[test]
    fn the_options_map_the_table_options() {
        let options = scan_options(Some(vec!["time".to_string()]), true);
        assert_eq!(options.grid, GridSelection::Given(vec!["time".to_string()]));
        assert!(options.skip_unbroadcastable);
        assert!(!options.skip_invalid_files);
        assert_eq!(options.min_slice_size, Some(0));

        assert_eq!(scan_options(None, false).grid, GridSelection::PerFile);
    }

    #[test]
    fn the_source_reports_the_zarr_file_type() {
        let source = zarr_source(encoded_table_schema(&Schema::empty()), None, false, None);
        assert_eq!(source.file_type(), "zarr");
    }
}
