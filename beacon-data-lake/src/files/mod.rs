//! Dataset file helpers over the datasets object store.
//!
//! These are free functions over a [`SessionContext`]: resolving listing URLs,
//! creating temporary output files, and discovering datasets / inferring their
//! schemas. All paths are resolved relative to [`DATASETS_OBJECT_STORE_URL`].

pub mod temp_output_file;

use std::{collections::HashMap, path::Path, sync::Arc};

use arrow::datatypes::SchemaRef;
use beacon_common::listing_url::parse_listing_table_url;
use beacon_datafusion_ext::file_collection::FileCollection;
use beacon_datafusion_ext::format_ext::{DatasetMetadata, FileFormatFactoryExt};
use datafusion::{
    catalog::TableProvider, datasource::listing::ListingTableUrl, error::DataFusionError,
    prelude::SessionContext,
};
use futures::StreamExt;

use crate::DATASETS_OBJECT_STORE_URL;
use temp_output_file::TempOutputFile;

/// Resolve a (possibly globbed) path to a [`ListingTableUrl`] under the datasets
/// object store.
#[inline]
pub fn create_listing_url(path: String) -> datafusion::error::Result<ListingTableUrl> {
    parse_listing_table_url(&DATASETS_OBJECT_STORE_URL, &path)
}

/// Create a temporary output file with the given extension, used to stage query
/// results before they are streamed back to the client.
pub fn create_temp_output_file(extension: &str) -> TempOutputFile {
    TempOutputFile::new(extension)
}

/// Discover the datasets matching `pattern` (default `*`) under the datasets
/// object store, asking each registered file format which objects it owns.
pub async fn list_datasets(
    session_ctx: &SessionContext,
    file_formats: &[Arc<dyn FileFormatFactoryExt>],
    offset: Option<usize>,
    limit: Option<usize>,
    pattern: Option<String>,
) -> datafusion::error::Result<Vec<DatasetMetadata>> {
    let state = session_ctx.state();
    let object_store = session_ctx
        .runtime_env()
        .object_store(&*DATASETS_OBJECT_STORE_URL)?;

    let listing_url = create_listing_url(pattern.unwrap_or_else(|| "*".to_string()))?;

    let mut objects = Vec::new();
    let mut entry_stream = listing_url.list_all_files(&state, &object_store, "").await?;

    while let Some(entry) = entry_stream.next().await {
        if let Ok(entry) = entry {
            // Skip Beacon-internal storage (e.g. materialized view data) so it is
            // not surfaced as a user dataset.
            if entry.location.as_ref().starts_with("__beacon__/") {
                continue;
            }
            objects.push(entry);
        }
    }

    let mut datasets = vec![];

    for file_format in file_formats.iter() {
        let format_datasets = file_format.discover_datasets(&objects)?;
        datasets.extend(format_datasets);
    }

    // Keep current pagination semantics to avoid behavior regressions.
    let start = offset.unwrap_or(0);
    let end = limit.map(|l| start + l).unwrap_or(datasets.len());
    let datasets = datasets.into_iter().skip(start).take(end - start).collect();

    Ok(datasets)
}

/// Infer the Arrow schema of the dataset(s) matching `file_pattern` by resolving
/// the file format from the extension and reading the matching files.
pub async fn list_dataset_schema(
    session_ctx: &SessionContext,
    file_pattern: &str,
) -> datafusion::error::Result<SchemaRef> {
    let session_state = session_ctx.state();
    let extension = if file_pattern.ends_with("zarr.json") {
        "zarr.json".to_string()
    } else if file_pattern.contains("/atlas.json") {
        "atlas.json".to_string()
    } else {
        match Path::new(file_pattern).extension() {
            Some(ext) => ext.to_string_lossy().to_string(),
            None => {
                return Err(DataFusionError::Plan(format!(
                    "No file extension found for {}. No file type information available.",
                    file_pattern
                )));
            }
        }
    };

    tracing::debug!("Interpreted file extension: {}", extension);
    let listing_url = create_listing_url(file_pattern.to_string())?;

    let file_format_factory = session_state
        .get_file_format_factory(&extension)
        .ok_or_else(|| {
            DataFusionError::Plan(format!("No file format reader found for {}", extension))
        })?;
    let file_format = file_format_factory.create(&session_state, &HashMap::new())?;
    tracing::debug!("Using file format: {:?}", file_format);

    let file_collection =
        FileCollection::new(&session_state, file_format, vec![listing_url]).await?;

    Ok(file_collection.schema())
}
