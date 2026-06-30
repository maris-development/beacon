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
    catalog::TableProvider,
    datasource::{file_format::FileFormatFactory, listing::ListingTableUrl},
    error::DataFusionError,
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
///
/// `tmp_dir` MUST be the directory the tmp object store
/// ([`crate::TMP_OBJECT_STORE_URL`]) is rooted at, so the COPY-written bytes are
/// visible when the file is read back. See [`TempOutputFile::new`].
pub fn create_temp_output_file(tmp_dir: &Path, extension: &str) -> TempOutputFile {
    TempOutputFile::new(tmp_dir, extension)
}

/// Discover the datasets matching `pattern` (default `*`) under the datasets
/// object store, asking each registered file format which objects it owns.
pub async fn list_datasets(
    session_ctx: &SessionContext,
    object_store: Arc<dyn object_store::ObjectStore>,
    file_formats: &[Arc<dyn FileFormatFactoryExt>],
    offset: Option<usize>,
    limit: Option<usize>,
    pattern: Option<String>,
) -> datafusion::error::Result<Vec<DatasetMetadata>> {
    let state = session_ctx.state();

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

    // Enrich each dataset with size + last-modified from the object listing. A
    // single-file dataset matches an object exactly; a directory-shaped dataset
    // (e.g. Zarr) aggregates every object under its prefix (sum of sizes, newest
    // mtime). Datasets with no matching object keep `None`.
    let by_path: HashMap<&str, &object_store::ObjectMeta> =
        objects.iter().map(|o| (o.location.as_ref(), o)).collect();
    for ds in datasets.iter_mut() {
        if let Some(obj) = by_path.get(ds.file_path.as_str()) {
            ds.size = Some(obj.size);
            ds.last_modified = Some(obj.last_modified);
        } else {
            let prefix = format!("{}/", ds.file_path);
            let mut total = 0u64;
            let mut latest = None;
            for o in &objects {
                if o.location.as_ref().starts_with(&prefix) {
                    total += o.size;
                    latest = Some(match latest {
                        Some(l) if l >= o.last_modified => l,
                        _ => o.last_modified,
                    });
                }
            }
            if latest.is_some() {
                ds.size = Some(total);
                ds.last_modified = latest;
            }
        }
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
    file_formats: &[Arc<dyn FileFormatFactoryExt>],
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

    // Resolve the format from the raw filename extension. The session registry
    // keys each format only under its canonical extension, so aliases (e.g.
    // `.tif` for the `tiff` format) are matched against each factory's declared
    // `file_extensions()` first, falling back to the registry for anything not
    // in the format list.
    let file_format_factory = file_formats
        .iter()
        .find(|factory| factory.file_extensions().iter().any(|ext| ext == &extension))
        .map(|factory| factory.clone() as Arc<dyn FileFormatFactory>)
        .or_else(|| session_state.get_file_format_factory(&extension))
        .ok_or_else(|| {
            DataFusionError::Plan(format!("No file format reader found for {}", extension))
        })?;
    let file_format = file_format_factory.create(&session_state, &HashMap::new())?;
    tracing::debug!("Using file format: {:?}", file_format);

    let file_collection =
        FileCollection::new(&session_state, file_format, vec![listing_url]).await?;

    Ok(file_collection.schema())
}
