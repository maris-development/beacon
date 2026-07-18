use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Weak};

use arrow::{
    array::StringArray,
    datatypes::{DataType, Field, Schema, SchemaRef},
    record_batch::RecordBatch,
};
use beacon_common::listing_url::parse_listing_table_url;
use beacon_datafusion_ext::file_collection::FileCollection;
use beacon_datafusion_ext::format_ext::{DatasetMetadata, FileFormatFactoryExt};
use datafusion::{
    catalog::{MemTable, TableFunctionImpl, TableProvider},
    datasource::file_format::FileFormatFactory,
    error::DataFusionError,
    execution::object_store::ObjectStoreUrl,
    prelude::{Expr, SessionContext},
};
use futures::StreamExt;

use crate::file_formats::BeaconTableFunctionImpl;

/// Discover the datasets matching `pattern` (default `*`) under the datasets
/// object store at `datasets_url`, asking each registered file format which
/// objects it owns.
pub async fn list_datasets(
    session_ctx: &SessionContext,
    datasets_url: &ObjectStoreUrl,
    object_store: Arc<dyn object_store::ObjectStore>,
    file_formats: &[Arc<dyn FileFormatFactoryExt>],
    offset: Option<usize>,
    limit: Option<usize>,
    pattern: Option<String>,
) -> datafusion::error::Result<Vec<DatasetMetadata>> {
    let state = session_ctx.state();

    let listing_url =
        parse_listing_table_url(datasets_url, &pattern.unwrap_or_else(|| "*".to_string()))?;

    let mut objects = Vec::new();
    let mut entry_stream = listing_url.list_all_files(&state, &object_store, "").await?;

    while let Some(entry) = entry_stream.next().await {
        if let Ok(entry) = entry {
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
    datasets_url: &ObjectStoreUrl,
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
    let listing_url = parse_listing_table_url(datasets_url, file_pattern)?;

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

pub struct ListDatasetsFunc {
    runtime_handle: tokio::runtime::Handle,
    session_ctx: Weak<SessionContext>,
    data_object_store_url: ObjectStoreUrl,
    file_formats: Vec<Arc<dyn FileFormatFactoryExt>>,
}

impl ListDatasetsFunc {
    pub fn new(
        runtime_handle: tokio::runtime::Handle,
        session_ctx: Weak<SessionContext>,
        data_object_store_url: ObjectStoreUrl,
        file_formats: Vec<Arc<dyn FileFormatFactoryExt>>,
    ) -> Self {
        Self {
            runtime_handle,
            session_ctx,
            data_object_store_url,
            file_formats,
        }
    }
}

impl std::fmt::Debug for ListDatasetsFunc {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ListDatasetsFunc")
    }
}

impl BeaconTableFunctionImpl for ListDatasetsFunc {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> String {
        "list_datasets".to_string()
    }

    fn description(&self) -> Option<String> {
        Some("Lists all datasets stored in beacon, returning file name and format.".to_string())
    }
}

impl TableFunctionImpl for ListDatasetsFunc {
    fn call(&self, _args: &[Expr]) -> datafusion::error::Result<Arc<dyn TableProvider>> {
        let file_formats = self.file_formats.clone();
        let session_ctx = self.session_ctx.upgrade().ok_or_else(|| {
            datafusion::common::plan_datafusion_err!("session context has been dropped")
        })?;
        let data_object_store_url = self.data_object_store_url.clone();

        let datasets: Vec<DatasetMetadata> = tokio::task::block_in_place(|| {
            self.runtime_handle.block_on(async move {
                let object_store = session_ctx
                    .runtime_env()
                    .object_store(data_object_store_url.clone())?;

                // Recursive scan (`**/*`), unpaginated — the historical UDTF
                // behaviour — reusing the shared discovery helper.
                list_datasets(
                    &session_ctx,
                    &data_object_store_url,
                    object_store,
                    &file_formats,
                    None,
                    None,
                    Some("**/*".to_string()),
                )
                .await
            })
        })?;

        let schema = Arc::new(Schema::new(vec![
            Field::new("file_name", DataType::Utf8, false),
            Field::new("file_format", DataType::Utf8, false),
        ]));

        let file_names: StringArray = datasets
            .iter()
            .map(|d| Some(d.file_path.as_str()))
            .collect();
        let file_formats: StringArray = datasets.iter().map(|d| Some(d.format.as_str())).collect();

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(file_names), Arc::new(file_formats)],
        )?;

        Ok(Arc::new(MemTable::try_new(schema, vec![vec![batch]])?))
    }
}
