use std::collections::HashMap;
use std::sync::{Arc, Weak};

use arrow::{
    array::StringArray,
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use beacon_datafusion_ext::format_ext::{DatasetMetadata, FileFormatFactoryExt};
use beacon_datafusion_ext::listing_factory::ListingFactory;
use datafusion::{
    catalog::{MemTable, TableFunctionImpl, TableProvider},
    error::DataFusionError,
    prelude::{Expr, SessionContext},
};
use futures::StreamExt;

use crate::file_formats::BeaconTableFunctionImpl;

/// Discover the datasets matching `pattern` (default `**/*`) under the datasets
/// object store at `datasets_url`, asking each registered file format which
/// objects it owns.
pub async fn list_datasets(
    session_ctx: &SessionContext,
    file_formats: &[Arc<dyn FileFormatFactoryExt>],
    offset: Option<usize>,
    limit: Option<usize>,
    search_pattern: Option<String>,
) -> datafusion::error::Result<Vec<DatasetMetadata>> {
    let state = session_ctx.state();
    let listing_factory = state
        .config()
        .get_extension::<ListingFactory>()
        .ok_or_else(|| {
            DataFusionError::Execution(
                "list_datasets: the listing factory is not registered on the session".to_string(),
            )
        })?;

    let listing_url = listing_factory.parse_listing_table_url(
        &state,
        &search_pattern.unwrap_or_else(|| "**/*".to_string()),
    )?;
    let store_url = listing_url.object_store();
    let store = state
        .runtime_env()
        .object_store(store_url.clone())
        .map_err(|e| {
            DataFusionError::Execution(format!(
                "list_datasets: failed to get object store for {}: {}",
                store_url, e
            ))
        })?;

    let mut objects = Vec::new();
    let mut entry_stream = listing_url.list_all_files(&state, &store, "").await?;

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

pub struct ListDatasetsFunc {
    runtime_handle: tokio::runtime::Handle,
    session_ctx: Weak<SessionContext>,
    file_formats: Vec<Arc<dyn FileFormatFactoryExt>>,
}

impl ListDatasetsFunc {
    pub fn new(
        runtime_handle: tokio::runtime::Handle,
        session_ctx: Weak<SessionContext>,
        file_formats: Vec<Arc<dyn FileFormatFactoryExt>>,
    ) -> Self {
        Self {
            runtime_handle,
            session_ctx,
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

        let datasets: Vec<DatasetMetadata> = tokio::task::block_in_place(|| {
            self.runtime_handle.block_on(async move {
                // Recursive scan (`**/*`), unpaginated — the historical UDTF
                // behaviour — reusing the shared discovery helper.
                list_datasets(
                    &session_ctx,
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
