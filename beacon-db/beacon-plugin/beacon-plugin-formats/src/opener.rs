//! Openers of plugin files.

use std::sync::Arc;

use beacon_plugin_sdk::file_format::{FormatReader, ScanRequest};
use datafusion::arrow::datatypes::Schema;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::{FileOpenFuture, FileOpener};
use datafusion::error::{DataFusionError, Result};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::utils::{collect_columns, reassign_expr_columns, split_conjunction};
use futures::{StreamExt, TryStreamExt};
use object_store::ObjectStore;

/// Calls the plugin `open` for one file. The batches are as the plugin gives them.
pub(crate) struct PluginFileOpener {
    pub(crate) reader: Arc<dyn FormatReader>,
    pub(crate) store: Arc<dyn ObjectStore>,
    pub(crate) request: ScanRequest,
}

impl FileOpener for PluginFileOpener {
    fn open(&self, file: PartitionedFile) -> Result<FileOpenFuture> {
        let reader = Arc::clone(&self.reader);
        let store = Arc::clone(&self.store);
        let request = self.request.clone();
        let object = file.object_meta;
        Ok(Box::pin(async move {
            let stream = reader.open(store, &object, request).await?;
            Ok(stream.boxed())
        }))
    }
}

/// Puts the format name and the file path on every error of `inner`.
pub(crate) struct ContextOpener {
    pub(crate) inner: Arc<dyn FileOpener>,
    pub(crate) format: String,
}

impl FileOpener for ContextOpener {
    fn open(&self, file: PartitionedFile) -> Result<FileOpenFuture> {
        let context = format!(
            "file format {}: cannot read {}",
            self.format, file.object_meta.location
        );
        let opened = self
            .inner
            .open(file)
            .map_err(|e| e.context(context.clone()))?;
        Ok(Box::pin(async move {
            let stream = opened.await.map_err(|e| e.context(context.clone()))?;
            Ok(stream
                .map_err(move |e: DataFusionError| e.context(context.clone()))
                .boxed())
        }))
    }
}

/// The conjuncts of `predicate` that read only columns of `schema`, rewritten to its
/// column indices. The other conjuncts do not reach the plugin.
pub(crate) fn filters_for(
    predicate: Option<&Arc<dyn PhysicalExpr>>,
    schema: &Schema,
) -> Vec<Arc<dyn PhysicalExpr>> {
    let Some(predicate) = predicate else {
        return Vec::new();
    };
    split_conjunction(predicate)
        .into_iter()
        .filter(|conjunct| {
            collect_columns(conjunct)
                .iter()
                .all(|column| schema.index_of(column.name()).is_ok())
        })
        .filter_map(|conjunct| reassign_expr_columns(Arc::clone(conjunct), schema).ok())
        .collect()
}
