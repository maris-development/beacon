use arrow::datatypes::SchemaRef;
use datafusion::{
    common::Statistics, datasource::file_format::FileFormat, prelude::SessionContext,
};
use futures::prelude::future::BoxFuture;
use object_store::{ObjectMeta, ObjectStore};
use std::sync::Arc;

/// The statistics of one file, with the schema they are positional against.
///
/// Column statistic `i` describes field `i` of `schema`.
#[derive(Debug, Clone)]
pub struct FileStatisticsOutput {
    /// The file's own schema, not the schema of a table over the file.
    pub schema: SchemaRef,
    /// The statistics of the file.
    pub statistics: Statistics,
}

/// A source of the statistics of one file.
pub trait FileFormatStatistics: Send + Sync {
    /// Reads the schema and the statistics of `object`.
    ///
    /// # Errors
    ///
    /// Returns an error when the schema or the statistics cannot be read.
    fn fetch_statistics(
        &self,
        session: Arc<SessionContext>,
        store: Arc<dyn ObjectStore>,
        object: ObjectMeta,
    ) -> BoxFuture<'static, anyhow::Result<FileStatisticsOutput>>;
}

impl FileFormatStatistics for Arc<dyn FileFormat> {
    fn fetch_statistics(
        &self,
        session: Arc<SessionContext>,
        store: Arc<dyn ObjectStore>,
        object: ObjectMeta,
    ) -> BoxFuture<'static, anyhow::Result<FileStatisticsOutput>> {
        let format = Arc::clone(self);
        Box::pin(async move {
            let state = session.state();
            // `column_statistics` is positional against the schema, so the file
            // schema must come first. An empty schema gives no column ranges.
            let schema = format
                .infer_schema(&state, &store, std::slice::from_ref(&object))
                .await
                .map_err(|e| anyhow::anyhow!("schema for {}: {e}", object.location))?;
            let statistics = format
                .infer_stats(&state, &store, Arc::clone(&schema), &object)
                .await
                .map_err(|e| anyhow::anyhow!("statistics for {}: {e}", object.location))?;
            Ok(FileStatisticsOutput { schema, statistics })
        })
    }
}
