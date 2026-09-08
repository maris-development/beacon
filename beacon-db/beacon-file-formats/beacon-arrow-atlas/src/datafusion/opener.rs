use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use beacon_nd_array::arrow::metrics::ReadMetrics;
use datafusion::{
    datasource::{
        listing::PartitionedFile,
        physical_plan::{FileOpenFuture, FileOpener},
    },
    physical_plan::PhysicalExpr,
};
use futures::FutureExt;
use object_store::ObjectStore;

use crate::{datafusion::metrics::AtlasScanMetrics, store::AtlasReaderCache};

/// One partition's opener: a collection in, its batches out.
///
/// Every field is a handle or a clone, so the opener itself is cloned into the
/// stream it returns and outlives the call that made it.
#[derive(Clone)]
pub struct AtlasOpener {
    pub object_store: Arc<dyn ObjectStore>,
    pub cache: AtlasReaderCache,
    /// The scan's output schema, nd-encoded. Its field *names* are the columns
    /// to keep, and the encoding leaves names alone.
    pub projected_schema: SchemaRef,
    /// The same schema with the encoding unwrapped, which is what a predicate
    /// and the pruning engine are written against.
    pub logical_schema: SchemaRef,
    pub read_dimensions: Option<Vec<String>>,
    pub batch_size: usize,
    pub predicate: Option<Arc<dyn PhysicalExpr>>,
    pub read_metrics: ReadMetrics,
    pub scan_metrics: AtlasScanMetrics,
}

impl FileOpener for AtlasOpener {
    fn open(&self, file: PartitionedFile) -> datafusion::error::Result<FileOpenFuture> {
        let fut = async move {
            return Err(datafusion::error::DataFusionError::NotImplemented(
                "AtlasOpener::open is not implemented yet".to_string(),
            ));
        };

        Ok(fut.boxed())
    }
}
