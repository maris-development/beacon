use arrow::datatypes::SchemaRef;
use datafusion::{
    datasource::listing::PartitionedFile, functions_aggregate::count::Count,
    physical_optimizer::pruning::PruningPredicate, physical_plan::PhysicalExpr,
};
use object_store::ObjectStore;
use std::sync::Arc;
use tokio::runtime::Handle;

pub(crate) struct ScanContext {
    store: Arc<dyn ObjectStore>,
    file_schema: SchemaRef,
    predicate: Option<Arc<dyn PhysicalExpr>>,
    io_runtime: Option<tokio::runtime::Handle>,
    batch_size: usize,

    files_pruned: Count,
    chunks_pruned: Count,
    io_requests: Count,
}

impl ScanContext {
    pub(crate) fn prune_file(&self, file: PartitionedFile) -> bool {
        let predicate = match &self
            .predicate
            .and_then(|p| PruningPredicate::try_new(p.clone(), self.file_schema.clone()).ok())
        {
            Some(pred) => pred,
            None => return false,
        };

        match file.statistics() {
            Some(stats) => !predicate.prune(&stats),
            None => true,
        }
    }
}
