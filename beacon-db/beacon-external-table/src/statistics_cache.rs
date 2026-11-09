use beacon_datafusion_ext::type_widening::ArrowTypeWideningStrategy;
use datafusion::{
    arrow::datatypes::SchemaRef, common::pruning::PruningStatistics,
    execution::object_store::ObjectStoreUrl,
};
use futures::future::BoxFuture;
use object_store::{ObjectMeta, ObjectStore};
use std::sync::Arc;

/// Statistics that a `PruningPredicate` reads directly.
pub type SharedPruningStatistics = Arc<dyn PruningStatistics + Send + Sync>;

/// A source of per-file statistics that an external table scan prunes its files with.
///
/// Each method fails open: an object without statistics stays in the scan.
pub trait ExternalTableStatisticsProvider: Send + Sync {
    fn register_store(
        &self,
        store_url: ObjectStoreUrl,
        store: Arc<dyn ObjectStore>,
    ) -> BoxFuture<'static, anyhow::Result<()>>;

    /// Checks if the cached statistics are stale.
    /// Returns a future that resolves to `true` if the statistics are stale, `false` otherwise.
    fn is_stale(&self) -> BoxFuture<'static, bool>;

    /// Fetches statistics for the given objects from the external store.
    ///
    /// `schema` holds only the columns that the scan needs statistics for.
    ///
    /// Returns the objects that have statistics and the statistics of these objects.
    /// Container `i` of the statistics describes object `i`, so
    /// `num_containers()` is equal to the number of returned objects.
    /// A listed object that is not returned has no statistics, and the scan keeps it.
    fn fetch_statistics(
        &self,
        store_url: ObjectStoreUrl,
        objects: &[ObjectMeta],
        type_widening: Arc<dyn ArrowTypeWideningStrategy>,
        schema: SchemaRef,
    ) -> BoxFuture<'static, anyhow::Result<(Vec<ObjectMeta>, SharedPruningStatistics)>>;

    /// Requests a refresh of the statistics for the given objects in the external store.
    fn invalidate_objects(
        &self,
        store_url: ObjectStoreUrl,
        store: Arc<dyn ObjectStore>,
        objects: &[ObjectMeta],
    ) -> BoxFuture<'static, anyhow::Result<()>>;
}

/// The session extension that holds the default statistics provider of external tables.
///
/// A table that the builder gets no provider for takes this one.
#[derive(Clone)]
pub struct ExternalTableStatistics(pub Arc<dyn ExternalTableStatisticsProvider>);

impl std::fmt::Debug for ExternalTableStatistics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("ExternalTableStatistics")
            .finish_non_exhaustive()
    }
}
