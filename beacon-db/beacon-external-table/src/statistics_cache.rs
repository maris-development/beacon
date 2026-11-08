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

pub trait ExternalTableStatisticsProvider: Send + Sync {
    /// Checks if the cached statistics are stale.
    /// Returns a future that resolves to `true` if the statistics are stale, `false` otherwise.
    fn is_stale(&self) -> BoxFuture<'static, bool>;

    /// Fetches statistics for the given objects from the external store.
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

    fn refresh(&self) -> BoxFuture<'static, anyhow::Result<()>>;

    /// Requests a refresh of the statistics for the given objects in the external store.
    fn refresh_objects(
        &self,
        store_url: ObjectStoreUrl,
        store: Arc<dyn ObjectStore>,
        objects: &[ObjectMeta],
    ) -> BoxFuture<'static, anyhow::Result<()>>;
}
