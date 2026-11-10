//! [`FileStatistics`] as the statistics provider of external table scans.

use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use beacon_datafusion_ext::type_widening::ArrowTypeWideningStrategy;
use beacon_external_table::statistics_cache::{
    ExternalTableStatisticsProvider, SharedPruningStatistics,
};
use datafusion::execution::object_store::ObjectStoreUrl;
use futures::future::BoxFuture;
use object_store::{ObjectMeta, ObjectStore};

use crate::FileStatistics;
use crate::stamp::Stamp;

impl ExternalTableStatisticsProvider for FileStatistics {
    fn register_store(
        &self,
        store_url: ObjectStoreUrl,
        store: Arc<dyn ObjectStore>,
    ) -> BoxFuture<'static, anyhow::Result<()>> {
        FileStatistics::register_store(self, store_url, store);
        Box::pin(async { Ok(()) })
    }

    /// Stale while a file is queued or in the analysis.
    fn is_stale(&self) -> BoxFuture<'static, bool> {
        let stale = !self.queue.is_empty();
        Box::pin(async move { stale })
    }

    /// Returns the objects in their order, with container `i` for object `i`. An
    /// object with no statistics reads as null, so the scan keeps it.
    ///
    /// `type_widening` is not used: each stored type is cast to the schema type,
    /// and a failed cast keeps each file.
    fn fetch_statistics(
        &self,
        store_url: ObjectStoreUrl,
        objects: &[ObjectMeta],
        _type_widening: Arc<dyn ArrowTypeWideningStrategy>,
        schema: SchemaRef,
    ) -> BoxFuture<'static, anyhow::Result<(Vec<ObjectMeta>, SharedPruningStatistics)>> {
        let stamps: Vec<Stamp> = objects
            .iter()
            .map(|object| Stamp::new(&store_url, object))
            .collect();
        let objects = objects.to_vec();
        let store = Arc::clone(&self.store);
        Box::pin(async move {
            let statistics = store.get(&schema, &stamps).await?;
            Ok((objects, Arc::new(statistics) as SharedPruningStatistics))
        })
    }

    /// Queues each object version that the store has no statistics for.
    fn invalidate_objects(
        &self,
        store_url: ObjectStoreUrl,
        _store: Arc<dyn ObjectStore>,
        objects: &[ObjectMeta],
    ) -> BoxFuture<'static, anyhow::Result<()>> {
        let discovery = Arc::clone(&self.discovery);
        let objects = objects.to_vec();
        Box::pin(async move {
            discovery.queue_new(&store_url, &objects).await?;
            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use arrow::datatypes::{DataType, Field, Schema};
    use beacon_datafusion_ext::type_widening::DefaultArrowTypeWidening;
    use datafusion::common::DFSchema;
    use datafusion::execution::context::ExecutionProps;
    use datafusion::physical_expr::create_physical_expr;
    use datafusion::physical_optimizer::pruning::PruningPredicate;
    use datafusion::prelude::{ident, lit};
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use object_store::{ObjectStoreExt, PutPayload};

    use super::*;
    use crate::Config;
    use crate::process::tests::{put_parquet, session, store_url};

    /// The objects that `predicate` keeps, out of `objects` with `statistics`.
    fn kept(
        objects: &[ObjectMeta],
        statistics: &SharedPruningStatistics,
        threshold: f64,
    ) -> Vec<String> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "TEMP",
            DataType::Float64,
            false,
        )]));
        let df_schema = DFSchema::try_from(schema.as_ref().clone()).expect("a schema");
        let expr = create_physical_expr(
            &ident("TEMP").gt(lit(threshold)),
            &df_schema,
            &ExecutionProps::new(),
        )
        .expect("an expression");
        let predicate = PruningPredicate::try_new(expr, schema).expect("a predicate");
        let keep = predicate.prune(statistics.as_ref()).expect("a prune");
        objects
            .iter()
            .zip(keep)
            .filter(|(_, keep)| *keep)
            .map(|(object, _)| object.location.to_string())
            .collect()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_walk_gives_statistics_that_prune_a_file_out_of_range() {
        // Arrange
        let data: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let (_ctx, cell) = session(Arc::clone(&data));
        let low = put_parquet(data.as_ref(), "data/low.parquet", 0.0).await;
        let high = put_parquet(data.as_ref(), "data/high.parquet", 100.0).await;
        data.put(
            &Path::from("data/broken.parquet"),
            PutPayload::from_static(b"no parquet"),
        )
        .await
        .expect("stored");
        let broken = data
            .head(&Path::from("data/broken.parquet"))
            .await
            .expect("exists");
        let config = Config::default()
            .with_num_threads(2)
            .with_batch_age(Duration::from_millis(20));
        let statistics = FileStatistics::open(
            config,
            tokio::runtime::Handle::current(),
            cell,
            crate::store::tests::database(),
        )
        .await
        .expect("the pipeline opens");

        // Act
        FileStatistics::register_store(&statistics, store_url(), Arc::clone(&data));
        let stamps: Vec<Stamp> = [&low, &high, &broken]
            .into_iter()
            .map(|object| Stamp::new(&store_url(), object))
            .collect();
        tokio::time::timeout(Duration::from_secs(10), async {
            while statistics.store().contains(&stamps).await.expect("read") != [true; 3] {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("the walk analyzes each file");
        let unknown = put_parquet(data.as_ref(), "data/unknown.parquet", 0.0).await;
        let schema = Arc::new(Schema::new(vec![Field::new(
            "TEMP",
            DataType::Float64,
            false,
        )]));
        let (objects, pruning) = statistics
            .fetch_statistics(
                store_url(),
                &[low, high, broken, unknown],
                Arc::new(DefaultArrowTypeWidening::new()),
                schema,
            )
            .await
            .expect("statistics");

        // Assert
        assert_eq!(objects.len(), 4);
        let mut survivors = kept(&objects, &pruning, 50.0);
        survivors.sort();
        assert_eq!(
            survivors,
            [
                "data/broken.parquet",
                "data/high.parquet",
                "data/unknown.parquet"
            ],
            "the low file cannot match; the broken and the unknown file have no ranges"
        );
        assert!(!statistics.is_stale().await);
    }
}
