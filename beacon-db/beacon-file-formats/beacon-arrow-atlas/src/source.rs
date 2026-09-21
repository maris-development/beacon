//! The DataFusion [`FileSource`] and [`FileOpener`] for Atlas collections.
//!
//! A plan entry is one collection; partitions share its datasets through
//! [`CollectionQueues`] instead of splitting it by byte range.

use std::any::Any;
use std::sync::Arc;

use datafusion::{
    config::ConfigOptions,
    datasource::{
        listing::PartitionedFile,
        physical_plan::{FileOpenFuture, FileOpener, FileScanConfig, FileSource},
        table_schema::TableSchema,
    },
    error::Result,
    physical_expr::{PhysicalExpr, conjunction, projection::ProjectionExprs},
    physical_plan::{
        filter_pushdown::{FilterPushdownPropagation, PushedDown},
        metrics::ExecutionPlanMetricsSet,
    },
};
use futures::{FutureExt, StreamExt, TryStreamExt};
use object_store::ObjectStore;
use tokio_util::sync::CancellationToken;

use beacon_datafusion_ext::type_widening::{ArrowTypeWideningStrategy, DefaultArrowTypeWidening};

use crate::{
    error::external,
    metrics::AtlasScanMetrics,
    open::AtlasReaderCache,
    scan::{CollectionQueues, ScanSpec},
};

/// DataFusion [`FileSource`] for Atlas collections.
#[derive(Debug, Clone)]
pub struct AtlasSource {
    table_schema: TableSchema,
    execution_plan_metrics: ExecutionPlanMetricsSet,
    predicate: Option<Arc<dyn PhysicalExpr>>,
    read_dimensions: Option<Vec<String>>,
    projection: Option<ProjectionExprs>,
    /// The reader cache every open goes through.
    cache: AtlasReaderCache,
    /// The scan's queues, one per collection, shared by every partition.
    queues: Arc<CollectionQueues>,
    /// The rule that merged the table schema. It decides which casts read
    /// null. The format sets it from the session when it plans.
    type_widening: Arc<dyn ArrowTypeWideningStrategy>,
    /// The query's token. The format sets it from the session when it plans;
    /// until then it is one that never fires.
    cancel: CancellationToken,
}

impl AtlasSource {
    pub fn new(
        read_dimensions: Option<Vec<String>>,
        table_schema: TableSchema,
        cache: AtlasReaderCache,
    ) -> Self {
        Self {
            table_schema,
            execution_plan_metrics: ExecutionPlanMetricsSet::new(),
            predicate: None,
            read_dimensions,
            projection: None,
            cache,
            queues: Arc::new(CollectionQueues::new()),
            type_widening: Arc::new(DefaultArrowTypeWidening::new()),
            cancel: CancellationToken::new(),
        }
    }

    /// The same source, with the merge rule of the session.
    pub fn with_type_widening(mut self, strategy: Arc<dyn ArrowTypeWideningStrategy>) -> Self {
        self.type_widening = strategy;
        self
    }

    /// The same source, stopped by `cancel`.
    ///
    /// Every stage reads through the token and stops with an error when it fires.
    pub fn with_cancellation(mut self, cancel: CancellationToken) -> Self {
        self.cancel = cancel;
        self
    }

    /// Carry a projection the scan pushed down.
    ///
    /// Needed because the format rebuilds the source in `create_physical_plan`.
    pub fn with_projection(mut self, projection: Option<ProjectionExprs>) -> Self {
        self.projection = projection;
        self
    }
}

impl FileSource for AtlasSource {
    /// The opener of one partition. It refuses a scan of no column, see
    /// [`ScanSpec::new`].
    fn create_file_opener(
        &self,
        object_store: Arc<dyn ObjectStore>,
        base_config: &FileScanConfig,
        partition: usize,
    ) -> Result<Arc<dyn FileOpener>> {
        let spec = ScanSpec::new(
            base_config.projected_schema()?,
            self.read_dimensions.clone(),
            self.predicate.clone(),
            Arc::clone(&self.type_widening),
            self.cancel.clone(),
        )?;
        Ok(Arc::new(AtlasOpener {
            object_store,
            cache: self.cache.clone(),
            spec: Arc::new(spec),
            scan_metrics: AtlasScanMetrics::new(&self.execution_plan_metrics, partition),
            queues: Arc::clone(&self.queues),
        }))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_schema(&self) -> &TableSchema {
        &self.table_schema
    }

    /// A batch is one stored chunk, sized by the writer. The scan has no say.
    fn with_batch_size(&self, _batch_size: usize) -> Arc<dyn FileSource> {
        Arc::new(self.clone())
    }

    /// A container is one unit; a byte range of it names nothing a reader can
    /// open. The format deals collections to partitions itself, and the plan's
    /// groups stand as dealt.
    fn supports_repartitioning(&self) -> bool {
        false
    }

    fn metrics(&self) -> &ExecutionPlanMetricsSet {
        &self.execution_plan_metrics
    }

    fn file_type(&self) -> &str {
        "atlas"
    }

    fn projection(&self) -> Option<&ProjectionExprs> {
        self.projection.as_ref()
    }

    fn try_pushdown_projection(
        &self,
        projection: &ProjectionExprs,
    ) -> Result<Option<Arc<dyn FileSource>>> {
        let merged = match &self.projection {
            Some(existing) => existing.try_merge(projection)?,
            None => projection.clone(),
        };
        Ok(Some(Arc::new(Self {
            projection: Some(merged),
            ..self.clone()
        })))
    }

    /// Take the filters as a hint, and leave them above the scan. The scan
    /// only skips whole datasets with them, so `PushedDown::No` says the
    /// filter above still decides each row.
    fn try_pushdown_filters(
        &self,
        filters: Vec<Arc<dyn PhysicalExpr>>,
        _config: &ConfigOptions,
    ) -> Result<FilterPushdownPropagation<Arc<dyn FileSource>>> {
        let predicate = match self.predicate.clone() {
            Some(existing) => conjunction(std::iter::once(existing).chain(filters.clone())),
            None => conjunction(filters.clone()),
        };

        let source = Self {
            predicate: Some(predicate),
            ..self.clone()
        };

        Ok(FilterPushdownPropagation::with_parent_pushdown_result(vec![
            PushedDown::No;
            filters.len()
        ])
        .with_updated_node(Arc::new(source)))
    }
}

/// One partition's opener: a collection in, its batches out. Every field is
/// a handle, cheap to clone into the stream it returns.
#[derive(Clone)]
pub struct AtlasOpener {
    pub object_store: Arc<dyn ObjectStore>,
    /// The reader cache every open goes through.
    pub cache: AtlasReaderCache,
    /// What the scan reads.
    pub spec: Arc<ScanSpec>,
    pub scan_metrics: AtlasScanMetrics,
    /// The scan's queues, one per collection, shared by every partition.
    pub queues: Arc<CollectionQueues>,
}

impl FileOpener for AtlasOpener {
    /// One collection in, one batch per stored chunk out.
    ///
    /// The first partition to open it prunes and queues its datasets.
    fn open(&self, file: PartitionedFile) -> Result<FileOpenFuture> {
        let opener = self.clone();
        let fut = async move {
            let stream = opener
                .queues
                .open(
                    Some(&opener.cache),
                    opener.object_store,
                    file.object_meta,
                    opener.spec,
                    opener.scan_metrics,
                )
                .await
                .map_err(external)?;
            Ok(stream.map_err(external).boxed())
        };
        Ok(fut.boxed())
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::{Array, ArrayRef, RecordBatch};
    use beacon_datafusion_ext::nd::{decode_nd_record_batch, encoded_schema};
    use beacon_datafusion_ext::type_widening::{ArrowTypeWidening, DefaultArrowTypeWidening};
    use datafusion::logical_expr::Operator;
    use datafusion::physical_expr::expressions::{BinaryExpr, Column as ColumnExpr, Literal};
    use datafusion::physical_plan::PhysicalExpr;
    use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
    use datafusion::scalar::ScalarValue;
    use futures::TryStreamExt;
    use std::path::Path;

    use super::*;
    use crate::{schema, test_support};

    fn column<'a>(batch: &'a RecordBatch, name: &str) -> &'a ArrayRef {
        batch
            .column_by_name(name)
            .unwrap_or_else(|| panic!("no column {name}"))
    }

    /// An opener over a fixture, built the way `AtlasSource` builds one.
    async fn opener(
        dir: &Path,
        predicate: Option<Arc<dyn PhysicalExpr>>,
    ) -> (AtlasOpener, PartitionedFile) {
        let atlas = test_support::open(dir).await;
        let logical_schema = schema::collection_arrow_schema(
            &atlas.footer().collection_schema(),
            &ArrowTypeWidening::default_extension(),
        )
        .unwrap();
        let spec = ScanSpec::new(
            Arc::new(encoded_schema(&logical_schema)),
            None,
            predicate,
            Arc::new(DefaultArrowTypeWidening::new()),
            CancellationToken::new(),
        )
        .unwrap();
        let (store, marker) = test_support::store_and_marker(dir);
        let metrics = ExecutionPlanMetricsSet::new();
        let opener = AtlasOpener {
            object_store: store,
            cache: AtlasReaderCache::new(4),
            spec: Arc::new(spec),
            scan_metrics: AtlasScanMetrics::new(&metrics, 0),
            queues: Arc::new(CollectionQueues::new()),
        };
        (opener, PartitionedFile::from(marker))
    }

    async fn stream(opener: &AtlasOpener, file: PartitionedFile) -> Vec<RecordBatch> {
        opener
            .open(file)
            .unwrap()
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap()
    }

    /// One encoded batch per dataset, in write order, each on the scan's own
    /// schema.
    #[tokio::test]
    async fn the_opener_streams_one_encoded_batch_per_dataset() {
        let tmp = tempfile::tempdir().unwrap();
        test_support::two_datasets(tmp.path()).await;
        let (opener, file) = opener(tmp.path(), None).await;

        let batches = stream(&opener, file).await;

        assert_eq!(batches.len(), 2);
        for batch in &batches {
            assert_eq!(
                batch.schema(),
                opener.spec.projected_schema,
                "the scan's schema, marks and all"
            );
        }
        let rows: Vec<usize> = batches
            .iter()
            .map(|batch| decode_nd_record_batch(batch).unwrap().num_rows())
            .collect();
        assert_eq!(rows, vec![4, 3], "winter, then summer");
        assert_eq!(opener.scan_metrics.datasets_scanned.value(), 2);
    }

    /// The deletion mask hides a dataset from the scan, though not from the
    /// schema.
    #[tokio::test]
    async fn a_deleted_dataset_is_not_streamed() {
        let tmp = tempfile::tempdir().unwrap();
        test_support::two_datasets(tmp.path()).await;
        test_support::open(tmp.path())
            .await
            .delete_dataset("winter")
            .await
            .unwrap();
        let (opener, file) = opener(tmp.path(), None).await;

        let batches = stream(&opener, file).await;

        assert_eq!(batches.len(), 1);
        let summer = decode_nd_record_batch(&batches[0])
            .unwrap()
            .materialize()
            .unwrap();
        assert_eq!(summer.num_rows(), 3);
        assert_eq!(
            column(&summer, "cycle").null_count(),
            3,
            "winter's column, summer's nulls"
        );
    }

    /// A predicate the statistics can judge skips the datasets it rules out
    /// before any of them is read.
    #[tokio::test]
    async fn a_predicate_prunes_datasets_before_the_read() {
        let tmp = tempfile::tempdir().unwrap();
        test_support::ranged(tmp.path(), 10).await;
        let predicate: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
            Arc::new(ColumnExpr::new("temperature", 0)),
            Operator::Gt,
            Arc::new(Literal::new(ScalarValue::Float32(Some(45.0)))),
        ));
        let (opener, file) = opener(tmp.path(), Some(predicate)).await;

        let batches = stream(&opener, file).await;

        assert_eq!(batches.len(), 5, "d5 to d9 reach past 45");
        assert_eq!(opener.scan_metrics.datasets_pruned.value(), 5);
        assert_eq!(opener.scan_metrics.datasets_scanned.value(), 5);
    }
}
