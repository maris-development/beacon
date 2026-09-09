//! The DataFusion [`FileSource`] and [`FileOpener`] for Atlas collections.
//!
//! # One collection is one unit of work
//!
//! A plan entry is a collection: its `data.atlas` container, as the listing
//! found it. [`AtlasFormat`] dedupes the markers and deals them round-robin
//! over the target partitions, and a partition reads each collection it holds
//! from end to end. A container is never split by byte range, because a byte
//! range of one means nothing.
//!
//! # What an open does
//!
//! An open goes through the [`AtlasReaderPool`] the source holds. The first
//! partition to reach a collection opens it, at the cost of one footer read
//! through the reader cache, prunes its datasets in one vectorised pass over
//! the footer's statistics, and queues the names it has to read. Every
//! partition that opens the collection then streams the datasets it pops off
//! that queue: each dataset is built in turn, and its batches are yielded
//! before the next dataset is touched.
//!
//! So a pruned dataset costs nothing at all, a kept one costs its build and
//! its read, and a dataset is read by one partition and by no other. Nothing
//! is listed at plan time.
//!
//! [`AtlasFormat`]: super::AtlasFormat

use std::any::Any;
use std::sync::Arc;
use std::time::Instant;

use arrow::datatypes::SchemaRef;
use atlas::{Atlas, DatasetView};
use beacon_nd_array::arrow::{
    file_read::FileRead, metrics::ReadMetrics, partition::FilePartitions,
};
use datafusion::{
    config::ConfigOptions,
    datasource::{
        listing::PartitionedFile,
        physical_plan::{FileOpenFuture, FileOpener, FileScanConfig, FileSource},
        table_schema::TableSchema,
    },
    error::{DataFusionError, Result},
    physical_expr::{
        PhysicalExpr, conjunction,
        projection::ProjectionExprs,
        utils::{collect_columns, reassign_expr_columns},
    },
    physical_plan::{
        filter_pushdown::{FilterPushdownPropagation, PushedDown},
        metrics::ExecutionPlanMetricsSet,
    },
};
use futures::{FutureExt, StreamExt, TryStreamExt};
use object_store::ObjectStore;

use beacon_datafusion_ext::nd::logical_schema;

use crate::datafusion::{metrics::AtlasScanMetrics, pool::AtlasReaderPool};
use crate::store::{AtlasReaderCache, get_or_open_atlas};
use crate::{compat, datafusion::opener::AtlasOpener};

/// DataFusion [`FileSource`] for Atlas collections.
#[derive(Debug, Clone)]
pub struct AtlasSource {
    table_schema: TableSchema,
    execution_plan_metrics: ExecutionPlanMetricsSet,
    batch_size: usize,
    predicate: Option<Arc<dyn PhysicalExpr>>,
    read_dimensions: Option<Vec<String>>,
    projection: Option<ProjectionExprs>,
    /// The reader cache to consult, or `None` to open every collection afresh.
    cache: AtlasReaderCache,
    reader_pool: Arc<AtlasReaderPool>,
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
            batch_size: usize::MAX,
            predicate: None,
            read_dimensions,
            projection: None,
            cache,
            reader_pool: Arc::new(AtlasReaderPool::new()),
        }
    }

    /// Consult `cache` for opened collections, or open them afresh.
    pub fn with_cache(mut self, cache: AtlasReaderCache) -> Self {
        self.cache = cache;
        self
    }

    /// Carry a projection the scan pushed down.
    ///
    /// The format rebuilds the source in `create_physical_plan`, and without
    /// this the projection pushed into the old one would be lost.
    pub fn with_projection(mut self, projection: Option<ProjectionExprs>) -> Self {
        self.projection = projection;
        self
    }
}

impl FileSource for AtlasSource {
    fn create_file_opener(
        &self,
        object_store: Arc<dyn ObjectStore>,
        base_config: &FileScanConfig,
        partition: usize,
    ) -> Result<Arc<dyn FileOpener>> {
        let projected_schema = base_config.projected_schema()?;
        Ok(Arc::new(AtlasOpener {
            object_store,
            cache: self.cache.clone(),
            // A predicate is written against the values, not the encoding the
            // scan carries them in.
            logical_schema: logical_schema(&projected_schema)?,
            projected_schema,
            read_dimensions: self.read_dimensions.clone(),
            batch_size: self.batch_size,
            predicate: self.predicate.clone(),
            read_metrics: ReadMetrics::new(&self.execution_plan_metrics, partition),
            scan_metrics: AtlasScanMetrics::new(&self.execution_plan_metrics, partition),
            reader_pool: Arc::clone(&self.reader_pool),
        }))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_schema(&self) -> &TableSchema {
        &self.table_schema
    }

    fn with_batch_size(&self, batch_size: usize) -> Arc<dyn FileSource> {
        Arc::new(Self {
            batch_size,
            ..self.clone()
        })
    }

    /// A container is one unit. A byte range of it names nothing a reader can
    /// open, so the plan's groups stand as the format dealt them.
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

    /// Take the filters as a hint, and leave them above the scan.
    ///
    /// The scan uses a predicate twice: to skip a whole dataset whose recorded
    /// statistics cannot hold a matching row, and to skip a chunk whose
    /// coordinates cannot. Neither is exact — both work in whole datasets and
    /// whole chunks — so the filter above the scan still decides each row, and
    /// `PushedDown::No` is what says so.
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
