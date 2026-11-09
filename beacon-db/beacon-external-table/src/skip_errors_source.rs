//! Per-file error skip for a file scan.
//!
//! A `FileStream` stops its partition after the first error, and DataFusion 54
//! gives a `FileScanConfig` no way to set `OnError::Skip`. [`SkipErrorsFileSource`]
//! wraps the format source one level lower, at the morselizer. An error to open,
//! plan or read one file ends only that file. The `FileStream` then continues
//! with the next file of the partition.
//!
//! [`with_skip_errors`] puts the wrapper into a planned scan. A plan without a
//! file scan gets [`SkipErrorsExec`] above it.

use std::{fmt, sync::Arc};

use datafusion::{
    common::{Result, config::ConfigOptions},
    datasource::{
        listing::PartitionedFile,
        physical_plan::{FileOpener, FileScanConfig, FileSource},
        source::DataSourceExec,
        table_schema::TableSchema,
    },
    error::DataFusionError,
    physical_expr::{LexOrdering, PhysicalExpr, projection::ProjectionExprs},
    physical_expr_common::sort_expr::PhysicalSortExpr,
    physical_plan::{
        DisplayFormatType, ExecutionPlan, SortOrderPushdownResult,
        filter_pushdown::FilterPushdownPropagation,
        metrics::{Count, ExecutionPlanMetricsSet, MetricBuilder},
    },
};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion_datasource::morsel::{Morsel, MorselPlan, MorselPlanner, Morselizer};
use futures::{StreamExt, future::ready, stream::BoxStream};
use object_store::ObjectStore;

use crate::skip_errors::SkipErrorsExec;

/// The name of the counter that `EXPLAIN ANALYZE` shows on the scan.
pub(crate) const SKIPPED_FILE_ERRORS: &str = "skipped_file_errors";

/// Puts [`SkipErrorsFileSource`] into the file scan of `plan`.
///
/// The walk follows single children down, because an nd format puts decode and
/// broadcast nodes above its scan. Each node above rebuilds from its new child.
pub(crate) fn with_skip_errors(plan: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
    match wrap_file_scan(&plan) {
        Some(wrapped) => wrapped,
        None => {
            tracing::debug!("no file scan found to skip errors in; skipping stream errors above the plan");
            Arc::new(SkipErrorsExec::new(plan))
        }
    }
}

fn wrap_file_scan(plan: &Arc<dyn ExecutionPlan>) -> Option<Arc<dyn ExecutionPlan>> {
    if let Some(exec) = plan.downcast_ref::<DataSourceExec>() {
        let mut config = exec.data_source().downcast_ref::<FileScanConfig>()?.clone();
        if !config.file_source.is::<SkipErrorsFileSource>() {
            config.file_source = SkipErrorsFileSource::wrap(Arc::clone(&config.file_source));
        }
        return Some(DataSourceExec::from_data_source(config));
    }
    let children = plan.children();
    let [child] = children[..] else { return None };
    let child = wrap_file_scan(child)?;
    Arc::clone(plan).with_new_children(vec![child]).ok()
}

/// A format source whose files end on an error, and do not fail the scan.
///
/// All other behavior comes from the inner source. Each method that returns a
/// new source wraps that source again, so a pushdown keeps the skip.
#[derive(Clone)]
pub(crate) struct SkipErrorsFileSource {
    inner: Arc<dyn FileSource>,
}

impl SkipErrorsFileSource {
    pub(crate) fn wrap(inner: Arc<dyn FileSource>) -> Arc<dyn FileSource> {
        Arc::new(Self { inner })
    }
}

impl FileSource for SkipErrorsFileSource {
    fn create_file_opener(
        &self,
        object_store: Arc<dyn ObjectStore>,
        base_config: &FileScanConfig,
        partition: usize,
    ) -> Result<Arc<dyn FileOpener>> {
        self.inner
            .create_file_opener(object_store, base_config, partition)
    }

    fn create_morselizer(
        &self,
        object_store: Arc<dyn ObjectStore>,
        base_config: &FileScanConfig,
        partition: usize,
    ) -> Result<Box<dyn Morselizer>> {
        let inner = self
            .inner
            .create_morselizer(object_store, base_config, partition)?;
        let skipped = MetricBuilder::new(self.inner.metrics()).global_counter(SKIPPED_FILE_ERRORS);
        Ok(Box::new(SkipErrorsMorselizer { inner, skipped }))
    }

    fn table_schema(&self) -> &TableSchema {
        self.inner.table_schema()
    }

    fn with_batch_size(&self, batch_size: usize) -> Arc<dyn FileSource> {
        Self::wrap(self.inner.with_batch_size(batch_size))
    }

    fn filter(&self) -> Option<Arc<dyn PhysicalExpr>> {
        self.inner.filter()
    }

    fn projection(&self) -> Option<&ProjectionExprs> {
        self.inner.projection()
    }

    fn metrics(&self) -> &ExecutionPlanMetricsSet {
        self.inner.metrics()
    }

    fn file_type(&self) -> &str {
        self.inner.file_type()
    }

    fn fmt_extra(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        self.inner.fmt_extra(t, f)?;
        write!(f, ", skip_errors=true")
    }

    fn supports_repartitioning(&self) -> bool {
        self.inner.supports_repartitioning()
    }

    fn repartitioned(
        &self,
        target_partitions: usize,
        repartition_file_min_size: usize,
        output_ordering: Option<LexOrdering>,
        config: &FileScanConfig,
    ) -> Result<Option<FileScanConfig>> {
        let repartitioned = self.inner.repartitioned(
            target_partitions,
            repartition_file_min_size,
            output_ordering,
            config,
        )?;
        Ok(repartitioned.map(|mut config| {
            // An inner source can build its config with itself as the source.
            if !config.file_source.is::<Self>() {
                config.file_source = Self::wrap(config.file_source);
            }
            config
        }))
    }

    fn try_pushdown_filters(
        &self,
        filters: Vec<Arc<dyn PhysicalExpr>>,
        config: &ConfigOptions,
    ) -> Result<FilterPushdownPropagation<Arc<dyn FileSource>>> {
        let mut propagation = self.inner.try_pushdown_filters(filters, config)?;
        propagation.updated_node = propagation.updated_node.map(Self::wrap);
        Ok(propagation)
    }

    fn try_pushdown_sort(
        &self,
        order: &[PhysicalSortExpr],
        eq_properties: &EquivalenceProperties,
    ) -> Result<SortOrderPushdownResult<Arc<dyn FileSource>>> {
        Ok(self
            .inner
            .try_pushdown_sort(order, eq_properties)?
            .map(Self::wrap))
    }

    fn reorder_files(&self, files: Vec<PartitionedFile>) -> Vec<PartitionedFile> {
        self.inner.reorder_files(files)
    }

    fn try_pushdown_projection(
        &self,
        projection: &ProjectionExprs,
    ) -> Result<Option<Arc<dyn FileSource>>> {
        Ok(self
            .inner
            .try_pushdown_projection(projection)?
            .map(Self::wrap))
    }
}

/// The file and the counter that an error is reported against.
#[derive(Debug, Clone)]
struct SkipContext {
    path: Arc<str>,
    skipped: Count,
}

impl SkipContext {
    fn skip(&self, error: &DataFusionError) {
        tracing::warn!(file = %self.path, %error, "skipping a file that the scan cannot read");
        self.skipped.add(1);
    }
}

#[derive(Debug)]
struct SkipErrorsMorselizer {
    inner: Box<dyn Morselizer>,
    skipped: Count,
}

impl Morselizer for SkipErrorsMorselizer {
    fn plan_file(&self, file: PartitionedFile) -> Result<Box<dyn MorselPlanner>> {
        let context = SkipContext {
            path: file.object_meta.location.as_ref().into(),
            skipped: self.skipped.clone(),
        };
        Ok(match self.inner.plan_file(file) {
            Ok(inner) => Box::new(SkipErrorsPlanner { inner, context }),
            Err(error) => {
                context.skip(&error);
                Box::new(EmptyPlanner)
            }
        })
    }
}

#[derive(Debug)]
struct SkipErrorsPlanner {
    inner: Box<dyn MorselPlanner>,
    context: SkipContext,
}

impl MorselPlanner for SkipErrorsPlanner {
    fn plan(self: Box<Self>) -> Result<Option<MorselPlan>> {
        let Self { inner, context } = *self;
        match inner.plan() {
            Ok(plan) => Ok(plan.map(|plan| wrap_plan(plan, &context))),
            Err(error) => {
                context.skip(&error);
                Ok(None)
            }
        }
    }
}

/// Wraps each morsel, planner and pending planner of `plan`.
fn wrap_plan(mut plan: MorselPlan, context: &SkipContext) -> MorselPlan {
    let morsels = plan
        .take_morsels()
        .into_iter()
        .map(|inner| {
            Box::new(SkipErrorsMorsel {
                inner,
                context: context.clone(),
            }) as Box<dyn Morsel>
        })
        .collect();
    let planners = plan
        .take_ready_planners()
        .into_iter()
        .map(|inner| {
            Box::new(SkipErrorsPlanner {
                inner,
                context: context.clone(),
            }) as Box<dyn MorselPlanner>
        })
        .collect();
    let mut wrapped = MorselPlan::new()
        .with_morsels(morsels)
        .with_planners(planners);
    if let Some(pending) = plan.take_pending_planner() {
        let context = context.clone();
        wrapped.set_pending_planner(async move {
            Ok(match pending.await {
                Ok(inner) => Box::new(SkipErrorsPlanner { inner, context }) as Box<dyn MorselPlanner>,
                Err(error) => {
                    context.skip(&error);
                    Box::new(EmptyPlanner) as Box<dyn MorselPlanner>
                }
            })
        });
    }
    wrapped
}

/// A planner for a skipped file: it plans nothing.
#[derive(Debug)]
struct EmptyPlanner;

impl MorselPlanner for EmptyPlanner {
    fn plan(self: Box<Self>) -> Result<Option<MorselPlan>> {
        Ok(None)
    }
}

#[derive(Debug)]
struct SkipErrorsMorsel {
    inner: Box<dyn Morsel>,
    context: SkipContext,
}

impl Morsel for SkipErrorsMorsel {
    fn into_stream(self: Box<Self>) -> BoxStream<'static, Result<datafusion::arrow::array::RecordBatch>> {
        let Self { inner, context } = *self;
        // The first error ends this morsel. The batches before it stay.
        inner
            .into_stream()
            .scan((), move |_, item| {
                ready(match item {
                    Ok(batch) => Some(Ok(batch)),
                    Err(error) => {
                        context.skip(&error);
                        None
                    }
                })
            })
            .boxed()
    }
}

#[cfg(test)]
mod tests {
    use datafusion::{
        arrow::{
            array::Float64Array,
            datatypes::{DataType, Field, Schema},
        },
        catalog::TableProvider,
        datasource::{
            file_format::csv::CsvFormat,
            listing::{ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl},
        },
        physical_plan::{coalesce_partitions::CoalescePartitionsExec, collect, empty::EmptyExec},
        prelude::SessionContext,
    };

    use super::*;

    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("temp", DataType::Float64, true),
            Field::new("depth", DataType::Float64, true),
        ]))
    }

    /// One partition over `a.csv` (good), `b.csv` (a bad row) and `c.csv` (good).
    async fn scan(
        dir: &std::path::Path,
        ctx: &SessionContext,
        projection: Option<&Vec<usize>>,
    ) -> Arc<dyn ExecutionPlan> {
        std::fs::write(dir.join("a.csv"), "temp,depth\n1.0,10.0\n2.0,20.0\n").unwrap();
        std::fs::write(dir.join("b.csv"), "temp,depth\n3.0,30.0,1.0,2.0\n").unwrap();
        std::fs::write(dir.join("c.csv"), "temp,depth\n4.0,40.0\n").unwrap();
        let url = ListingTableUrl::parse(format!("{}/", dir.display())).unwrap();
        let options = ListingOptions::new(Arc::new(CsvFormat::default().with_has_header(true)))
            .with_file_extension(".csv")
            .with_target_partitions(1);
        let config = ListingTableConfig::new(url)
            .with_listing_options(options)
            .with_schema(schema());
        let table = ListingTable::try_new(config).unwrap();
        table.scan(&ctx.state(), projection, &[], None).await.unwrap()
    }

    fn skipped(plan: &Arc<dyn ExecutionPlan>) -> Option<usize> {
        plan.metrics()
            .and_then(|metrics| metrics.sum_by_name(SKIPPED_FILE_ERRORS))
            .map(|value| value.as_usize())
    }

    fn temps(batches: &[datafusion::arrow::array::RecordBatch]) -> Vec<f64> {
        let mut temps: Vec<f64> = batches
            .iter()
            .flat_map(|batch| {
                let column = batch.column_by_name("temp").unwrap();
                let column = column.as_any().downcast_ref::<Float64Array>().unwrap();
                column.values().to_vec()
            })
            .collect();
        temps.sort_by(f64::total_cmp);
        temps
    }

    #[tokio::test]
    async fn the_scan_fails_without_the_wrapper() {
        let dir = tempfile::tempdir().unwrap();
        let ctx = SessionContext::new();
        let plan = scan(dir.path(), &ctx, None).await;
        assert!(collect(plan, ctx.task_ctx()).await.is_err());
    }

    #[tokio::test]
    async fn the_scan_skips_the_bad_file_and_reads_the_next_files() {
        let dir = tempfile::tempdir().unwrap();
        let ctx = SessionContext::new();
        let plan = with_skip_errors(scan(dir.path(), &ctx, None).await);
        assert!(plan.is::<DataSourceExec>(), "the scan itself is wrapped");

        let batches = collect(Arc::clone(&plan), ctx.task_ctx()).await.unwrap();
        assert_eq!(temps(&batches), [1.0, 2.0, 4.0]);
        assert_eq!(skipped(&plan), Some(1));
    }

    #[tokio::test]
    async fn the_skip_survives_a_projection() {
        let dir = tempfile::tempdir().unwrap();
        let ctx = SessionContext::new();
        let plan = with_skip_errors(scan(dir.path(), &ctx, Some(&vec![0])).await);
        let batches = collect(plan, ctx.task_ctx()).await.unwrap();
        assert_eq!(batches[0].num_columns(), 1);
        assert_eq!(temps(&batches), [1.0, 2.0, 4.0]);
    }

    #[tokio::test]
    async fn the_scan_below_a_node_is_found_and_rebuilt() {
        let dir = tempfile::tempdir().unwrap();
        let ctx = SessionContext::new();
        let plan: Arc<dyn ExecutionPlan> =
            Arc::new(CoalescePartitionsExec::new(scan(dir.path(), &ctx, None).await));
        let plan = with_skip_errors(plan);
        assert!(plan.is::<CoalescePartitionsExec>());
        let batches = collect(plan, ctx.task_ctx()).await.unwrap();
        assert_eq!(temps(&batches), [1.0, 2.0, 4.0]);
    }

    #[test]
    fn a_plan_without_a_file_scan_gets_the_exec_fallback() {
        let plan: Arc<dyn ExecutionPlan> = Arc::new(EmptyExec::new(schema()));
        assert!(with_skip_errors(plan).is::<SkipErrorsExec>());
    }
}
