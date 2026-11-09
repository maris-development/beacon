//! [`SkipErrorsExec`]: the fallback skip for a plan without a file scan.
//!
//! [`with_skip_errors`](crate::skip_errors_source::with_skip_errors) skips per
//! file inside a file scan. A plan without a file scan gets this node. It only
//! removes an error from the stream. Its input can end after the error, so the
//! data after a bad file can be lost.

use std::{fmt, sync::Arc};

use datafusion::{
    arrow::datatypes::SchemaRef,
    common::{Result, Statistics, internal_err},
    config::ConfigOptions,
    execution::TaskContext,
    physical_expr::PhysicalExpr,
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
        execution_plan::CardinalityEffect,
        filter_pushdown::{
            ChildPushdownResult, FilterDescription, FilterPushdownPhase, FilterPushdownPropagation,
        },
        metrics::{ExecutionPlanMetricsSet, MetricBuilder, MetricsSet},
        projection::ProjectionExec,
        stream::RecordBatchStreamAdapter,
    },
};
use futures::{StreamExt, future::ready};

/// Passes the batches of its input through and drops each error.
///
/// The node changes no schema, partitioning or order. Filters, projections
/// and limits pass through it to the scan.
#[derive(Debug, Clone)]
pub(crate) struct SkipErrorsExec {
    input: Arc<dyn ExecutionPlan>,
    properties: Arc<PlanProperties>,
    metrics: ExecutionPlanMetricsSet,
}

impl SkipErrorsExec {
    pub(crate) fn new(input: Arc<dyn ExecutionPlan>) -> Self {
        let properties = Arc::clone(input.properties());
        Self {
            input,
            properties,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }

    fn with_input(&self, input: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
        Arc::new(Self::new(input))
    }
}

impl DisplayAs for SkipErrorsExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SkipErrorsExec")
    }
}

impl ExecutionPlan for SkipErrorsExec {
    fn name(&self) -> &str {
        "SkipErrorsExec"
    }

    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return internal_err!("SkipErrorsExec needs exactly one child");
        }
        Ok(self.with_input(children.swap_remove(0)))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let schema = self.input.schema();
        let stream = self.input.execute(partition, context)?;
        let skipped = MetricBuilder::new(&self.metrics).global_counter("skipped_stream_errors");
        let stream = stream.filter_map(move |item| {
            ready(match item {
                Ok(batch) => Some(Ok(batch)),
                Err(error) => {
                    tracing::warn!(partition, %error, "skipping a scan error of an external table");
                    skipped.add(1);
                    None
                }
            })
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn partition_statistics(&self, partition: Option<usize>) -> Result<Arc<Statistics>> {
        self.input.partition_statistics(partition)
    }

    fn supports_limit_pushdown(&self) -> bool {
        true
    }

    fn cardinality_effect(&self) -> CardinalityEffect {
        // A skipped error can drop rows, never add them.
        CardinalityEffect::LowerEqual
    }

    fn try_swapping_with_projection(
        &self,
        projection: &ProjectionExec,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        Ok(self
            .input
            .try_swapping_with_projection(projection)?
            .map(|input| self.with_input(input)))
    }

    fn gather_filters_for_pushdown(
        &self,
        _phase: FilterPushdownPhase,
        parent_filters: Vec<Arc<dyn PhysicalExpr>>,
        _config: &ConfigOptions,
    ) -> Result<FilterDescription> {
        FilterDescription::from_children(parent_filters, &self.children())
    }

    fn handle_child_pushdown_result(
        &self,
        _phase: FilterPushdownPhase,
        child_pushdown_result: ChildPushdownResult,
        _config: &ConfigOptions,
    ) -> Result<FilterPushdownPropagation<Arc<dyn ExecutionPlan>>> {
        Ok(FilterPushdownPropagation::if_all(child_pushdown_result))
    }
}

#[cfg(test)]
mod tests {
    use datafusion::{
        arrow::datatypes::{DataType, Field, Schema},
        catalog::TableProvider,
        datasource::{
            file_format::csv::CsvFormat,
            listing::{ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl},
        },
        physical_plan::collect,
        prelude::SessionContext,
    };

    use super::*;

    /// One partition over `a.csv` (two good rows) and then `b.csv` (a bad row count).
    async fn scan(dir: &std::path::Path, ctx: &SessionContext) -> Arc<dyn ExecutionPlan> {
        std::fs::write(dir.join("a.csv"), "temp\n1.0\n2.0\n").unwrap();
        std::fs::write(dir.join("b.csv"), "temp\n3.0,4.0,5.0\n").unwrap();
        let url = ListingTableUrl::parse(format!("{}/", dir.display())).unwrap();
        let options = ListingOptions::new(Arc::new(CsvFormat::default().with_has_header(true)))
            .with_file_extension(".csv")
            .with_target_partitions(1);
        let schema = Arc::new(Schema::new(vec![Field::new(
            "temp",
            DataType::Float64,
            true,
        )]));
        let config = ListingTableConfig::new(url)
            .with_listing_options(options)
            .with_schema(schema);
        let table = ListingTable::try_new(config).unwrap();
        table.scan(&ctx.state(), None, &[], None).await.unwrap()
    }

    #[tokio::test]
    async fn the_scan_fails_without_the_wrapper() {
        let dir = tempfile::tempdir().unwrap();
        let ctx = SessionContext::new();
        let plan = scan(dir.path(), &ctx).await;
        assert!(collect(plan, ctx.task_ctx()).await.is_err());
    }

    #[tokio::test]
    async fn the_wrapper_returns_the_rows_before_the_error() {
        let dir = tempfile::tempdir().unwrap();
        let ctx = SessionContext::new();
        let plan: Arc<dyn ExecutionPlan> =
            Arc::new(SkipErrorsExec::new(scan(dir.path(), &ctx).await));
        let batches = collect(Arc::clone(&plan), ctx.task_ctx()).await.unwrap();
        let rows: usize = batches.iter().map(|batch| batch.num_rows()).sum();
        assert_eq!(rows, 2);
        let skipped = plan
            .metrics()
            .and_then(|metrics| metrics.sum_by_name("skipped_stream_errors"))
            .map(|value| value.as_usize());
        assert_eq!(skipped, Some(1));
    }
}
