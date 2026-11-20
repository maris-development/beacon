//! [`ScanErrorPolicy`], and [`ScanErrorsExec`]: the fallback for a plan without a file scan.
//!
//! [`with_scan_errors`](crate::scan_errors_source::with_scan_errors) handles
//! errors per file inside a file scan. A plan without a file scan gets this
//! node. It knows no file, so a failed scan names only the table and the file
//! type. A skip only removes an error from the stream. Its input can end after
//! the error, so the data after a bad file can be lost.

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

use crate::error::{ErrorContext, ScanPhase};

/// What a scan does with the error of a file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum OnScanError {
    /// The error gets the table, the file type and the file, and fails the scan.
    Fail,
    /// The file ends at its error, and the scan continues with the next file.
    Skip,
}

/// The error context of a table and what its scan does with an error.
#[derive(Debug, Clone)]
pub(crate) struct ScanErrorPolicy {
    context: ErrorContext,
    on_error: OnScanError,
}

impl ScanErrorPolicy {
    pub(crate) fn new(context: ErrorContext, on_error: OnScanError) -> Self {
        Self { context, on_error }
    }

    pub(crate) fn context(&self) -> &ErrorContext {
        &self.context
    }

    pub(crate) fn on_error(&self) -> OnScanError {
        self.on_error
    }
}

/// Passes the batches of its input through and handles each error with its policy.
///
/// The node changes no schema, partitioning or order. Filters, projections
/// and limits pass through it to the scan.
#[derive(Debug, Clone)]
pub(crate) struct ScanErrorsExec {
    input: Arc<dyn ExecutionPlan>,
    policy: ScanErrorPolicy,
    properties: Arc<PlanProperties>,
    metrics: ExecutionPlanMetricsSet,
}

impl ScanErrorsExec {
    pub(crate) fn new(input: Arc<dyn ExecutionPlan>, policy: ScanErrorPolicy) -> Self {
        let properties = Arc::clone(input.properties());
        Self {
            input,
            policy,
            properties,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }

    fn with_input(&self, input: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
        Arc::new(Self::new(input, self.policy.clone()))
    }
}

impl DisplayAs for ScanErrorsExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.policy.on_error() {
            OnScanError::Fail => write!(f, "ScanErrorsExec"),
            OnScanError::Skip => write!(f, "ScanErrorsExec: skip_errors=true"),
        }
    }
}

impl ExecutionPlan for ScanErrorsExec {
    fn name(&self) -> &str {
        "ScanErrorsExec"
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
            return internal_err!("ScanErrorsExec needs exactly one child");
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
        let context = self.policy.context().clone();
        if self.policy.on_error() == OnScanError::Fail {
            let stream =
                stream.map(move |item| item.map_err(|error| context.error(ScanPhase::Read, None, error)));
            return Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)));
        }
        let skipped = MetricBuilder::new(&self.metrics).global_counter("skipped_stream_errors");
        let stream = stream.filter_map(move |item| {
            ready(match item {
                Ok(batch) => Some(Ok(batch)),
                Err(error) => {
                    tracing::warn!(
                        table = context.table(),
                        file_type = context.file_type(),
                        partition,
                        %error,
                        "skipping a scan error of an external table"
                    );
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
    use crate::error::ExternalTableError;

    fn policy(on_error: OnScanError) -> ScanErrorPolicy {
        ScanErrorPolicy::new(ErrorContext::new("obs", "csv"), on_error)
    }

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
        let plan: Arc<dyn ExecutionPlan> = Arc::new(ScanErrorsExec::new(
            scan(dir.path(), &ctx).await,
            policy(OnScanError::Skip),
        ));
        let batches = collect(Arc::clone(&plan), ctx.task_ctx()).await.unwrap();
        let rows: usize = batches.iter().map(|batch| batch.num_rows()).sum();
        assert_eq!(rows, 2);
        let skipped = plan
            .metrics()
            .and_then(|metrics| metrics.sum_by_name("skipped_stream_errors"))
            .map(|value| value.as_usize());
        assert_eq!(skipped, Some(1));
    }

    #[tokio::test]
    async fn the_wrapper_adds_the_table_and_the_file_type_to_a_failed_scan() {
        let dir = tempfile::tempdir().unwrap();
        let ctx = SessionContext::new();
        let plan: Arc<dyn ExecutionPlan> = Arc::new(ScanErrorsExec::new(
            scan(dir.path(), &ctx).await,
            policy(OnScanError::Fail),
        ));
        let error = collect(plan, ctx.task_ctx()).await.unwrap_err();
        let found = ExternalTableError::find(&error).expect("an external table error");
        assert_eq!(found.table, "obs");
        assert_eq!(found.file_type, "csv");
        assert_eq!(found.phase, ScanPhase::Read);
        assert_eq!(found.object, None);
    }
}
