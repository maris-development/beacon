//! A limit on the rows one query outputs.
//!
//! The limit comes from the `query_output_row_limit` setting of the caller's
//! roles; see [`row_limit_for`]. [`limit_output`] puts a [`RowLimitExec`] where
//! the rows of the query leave the plan:
//!
//! - For a streamed result, on top of the plan.
//! - For a file output (`COPY TO`), below the `DataSinkExec`, so the sink never
//!   writes more rows than the limit.
//!
//! When the query outputs more rows than its limit, it fails with
//! [`DataFusionError::ResourcesExhausted`]. It does not cut the result, so a
//! client never takes a partial result for a complete one.

use std::any::Any;
use std::fmt;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use beacon_auth::{AuthContext, AuthIdentity};
use datafusion::common::Statistics;
use datafusion::datasource::sink::DataSinkExec;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::execution_plan::CardinalityEffect;
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use futures::{Stream, StreamExt};

/// The most rows a query that `identity` runs may output. `None` means no limit.
///
/// 1. A super-user has no limit.
/// 2. Else the `query_output_row_limit` setting of the identity's roles applies,
///    set with `ALTER ROLE <role> SET query_output_row_limit = <n>`. The most
///    generous role wins, and `0` means no limit.
/// 3. Else there is no limit.
pub fn row_limit_for(auth: &AuthContext, identity: &AuthIdentity) -> Option<u64> {
    if identity.is_super_user {
        return None;
    }
    auth.query_output_row_limit(&identity.roles)
        .filter(|&limit| limit > 0)
}

/// Puts a [`RowLimitExec`] where the rows of `plan` leave it.
///
/// The child of a `DataSinkExec` gets the limit, so a `COPY TO` writes at most
/// `limit` rows. The sink formats put only nodes that keep the row count
/// between the query and the sink, such as a sort. Any other plan gets the
/// limit on top.
pub fn limit_output(
    plan: Arc<dyn ExecutionPlan>,
    limit: u64,
) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
    if plan.as_any().is::<DataSinkExec>() {
        let input = plan.children()[0].clone();
        return plan.with_new_children(vec![Arc::new(RowLimitExec::new(input, limit))]);
    }
    Ok(Arc::new(RowLimitExec::new(plan, limit)))
}

/// Fails the query when its partitions output more than `limit` rows together.
#[derive(Debug)]
pub struct RowLimitExec {
    input: Arc<dyn ExecutionPlan>,
    limit: u64,
    /// The rows of all partitions until now.
    rows: Arc<AtomicU64>,
}

impl RowLimitExec {
    pub fn new(input: Arc<dyn ExecutionPlan>, limit: u64) -> Self {
        Self {
            input,
            limit,
            rows: Arc::new(AtomicU64::new(0)),
        }
    }
}

impl DisplayAs for RowLimitExec {
    fn fmt_as(&self, _format: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "RowLimitExec: limit={}", self.limit)
    }
}

impl ExecutionPlan for RowLimitExec {
    fn name(&self) -> &str {
        "RowLimitExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        self.input.properties()
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
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self {
            input: children.swap_remove(0),
            limit: self.limit,
            rows: self.rows.clone(),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        Ok(Box::pin(RowLimitStream {
            inner: self.input.execute(partition, context)?,
            limit: self.limit,
            rows: self.rows.clone(),
            failed: false,
        }))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        None
    }

    fn partition_statistics(&self, partition: Option<usize>) -> DataFusionResult<Statistics> {
        self.input.partition_statistics(partition)
    }

    fn cardinality_effect(&self) -> CardinalityEffect {
        CardinalityEffect::Equal
    }
}

/// The stream of a [`RowLimitExec`].
struct RowLimitStream {
    inner: SendableRecordBatchStream,
    limit: u64,
    rows: Arc<AtomicU64>,
    /// Set after the limit error, so the stream ends instead of repeating it.
    failed: bool,
}

impl Stream for RowLimitStream {
    type Item = DataFusionResult<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.failed {
            return Poll::Ready(None);
        }
        let poll = this.inner.poll_next_unpin(cx);
        if let Poll::Ready(Some(Ok(batch))) = &poll {
            let rows = batch.num_rows() as u64;
            let before = this.rows.fetch_add(rows, Ordering::Relaxed);
            // The batch that crosses the limit is not passed on, so no more than
            // `limit` rows ever leave the plan.
            if before + rows > this.limit {
                this.failed = true;
                return Poll::Ready(Some(Err(DataFusionError::ResourcesExhausted(format!(
                    "query output exceeds its row limit of {} rows",
                    this.limit
                )))));
            }
        }
        poll
    }
}

impl RecordBatchStream for RowLimitStream {
    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }
}

#[cfg(test)]
mod tests {
    use datafusion::physical_plan::{collect, displayable};
    use datafusion::prelude::{SessionConfig, SessionContext};

    use super::*;

    async fn physical_plan(ctx: &SessionContext, sql: &str) -> Arc<dyn ExecutionPlan> {
        ctx.sql(sql)
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap()
    }

    fn rows(batches: &[RecordBatch]) -> usize {
        batches.iter().map(RecordBatch::num_rows).sum()
    }

    /// Four partitions, so the limit holds across partitions.
    fn ctx() -> SessionContext {
        SessionContext::new_with_config(SessionConfig::new().with_target_partitions(4))
    }

    const SQL: &str = "SELECT v FROM generate_series(1, 100000) AS t(v) WHERE v % 2 = 0";

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn exactly_the_limit_passes() {
        let ctx = ctx();
        let plan = limit_output(physical_plan(&ctx, SQL).await, 50_000).unwrap();
        let batches = collect(plan, ctx.task_ctx()).await.unwrap();
        assert_eq!(rows(&batches), 50_000);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn one_row_over_the_limit_fails() {
        let ctx = ctx();
        let plan = limit_output(physical_plan(&ctx, SQL).await, 49_999).unwrap();
        let error = collect(plan, ctx.task_ctx()).await.unwrap_err();
        assert!(
            error.to_string().contains("row limit of 49999 rows"),
            "{error}"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_copy_gets_the_limit_below_its_sink() {
        let ctx = ctx();
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("out.csv");
        let sql = format!("COPY ({SQL}) TO '{}' STORED AS CSV", target.display());

        let plan = limit_output(physical_plan(&ctx, &sql).await, 10).unwrap();
        assert!(plan.as_any().is::<DataSinkExec>(), "the sink stays on top");
        assert!(
            plan.children()[0].as_any().is::<RowLimitExec>(),
            "{}",
            displayable(plan.as_ref()).indent(true)
        );
        let error = collect(plan, ctx.task_ctx()).await.unwrap_err();
        assert!(
            error.to_string().contains("row limit of 10 rows"),
            "{error}"
        );

        let plan = limit_output(physical_plan(&ctx, &sql).await, 50_000).unwrap();
        collect(plan, ctx.task_ctx())
            .await
            .expect("a COPY at the limit writes the file");
    }
}
