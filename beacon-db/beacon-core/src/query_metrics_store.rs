//! Managed-table persistence for per-query metrics.
//!
//! Every query that runs to completion writes one row into the internal managed
//! table `__beacon_query_metrics`, so metrics survive a restart and are queryable
//! like any other table — rather than living in a process-local map that is lost
//! on shutdown and unbounded only in memory. `beacon.system.query_metrics` is
//! this table under its public name, and [`Runtime::get_query_metrics`] reads a
//! single row out of it.
//!
//! # Reaching the session without a cycle
//!
//! Like [`crate::auth_store`], the store holds the planner's [`SessionCell`] (a
//! late-filled `Weak`) and upgrades it per call: a strong `Arc<SessionContext>`
//! would close a `SessionContext -> Runtime -> store -> SessionContext` cycle and
//! leak the session — and the redb `beacon.db` exclusive lock — for the process
//! lifetime.
//!
//! # Not a query
//!
//! Writes and reads here go through `lower_df_statement`/`execute_statement_plan`
//! rather than `Runtime::run_query`. That is deliberate: `run_query` records
//! metrics, so routing the metrics write through it would record the write, whose
//! write would record *that* write, without end.
//!
//! [`Runtime::get_query_metrics`]: crate::runtime::Runtime::get_query_metrics

use std::sync::Arc;

use arrow::{
    array::{ArrayRef, StringArray, TimestampMillisecondArray, UInt64Array},
    datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit},
    record_batch::RecordBatch,
};
use datafusion::{catalog::TableProvider, prelude::SessionContext, sql::parser::DFParserBuilder};
use futures::TryStreamExt;

use crate::metrics::ConsolidatedMetrics;
use crate::statement_plan::SessionCell;

/// The internal managed table the metrics land in. The `__beacon_` prefix is what
/// keeps it out of user-facing listings and gates it to the super-user (see
/// `statement_plan::authz`).
pub(crate) const QUERY_METRICS_TABLE: &str = "__beacon_query_metrics";

/// Who ran the query, when it finished, and the scalar counters are typed
/// columns; everything with an open-ended shape
/// (the query itself, both logical plans, the physical-plan metric tree, the file
/// list) stays a single JSON string column. Modelling those as nested Arrow types
/// would pin the table schema to DataFusion's plan and metric representations,
/// which change between versions.
pub(crate) fn query_metrics_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("query_id", DataType::Utf8, false),
        Field::new("username", DataType::Utf8, false),
        // UTC by construction — the managed engine stores timestamps without a
        // zone, so the column carries the instant and the reader supplies the `Z`.
        Field::new(
            "finished_at",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
        Field::new("query", DataType::Utf8, false),
        Field::new("input_rows", DataType::UInt64, false),
        Field::new("input_bytes", DataType::UInt64, false),
        Field::new("result_num_rows", DataType::UInt64, false),
        Field::new("result_size_in_bytes", DataType::UInt64, false),
        Field::new("execution_time_ms", DataType::UInt64, false),
        Field::new("file_paths", DataType::Utf8, false),
        Field::new("parsed_logical_plan", DataType::Utf8, false),
        Field::new("optimized_logical_plan", DataType::Utf8, false),
        Field::new("node_metrics", DataType::Utf8, false),
    ]))
}

fn json_string(value: &serde_json::Value) -> String {
    serde_json::to_string(value).unwrap_or_else(|_| "null".to_string())
}

/// One consolidated result as a single-row batch in [`query_metrics_schema`].
fn metrics_batch(metrics: &ConsolidatedMetrics) -> anyhow::Result<RecordBatch> {
    let columns: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(vec![metrics.query_id.to_string()])),
        Arc::new(StringArray::from(vec![metrics.username.clone()])),
        Arc::new(TimestampMillisecondArray::from(vec![metrics
            .finished_at
            .timestamp_millis()])),
        Arc::new(StringArray::from(vec![json_string(&metrics.query)])),
        Arc::new(UInt64Array::from(vec![metrics.input_rows])),
        Arc::new(UInt64Array::from(vec![metrics.input_bytes])),
        Arc::new(UInt64Array::from(vec![metrics.result_num_rows])),
        Arc::new(UInt64Array::from(vec![metrics.result_size_in_bytes])),
        Arc::new(UInt64Array::from(vec![metrics.execution_time_ms])),
        Arc::new(StringArray::from(vec![serde_json::to_string(
            &metrics.file_paths,
        )
        .unwrap_or_else(|_| "[]".to_string())])),
        Arc::new(StringArray::from(vec![json_string(
            &metrics.parsed_logical_plan,
        )])),
        Arc::new(StringArray::from(vec![json_string(
            &metrics.optimized_logical_plan,
        )])),
        Arc::new(StringArray::from(vec![serde_json::to_string(
            &metrics.node_metrics,
        )
        .unwrap_or_else(|_| "null".to_string())])),
    ];
    Ok(RecordBatch::try_new(query_metrics_schema(), columns)?)
}

/// Writes and reads the query-metrics table.
pub(crate) struct QueryMetricsStore {
    /// Late-filled weak handle to the session, upgraded per call.
    session: SessionCell,
    /// A read-only database records nothing: the write would be refused, and the
    /// engine must not fail a caller's query over its own bookkeeping.
    read_only: bool,
}

impl std::fmt::Debug for QueryMetricsStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QueryMetricsStore").finish_non_exhaustive()
    }
}

impl QueryMetricsStore {
    pub(crate) fn new(session: SessionCell, read_only: bool) -> Self {
        Self { session, read_only }
    }

    fn session(&self) -> anyhow::Result<Arc<SessionContext>> {
        self.session
            .get()
            .and_then(|weak| weak.upgrade())
            .ok_or_else(|| anyhow::anyhow!("query metrics store: beacon session is unavailable"))
    }

    /// Runs one statement through beacon's lowering + execution path, collecting
    /// its batches. Not `run_query`: see the module docs on recursion.
    async fn run(&self, sql: String) -> anyhow::Result<Vec<RecordBatch>> {
        let session = self.session()?;
        // Scoped so the (`!Send`) parser is dropped before the first `.await`.
        let statement = {
            let mut parser = DFParserBuilder::new(sql.as_str()).build()?;
            parser.parse_statement()?
        };
        let plan = crate::statement_plan::lower_df_statement(&session, statement).await?;
        let stream = crate::statement_plan::execute_statement_plan(&session, plan).await?;
        Ok(stream.try_collect::<Vec<_>>().await?)
    }

    /// Creates the table if it does not already exist. Idempotent, so it is safe
    /// on every start; a read-only database skips it and simply records nothing.
    pub(crate) async fn ensure_table(&self) -> anyhow::Result<()> {
        if self.read_only {
            return Ok(());
        }
        self.run(format!(
            "CREATE TABLE IF NOT EXISTS {QUERY_METRICS_TABLE} (\
                 query_id VARCHAR, \
                 username VARCHAR, \
                 finished_at TIMESTAMP(3), \
                 query VARCHAR, \
                 input_rows BIGINT UNSIGNED, \
                 input_bytes BIGINT UNSIGNED, \
                 result_num_rows BIGINT UNSIGNED, \
                 result_size_in_bytes BIGINT UNSIGNED, \
                 execution_time_ms BIGINT UNSIGNED, \
                 file_paths VARCHAR, \
                 parsed_logical_plan VARCHAR, \
                 optimized_logical_plan VARCHAR, \
                 node_metrics VARCHAR)"
        ))
        .await
        .map(|_| ())
    }

    /// The live table provider, or `None` when the table is not registered (a
    /// read-only database, or a start where `ensure_table` failed).
    async fn provider(&self) -> Option<Arc<dyn TableProvider>> {
        self.session()
            .ok()?
            .table_provider(QUERY_METRICS_TABLE)
            .await
            .ok()
    }

    /// Appends one query's metrics.
    ///
    /// Failures are logged, never propagated: bookkeeping must not turn a
    /// successful query into a failed one for the caller draining its results.
    pub(crate) async fn record(&self, metrics: ConsolidatedMetrics) {
        if self.read_only {
            return;
        }
        let query_id = metrics.query_id;
        if let Err(error) = self.insert(metrics).await {
            tracing::warn!(%query_id, ?error, "failed to record query metrics");
        }
    }

    async fn insert(&self, metrics: ConsolidatedMetrics) -> anyhow::Result<()> {
        let session = self.session()?;
        let target = self
            .provider()
            .await
            .ok_or_else(|| anyhow::anyhow!("`{QUERY_METRICS_TABLE}` is not registered"))?;

        // Cast to whatever the table settled on (a managed table may store the
        // view string layout, or a different integer width) so the insert's input
        // matches its target exactly.
        let batch = cast_to(&metrics_batch(&metrics)?, target.schema())?;
        session
            .read_batch(batch)?
            .write_table(
                QUERY_METRICS_TABLE,
                datafusion::dataframe::DataFrameWriteOptions::new().with_insert_operation(
                    datafusion::logical_expr::dml::InsertOp::Append,
                ),
            )
            .await?;
        Ok(())
    }

    /// The metrics recorded for `query_id`, as the table stores them. Empty when
    /// the id is unknown.
    pub(crate) async fn read(&self, query_id: uuid::Uuid) -> anyhow::Result<Vec<RecordBatch>> {
        // A UUID's rendering is hex and hyphens, so it cannot escape the literal.
        self.run(format!(
            "SELECT * FROM {QUERY_METRICS_TABLE} WHERE query_id = '{query_id}'"
        ))
        .await
    }
}

/// Casts every column of `batch` to `schema`'s corresponding type.
fn cast_to(batch: &RecordBatch, schema: SchemaRef) -> anyhow::Result<RecordBatch> {
    anyhow::ensure!(
        batch.num_columns() == schema.fields().len(),
        "query metrics row has {} columns but the table has {}",
        batch.num_columns(),
        schema.fields().len()
    );
    let columns = batch
        .columns()
        .iter()
        .zip(schema.fields())
        .map(|(column, field)| arrow::compute::cast(column, field.data_type()))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(RecordBatch::try_new(schema, columns)?)
}

