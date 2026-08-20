//! [`DatasetsExec`]: the plan node that turns a listing stream into result rows.
//!
//! A listing is a stream of paths from an object store, and a query wants
//! batches of rows. Everything between those two is here.
//!
//! # Why not a `MemTable`
//!
//! Wrapping the finished listing in a `MemTable` was simpler, and it forced the
//! whole walk to finish, and every row to exist at once, before the plan could
//! start. On a store of 2 853 217 objects that is a two-figure number of seconds
//! before the first row and about a gigabyte held while it happens.
//!
//! This node emits a batch as soon as it has [`BATCH_ROWS`] of them. First rows
//! arrive in the time of the first listing page, and memory is bounded by one
//! batch rather than by the store.
//!
//! # Limit
//!
//! `limit` stops the node, and because the source is a lazy stream, stopping the
//! node stops the walk. A `LIMIT 50` over a bucket of millions reads one page,
//! not all of it. That is the part a `MemTable` could not do however the limit
//! was pushed down.

use std::any::Any;
use std::fmt;
use std::sync::Arc;

use arrow::{
    array::{BooleanArray, StringArray, UInt64Array},
    datatypes::SchemaRef,
    record_batch::RecordBatch,
};
use datafusion::{
    error::{DataFusionError, Result},
    execution::{SendableRecordBatchStream, TaskContext},
    physical_expr::EquivalenceProperties,
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
        execution_plan::{Boundedness, EmissionType},
        stream::RecordBatchStreamAdapter,
    },
};
use futures::stream::{BoxStream, StreamExt};

use super::provider::Row;

/// Rows gathered before a batch is emitted.
///
/// Small enough that the first rows reach the caller promptly, large enough that
/// per-batch overhead stays out of the way.
pub const BATCH_ROWS: usize = 8192;

/// Builds the listing stream. Called once per partition, at execute time, so
/// nothing touches the store until the plan runs.
pub type RowStreamFactory =
    Arc<dyn Fn() -> Result<BoxStream<'static, Result<Row>>> + Send + Sync>;

/// Streams listing rows as record batches.
#[derive(Clone)]
pub struct DatasetsExec {
    schema: SchemaRef,
    rows: RowStreamFactory,
    /// Rows to skip before emitting. Applied here so a page is never built and
    /// thrown away.
    offset: usize,
    /// Rows to emit at most. `None` reads the listing out.
    limit: Option<usize>,
    /// How the plan prints itself, for `EXPLAIN`.
    label: String,
    properties: Arc<PlanProperties>,
}

impl DatasetsExec {
    pub fn new(
        schema: SchemaRef,
        rows: RowStreamFactory,
        offset: usize,
        limit: Option<usize>,
        label: String,
    ) -> Self {
        let properties = PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&schema)),
            // One partition: a listing is one walk, and splitting it would mean
            // splitting the object store's own pagination.
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );
        Self {
            schema,
            rows,
            offset,
            limit,
            label,
            properties: Arc::new(properties),
        }
    }
}

impl fmt::Debug for DatasetsExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "DatasetsExec({})", self.label)
    }
}

impl DisplayAs for DatasetsExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "DatasetsExec: {}", self.label)?;
        if self.offset > 0 {
            write!(f, ", offset={}", self.offset)?;
        }
        if let Some(limit) = self.limit {
            write!(f, ", limit={limit}")?;
        }
        Ok(())
    }
}

impl ExecutionPlan for DatasetsExec {
    fn name(&self) -> &str {
        "DatasetsExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Internal(format!(
                "DatasetsExec has one partition; asked for {partition}"
            )));
        }

        let schema = Arc::clone(&self.schema);
        let rows = (self.rows)()?;
        let batches = batch_rows(rows, Arc::clone(&schema), self.offset, self.limit);

        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, batches)))
    }
}

/// Gather rows into batches, applying `offset` and `limit` as they pass.
///
/// The limit is applied before batching, so the stream behind it is dropped as
/// soon as enough rows exist — which is what stops the walk.
fn batch_rows(
    rows: BoxStream<'static, Result<Row>>,
    schema: SchemaRef,
    offset: usize,
    limit: Option<usize>,
) -> BoxStream<'static, Result<RecordBatch>> {
    let paged = rows.skip(offset);
    let paged: BoxStream<'static, Result<Row>> = match limit {
        Some(limit) => paged.take(limit).boxed(),
        None => paged.boxed(),
    };

    async_stream::try_stream! {
        let mut pending: Vec<Row> = Vec::with_capacity(BATCH_ROWS);
        futures::pin_mut!(paged);
        while let Some(row) = paged.next().await {
            pending.push(row?);
            if pending.len() >= BATCH_ROWS {
                yield rows_batch(Arc::clone(&schema), &std::mem::take(&mut pending))?;
                pending.reserve(BATCH_ROWS);
            }
        }
        // The tail, and the empty case: a listing that matched nothing still has
        // to produce a schema-shaped result rather than no stream at all.
        if !pending.is_empty() {
            yield rows_batch(Arc::clone(&schema), &pending)?;
        }
    }
    .boxed()
}

/// Pack rows into one record batch.
pub(super) fn rows_batch(schema: SchemaRef, rows: &[Row]) -> Result<RecordBatch> {
    let file_names: StringArray = rows.iter().map(|r| Some(r.file_name.as_str())).collect();
    let formats: StringArray = rows.iter().map(|r| Some(r.file_format.as_str())).collect();
    let can_inspect = BooleanArray::from(rows.iter().map(|r| r.can_inspect).collect::<Vec<_>>());
    let can_partial_explore =
        BooleanArray::from(rows.iter().map(|r| r.can_partial_explore).collect::<Vec<_>>());
    let sizes = UInt64Array::from(rows.iter().map(|r| r.size).collect::<Vec<_>>());
    let last_modified: StringArray = rows.iter().map(|r| r.last_modified.clone()).collect();
    let is_directory = BooleanArray::from(rows.iter().map(|r| r.is_directory).collect::<Vec<_>>());

    Ok(RecordBatch::try_new(
        schema,
        vec![
            Arc::new(file_names),
            Arc::new(formats),
            Arc::new(can_inspect),
            Arc::new(can_partial_explore),
            Arc::new(sizes),
            Arc::new(last_modified),
            Arc::new(is_directory),
        ],
    )?)
}
