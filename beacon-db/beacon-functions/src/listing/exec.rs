//! [`DatasetsExec`]: the plan node that streams listing rows as record batches.
//!
//! A batch leaves once [`BATCH_ROWS`] rows exist, so memory is bounded by one
//! batch. `limit` is applied to the row stream, so reaching it drops the
//! stream and stops the walk behind it.

use std::any::Any;
use std::fmt;
use std::sync::Arc;

use arrow::{
    array::{BooleanArray, StringArray, UInt64Array},
    datatypes::SchemaRef,
    record_batch::RecordBatch,
};
use beacon_datafusion_ext::format_ext::DatasetMetadata;
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

/// Rows gathered before a batch is emitted.
pub const BATCH_ROWS: usize = 8192;

/// Builds the listing stream. Called once per execution, so the plan can run
/// more than once.
pub type RowStreamFactory =
    Arc<dyn Fn() -> Result<BoxStream<'static, Result<DatasetMetadata>>> + Send + Sync>;

/// Streams listing rows as record batches.
#[derive(Clone)]
pub struct DatasetsExec {
    schema: SchemaRef,
    rows: RowStreamFactory,
    offset: usize,
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
            // One partition: the store wrapper parallelises the walk itself.
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

/// Gather rows into batches, applying `offset` and `limit` to the rows first.
pub(super) fn batch_rows(
    rows: BoxStream<'static, Result<DatasetMetadata>>,
    schema: SchemaRef,
    offset: usize,
    limit: Option<usize>,
) -> BoxStream<'static, Result<RecordBatch>> {
    let paged = rows.skip(offset);
    let paged: BoxStream<'static, Result<DatasetMetadata>> = match limit {
        Some(limit) => paged.take(limit).boxed(),
        None => paged.boxed(),
    };

    paged
        .chunks(BATCH_ROWS)
        .map(move |chunk| {
            let rows: Vec<DatasetMetadata> = chunk.into_iter().collect::<Result<_>>()?;
            rows_batch(Arc::clone(&schema), &rows)
        })
        .boxed()
}

/// Pack rows into one record batch, in the order of `list_datasets_schema`.
fn rows_batch(schema: SchemaRef, rows: &[DatasetMetadata]) -> Result<RecordBatch> {
    let file_names: StringArray = rows.iter().map(|r| Some(r.file_path.as_str())).collect();
    let formats: StringArray = rows.iter().map(|r| Some(r.format.as_str())).collect();
    let can_inspect = BooleanArray::from(rows.iter().map(|r| r.can_inspect).collect::<Vec<_>>());
    let can_partial_explore =
        BooleanArray::from(rows.iter().map(|r| r.can_partial_explore).collect::<Vec<_>>());
    let sizes = UInt64Array::from(rows.iter().map(|r| r.size).collect::<Vec<_>>());
    let last_modified: StringArray = rows
        .iter()
        .map(|r| r.last_modified.map(|ts| ts.to_rfc3339()))
        .collect();

    Ok(RecordBatch::try_new(
        schema,
        vec![
            Arc::new(file_names),
            Arc::new(formats),
            Arc::new(can_inspect),
            Arc::new(can_partial_explore),
            Arc::new(sizes),
            Arc::new(last_modified),
        ],
    )?)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use beacon_datafusion_ext::format_ext::DatasetMetadata;
    use datafusion::error::Result;
    use futures::stream::{BoxStream, StreamExt, TryStreamExt};

    use super::{BATCH_ROWS, batch_rows};
    use crate::listing::provider::list_datasets_schema;

    fn row(i: usize) -> DatasetMetadata {
        DatasetMetadata::new(format!("f{i}.csv"), "csv".to_string())
    }

    fn rows(n: usize) -> BoxStream<'static, Result<DatasetMetadata>> {
        futures::stream::iter((0..n).map(|i| Ok(row(i)))).boxed()
    }

    async fn batched(
        rows: BoxStream<'static, Result<DatasetMetadata>>,
        offset: usize,
        limit: Option<usize>,
    ) -> Vec<usize> {
        batch_rows(rows, list_datasets_schema(), offset, limit)
            .map_ok(|batch| batch.num_rows())
            .try_collect()
            .await
            .expect("batching succeeds")
    }

    #[tokio::test]
    async fn batches_fill_to_the_cap_and_the_tail_follows() {
        let sizes = batched(rows(2 * BATCH_ROWS + 5), 0, None).await;
        assert_eq!(sizes, vec![BATCH_ROWS, BATCH_ROWS, 5]);
    }

    #[tokio::test]
    async fn offset_and_limit_apply_to_the_rows() {
        let batches: Vec<_> = batch_rows(rows(10), list_datasets_schema(), 3, Some(4))
            .try_collect()
            .await
            .expect("batching succeeds");
        assert_eq!(batches.len(), 1);
        let names = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .expect("file_name is Utf8");
        let names: Vec<&str> = names.iter().map(|n| n.unwrap()).collect();
        assert_eq!(names, vec!["f3.csv", "f4.csv", "f5.csv", "f6.csv"]);
    }

    #[tokio::test]
    async fn an_empty_listing_yields_no_batch() {
        assert!(batched(rows(0), 0, None).await.is_empty());
    }

    /// An endless listing ends after `limit` rows and is polled little beyond it.
    #[tokio::test]
    async fn the_limit_stops_the_walk() {
        let polled = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&polled);
        let endless = futures::stream::repeat_with(move || {
            counter.fetch_add(1, Ordering::SeqCst);
            Ok(row(0))
        })
        .boxed();

        let sizes = batched(endless, 0, Some(5)).await;
        assert_eq!(sizes, vec![5]);
        assert!(
            polled.load(Ordering::SeqCst) <= 6,
            "the walk was polled {} times for 5 rows",
            polled.load(Ordering::SeqCst)
        );
    }
}
