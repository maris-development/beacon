//! Deletion vector execution for Beacon tables.
//!
//! Provides [`DeletionVectorExec`], a DataFusion [`ExecutionPlan`] that filters
//! out logically deleted rows by applying one or more boolean deletion vector
//! streams to the data stream. A row is kept only if **none** of the deletion
//! vectors mark it as deleted.
//!
//! **Note:** This module is not yet wired into the scan path; it exists as
//! infrastructure for future delete support.

use std::{any::Any, sync::Arc};

use arrow::{
    array::{Array, BooleanArray},
    compute::filter_record_batch,
    record_batch::RecordBatch,
};
use datafusion::{
    common::Statistics,
    error::{DataFusionError, Result as DataFusionResult},
    execution::{SendableRecordBatchStream, TaskContext},
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties,
        stream::RecordBatchStreamAdapter,
    },
};
use futures::{StreamExt, stream};

/// Creates a [`DataFusionError::Execution`](DataFusionError::Execution) from a string.
fn execution_error(message: impl Into<String>) -> DataFusionError {
    DataFusionError::Execution(message.into())
}

/// An execution plan that applies deletion vectors to filter out deleted rows.
///
/// Each deletion vector is a stream of boolean batches aligned 1:1 with the
/// data stream's rows. A `true` value means the row is deleted and should be
/// excluded from the output.
#[derive(Debug)]
pub struct DeletionVectorExec {
    /// The data scan plan whose rows will be filtered.
    data: Arc<dyn ExecutionPlan>,
    /// One or more deletion vector scan plans, each aligned with `data`.
    deletion_vectors: Vec<Arc<dyn ExecutionPlan>>,
}

impl DeletionVectorExec {
    /// Creates a new `DeletionVectorExec`.
    ///
    /// # Errors
    ///
    /// Returns an error if any deletion vector plan has a different partition
    /// count than the data plan.
    pub fn try_new(
        data: Arc<dyn ExecutionPlan>,
        deletion_vectors: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Self> {
        let expected_partitions = data.output_partitioning().partition_count();
        for deletion_vector in &deletion_vectors {
            let deletion_vector_partitions =
                deletion_vector.output_partitioning().partition_count();
            if deletion_vector_partitions != expected_partitions {
                return Err(execution_error(format!(
                    "deletion vector partition count mismatch: expected {expected_partitions}, found {deletion_vector_partitions}"
                )));
            }
        }

        Ok(Self {
            data,
            deletion_vectors,
        })
    }
}

impl DisplayAs for DeletionVectorExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "DeletionVectorExec")
    }
}

impl ExecutionPlan for DeletionVectorExec {
    fn name(&self) -> &str {
        "DeletionVectorExec"
    }

    fn properties(&self) -> &datafusion::physical_plan::PlanProperties {
        self.data.properties()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        let mut children = Vec::with_capacity(1 + self.deletion_vectors.len());
        children.push(&self.data);
        children.extend(self.deletion_vectors.iter());
        children
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false; 1 + self.deletion_vectors.len()]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true; 1 + self.deletion_vectors.len()]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let (data, deletion_vectors) = children
            .split_first()
            .ok_or_else(|| execution_error("DeletionVectorExec expects at least one child"))?;

        Ok(Arc::new(Self::try_new(
            data.clone(),
            deletion_vectors.to_vec(),
        )?))
    }

    fn repartitioned(
        &self,
        _target_partitions: usize,
        _config: &datafusion::config::ConfigOptions,
    ) -> DataFusionResult<Option<Arc<dyn ExecutionPlan>>> {
        Ok(None)
    }

    fn statistics(&self) -> DataFusionResult<Statistics> {
        Ok(Statistics::new_unknown(&self.schema()))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        let schema = self.schema();
        let mut deletion_vector_cursors = Vec::with_capacity(self.deletion_vectors.len());
        for deletion_vector in &self.deletion_vectors {
            deletion_vector_cursors.push(DeletionVectorCursor::new(
                deletion_vector.execute(partition, context.clone())?,
            ));
        }

        let state = DeletionVectorState {
            data_stream: self.data.execute(partition, context)?,
            deletion_vector_cursors,
        };

        let stream = stream::try_unfold(state, |mut state| async move {
            match state.data_stream.next().await {
                Some(Ok(batch)) => {
                    let filtered_batch =
                        filter_batch(batch, &mut state.deletion_vector_cursors).await?;
                    Ok(Some((filtered_batch, state)))
                }
                Some(Err(error)) => Err(error),
                None => {
                    for deletion_vector_cursor in &mut state.deletion_vector_cursors {
                        deletion_vector_cursor.ensure_exhausted().await?;
                    }

                    Ok(None)
                }
            }
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

/// Internal state for the deletion-vector filtering stream.
struct DeletionVectorState {
    /// The underlying data stream being filtered.
    data_stream: SendableRecordBatchStream,
    /// Cursors into each deletion vector stream.
    deletion_vector_cursors: Vec<DeletionVectorCursor>,
}

/// A cursor that reads boolean deletion masks from a stream, one batch at a
/// time, and provides `next_mask(len)` to consume exactly `len` boolean values.
struct DeletionVectorCursor {
    /// The underlying deletion vector stream.
    stream: SendableRecordBatchStream,
    /// The current batch of boolean values being consumed.
    current_batch: Option<BooleanArray>,
    /// The read position within `current_batch`.
    current_index: usize,
}

impl DeletionVectorCursor {
    /// Creates a new cursor over the given deletion vector stream.
    fn new(stream: SendableRecordBatchStream) -> Self {
        Self {
            stream,
            current_batch: None,
            current_index: 0,
        }
    }

    /// Consumes exactly `len` boolean values from the stream, returning them
    /// as a `Vec<bool>`. Advances to the next batch as needed.
    async fn next_mask(&mut self, len: usize) -> DataFusionResult<Vec<bool>> {
        let mut values = Vec::with_capacity(len);

        while values.len() < len {
            if !self.has_remaining_values() {
                let next_batch = self.stream.next().await.transpose()?.ok_or_else(|| {
                    execution_error("deletion vector ended before data stream finished")
                })?;

                self.set_current_batch(next_batch)?;
                continue;
            }

            let current_batch = self
                .current_batch
                .as_ref()
                .expect("current batch should exist when values remain");
            let available_values = current_batch.len() - self.current_index;
            let values_to_take = (len - values.len()).min(available_values);

            for index in self.current_index..self.current_index + values_to_take {
                if current_batch.is_null(index) {
                    return Err(execution_error(
                        "deletion vector batches must not contain null values",
                    ));
                }

                values.push(current_batch.value(index));
            }

            self.current_index += values_to_take;
            if self.current_index == current_batch.len() {
                self.current_batch = None;
                self.current_index = 0;
            }
        }

        Ok(values)
    }

    /// Verifies that the deletion vector stream has no remaining rows.
    ///
    /// Returns an error if there are unconsumed values, indicating a
    /// row-count mismatch between data and deletion vector.
    async fn ensure_exhausted(&mut self) -> DataFusionResult<()> {
        if self.has_remaining_values() {
            return Err(execution_error(
                "deletion vector has more rows than the data stream",
            ));
        }

        while let Some(batch) = self.stream.next().await.transpose()? {
            if batch.num_rows() > 0 {
                return Err(execution_error(
                    "deletion vector has more rows than the data stream",
                ));
            }
        }

        Ok(())
    }

    /// Returns `true` if the current batch has unconsumed values.
    fn has_remaining_values(&self) -> bool {
        self.current_batch
            .as_ref()
            .map(|batch| self.current_index < batch.len())
            .unwrap_or(false)
    }

    /// Sets the next batch to consume, validating it contains exactly one
    /// boolean column.
    fn set_current_batch(&mut self, batch: RecordBatch) -> DataFusionResult<()> {
        if batch.num_columns() != 1 {
            return Err(execution_error(format!(
                "deletion vector batches must contain exactly one column, found {}",
                batch.num_columns()
            )));
        }

        let values = batch
            .column(0)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| {
                execution_error("deletion vector batches must contain a boolean column")
            })?
            .clone();

        self.current_batch = Some(values);
        self.current_index = 0;
        Ok(())
    }
}

/// Applies all deletion vectors to a single data batch, keeping only rows
/// where no deletion vector marks the row as deleted.
async fn filter_batch(
    batch: RecordBatch,
    deletion_vector_cursors: &mut [DeletionVectorCursor],
) -> DataFusionResult<RecordBatch> {
    if deletion_vector_cursors.is_empty() || batch.num_rows() == 0 {
        return Ok(batch);
    }

    let mut deleted = vec![false; batch.num_rows()];
    for deletion_vector_cursor in deletion_vector_cursors {
        let next_mask = deletion_vector_cursor.next_mask(batch.num_rows()).await?;
        eprintln!("deletion mask: {:?}", next_mask);
        for (deleted_row, row_is_deleted) in deleted.iter_mut().zip(next_mask.into_iter()) {
            *deleted_row |= row_is_deleted;
        }
    }

    let keep_mask = BooleanArray::from(
        deleted
            .into_iter()
            .map(|row_is_deleted| !row_is_deleted)
            .collect::<Vec<_>>(),
    );
    filter_record_batch(&batch, &keep_mask).map_err(DataFusionError::from)
}
