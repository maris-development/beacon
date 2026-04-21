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

fn execution_error(message: impl Into<String>) -> DataFusionError {
    DataFusionError::Execution(message.into())
}

#[derive(Debug)]
pub struct DeletionVectorExec {
    data: Arc<dyn ExecutionPlan>,
    deletion_vectors: Vec<Arc<dyn ExecutionPlan>>,
}

impl DeletionVectorExec {
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

struct DeletionVectorState {
    data_stream: SendableRecordBatchStream,
    deletion_vector_cursors: Vec<DeletionVectorCursor>,
}

struct DeletionVectorCursor {
    stream: SendableRecordBatchStream,
    current_batch: Option<BooleanArray>,
    current_index: usize,
}

impl DeletionVectorCursor {
    fn new(stream: SendableRecordBatchStream) -> Self {
        Self {
            stream,
            current_batch: None,
            current_index: 0,
        }
    }

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

    fn has_remaining_values(&self) -> bool {
        self.current_batch
            .as_ref()
            .map(|batch| self.current_index < batch.len())
            .unwrap_or(false)
    }

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
