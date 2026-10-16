//! DataFusion [`FileOpener`] that streams the morsels of a partition.

use std::sync::Arc;

use arrow::{
    datatypes::SchemaRef,
    record_batch::{RecordBatch, RecordBatchOptions},
};
use beacon_datafusion_ext::{
    morsel_planner::{MorselPlannerQueue, PartitionMorsels},
    nd::{encode_nd_record_batch, NdRecordBatch},
};
use datafusion::{
    datasource::{
        listing::PartitionedFile,
        physical_plan::{FileOpenFuture, FileOpener},
    },
    error::{DataFusionError, Result},
};
use futures::{FutureExt, StreamExt};

use super::{planner::OxcdfMorselPlanner, source::split_by_size};

/// Streams the morsels of one partition as nd-encoded batches.
pub struct OxcdfOpener {
    projected_schema: SchemaRef, // nd-encoded, as the scan declares it
    planner: Arc<OxcdfMorselPlanner>,
    queue: Option<MorselPlannerQueue>, // `Some` in a morsel scan, where the file is a placeholder
    partition: usize,
    local_file_max_size: u64,
}

impl OxcdfOpener {
    /// Create the opener of one partition.
    ///
    /// # Arguments
    ///
    /// * `projected_schema` - The nd-encoded schema that the scan projects
    /// * `planner` - The planner that cuts a file into morsels
    /// * `queue` - The queue of a morsel scan, or `None` when each file is opened on its own
    /// * `partition` - The partition of this opener
    /// * `local_file_max_size` - Files below this size keep their morsels local
    pub fn new(
        projected_schema: SchemaRef,
        planner: Arc<OxcdfMorselPlanner>,
        queue: Option<MorselPlannerQueue>,
        partition: usize,
        local_file_max_size: u64,
    ) -> Self {
        Self {
            projected_schema,
            planner,
            queue,
            partition,
            local_file_max_size,
        }
    }

    /// The handle of this partition: on the scan queue, or on a queue of `file` alone.
    fn handle(&self, file: PartitionedFile) -> PartitionMorsels {
        match &self.queue {
            Some(queue) => queue.partition(self.partition),
            None => {
                let (local, shared) = split_by_size([file], self.local_file_max_size);
                MorselPlannerQueue::new(local, shared, Arc::clone(&self.planner) as _, 1)
                    .partition(0)
            }
        }
    }
}

impl FileOpener for OxcdfOpener {
    fn open(&self, file: PartitionedFile) -> Result<FileOpenFuture> {
        let schema = Arc::clone(&self.projected_schema);
        let stream = futures::stream::unfold(self.handle(file), move |mut handle| {
            let schema = Arc::clone(&schema);
            async move {
                let batch = match handle.next_morsel().await? {
                    Ok(morsel) => morsel
                        .produce()
                        .await
                        .and_then(|nd| to_record_batch(&nd, schema)),
                    Err(error) => Err(error),
                };
                Some((batch, handle))
            }
        });
        Ok(futures::future::ready(Ok(stream.boxed())).boxed())
    }
}

/// Turn a morsel's batch into what `NdSourceExec` above the scan reads.
///
/// Columns travel nd-encoded. A scan without columns (`COUNT(*)`) gets a batch
/// with only a row count.
fn to_record_batch(nd: &NdRecordBatch, schema: SchemaRef) -> Result<RecordBatch> {
    let arrow_error = |e| DataFusionError::ArrowError(Box::new(e), None);
    if schema.fields().is_empty() {
        let options = RecordBatchOptions::new().with_row_count(Some(nd.num_rows()));
        return RecordBatch::try_new_with_options(schema, vec![], &options).map_err(arrow_error);
    }
    encode_nd_record_batch(nd)?
        .with_schema(schema)
        .map_err(arrow_error)
}

#[cfg(test)]
mod tests {
    use arrow::{
        array::{ArrayRef, Float64Array},
        datatypes::{DataType, Field, Schema},
    };
    use beacon_datafusion_ext::nd::{encoded_schema, Dimension, Dimensions, NdArrowArray};

    use super::*;

    fn nd_batch() -> NdRecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("t", DataType::Float64, true)]));
        let dims = Dimensions::try_new(vec![Dimension::new("x", 3)]).unwrap();
        let values: ArrayRef = Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0]));
        let column = NdArrowArray::try_new(values, dims.clone()).unwrap();
        NdRecordBatch::try_new(schema, vec![column], dims).unwrap()
    }

    #[test]
    fn columns_travel_nd_encoded_on_the_scan_schema() {
        let logical = Schema::new(vec![Field::new("t", DataType::Float64, true)]);
        let schema = Arc::new(encoded_schema(&logical));

        let batch = to_record_batch(&nd_batch(), Arc::clone(&schema)).unwrap();

        assert_eq!(batch.schema(), schema);
        assert_eq!(batch.num_rows(), 1);
    }

    #[test]
    fn count_star_gets_only_the_row_count() {
        let schema = Arc::new(Schema::empty());

        let batch = to_record_batch(&nd_batch(), schema).unwrap();

        assert_eq!(batch.num_columns(), 0);
        assert_eq!(batch.num_rows(), 3);
    }
}
