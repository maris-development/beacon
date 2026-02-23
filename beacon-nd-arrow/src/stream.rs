use std::sync::Arc;
use std::task::Poll;

use arrow::array::ArrayRef;
use arrow::array::RecordBatch;
use futures::Stream;
use futures::StreamExt;
use futures::stream::BoxStream;

use crate::batch::NdRecordBatch;

pub struct NdBatchStreamAdapter {
    schema: Arc<arrow::datatypes::Schema>,
    arrays: Vec<BoxStream<'static, anyhow::Result<ArrayRef>>>,
    pending: Vec<Option<ArrayRef>>, // or your concrete array type
}

impl NdBatchStreamAdapter {
    pub fn new(batch: NdRecordBatch, chunk_size: usize) -> anyhow::Result<Self> {
        // Get the biggest dimensions across all arrays to determine broadcasting.
        let max_dims_opt = batch.arrays().iter().max_by_key(|a| a.dimensions().len());

        let (dims, shape) = match max_dims_opt {
            Some(array) => (array.dimensions(), array.shape()),
            None => return Err(anyhow::anyhow!("No arrays in batch")),
        };

        let mut broadcasted_arrays = Vec::new();
        // Check for each array if it needs to be broadcasted and if it can be broadcasted to the max dimensions
        for array in batch.arrays() {
            if array.dimensions() == dims {
                broadcasted_arrays.push(array.clone());
            } else if array.dimensions().is_empty() {
                // This is a scalar array, we can broadcast it to the max dimensions
                let broadcasted = array.broadcast(shape, dims)?;
                broadcasted_arrays.push(broadcasted);
            } else if array.dimensions().len() == 1 && dims.contains(&array.dimensions()[0]) {
                // This array can be broadcasted to the max dimensions
                let broadcasted = array.broadcast(shape, dims)?;
                broadcasted_arrays.push(broadcasted);
            } else {
                return Err(anyhow::anyhow!(
                    "Array with dimensions {:?} cannot be broadcasted to {:?}",
                    array.dimensions(),
                    dims
                ));
            }
        }

        let mut streams = vec![];
        for array in broadcasted_arrays {
            let stream = array.read_chunked(chunk_size)?;
            streams.push(stream);
        }

        Ok(Self {
            schema: batch.schema.clone(),
            arrays: streams,
            pending: vec![None; batch.arrays().len()],
        })
    }
}

impl Stream for NdBatchStreamAdapter {
    type Item = anyhow::Result<RecordBatch>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let this = self.get_mut();
        // poll each array stream to get the next batch of arrays
        // once we have a batch of arrays, construct a RecordBatch and return it
        let mut all_ready = true;

        for (i, stream) in this.arrays.iter_mut().enumerate() {
            // Skip if we already have a value buffered
            if this.pending[i].is_some() {
                continue;
            }

            match stream.poll_next_unpin(cx) {
                Poll::Ready(Some(Ok(array))) => {
                    this.pending[i] = Some(array);
                }
                Poll::Ready(Some(Err(e))) => return Poll::Ready(Some(Err(e))),
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Pending => {
                    all_ready = false;
                }
            }
        }

        if !all_ready {
            return Poll::Pending;
        }

        // We have one item from each stream
        let arrays = this
            .pending
            .iter_mut()
            .map(|opt| opt.take().unwrap())
            .collect::<Vec<_>>();

        let batch = RecordBatch::try_new(this.schema.clone(), arrays)
            .map_err(|e| anyhow::anyhow!(e.to_string()))?;

        Poll::Ready(Some(Ok(batch)))
    }
}

#[pin_project::pin_project]
pub struct NdSendableBatchStream<S: Stream<Item = NdRecordBatch>> {
    #[pin]
    pub stream: S,
    pub current_stream: Option<NdBatchStreamAdapter>,
    pub chunk_size: usize,
}

impl<S: Stream<Item = NdRecordBatch>> Stream for NdSendableBatchStream<S> {
    type Item = anyhow::Result<RecordBatch>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let this = self.project();

        if let Some(current_stream) = this.current_stream.as_mut() {
            match current_stream.poll_next_unpin(cx) {
                std::task::Poll::Ready(Some(batch)) => return std::task::Poll::Ready(Some(batch)),
                std::task::Poll::Ready(None) => {
                    // Current stream is exhausted, move to the next one
                    *this.current_stream = None;
                }
                std::task::Poll::Pending => return std::task::Poll::Pending,
            }
        }

        match this.stream.poll_next(cx) {
            std::task::Poll::Ready(Some(nd_batch)) => {
                // Create a new NdBatchStream for the next batch
                let new_stream = NdBatchStreamAdapter::new(nd_batch, *this.chunk_size)?;
                *this.current_stream = Some(new_stream);
                // Poll the new stream immediately
                this.current_stream.as_mut().unwrap().poll_next_unpin(cx)
            }
            std::task::Poll::Ready(None) => std::task::Poll::Ready(None), // No more batches
            std::task::Poll::Pending => std::task::Poll::Pending,
        }
    }
}

impl<S: Stream<Item = NdRecordBatch>> NdSendableBatchStream<S> {
    pub fn new(stream: S, chunk_size: usize) -> Self {
        Self {
            stream,
            current_stream: None,
            chunk_size,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Array, Int32Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use futures::{StreamExt, stream};

    use crate::array::{NdArrowArray, backend::ArrayBackend, backend::mem::InMemoryArrayBackend};

    use super::{NdBatchStreamAdapter, NdRecordBatch, NdSendableBatchStream};

    fn make_i32_array(
        values: Vec<i32>,
        shape: Vec<usize>,
        dimensions: Vec<&str>,
    ) -> NdArrowArray<Arc<dyn ArrayBackend>> {
        let array: Arc<dyn Array> = Arc::new(Int32Array::from(values));
        let dim_names: Vec<String> = dimensions.into_iter().map(|d| d.to_string()).collect();
        let backend: Arc<dyn ArrayBackend> =
            Arc::new(InMemoryArrayBackend::new(array, shape, dim_names));

        NdArrowArray::new(backend, DataType::Int32).unwrap()
    }

    fn i32_values(record_batch: &arrow::array::RecordBatch, column_idx: usize) -> Vec<i32> {
        let array = record_batch
            .column(column_idx)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        (0..array.len()).map(|idx| array.value(idx)).collect()
    }

    #[test]
    fn nd_batch_stream_adapter_new_errors_for_empty_batch() {
        let schema = Arc::new(Schema::new(Vec::<Field>::new()));
        let batch = NdRecordBatch::new(schema, vec![]).unwrap();

        let result = NdBatchStreamAdapter::new(batch, 2);
        match result {
            Ok(_) => panic!("expected error for empty batch"),
            Err(error) => assert!(error.to_string().contains("No arrays in batch")),
        }
    }

    #[tokio::test]
    async fn nd_batch_stream_adapter_broadcasts_scalar_and_chunks() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("v", DataType::Int32, false),
            Field::new("s", DataType::Int32, false),
        ]));
        let vector = make_i32_array(vec![1, 2, 3, 4], vec![4], vec!["x"]);
        let scalar = make_i32_array(vec![10], vec![], vec![]);
        let batch = NdRecordBatch::new(schema, vec![vector, scalar]).unwrap();

        let mut stream = NdBatchStreamAdapter::new(batch, 2).unwrap();

        let first = stream.next().await.unwrap().unwrap();
        assert_eq!(i32_values(&first, 0), vec![1, 2]);
        assert_eq!(i32_values(&first, 1), vec![10, 10]);

        let second = stream.next().await.unwrap().unwrap();
        assert_eq!(i32_values(&second, 0), vec![3, 4]);
        assert_eq!(i32_values(&second, 1), vec![10, 10]);

        assert!(stream.next().await.is_none());
    }

    #[tokio::test]
    async fn nd_sendable_batch_stream_yields_chunks_across_input_batches() {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));

        let batch1 = NdRecordBatch::new(
            schema.clone(),
            vec![make_i32_array(vec![1, 2, 3], vec![3], vec!["x"])],
        )
        .unwrap();
        let batch2 =
            NdRecordBatch::new(schema, vec![make_i32_array(vec![4, 5], vec![2], vec!["x"])])
                .unwrap();

        let source = stream::iter(vec![batch1, batch2]);
        let mut stream = NdSendableBatchStream::new(source, 2);

        let first = stream.next().await.unwrap().unwrap();
        assert_eq!(i32_values(&first, 0), vec![1, 2]);

        let second = stream.next().await.unwrap().unwrap();
        assert_eq!(i32_values(&second, 0), vec![3]);

        let third = stream.next().await.unwrap().unwrap();
        assert_eq!(i32_values(&third, 0), vec![4, 5]);

        assert!(stream.next().await.is_none());
    }

    #[tokio::test]
    async fn nd_batch_stream_adapter_broadcasts_three_1d_one_3d_and_scalar() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("time_1d", DataType::Int32, false),
            Field::new("lat_1d", DataType::Int32, false),
            Field::new("lon_1d", DataType::Int32, false),
            Field::new("v_3d", DataType::Int32, false),
            Field::new("s_scalar", DataType::Int32, false),
        ]));

        let time_1d = make_i32_array(vec![100, 200], vec![2], vec!["time"]);
        let lat_1d = make_i32_array(vec![10, 20], vec![2], vec!["lat"]);
        let lon_1d = make_i32_array(vec![1, 2, 3], vec![3], vec!["lon"]);
        let v_3d = make_i32_array(
            (0..12).collect::<Vec<_>>(),
            vec![2, 2, 3],
            vec!["time", "lat", "lon"],
        );
        let s_scalar = make_i32_array(vec![7], vec![], vec![]);

        let batch =
            NdRecordBatch::new(schema, vec![time_1d, lat_1d, lon_1d, v_3d, s_scalar]).unwrap();
        let mut stream = NdBatchStreamAdapter::new(batch, 5).unwrap();

        let mut batches = Vec::new();
        while let Some(batch) = stream.next().await {
            batches.push(batch.unwrap());
        }

        assert_eq!(batches.len(), 3);
        assert_eq!(batches[0].num_rows(), 5);
        assert_eq!(batches[1].num_rows(), 5);
        assert_eq!(batches[2].num_rows(), 2);

        let mut col0 = Vec::new();
        let mut col1 = Vec::new();
        let mut col2 = Vec::new();
        let mut col3 = Vec::new();
        let mut col4 = Vec::new();
        for batch in &batches {
            col0.extend(i32_values(batch, 0));
            col1.extend(i32_values(batch, 1));
            col2.extend(i32_values(batch, 2));
            col3.extend(i32_values(batch, 3));
            col4.extend(i32_values(batch, 4));
        }

        assert_eq!(
            col0,
            vec![100, 100, 100, 100, 100, 100, 200, 200, 200, 200, 200, 200]
        );
        assert_eq!(col1, vec![10, 10, 10, 20, 20, 20, 10, 10, 10, 20, 20, 20]);
        assert_eq!(col2, vec![1, 2, 3, 1, 2, 3, 1, 2, 3, 1, 2, 3]);
        assert_eq!(col3, (0..12).collect::<Vec<_>>());
        assert_eq!(col4, vec![7; 12]);
    }
}
