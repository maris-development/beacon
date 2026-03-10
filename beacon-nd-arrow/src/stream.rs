use arrow::record_batch::RecordBatch;
use futures::{
    Stream, StreamExt,
    stream::{self, BoxStream},
};

use crate::batch::NdRecordBatch;

/// Configuration for piping a stream of [`NdRecordBatch`] into Arrow [`RecordBatch`] output.
#[derive(Debug, Clone, Copy)]
pub struct NdToArrowPipeOptions {
    /// Preferred number of logical elements per chunk when splitting each input batch.
    pub preferred_chunk_size: usize,
    /// Number of input batches to process concurrently.
    pub batch_parallelism: usize,
    /// Number of inner chunk streams to poll concurrently while flattening output.
    pub output_buffer: usize,
}

impl Default for NdToArrowPipeOptions {
    fn default() -> Self {
        Self {
            preferred_chunk_size: 64 * 1024,
            batch_parallelism: 4,
            output_buffer: 4,
        }
    }
}

/// Convert and flatten an async stream of [`NdRecordBatch`] values into a single async stream of
/// Arrow [`RecordBatch`] values.
///
/// - Each input batch is converted using [`NdRecordBatch::try_as_arrow_stream`].
/// - Multiple input batches are converted in parallel (`batch_parallelism`).
/// - Produced inner chunk streams are merged with bounded buffering (`output_buffer`).
pub fn pipe_nd_record_batch_stream<S>(
    input: S,
    options: NdToArrowPipeOptions,
) -> BoxStream<'static, anyhow::Result<RecordBatch>>
where
    S: Stream<Item = anyhow::Result<NdRecordBatch>> + Send + 'static,
{
    let preferred_chunk_size = options.preferred_chunk_size;
    let batch_parallelism = options.batch_parallelism.max(1);
    let output_buffer = options.output_buffer.max(1);

    input
        .map(move |batch_result| async move {
            match batch_result {
                Ok(batch) => batch.try_as_arrow_stream(preferred_chunk_size).await,
                Err(error) => Err(error),
            }
        })
        .buffer_unordered(batch_parallelism)
        .map(|stream_result| match stream_result {
            Ok(stream) => stream,
            Err(error) => stream::once(async move { Err(error) }).boxed(),
        })
        .flatten_unordered(output_buffer)
        .boxed()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{ArrayRef, Int32Array};
    use arrow_schema::{DataType, Field, Schema};
    use futures::TryStreamExt;

    use crate::{
        NdRecordBatch,
        array::{NdArrowArray, NdArrowArrayDispatch, backend::mem::InMemoryArrayBackend},
    };

    use super::{NdToArrowPipeOptions, pipe_nd_record_batch_stream};

    fn int32_values(column: &ArrayRef) -> Vec<i32> {
        column
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("expected Int32Array")
            .values()
            .to_vec()
    }

    fn make_batch(values: Vec<i32>, shape: Vec<usize>, dims: Vec<&str>) -> NdRecordBatch {
        let array = Arc::new(
            NdArrowArrayDispatch::new(InMemoryArrayBackend::new(
                ndarray::ArrayD::from_shape_vec(shape.clone(), values).unwrap(),
                shape.clone(),
                dims.iter().map(|d| d.to_string()).collect(),
                None,
            ))
            .unwrap(),
        ) as Arc<dyn NdArrowArray>;

        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
        NdRecordBatch::new(schema, vec![array]).unwrap()
    }

    #[tokio::test]
    async fn pipe_nd_record_batch_stream_flattens_batches() {
        let nd_1 = make_batch(vec![1, 2, 3, 4], vec![2, 2], vec!["time", "lat"]);
        let nd_2 = make_batch(vec![10, 20, 30, 40], vec![2, 2], vec!["time", "lat"]);

        let stream = futures::stream::iter(vec![Ok(nd_1), Ok(nd_2)]);
        let out = pipe_nd_record_batch_stream(
            stream,
            NdToArrowPipeOptions {
                preferred_chunk_size: 2,
                batch_parallelism: 2,
                output_buffer: 2,
            },
        )
        .try_collect::<Vec<_>>()
        .await
        .unwrap();

        assert_eq!(out.len(), 4);
        let mut all_values: Vec<i32> = out
            .iter()
            .flat_map(|batch| int32_values(batch.column(0)))
            .collect();
        all_values.sort_unstable();
        assert_eq!(all_values, vec![1, 2, 3, 4, 10, 20, 30, 40]);
    }

    #[tokio::test]
    async fn pipe_nd_record_batch_stream_forwards_input_errors() {
        let error = anyhow::anyhow!("input stream failed");
        let stream = futures::stream::iter(vec![Err(error)]);

        let result = pipe_nd_record_batch_stream(stream, NdToArrowPipeOptions::default())
            .try_collect::<Vec<_>>()
            .await;

        assert!(result.is_err());
        assert_eq!(result.err().unwrap().to_string(), "input stream failed");
    }
}
