use std::time::Duration;

use arrow::{compute::concat_batches, datatypes::SchemaRef, record_batch::RecordBatch};
use datafusion::{
    error::DataFusionError, execution::SendableRecordBatchStream,
    physical_plan::stream::RecordBatchStreamAdapter,
};
use futures::StreamExt;

#[derive(Debug, Clone, Copy)]
struct SqlStreamCoalesceOptions {
    enabled: bool,
    target_rows: usize,
    flush_timeout_ms: u64,
    max_rows: usize,
}

impl SqlStreamCoalesceOptions {
    fn from_config() -> Self {
        let coalesce = &beacon_config::CONFIG.sql.stream_coalesce;

        Self {
            enabled: coalesce.enabled,
            target_rows: coalesce.target_rows,
            flush_timeout_ms: coalesce.flush_timeout_ms,
            max_rows: coalesce.max_rows,
        }
    }

    fn flush_timeout(self) -> Option<Duration> {
        if self.flush_timeout_ms == 0 {
            None
        } else {
            Some(Duration::from_millis(self.flush_timeout_ms))
        }
    }
}

pub(crate) fn coalesce_sql_stream(stream: SendableRecordBatchStream) -> SendableRecordBatchStream {
    coalesce_sql_stream_with_options(stream, SqlStreamCoalesceOptions::from_config())
}

fn coalesce_sql_stream_with_options(
    stream: SendableRecordBatchStream,
    options: SqlStreamCoalesceOptions,
) -> SendableRecordBatchStream {
    if !options.enabled || options.target_rows <= 1 {
        return stream;
    }

    let target_rows = options.target_rows.max(1);
    let max_rows = options.max_rows.max(target_rows);
    let flush_timeout = options.flush_timeout();

    let stream_schema = stream.schema();
    let concat_schema = stream_schema.clone();

    let output_stream = async_stream::try_stream! {
        let mut input_stream = stream;
        let mut buffered_batches: Vec<RecordBatch> = Vec::new();
        let mut buffered_rows = 0usize;
        let mut flush_deadline: Option<tokio::time::Instant> = None;

        loop {
            if buffered_rows >= target_rows || buffered_rows >= max_rows {
                let combined_batch = combine_buffered_batches(&concat_schema, &mut buffered_batches)?;
                buffered_rows = 0;
                flush_deadline = None;
                yield combined_batch;
                continue;
            }

            let next_batch = if buffered_batches.is_empty() || flush_timeout.is_none() {
                input_stream.next().await
            } else {
                let deadline = flush_deadline.expect("flush deadline must exist when buffer is non-empty");
                match tokio::time::timeout_at(deadline, input_stream.next()).await {
                    Ok(next_batch) => next_batch,
                    Err(_) => {
                        let combined_batch = combine_buffered_batches(&concat_schema, &mut buffered_batches)?;
                        buffered_rows = 0;
                        flush_deadline = None;
                        yield combined_batch;
                        continue;
                    }
                }
            };

            match next_batch {
                Some(Ok(batch)) => {
                    buffered_rows = buffered_rows.saturating_add(batch.num_rows());
                    buffered_batches.push(batch);

                    if buffered_batches.len() == 1 {
                        if let Some(timeout) = flush_timeout {
                            flush_deadline = Some(tokio::time::Instant::now() + timeout);
                        }
                    }
                }
                Some(Err(error)) => {
                    if !buffered_batches.is_empty() {
                        let combined_batch = combine_buffered_batches(&concat_schema, &mut buffered_batches)?;
                        buffered_rows = 0;
                        flush_deadline = None;
                        yield combined_batch;
                    }

                    Err(error)?;
                }
                None => {
                    if !buffered_batches.is_empty() {
                        let combined_batch = combine_buffered_batches(&concat_schema, &mut buffered_batches)?;
                        buffered_rows = 0;
                        flush_deadline = None;
                        yield combined_batch;
                    }

                    break;
                }
            }
        }
    };

    Box::pin(RecordBatchStreamAdapter::new(stream_schema, output_stream))
}

fn combine_buffered_batches(
    schema: &SchemaRef,
    buffered_batches: &mut Vec<RecordBatch>,
) -> Result<RecordBatch, DataFusionError> {
    match buffered_batches.len() {
        0 => Err(DataFusionError::Execution(
            "cannot combine an empty batch buffer".to_string(),
        )),
        1 => Ok(buffered_batches
            .pop()
            .expect("single buffered batch must exist")),
        _ => {
            let combined_batch =
                concat_batches(schema, buffered_batches.iter()).map_err(DataFusionError::from)?;
            buffered_batches.clear();
            Ok(combined_batch)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use arrow::{
        array::Int32Array,
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };
    use datafusion::{
        execution::SendableRecordBatchStream, physical_plan::stream::RecordBatchStreamAdapter,
    };
    use futures::{stream, TryStreamExt};

    use super::{coalesce_sql_stream_with_options, SqlStreamCoalesceOptions};

    fn test_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int32,
            false,
        )]))
    }

    fn make_batch(offset: i32, rows: usize) -> RecordBatch {
        let values: Vec<i32> = (offset..offset + rows as i32).collect();
        RecordBatch::try_new(test_schema(), vec![Arc::new(Int32Array::from(values))]).unwrap()
    }

    fn make_stream(batches: Vec<RecordBatch>) -> SendableRecordBatchStream {
        let schema = test_schema();
        let stream = stream::iter(batches.into_iter().map(Ok));
        Box::pin(RecordBatchStreamAdapter::new(schema, stream))
    }

    #[tokio::test]
    async fn coalesces_until_target_rows() {
        let input_stream = make_stream(vec![
            make_batch(0, 10_000),
            make_batch(10_000, 10_000),
            make_batch(20_000, 10_000),
            make_batch(30_000, 8_000),
        ]);

        let options = SqlStreamCoalesceOptions {
            enabled: true,
            target_rows: 30_000,
            flush_timeout_ms: 10_000,
            max_rows: 200_000,
        };

        let output_stream = coalesce_sql_stream_with_options(input_stream, options);
        let output_batches = output_stream.try_collect::<Vec<_>>().await.unwrap();

        assert_eq!(output_batches.len(), 2);
        assert_eq!(output_batches[0].num_rows(), 30_000);
        assert_eq!(output_batches[1].num_rows(), 8_000);
    }

    #[tokio::test]
    async fn flushes_buffer_when_timeout_expires() {
        let schema = test_schema();
        let source_stream = async_stream::stream! {
            yield Ok(make_batch(0, 5_000));
            tokio::time::sleep(Duration::from_millis(40)).await;
            yield Ok(make_batch(5_000, 5_000));
        };

        let input_stream = Box::pin(RecordBatchStreamAdapter::new(schema, source_stream));
        let options = SqlStreamCoalesceOptions {
            enabled: true,
            target_rows: 100_000,
            flush_timeout_ms: 20,
            max_rows: 200_000,
        };

        let output_stream = coalesce_sql_stream_with_options(input_stream, options);
        let output_batches = output_stream.try_collect::<Vec<_>>().await.unwrap();

        assert_eq!(output_batches.len(), 2);
        assert_eq!(output_batches[0].num_rows(), 5_000);
        assert_eq!(output_batches[1].num_rows(), 5_000);
    }

    #[tokio::test]
    async fn does_not_coalesce_when_disabled() {
        let input_stream = make_stream(vec![make_batch(0, 1_000), make_batch(1_000, 2_000)]);
        let options = SqlStreamCoalesceOptions {
            enabled: false,
            target_rows: 64_000,
            flush_timeout_ms: 25,
            max_rows: 256_000,
        };

        let output_stream = coalesce_sql_stream_with_options(input_stream, options);
        let output_batches = output_stream.try_collect::<Vec<_>>().await.unwrap();

        assert_eq!(output_batches.len(), 2);
        assert_eq!(output_batches[0].num_rows(), 1_000);
        assert_eq!(output_batches[1].num_rows(), 2_000);
    }
}
