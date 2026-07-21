use arrow::{compute::concat_batches, datatypes::SchemaRef, record_batch::RecordBatch};
use datafusion::{
    error::DataFusionError, execution::SendableRecordBatchStream,
    physical_plan::stream::RecordBatchStreamAdapter, prelude::SessionContext,
};
use futures::StreamExt;

use crate::settings::SqlStreamCoalesceSettings;

/// Merges the small record batches a physical plan emits into batches of at
/// least [`SqlStreamCoalesceSettings::target_rows`] rows before they reach a
/// client.
///
/// Published on the session config as an extension by the runtime builder, so
/// execution-time code recovers it with [`CoalesceSqlStream::from_session`]
/// rather than threading a `Runtime` handle through.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct CoalesceSqlStream {
    settings: SqlStreamCoalesceSettings,
}

impl CoalesceSqlStream {
    pub(crate) fn new(settings: SqlStreamCoalesceSettings) -> Self {
        Self { settings }
    }

    /// Recovers the coalescer published on `session_ctx`, falling back to the
    /// defaults if the extension is absent (a session beacon did not build).
    pub(crate) fn from_session(session_ctx: &SessionContext) -> Self {
        session_ctx
            .state()
            .config()
            .get_extension::<CoalesceSqlStream>()
            .map(|coalescer| *coalescer)
            .unwrap_or_default()
    }

    /// Wraps `stream` in the coalescing adapter. Returns `stream` unchanged when
    /// coalescing is disabled or would be a no-op.
    pub(crate) fn coalesce(&self, stream: SendableRecordBatchStream) -> SendableRecordBatchStream {
        if !self.settings.enabled || self.settings.target_rows <= 1 {
            return stream;
        }

        let target_rows = self.settings.target_rows.max(1);
        let max_rows = self.settings.max_rows.max(target_rows);
        let flush_timeout = self.settings.flush_timeout();

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
        execution::SendableRecordBatchStream,
        physical_plan::stream::RecordBatchStreamAdapter,
        prelude::{SessionConfig, SessionContext},
    };
    use futures::{stream, TryStreamExt};

    use super::CoalesceSqlStream;
    use crate::settings::SqlStreamCoalesceSettings;

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

        let coalescer = CoalesceSqlStream::new(SqlStreamCoalesceSettings {
            enabled: true,
            target_rows: 30_000,
            flush_timeout_ms: 10_000,
            max_rows: 200_000,
        });

        let output_batches = coalescer
            .coalesce(input_stream)
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

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
        let coalescer = CoalesceSqlStream::new(SqlStreamCoalesceSettings {
            enabled: true,
            target_rows: 100_000,
            flush_timeout_ms: 20,
            max_rows: 200_000,
        });

        let output_batches = coalescer
            .coalesce(input_stream)
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        assert_eq!(output_batches.len(), 2);
        assert_eq!(output_batches[0].num_rows(), 5_000);
        assert_eq!(output_batches[1].num_rows(), 5_000);
    }

    #[tokio::test]
    async fn does_not_coalesce_when_disabled() {
        let input_stream = make_stream(vec![make_batch(0, 1_000), make_batch(1_000, 2_000)]);
        let coalescer = CoalesceSqlStream::new(SqlStreamCoalesceSettings {
            enabled: false,
            ..Default::default()
        });

        let output_batches = coalescer
            .coalesce(input_stream)
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        assert_eq!(output_batches.len(), 2);
        assert_eq!(output_batches[0].num_rows(), 1_000);
        assert_eq!(output_batches[1].num_rows(), 2_000);
    }

    /// The coalescer is recovered from the session extension the runtime builder
    /// publishes, and falls back to the defaults on a session without one.
    #[tokio::test]
    async fn reads_settings_from_the_session_extension() {
        let settings = SqlStreamCoalesceSettings {
            enabled: true,
            target_rows: 30_000,
            flush_timeout_ms: 10_000,
            max_rows: 200_000,
        };
        let config = SessionConfig::new().with_extension(Arc::new(CoalesceSqlStream::new(settings)));
        let session_ctx = SessionContext::new_with_config(config);

        let output_batches = CoalesceSqlStream::from_session(&session_ctx)
            .coalesce(make_stream(vec![
                make_batch(0, 10_000),
                make_batch(10_000, 25_000),
            ]))
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        assert_eq!(output_batches.len(), 1);
        assert_eq!(output_batches[0].num_rows(), 35_000);

        assert_eq!(
            CoalesceSqlStream::from_session(&SessionContext::new()).settings,
            SqlStreamCoalesceSettings::default(),
            "a session without the extension should fall back to the defaults"
        );
    }
}
