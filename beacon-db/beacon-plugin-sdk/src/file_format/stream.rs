//! Record batch streams that cross the plugin boundary.

use std::panic::{AssertUnwindSafe, catch_unwind};
use std::pin::Pin;
use std::task::{Context, Poll};

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::RecordBatchStream;
use datafusion_ffi::record_batch_stream::FFI_RecordBatchStream;
use futures::{Stream, StreamExt};

use crate::panic_safe::panic_message;

/// Catches panics of the inner stream in `poll_next` and in drop.
///
/// `datafusion-ffi` 54.1 does not catch panics in its stream wrappers.
struct PanicSafeStream {
    schema: SchemaRef,
    inner: Option<SendableRecordBatchStream>,
}

impl PanicSafeStream {
    fn drop_inner(&mut self) {
        if let Some(inner) = self.inner.take() {
            let _ = catch_unwind(AssertUnwindSafe(move || drop(inner)));
        }
    }
}

impl Stream for PanicSafeStream {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let Some(inner) = self.inner.as_mut() else {
            return Poll::Ready(None);
        };
        match catch_unwind(AssertUnwindSafe(|| inner.poll_next_unpin(cx))) {
            Ok(Poll::Ready(None)) => {
                self.drop_inner();
                Poll::Ready(None)
            }
            Ok(poll) => poll,
            Err(payload) => {
                self.drop_inner();
                Poll::Ready(Some(Err(DataFusionError::Execution(format!(
                    "panic in plugin stream: {}",
                    panic_message(payload.as_ref())
                )))))
            }
        }
    }
}

impl RecordBatchStream for PanicSafeStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl Drop for PanicSafeStream {
    fn drop(&mut self) {
        self.drop_inner();
    }
}

/// Exports `stream`. A panic in its poll becomes one error item, and the stream then ends.
/// A panic in its drop is caught.
pub fn export_batch_stream(stream: SendableRecordBatchStream) -> FFI_RecordBatchStream {
    let schema = stream.schema();
    let safe = PanicSafeStream {
        schema,
        inner: Some(stream),
    };
    FFI_RecordBatchStream::new(Box::pin(safe), None)
}

/// Imports a stream from [`export_batch_stream`].
pub fn import_batch_stream(stream: FFI_RecordBatchStream) -> SendableRecordBatchStream {
    Box::pin(stream)
}
