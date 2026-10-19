//! A [`FileOpener`] wrapper that skips errors, so one bad file does not fail a scan.

use std::sync::Arc;

use datafusion::{
    datasource::{
        listing::PartitionedFile,
        physical_plan::{FileOpenFuture, FileOpener},
    },
    error::Result,
};
use futures::{FutureExt, StreamExt};

/// A [`FileOpener`] that logs each error of `inner` and drops it, so the scan goes on.
///
/// A file that fails to open gives no rows. A batch that fails is dropped, and the
/// stream goes on with the next batch.
pub struct SkipErrorAdapter {
    inner: Arc<dyn FileOpener>,
}

impl SkipErrorAdapter {
    /// Wrap `inner`, as a `FileOpener` to hand on.
    ///
    /// # Arguments
    ///
    /// * `inner` - The opener whose errors to skip
    pub fn wrap(inner: Arc<dyn FileOpener>) -> Arc<dyn FileOpener> {
        Arc::new(Self { inner })
    }
}

impl FileOpener for SkipErrorAdapter {
    /// Open `partitioned_file` with `inner`. An error gives an empty stream.
    fn open(&self, partitioned_file: PartitionedFile) -> Result<FileOpenFuture> {
        open_skip_errors(self.inner.as_ref(), partitioned_file)
    }
}

/// Open `partitioned_file` with `inner`, then log and drop each error.
///
/// # Arguments
///
/// * `inner` - The opener whose errors to skip
/// * `partitioned_file` - The file to open
///
/// # Returns
///
/// A future that never fails. A file that fails to open gives an empty stream.
pub fn open_skip_errors(
    inner: &dyn FileOpener,
    partitioned_file: PartitionedFile,
) -> Result<FileOpenFuture> {
    let inner = match inner.open(partitioned_file) {
        Ok(inner) => inner,
        Err(error) => {
            tracing::error!("Skip a file that failed to open: {error}");
            return Ok(futures::future::ready(Ok(futures::stream::empty().boxed())).boxed());
        }
    };
    Ok(async move {
        let stream = match inner.await {
            Ok(stream) => stream,
            Err(error) => {
                tracing::error!("Skip a file that failed to open: {error}");
                return Ok(futures::stream::empty().boxed());
            }
        };
        // `filter_map` drops each `Err` batch and keeps the stream alive for the next batch.
        Ok(stream
            .filter_map(|batch| {
                let batch = batch
                    .inspect_err(|error| tracing::error!("Skip a batch that failed: {error}"))
                    .ok()
                    .map(Ok);
                futures::future::ready(batch)
            })
            .boxed())
    }
    .boxed())
}

#[cfg(test)]
mod tests {
    use arrow::{
        array::{Int32Array, RecordBatch},
        datatypes::{DataType, Field, Schema},
    };
    use datafusion::error::DataFusionError;
    use futures::TryStreamExt;

    use super::*;

    /// How [`MockOpener`] fails.
    #[derive(Debug, Clone, Copy)]
    enum Failure {
        Open,
        Future,
        Batch,
    }

    /// Gives batches `[1]`, `[2]`, `[3]`, or fails as `failure` states.
    /// `Failure::Batch` replaces the second batch with an error.
    struct MockOpener {
        failure: Option<Failure>,
    }

    fn batch(value: i32) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
        RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![value]))])
            .expect("a valid batch")
    }

    fn error() -> DataFusionError {
        DataFusionError::Execution("mock failure".to_string())
    }

    impl FileOpener for MockOpener {
        fn open(&self, _partitioned_file: PartitionedFile) -> Result<FileOpenFuture> {
            let failure = self.failure;
            if let Some(Failure::Open) = failure {
                return Err(error());
            }
            Ok(async move {
                if let Some(Failure::Future) = failure {
                    return Err(error());
                }
                let batches = (1..=3).map(move |value| match failure {
                    Some(Failure::Batch) if value == 2 => Err(error()),
                    _ => Ok(batch(value)),
                });
                Ok(futures::stream::iter(batches).boxed())
            }
            .boxed())
        }
    }

    /// The row count that `SkipErrorAdapter` around a `MockOpener` with `failure` gives.
    async fn rows(failure: Option<Failure>) -> usize {
        let opener = SkipErrorAdapter::wrap(Arc::new(MockOpener { failure }));
        let stream = opener
            .open(PartitionedFile::new("file.nc", 10))
            .expect("the adapter never fails to open")
            .await
            .expect("the adapter never fails the future");
        let batches: Vec<RecordBatch> = stream.try_collect().await.expect("no error batch");
        batches.iter().map(RecordBatch::num_rows).sum()
    }

    #[tokio::test]
    async fn a_scan_without_errors_keeps_every_batch() {
        assert_eq!(rows(None).await, 3);
    }

    #[tokio::test]
    async fn an_open_error_gives_no_rows() {
        assert_eq!(rows(Some(Failure::Open)).await, 0);
    }

    #[tokio::test]
    async fn a_future_error_gives_no_rows() {
        assert_eq!(rows(Some(Failure::Future)).await, 0);
    }

    #[tokio::test]
    async fn a_batch_error_drops_only_that_batch() {
        assert_eq!(rows(Some(Failure::Batch)).await, 2);
    }
}
