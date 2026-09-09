//! The runtime that runs queries, kept apart from the runtime that serves them.
//!
//! A partition decode holds a worker until it yields, and Tokio cannot preempt
//! it. When queries and the API share one runtime, a long scan takes every
//! worker and every other request waits. The executor pins each query to a
//! runtime of its own.
//!
//! A plain `spawn` is not enough. DataFusion builds its stream lazily, and
//! `RepartitionExec` spawns its tasks onto the runtime that polls the stream.
//! [`QueryExecutor::bridge_stream`] therefore polls the stream on the query
//! runtime and hands each batch to the caller over a bounded channel.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use futures::{Stream, StreamExt};
use tokio::runtime::Handle;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::Instrument;

/// Batches in flight between the query runtime and the caller.
///
/// The producer waits when the channel is full, so a slow client holds at most
/// this many batches ahead of what it has read.
const BRIDGE_CAPACITY: usize = 4;

/// A handle to the runtime that runs queries.
///
/// Cheap to clone. The embedded database passes the one runtime it has. The
/// query and the caller then share a runtime, and the bridge costs one channel
/// hop per batch.
#[derive(Clone, Debug)]
pub struct QueryExecutor {
    handle: Handle,
}

impl QueryExecutor {
    pub fn new(handle: Handle) -> Self {
        Self { handle }
    }

    /// The runtime this executor spawns onto.
    pub fn handle(&self) -> &Handle {
        &self.handle
    }

    /// Runs `future` to completion on the query runtime.
    ///
    /// A panic in the future comes back as an error, not as a panic in the
    /// caller. The current tracing span follows the future.
    pub async fn run<F, T>(&self, future: F) -> anyhow::Result<T>
    where
        F: Future<Output = anyhow::Result<T>> + Send + 'static,
        T: Send + 'static,
    {
        self.handle
            .spawn(future.in_current_span())
            .await
            .map_err(|error| anyhow::anyhow!("query task failed: {error}"))?
    }

    /// Polls `stream` on the query runtime and yields its batches to the caller.
    ///
    /// The producer task ends when the stream ends or when the caller drops the
    /// returned stream. A panic in the producer reaches the caller as an error,
    /// so a short result never looks complete.
    pub fn bridge_stream(&self, stream: SendableRecordBatchStream) -> SendableRecordBatchStream {
        let schema = stream.schema();
        let (sender, receiver) = mpsc::channel(BRIDGE_CAPACITY);
        let producer = self.handle.spawn(
            async move {
                let mut stream = stream;
                while let Some(item) = stream.next().await {
                    // A closed channel means the caller is gone. Stop the query.
                    if sender.send(item).await.is_err() {
                        break;
                    }
                }
            }
            .in_current_span(),
        );
        Box::pin(RecordBatchStreamAdapter::new(
            schema,
            BridgedStream {
                receiver,
                producer: Some(producer),
            },
        ))
    }
}

/// The receiving end of a bridged stream.
struct BridgedStream {
    receiver: mpsc::Receiver<DataFusionResult<RecordBatch>>,
    /// The producer task. `None` once it is joined.
    producer: Option<JoinHandle<()>>,
}

impl Stream for BridgedStream {
    type Item = DataFusionResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.receiver.poll_recv(cx) {
            Poll::Ready(Some(item)) => Poll::Ready(Some(item)),
            Poll::Pending => Poll::Pending,
            // The channel is closed and empty, so the producer is done or about
            // to be. Join it: a panic must reach the caller, not end the stream.
            Poll::Ready(None) => {
                let Some(producer) = self.producer.as_mut() else {
                    return Poll::Ready(None);
                };
                match Pin::new(producer).poll(cx) {
                    Poll::Pending => Poll::Pending,
                    Poll::Ready(outcome) => {
                        self.producer = None;
                        Poll::Ready(outcome.err().map(|error| {
                            Err(DataFusionError::Execution(format!(
                                "query execution task failed: {error}"
                            )))
                        }))
                    }
                }
            }
        }
    }
}

impl Drop for BridgedStream {
    fn drop(&mut self) {
        // A dropped result cancels the query at once, even inside a read.
        if let Some(producer) = &self.producer {
            producer.abort();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, Instant};

    use arrow::array::{AsArray, Int32Array};
    use arrow::datatypes::{DataType, Field, Int32Type, Schema, SchemaRef};
    use futures::TryStreamExt;
    use tokio::runtime::{Builder, Runtime};

    use super::*;

    const QUERY_THREAD: &str = "test-query";

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]))
    }

    fn batch(value: i32) -> RecordBatch {
        RecordBatch::try_new(schema(), vec![Arc::new(Int32Array::from(vec![value]))]).unwrap()
    }

    fn source<S>(stream: S) -> SendableRecordBatchStream
    where
        S: Stream<Item = DataFusionResult<RecordBatch>> + Send + 'static,
    {
        Box::pin(RecordBatchStreamAdapter::new(schema(), stream))
    }

    /// An API runtime and a query runtime, as the server has them.
    fn two_runtimes() -> (Runtime, QueryExecutor, Runtime) {
        let api = Builder::new_current_thread().enable_all().build().unwrap();
        let query = Builder::new_multi_thread()
            .worker_threads(2)
            .thread_name(QUERY_THREAD)
            .enable_all()
            .build()
            .unwrap();
        let executor = QueryExecutor::new(query.handle().clone());
        (api, executor, query)
    }

    #[test]
    fn batches_cross_runtimes_in_order() {
        let (api, executor, _query) = two_runtimes();
        let batches = api.block_on(async {
            let stream = executor.bridge_stream(source(futures::stream::iter(
                (0..10).map(|value| Ok(batch(value))),
            )));
            stream.try_collect::<Vec<_>>().await.unwrap()
        });
        let values: Vec<i32> = batches
            .iter()
            .map(|batch| batch.column(0).as_primitive::<Int32Type>().value(0))
            .collect();
        assert_eq!(values, (0..10).collect::<Vec<_>>());
    }

    #[test]
    fn the_source_is_polled_on_the_query_runtime() {
        let (api, executor, _query) = two_runtimes();
        let seen = Arc::new(Mutex::new(None));
        let record = seen.clone();
        api.block_on(async {
            let stream = executor.bridge_stream(source(futures::stream::once(async move {
                *record.lock().unwrap() = std::thread::current().name().map(String::from);
                Ok(batch(1))
            })));
            stream.try_collect::<Vec<_>>().await.unwrap();
        });
        assert_eq!(seen.lock().unwrap().as_deref(), Some(QUERY_THREAD));
    }

    #[test]
    fn an_error_item_passes_through() {
        let (api, executor, _query) = two_runtimes();
        let error = api.block_on(async {
            let stream = executor.bridge_stream(source(futures::stream::iter([
                Ok(batch(1)),
                Err(DataFusionError::Execution("scan failed".into())),
            ])));
            stream.try_collect::<Vec<_>>().await.unwrap_err()
        });
        assert!(error.to_string().contains("scan failed"), "{error}");
    }

    #[test]
    fn a_panic_in_the_producer_is_an_error_not_an_end() {
        let (api, executor, _query) = two_runtimes();
        let error = api.block_on(async {
            let stream = executor.bridge_stream(source(futures::stream::poll_fn(
                |_: &mut Context<'_>| -> Poll<Option<DataFusionResult<RecordBatch>>> {
                    panic!("decoder bug")
                },
            )));
            stream.try_collect::<Vec<_>>().await.unwrap_err()
        });
        assert!(
            error.to_string().contains("query execution task failed"),
            "{error}"
        );
    }

    #[test]
    fn dropping_the_stream_cancels_the_producer() {
        struct Guard(Arc<AtomicBool>);
        impl Drop for Guard {
            fn drop(&mut self) {
                self.0.store(true, Ordering::SeqCst);
            }
        }

        let (api, executor, _query) = two_runtimes();
        let dropped = Arc::new(AtomicBool::new(false));
        let guard = Guard(dropped.clone());
        api.block_on(async {
            // The source never yields, so only a cancel can release the guard.
            let mut stream = executor.bridge_stream(source(futures::stream::poll_fn(
                move |_: &mut Context<'_>| -> Poll<Option<DataFusionResult<RecordBatch>>> {
                    let _held = &guard;
                    Poll::Pending
                },
            )));
            assert!(futures::poll!(stream.next()).is_pending());
            drop(stream);
        });

        let deadline = Instant::now() + Duration::from_secs(5);
        while !dropped.load(Ordering::SeqCst) {
            assert!(Instant::now() < deadline, "the producer kept running");
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    #[test]
    fn run_reports_a_panic_as_an_error() {
        async fn faulty() -> anyhow::Result<()> {
            panic!("planner bug")
        }

        let (api, executor, _query) = two_runtimes();
        let error = api.block_on(executor.run(faulty())).unwrap_err();
        assert!(error.to_string().contains("query task failed"), "{error}");
    }

    #[test]
    fn run_returns_the_value() {
        let (api, executor, _query) = two_runtimes();
        let value = api
            .block_on(executor.run(async { Ok::<_, anyhow::Error>(41 + 1) }))
            .unwrap();
        assert_eq!(value, 42);
    }
}
