//! Cooperative scheduler for joining a batch of async operations.
//!
//! `AsyncStreamScheduler` gives callers a stream view over a list of futures
//! while bounding concurrency via a semaphore.

use std::{future::Future, sync::Arc};

use futures::Stream;
use tokio::sync::Semaphore;

#[derive(Debug, Clone)]
/// Wraps a bounded channel that yields results as futures complete.
pub struct AsyncStreamScheduler<T: Send + 'static> {
    receiver: flume::Receiver<T>,
}

impl<T: Send + 'static> AsyncStreamScheduler<T> {
    /// Spawn the provided futures and stream their outputs with at most
    /// `parallelism` concurrent tasks in-flight.
    pub fn new<Fut>(futures: Vec<Fut>, parallelism: usize) -> Self
    where
        Fut: Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        let (sender, receiver) = flume::bounded::<T>(parallelism);
        let sem = Arc::new(Semaphore::new(parallelism));

        tokio::spawn({
            let sender = sender.clone();
            async move {
                let mut handles = Vec::with_capacity(futures.len());
                for fut in futures {
                    let permit = sem.clone().acquire_owned().await.unwrap();
                    let sender = sender.clone();
                    handles.push(tokio::spawn(async move {
                        let _permit = permit; // lives until this task finishes
                        let res = fut.await;
                        let _ = sender.send_async(res).await;
                    }));
                }

                for h in handles {
                    let _ = h.await;
                }
                drop(sender);
            }
        });

        Self { receiver }
    }

    /// Obtain a clone of the underlying stream so multiple readers can drive
    /// completion concurrently.
    pub async fn shared_pollable_stream_ref(&self) -> impl Stream<Item = T> + '_ {
        self.receiver.clone().into_stream()
    }
}
