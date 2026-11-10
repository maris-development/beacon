//! The sink: batches the records of analyzed files, and stores each batch in one transaction.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc::Receiver;
use tokio::task::JoinHandle;
use tokio::time::MissedTickBehavior;

use crate::queue::AnalyzeQueue;
use crate::stamp::Stamp;
use crate::store::{FileRecord, StatisticsStore};

/// When the sink commits a batch.
#[derive(Debug, Clone, Copy)]
pub(crate) struct BatchLimits {
    /// The number of files that makes a full batch.
    pub size: usize,
    /// The longest time a file waits in a batch.
    pub age: Duration,
}

/// Starts the sink. It commits the last batch and stops when all senders are dropped.
pub(crate) fn spawn_sink(
    mut records: Receiver<FileRecord>,
    store: Arc<StatisticsStore>,
    queue: Arc<AnalyzeQueue>,
    limits: BatchLimits,
    handle: &tokio::runtime::Handle,
) -> JoinHandle<()> {
    handle.spawn(async move {
        let mut batch: Vec<FileRecord> = Vec::with_capacity(limits.size);
        let mut ticker = tokio::time::interval(limits.age);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                received = records.recv() => match received {
                    Some(record) => {
                        batch.push(record);
                        if batch.len() >= limits.size {
                            flush(&mut batch, &store, &queue).await;
                            // The age counts from the last commit, so a full batch is not followed by a tiny one.
                            ticker.reset();
                        }
                    }
                    None => {
                        flush(&mut batch, &store, &queue).await;
                        return;
                    }
                },
                _ = ticker.tick() => flush(&mut batch, &store, &queue).await,
            }
        }
    })
}

/// Stores `batch`, then releases its stamps from the queue.
///
/// A failed write stores nothing, and the next walk queues the files again.
async fn flush(batch: &mut Vec<FileRecord>, store: &StatisticsStore, queue: &AnalyzeQueue) {
    if batch.is_empty() {
        return;
    }
    let records = std::mem::take(batch);
    let stamps: Vec<Stamp> = records.iter().map(FileRecord::stamp).collect();
    if let Err(error) = store.put(records).await {
        tracing::warn!(%error, files = stamps.len(), "a statistics batch was not committed");
    }
    for stamp in stamps {
        queue.finish(stamp);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::queue::tests::task;
    use crate::store::tests::{record, stamp, store};
    use tokio::sync::mpsc;

    #[tokio::test]
    async fn a_full_batch_is_stored_and_releases_its_stamps() {
        // Arrange
        let store = Arc::new(store().await);
        let queue = Arc::new(AnalyzeQueue::new(8));
        let (first, second) = (task("a.nc"), task("b.nc"));
        queue.enqueue(first.clone()).await;
        queue.enqueue(second.clone()).await;
        let (sender, receiver) = mpsc::channel(8);
        let limits = BatchLimits {
            size: 2,
            age: Duration::from_secs(3600),
        };
        let sink = spawn_sink(
            receiver,
            Arc::clone(&store),
            Arc::clone(&queue),
            limits,
            &tokio::runtime::Handle::current(),
        );

        // Act
        for task in [&first, &second] {
            let record = FileRecord::failed(task.stamp);
            sender.send(record).await.expect("the sink runs");
        }
        drop(sender);
        sink.await.expect("the sink stops without a panic");

        // Assert
        let stored = store
            .contains(&[first.stamp, second.stamp])
            .await
            .expect("read");
        assert_eq!(stored, [true, true]);
        assert!(queue.is_empty(), "each stamp is released");
    }

    #[tokio::test(start_paused = true)]
    async fn an_old_batch_is_stored_on_its_age() {
        let store = Arc::new(store().await);
        let queue = Arc::new(AnalyzeQueue::new(8));
        let (sender, receiver) = mpsc::channel(8);
        let limits = BatchLimits {
            size: 100,
            age: Duration::from_secs(30),
        };
        let _sink = spawn_sink(
            receiver,
            Arc::clone(&store),
            queue,
            limits,
            &tokio::runtime::Handle::current(),
        );

        sender.send(record(1, 0.0)).await.expect("the sink runs");
        tokio::time::sleep(Duration::from_secs(31)).await;
        // The write runs on a blocking thread, which paused time does not wait for.
        tokio::time::timeout(Duration::from_secs(60), async {
            while store.contains(&[stamp(1)]).await.expect("read") == [false] {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the batch is stored");
    }
}
