use beacon_datafusion_ext::nd::NdRecordBatch;
use crossbeam::queue::ArrayQueue;
use std::{any::Any, sync::Arc};

#[async_trait::async_trait]
pub trait DatasetSource: Send + Sync + std::fmt::Debug {
    fn chunks(&self) -> Vec<Arc<dyn Any + Send + Sync>>;

    /// How many rows `chunk` holds, when that is known without a read.
    ///
    /// A read that projects no column counts rows and reads nothing else.
    /// `None` says the count is not known ahead: read the chunk and count
    /// what comes back.
    fn chunk_rows(&self, _chunk: &Arc<dyn Any + Send + Sync>) -> Option<usize> {
        None
    }

    async fn poll_next(
        &self,
        chunk: Arc<dyn Any + Send + Sync>,
    ) -> anyhow::Result<Option<NdRecordBatch>>;
}

/// A source shared by the partitions that read it, and the steps left to read.
///
/// The queue is filled once, on construction. Every partition pops from the
/// same queue, so each step is read by one partition and by no other.
#[derive(Debug, Clone)]
pub struct SharedDatasetSource {
    source: Arc<dyn DatasetSource>,
    chunks: Arc<ArrayQueue<Arc<dyn Any + Send + Sync>>>,
}

impl SharedDatasetSource {
    /// Cut `source` into steps and queue them.
    ///
    /// A step is one slab along the outermost axis: every chunk that shares
    /// one chunk index on axis 0. The chunks of a step are in C order, and the
    /// steps are queued in C order too, so a reader that drains the queue alone
    /// sees the chunks in the order the source stores them.
    ///
    /// See [`chunk_steps`] for the cut.
    pub fn new(source: Arc<dyn DatasetSource>) -> anyhow::Result<Self> {
        // Cut the source into steps, and queue them. The queue is shared by all
        let steps = source.chunks();
        if steps.is_empty() {
            return Err(anyhow::anyhow!("dataset source has no chunks"));
        }
        if steps.len() == 1 {
            return Err(anyhow::anyhow!(
                "dataset source has only one chunk, so no steps"
            ));
        }
        let queue = ArrayQueue::new(steps.len());
        for step in steps {
            queue
                .push(step)
                .map_err(|_| anyhow::anyhow!("dataset source queue is full, cannot push step"))?;
        }
        Ok(Self {
            source,
            chunks: Arc::new(queue),
        })
    }

    /// How many steps are left. For tests and diagnostics.
    pub fn remaining_steps(&self) -> usize {
        self.chunks.len()
    }

    pub fn next_step(&self) -> Option<Arc<dyn Any + Send + Sync>> {
        self.chunks.pop()
    }

    pub async fn poll_next(
        &self,
        chunk: Arc<dyn Any + Send + Sync>,
    ) -> anyhow::Result<Option<NdRecordBatch>> {
        self.source.poll_next(chunk).await
    }
}
