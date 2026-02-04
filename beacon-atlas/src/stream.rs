use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use beacon_nd_arrow::NdArrowArray;
use futures::{
    FutureExt, Stream, StreamExt,
    future::BoxFuture,
    stream::{self, FuturesUnordered},
};

#[async_trait::async_trait]
pub trait StreamingRead {
    async fn read(&self, dataset_index: u32) -> anyhow::Result<Option<NdArrowArray>>;
}

/// Represents the context required for parallel streaming reads.
///
pub struct StreamingReadContext {
    atlas_store_prefix: Arc<object_store::path::Path>,
    projected_atlas_schema: Arc<crate::schema::AtlasSchema>,
    projected_readers: Vec<Arc<dyn StreamingRead + Send + Sync>>,
    streaming_handle:
        StreamingReadHandle<BoxFuture<'static, anyhow::Result<Vec<Option<NdArrowArray>>>>>,
}

impl StreamingReadContext {
    pub fn new(
        store_prefix: Arc<object_store::path::Path>,
        projected_atlas_schema: Arc<crate::schema::AtlasSchema>,
        projected_readers: Vec<Arc<dyn StreamingRead + Send + Sync>>,
        dataset_indexes: Vec<u32>,
    ) -> Self {
        // Self {
        //     streaming_handle: Self::create_streaming_handle(dataset_indexes),
        //     atlas_store_prefix: store_prefix,
        //     projected_atlas_schema,
        //     projected_readers,
        // }
        todo!()
    }

    fn create_streaming_handle(
        dataset_indexes: Vec<u32>,
        readers: Arc<[Arc<dyn StreamingRead + Send + Sync>]>,
        schema: SchemaRef,
    ) -> StreamingReadHandle<BoxFuture<'static, anyhow::Result<Vec<Option<NdArrowArray>>>>> {
        let futs = stream::iter(dataset_indexes).map(move |i| {
            let readers = readers.clone();
            async move {
                let mut in_flight = FuturesUnordered::new();

                for (idx, reader) in readers.iter().enumerate() {
                    in_flight.push(async move { (idx, reader.read(i).await) });
                }

                let mut results = vec![None; readers.len()];

                while let Some((idx, res)) = in_flight.next().await {
                    results[idx] = res?;
                }

                Ok::<_, anyhow::Error>(results)
            }
            .boxed()
        });
        StreamingReadHandle::new(futs, 100)
    }

    pub fn projected_atlas_schema(&self) -> &crate::schema::AtlasSchema {
        &self.projected_atlas_schema
    }

    pub fn get_streaming_handle(
        &self,
        per_worker_buffer: usize,
    ) -> impl Stream<Item = anyhow::Result<Vec<Option<NdArrowArray>>>> {
        self.streaming_handle.as_stream(per_worker_buffer)
    }
}

pub struct StreamingReadHandle<Fut> {
    work_rx: flume::Receiver<Fut>,
}

impl<Fut> StreamingReadHandle<Fut>
where
    Fut: Future<Output = anyhow::Result<Vec<Option<NdArrowArray>>>> + Send + 'static,
{
    /// Create from a stream of futures with only a LOCAL bound
    pub fn new<S>(stream: S, queue_bound: usize) -> Self
    where
        S: Stream<Item = Fut> + Send + 'static,
    {
        let (tx, rx) = flume::bounded(queue_bound);

        tokio::spawn(async move {
            let mut stream = std::pin::pin!(stream);

            while let Some(fut) = stream.next().await {
                if tx.send_async(fut).await.is_err() {
                    break;
                }
            }
        });

        Self { work_rx: rx }
    }

    /// Called by each worker.
    /// Each worker has its OWN concurrency limit.
    pub fn as_stream(
        &self,
        per_worker_buffer: usize,
    ) -> impl Stream<Item = anyhow::Result<Vec<Option<NdArrowArray>>>> {
        let rx = self.work_rx.clone();

        futures::stream::unfold(FuturesUnordered::new(), move |mut in_flight| {
            let rx = rx.clone();

            async move {
                while in_flight.len() < per_worker_buffer {
                    match rx.recv_async().await {
                        Ok(fut) => in_flight.push(fut),
                        Err(_) => break,
                    }
                }

                in_flight.next().await.map(|result| (result, in_flight))
            }
        })
    }
}
