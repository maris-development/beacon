//! One partition's reader.
//!
//! Takes files from the shared [`Ready`] one at a time and opens the next while
//! the current one is still being read. See the [module docs](super).

use std::ops::Range;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::Result;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::{FileOpenFuture, FileOpener};
use datafusion::execution::RecordBatchStream;
use futures::Stream;
use futures::future::BoxFuture;
use futures::stream::BoxStream;
use object_store::ObjectMeta;

use super::plan::{Ready, Work};

/// The file identity the opener reads, built at the moment it is opened and
/// dropped after.
pub(super) fn partitioned_file(objects: &[ObjectMeta], work: &Work) -> PartitionedFile {
    let file = PartitionedFile::from(objects[work.index()].clone());
    match work {
        Work::Whole(_) => file,
        Work::Part(_, range) => file.with_range(range.start, range.end),
    }
}

/// One partition's reader: files taken from the shared plan, one at a time.
pub(super) struct FastObjectStream {
    schema: SchemaRef,
    objects: Arc<Vec<ObjectMeta>>,
    partition: usize,
    /// The decided file list, once the shared prune has resolved.
    ready: Option<Arc<Ready>>,
    /// This partition's remaining slice of `Ready::order`, under a limit.
    lane: Range<usize>,
    opener: Arc<dyn FileOpener>,
    state: StreamState,
    /// Rows this partition may still emit under the scan's limit.
    remaining: Option<usize>,
}

impl FastObjectStream {
    /// A reader for `partition`, waiting on the shared prune in `prepare`.
    ///
    /// The lane it reads under a limit is not known until that resolves, so it
    /// starts empty and is filled when the future lands.
    pub(super) fn new(
        schema: SchemaRef,
        objects: Arc<Vec<ObjectMeta>>,
        partition: usize,
        opener: Arc<dyn FileOpener>,
        remaining: Option<usize>,
        prepare: BoxFuture<'static, Arc<Ready>>,
    ) -> Self {
        Self {
            schema,
            objects,
            partition,
            ready: None,
            lane: 0..0,
            opener,
            state: StreamState::Preparing(prepare),
            remaining,
        }
    }
}

enum StreamState {
    /// Waiting on the shared prune. The first partition here runs it.
    Preparing(BoxFuture<'static, Arc<Ready>>),
    /// No file is open; the next one is due.
    Idle,
    /// A file is being opened.
    Opening(FileOpenFuture),
    /// A file's batches are being read, while the next one opens alongside it.
    Reading {
        reader: BoxStream<'static, Result<RecordBatch>>,
        next: Option<NextOpen>,
    },
    /// Every file is read, or an error ended the stream.
    Done,
}

/// The file after the one being read.
///
/// Opening it costs a round trip, so it starts while the current file is still
/// being scanned and is waiting by the time it is due — the same overlap
/// DataFusion's own `FileStream` performs.
enum NextOpen {
    Pending(FileOpenFuture),
    Ready(Result<BoxStream<'static, Result<RecordBatch>>>),
}

/// Begin opening the next file, if there is one.
///
/// Takes the fields it needs rather than `&mut self`, so it can be called while
/// the state machine holds a borrow of `state`.
fn begin_next_open(
    opener: &Arc<dyn FileOpener>,
    objects: &[ObjectMeta],
    ready: &Ready,
    lane: &mut Range<usize>,
) -> Option<NextOpen> {
    let work = ready.next(lane)?;
    match opener.open(partitioned_file(objects, &work)) {
        Ok(future) => Some(NextOpen::Pending(future)),
        Err(error) => Some(NextOpen::Ready(Err(error))),
    }
}

impl Stream for FastObjectStream {
    type Item = Result<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        loop {
            match &mut this.state {
                StreamState::Done => return Poll::Ready(None),
                StreamState::Preparing(future) => match future.as_mut().poll(cx) {
                    Poll::Ready(ready) => {
                        // Under a limit this partition reads one slice of the
                        // survivors; otherwise `lanes` is empty and it pops the
                        // shared queue instead.
                        this.lane = ready.lane(this.partition);
                        this.ready = Some(ready);
                        this.state = StreamState::Idle;
                    }
                    Poll::Pending => return Poll::Pending,
                },
                StreamState::Idle => {
                    if this.remaining == Some(0) {
                        this.state = StreamState::Done;
                        continue;
                    }
                    let Some(ready) = this.ready.as_ref() else {
                        this.state = StreamState::Done;
                        continue;
                    };
                    match ready.next(&mut this.lane) {
                        Some(work) => {
                            match this.opener.open(partitioned_file(&this.objects, &work)) {
                                Ok(future) => this.state = StreamState::Opening(future),
                                Err(error) => {
                                    this.state = StreamState::Done;
                                    return Poll::Ready(Some(Err(error)));
                                }
                            }
                        }
                        None => this.state = StreamState::Done,
                    }
                }
                StreamState::Opening(future) => match Pin::new(future).poll(cx) {
                    Poll::Ready(Ok(reader)) => {
                        // The next file starts opening now, so its round trip
                        // overlaps this one's scan.
                        let next = this.ready.as_ref().and_then(|ready| {
                            begin_next_open(&this.opener, &this.objects, ready, &mut this.lane)
                        });
                        this.state = StreamState::Reading { reader, next };
                    }
                    Poll::Ready(Err(error)) => {
                        this.state = StreamState::Done;
                        return Poll::Ready(Some(Err(error)));
                    }
                    Poll::Pending => return Poll::Pending,
                },
                StreamState::Reading { reader, next } => {
                    // Drive the next file's open forward, so it is ready — or
                    // nearly — by the time this reader runs out.
                    if let Some(NextOpen::Pending(future)) = next
                        && let Poll::Ready(opened) = Pin::new(future).poll(cx)
                    {
                        *next = Some(NextOpen::Ready(opened));
                    }

                    match Pin::new(reader).poll_next(cx) {
                        Poll::Ready(Some(Ok(batch))) => {
                            let batch = match &mut this.remaining {
                                Some(remaining) => {
                                    let take = batch.num_rows().min(*remaining);
                                    *remaining -= take;
                                    if take < batch.num_rows() {
                                        batch.slice(0, take)
                                    } else {
                                        batch
                                    }
                                }
                                None => batch,
                            };
                            return Poll::Ready(Some(Ok(batch)));
                        }
                        Poll::Ready(Some(Err(error))) => {
                            this.state = StreamState::Done;
                            return Poll::Ready(Some(Err(error)));
                        }
                        Poll::Ready(None) => {
                            this.state = match next.take() {
                                Some(NextOpen::Ready(Ok(reader))) => {
                                    let next = this.ready.as_ref().and_then(|ready| {
                                        begin_next_open(
                                            &this.opener,
                                            &this.objects,
                                            ready,
                                            &mut this.lane,
                                        )
                                    });
                                    StreamState::Reading { reader, next }
                                }
                                Some(NextOpen::Ready(Err(error))) => {
                                    this.state = StreamState::Done;
                                    return Poll::Ready(Some(Err(error)));
                                }
                                Some(NextOpen::Pending(future)) => StreamState::Opening(future),
                                // Nothing was queued behind this file, so ask
                                // again: the shared queue may have more.
                                None => StreamState::Idle,
                            };
                        }
                        Poll::Pending => return Poll::Pending,
                    }
                }
            }
        }
    }
}

impl RecordBatchStream for FastObjectStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}
