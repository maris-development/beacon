use std::future::Future;
use std::sync::Arc;

use beacon_nd_array::arrow::batch::ParallelDatasetStream;
use datafusion::physical_expr_adapter::BatchAdapter;

/// The shared, per-file outcome of opening a TIFF dataset.
///
/// Built once per object path (see [`TiffStreamShare`]) and cloned by every
/// partition that opens the same file. For a normal projection it is a
/// [`ParallelDatasetStream`] — a cloneable MPMC producer whose batches are
/// work-shared across the cloning consumers — together with the schema adapter
/// that maps file batches onto the projected output schema. For an empty
/// projection it is just the dataset row count, from which each opener rebuilds
/// the (single) row-count batch.
#[derive(Clone, Debug)]
pub enum SharedTiffStream {
    Data {
        stream: ParallelDatasetStream,
        adapter: Arc<BatchAdapter>,
    },
    RowSize {
        rows: usize,
    },
}

/// A `OnceCell` holding the shared read for a single TIFF file. The first
/// partition to open the file initializes it (opening the dataset and spawning
/// the parallel producer); later partitions reuse the same producer and
/// work-share its batches.
#[derive(Debug, Default)]
pub struct TiffStreamShare {
    inner: tokio::sync::OnceCell<SharedTiffStream>,
}

impl TiffStreamShare {
    pub fn new() -> Self {
        Self {
            inner: tokio::sync::OnceCell::new(),
        }
    }

    pub fn get_or_try_init<F, Fut>(
        &self,
        f: F,
    ) -> impl Future<Output = Result<&SharedTiffStream, datafusion::error::DataFusionError>>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<SharedTiffStream, datafusion::error::DataFusionError>>,
    {
        self.inner.get_or_try_init(f)
    }
}
