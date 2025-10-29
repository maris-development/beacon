use std::{future::Future, sync::Arc};

use arrow::datatypes::SchemaRef;
use datafusion::datasource::schema_adapter::SchemaMapper;
use futures::FutureExt;

pub struct PartitionedZarrStreamShare {
    pub stream_share: Arc<ZarrStreamShare>,
    pub schema_mapper: Arc<dyn SchemaMapper>,
    pub schema: SchemaRef,
}

pub struct ZarrStreamShare {
    partitions: tokio::sync::OnceCell<Arc<[PartitionedZarrStreamShare]>>,
}

impl ZarrStreamShare {
    pub fn new() -> Self {
        Self {
            partitions: tokio::sync::OnceCell::new(),
        }
    }

    pub fn get_or_try_init<F, Fut>(&self, f: F) -> impl Future<Output = Fut::Output>
    where
        F: FnOnce() -> Fut,
        Fut: Future<
            Output = Result<Arc<[PartitionedZarrStreamShare]>, datafusion::error::DataFusionError>,
        >,
    {
        // Clone the Arc inside the OnceCell to return it.
        self.partitions.get_or_try_init(f).map(|f| f.cloned())
    }
}

impl Default for ZarrStreamShare {
    fn default() -> Self {
        Self::new()
    }
}
