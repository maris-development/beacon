use std::{future::Future, sync::Arc};

use arrow::datatypes::SchemaRef;
use beacon_arrow_zarr::stream::ArrowZarrStreamComposerRef;
use datafusion::datasource::schema_adapter::SchemaMapper;

pub struct ZarrStreamShare {
    inner: tokio::sync::OnceCell<(ArrowZarrStreamComposerRef, Arc<dyn SchemaMapper>, SchemaRef)>,
}

impl ZarrStreamShare {
    pub fn new() -> Self {
        Self {
            inner: tokio::sync::OnceCell::new(),
        }
    }

    pub fn get_or_try_init<F, Fut>(
        &self,
        f: F,
    ) -> impl Future<
        Output = Result<
            &(ArrowZarrStreamComposerRef, Arc<dyn SchemaMapper>, SchemaRef),
            datafusion::error::DataFusionError,
        >,
    >
    where
        F: FnOnce() -> Fut,
        Fut: Future<
            Output = Result<
                (ArrowZarrStreamComposerRef, Arc<dyn SchemaMapper>, SchemaRef),
                datafusion::error::DataFusionError,
            >,
        >,
    {
        self.inner.get_or_try_init(f)
    }
}

impl Default for ZarrStreamShare {
    fn default() -> Self {
        Self::new()
    }
}
