use std::{fmt::Display, sync::Arc};

use futures::{StreamExt, stream::BoxStream};
use object_store::{
    GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult,
};

use crate::{
    error::StorageError,
    event::{EventHandler, ObjectEvent},
    object_cache::ObjectCache,
};

pub type StorageResult<T> = Result<T, StorageError>;

#[derive(Debug, Clone)]
pub struct NotifiedStore<O: ObjectStore> {
    object_cache: Arc<parking_lot::Mutex<ObjectCache>>,
    inner: O,
}

impl<O: ObjectStore> Display for NotifiedStore<O> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "NotifiedStore wrapping: {}", self.inner)
    }
}

impl<O: ObjectStore> NotifiedStore<O> {
    pub async fn new(inner: O) -> Self {
        // List all existing objects to populate the cache
        let mut stream = inner.list(None);

        let mut objects = Vec::new();
        while let Some(result) = stream.next().await {
            if let Ok(meta) = result {
                objects.push(meta);
            }
        }
        let object_cache = Arc::new(parking_lot::Mutex::new(ObjectCache::new(objects)));

        NotifiedStore {
            object_cache,
            inner,
        }
    }

    pub fn handle_event<H, I>(&self, input: I) -> StorageResult<()>
    where
        H: EventHandler<I>,
    {
        // Implementation goes here
        let events = H::handle_event(input)?;
        for event in events {
            match &event {
                ObjectEvent::Created(meta) | ObjectEvent::Modified(meta) => {
                    self.object_cache.lock().insert(meta.clone());
                }
                ObjectEvent::Deleted(path) => {
                    self.object_cache.lock().remove(path);
                }
            }
        }
        Ok(())
    }
}

/// ToDo: Events such as renames/copies/moves/deletes/puts should be pushed as events to update the cache and not rely on the event driven handler only
#[async_trait::async_trait]
impl<O: ObjectStore> ObjectStore for NotifiedStore<O> {
    async fn put_opts(
        &self,
        location: &object_store::path::Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &object_store::path::Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    /// Perform a get request with options
    async fn get_opts(
        &self,
        location: &object_store::path::Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        self.inner.get_opts(location, options).await
    }

    async fn delete(&self, location: &object_store::path::Path) -> object_store::Result<()> {
        self.inner.delete(location).await
    }

    fn list(
        &self,
        prefix: Option<&object_store::path::Path>,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        // Go through the cache to retrieve objects
        let cache = self.object_cache.lock();
        let objects = cache
            .list_prefix(prefix.map(|p| p.as_ref()).unwrap_or(""))
            .collect::<Vec<_>>();
        drop(cache);
        let stream = futures::stream::iter(objects.into_iter().map(Ok));
        Box::pin(stream)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&object_store::path::Path>,
    ) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy(
        &self,
        from: &object_store::path::Path,
        to: &object_store::path::Path,
    ) -> object_store::Result<()> {
        self.inner.copy(from, to).await
    }

    async fn copy_if_not_exists(
        &self,
        from: &object_store::path::Path,
        to: &object_store::path::Path,
    ) -> object_store::Result<()> {
        self.inner.copy_if_not_exists(from, to).await
    }
}
