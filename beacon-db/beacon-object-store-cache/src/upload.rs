//! Multipart upload that updates the index when it completes.

use std::sync::Arc;

use async_trait::async_trait;
use object_store::path::Path;
use object_store::{MultipartUpload, PutPayload, PutResult, Result, UploadPart};

use crate::state::CacheState;

#[derive(Debug)]
pub(crate) struct CachedUpload {
    inner: Box<dyn MultipartUpload>,
    state: Arc<CacheState>,
    location: Path,
}

impl CachedUpload {
    pub(crate) fn new(inner: Box<dyn MultipartUpload>, state: Arc<CacheState>, location: Path) -> Self {
        Self {
            inner,
            state,
            location,
        }
    }
}

#[async_trait]
impl MultipartUpload for CachedUpload {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        self.inner.put_part(data)
    }

    async fn complete(&mut self) -> Result<PutResult> {
        let result = self.inner.complete().await?;
        self.state.refresh(&self.location).await;
        Ok(result)
    }

    async fn abort(&mut self) -> Result<()> {
        self.inner.abort().await
    }
}
