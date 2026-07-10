//! Server-side state for chunked (resumable) dataset uploads.
//!
//! Large files are uploaded as a sequence of parts across separate HTTP requests
//! (initiate → put part × N → complete), so a single request never has to carry a
//! multi-gigabyte body and a dropped connection only loses the part in flight.
//!
//! [`UploadManager`] keeps one [`UploadSession`] per in-progress upload, keyed by
//! an `upload_id`. Each session holds an object-store [`MultipartUpload`] handle;
//! every client part is buffered in full by the transport and submitted as one
//! atomic object-store part (so a part can be safely retried). Abandoned sessions
//! are aborted by a background sweeper after an idle timeout, which also releases
//! the underlying multipart upload on stores (S3/GCS) that need explicit cleanup.

use std::{
    collections::HashMap,
    sync::{Arc, Weak},
    time::{Duration, Instant},
};

use beacon_object_storage::DatasetsStore;
use bytes::Bytes;
use object_store::{path::Path, MultipartUpload, ObjectStoreExt};
use parking_lot::Mutex;
use uuid::Uuid;

use crate::dataset_files::{FileError, UploadResult};

/// Built-in part size (8 MiB) used when the configured size is `0`. Kept at or
/// above S3's 5 MiB minimum so every non-final part is individually valid.
const DEFAULT_PART_SIZE: usize = 8 * 1024 * 1024;
/// Built-in idle timeout (1 hour) used when the configured TTL is `0`.
const DEFAULT_TTL: Duration = Duration::from_secs(3600);
/// Lower bound on how often the sweeper runs, regardless of TTL.
const MIN_SWEEP_INTERVAL: Duration = Duration::from_secs(30);

/// A single in-progress chunked upload.
struct UploadSession {
    /// Destination key (already validated at initiate time).
    path: Path,
    /// The object-store multipart upload, taken on complete/abort.
    upload: Option<Box<dyn MultipartUpload>>,
    /// Bytes accepted so far across all parts.
    total: u64,
    /// Cumulative size cap (`0` = unlimited), carried from initiate.
    max_bytes: u64,
    /// Count of parts accepted so far; the next expected 1-based part number is
    /// `accepted_parts + 1`. A resend of an already-accepted part is idempotent.
    accepted_parts: u32,
    /// Last time this session saw activity, for idle expiry.
    last_active: Instant,
}

/// Owns and manages all in-progress chunked uploads for a runtime.
pub struct UploadManager {
    sessions: Mutex<HashMap<Uuid, Arc<tokio::sync::Mutex<UploadSession>>>>,
    part_size: usize,
    ttl: Duration,
}

impl UploadManager {
    /// Build the manager and spawn its idle-session sweeper. `part_size` and
    /// `ttl_secs` of `0` fall back to the built-in defaults.
    pub fn new(part_size: usize, ttl_secs: u64) -> Arc<Self> {
        let part_size = if part_size == 0 {
            DEFAULT_PART_SIZE
        } else {
            part_size
        };
        let ttl = if ttl_secs == 0 {
            DEFAULT_TTL
        } else {
            Duration::from_secs(ttl_secs)
        };
        let manager = Arc::new(Self {
            sessions: Mutex::new(HashMap::new()),
            part_size,
            ttl,
        });
        Self::spawn_sweeper(Arc::downgrade(&manager), ttl);
        manager
    }

    /// The part size clients should slice large files into.
    pub fn part_size(&self) -> usize {
        self.part_size
    }

    /// Open a new chunked upload for `path`, returning its session id. The caller
    /// is responsible for validating `path` and the overwrite policy first.
    pub async fn initiate(
        &self,
        store: &DatasetsStore,
        path: Path,
        max_bytes: u64,
    ) -> Result<Uuid, FileError> {
        let upload = store.put_multipart(&path).await?;
        let id = Uuid::new_v4();
        let session = UploadSession {
            path,
            upload: Some(upload),
            total: 0,
            max_bytes,
            accepted_parts: 0,
            last_active: Instant::now(),
        };
        self.sessions
            .lock()
            .insert(id, Arc::new(tokio::sync::Mutex::new(session)));
        Ok(id)
    }

    /// Submit one part. `part_number` is 1-based and must be the next expected
    /// part, or an already-accepted one (treated as an idempotent retry).
    ///
    /// On an object-store failure the whole session is aborted and removed, since
    /// a partially-written multipart upload cannot be safely resumed; the client
    /// must restart the upload.
    pub async fn put_part(&self, id: Uuid, part_number: u32, data: Bytes) -> Result<(), FileError> {
        let session = self.session(id)?;
        let mut guard = session.lock().await;

        let expected = guard.accepted_parts + 1;
        // A resend of an already-accepted part (e.g. a lost response) is a no-op.
        if part_number <= guard.accepted_parts {
            guard.last_active = Instant::now();
            return Ok(());
        }
        if part_number != expected {
            return Err(FileError::PartOutOfOrder {
                got: part_number,
                expected,
            });
        }

        let new_total = guard.total + data.len() as u64;
        if guard.max_bytes != 0 && new_total > guard.max_bytes {
            let limit = guard.max_bytes;
            drop(guard);
            self.abort(id).await.ok();
            return Err(FileError::TooLarge { limit });
        }

        let upload = guard
            .upload
            .as_mut()
            .ok_or_else(|| FileError::UnknownUpload(id.to_string()))?;
        if let Err(e) = upload.put_part(data.into()).await {
            // The multipart upload is now in an indeterminate state; tear it down.
            drop(guard);
            self.abort(id).await.ok();
            return Err(e.into());
        }

        guard.total = new_total;
        guard.accepted_parts += 1;
        guard.last_active = Instant::now();
        Ok(())
    }

    /// Finalize the upload, returning the resulting object's key and size.
    pub async fn complete(&self, id: Uuid) -> Result<UploadResult, FileError> {
        let session = self.remove(id)?;
        let mut guard = session.lock().await;
        let mut upload = guard
            .upload
            .take()
            .ok_or_else(|| FileError::UnknownUpload(id.to_string()))?;
        upload.complete().await?;
        Ok(UploadResult {
            path: guard.path.to_string(),
            size: guard.total,
        })
    }

    /// Abort and discard an in-progress upload, cleaning up object-store state.
    pub async fn abort(&self, id: Uuid) -> Result<(), FileError> {
        let session = self.remove(id)?;
        let mut guard = session.lock().await;
        if let Some(mut upload) = guard.upload.take() {
            upload.abort().await?;
        }
        Ok(())
    }

    fn session(&self, id: Uuid) -> Result<Arc<tokio::sync::Mutex<UploadSession>>, FileError> {
        self.sessions
            .lock()
            .get(&id)
            .cloned()
            .ok_or_else(|| FileError::UnknownUpload(id.to_string()))
    }

    fn remove(&self, id: Uuid) -> Result<Arc<tokio::sync::Mutex<UploadSession>>, FileError> {
        self.sessions
            .lock()
            .remove(&id)
            .ok_or_else(|| FileError::UnknownUpload(id.to_string()))
    }

    /// Spawn the background task that aborts sessions idle longer than `ttl`. It
    /// stops once the manager is dropped (the `Weak` no longer upgrades).
    fn spawn_sweeper(manager: Weak<UploadManager>, ttl: Duration) {
        let interval = ttl
            .min(MIN_SWEEP_INTERVAL.max(ttl / 4))
            .max(MIN_SWEEP_INTERVAL);
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(interval).await;
                let Some(manager) = manager.upgrade() else {
                    return; // manager (and its runtime) is gone
                };
                manager.sweep_expired().await;
            }
        });
    }

    /// Abort every session whose last activity is older than the TTL.
    async fn sweep_expired(&self) {
        let now = Instant::now();
        let expired: Vec<Uuid> = {
            let sessions = self.sessions.lock();
            let mut ids = Vec::new();
            for (id, session) in sessions.iter() {
                // try_lock avoids blocking on a session mid-part; a busy session is
                // by definition active, so skipping it is correct.
                if let Ok(guard) = session.try_lock() {
                    if now.duration_since(guard.last_active) > self.ttl {
                        ids.push(*id);
                    }
                }
            }
            ids
        };
        for id in expired {
            if let Err(e) = self.abort(id).await {
                tracing::debug!(%id, error = %e, "failed to abort expired upload session");
            } else {
                tracing::info!(%id, "aborted idle upload session");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use beacon_object_storage::datasets_store::local_datasets_store;

    async fn store() -> (tempfile::TempDir, DatasetsStore) {
        let dir = tempfile::tempdir().expect("temp dir");
        let store = local_datasets_store(dir.path().to_path_buf())
            .await
            .expect("store");
        (dir, store)
    }

    #[tokio::test]
    async fn chunked_round_trip() {
        let (_dir, store) = store().await;
        let mgr = UploadManager::new(0, 0);
        let path = Path::from("big/data.parquet");

        let id = mgr.initiate(&store, path.clone(), 0).await.unwrap();
        mgr.put_part(id, 1, Bytes::from_static(b"aaaa"))
            .await
            .unwrap();
        mgr.put_part(id, 2, Bytes::from_static(b"bbbb"))
            .await
            .unwrap();
        let res = mgr.complete(id).await.unwrap();
        assert_eq!(res.size, 8);
        assert_eq!(res.path, "big/data.parquet");

        let got = store.get(&path).await.unwrap().bytes().await.unwrap();
        assert_eq!(got.as_ref(), b"aaaabbbb");
    }

    #[tokio::test]
    async fn resent_part_is_idempotent() {
        let (_dir, store) = store().await;
        let mgr = UploadManager::new(0, 0);
        let id = mgr
            .initiate(&store, Path::from("a.parquet"), 0)
            .await
            .unwrap();
        mgr.put_part(id, 1, Bytes::from_static(b"one"))
            .await
            .unwrap();
        // Re-send part 1 (e.g. a lost response): accepted as a no-op.
        mgr.put_part(id, 1, Bytes::from_static(b"one"))
            .await
            .unwrap();
        mgr.put_part(id, 2, Bytes::from_static(b"two"))
            .await
            .unwrap();
        let res = mgr.complete(id).await.unwrap();
        assert_eq!(res.size, 6);
    }

    #[tokio::test]
    async fn out_of_order_part_is_rejected() {
        let (_dir, store) = store().await;
        let mgr = UploadManager::new(0, 0);
        let id = mgr
            .initiate(&store, Path::from("a.parquet"), 0)
            .await
            .unwrap();
        mgr.put_part(id, 1, Bytes::from_static(b"one"))
            .await
            .unwrap();
        // Skipping part 2 → 3 is a gap.
        assert!(matches!(
            mgr.put_part(id, 3, Bytes::from_static(b"three")).await,
            Err(FileError::PartOutOfOrder {
                got: 3,
                expected: 2
            })
        ));
    }

    #[tokio::test]
    async fn unknown_session_errors() {
        let mgr = UploadManager::new(0, 0);
        let id = Uuid::new_v4();
        assert!(matches!(
            mgr.put_part(id, 1, Bytes::from_static(b"x")).await,
            Err(FileError::UnknownUpload(_))
        ));
        assert!(matches!(
            mgr.complete(id).await,
            Err(FileError::UnknownUpload(_))
        ));
    }

    #[tokio::test]
    async fn cap_is_enforced_across_parts() {
        let (_dir, store) = store().await;
        let mgr = UploadManager::new(0, 0);
        let id = mgr
            .initiate(&store, Path::from("a.parquet"), 5)
            .await
            .unwrap();
        mgr.put_part(id, 1, Bytes::from_static(b"abc"))
            .await
            .unwrap();
        // 3 + 4 = 7 > cap of 5 → rejected, session torn down.
        assert!(matches!(
            mgr.put_part(id, 2, Bytes::from_static(b"defg")).await,
            Err(FileError::TooLarge { .. })
        ));
        assert!(matches!(
            mgr.complete(id).await,
            Err(FileError::UnknownUpload(_))
        ));
    }
}
