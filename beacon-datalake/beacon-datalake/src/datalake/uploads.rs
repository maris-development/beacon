//! In-flight chunked uploads.
//!
//! A chunked upload is a multipart write held open across several HTTP requests,
//! so the session state lives here, on the lake that owns the store. Sessions are
//! in memory only and deliberately so: the object store's multipart upload dies
//! with the process anyway, so persisting the bookkeeping would only let a client
//! resume into an upload that no longer exists.
//!
//! Expiry is swept lazily on every operation rather than by a background task —
//! there is no work to do while nothing is uploading.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use bytes::Bytes;
use object_store::{path::Path, ObjectStore, ObjectStoreExt, WriteMultipart};
use tokio::sync::Mutex;
use uuid::Uuid;

use super::files::{FileError, UploadResult};

/// Part size advertised to clients. Large enough that a multi-gigabyte file needs
/// a sane number of round trips, small enough to retry cheaply.
pub const PART_SIZE: usize = 32 * 1024 * 1024;

/// How long a session may sit idle before it is swept.
const SESSION_TTL: Duration = Duration::from_secs(60 * 60);

struct Session {
    path: Path,
    writer: WriteMultipart,
    /// The part number expected next. Parts must arrive in order, so a gap is an
    /// error rather than a silently corrupt object.
    next_part: u32,
    total: u64,
    max_bytes: u64,
    touched: Instant,
}

/// The lake's in-flight chunked uploads.
#[derive(Default)]
pub struct UploadSessions {
    sessions: Mutex<HashMap<Uuid, Session>>,
}

impl UploadSessions {
    /// Open a multipart write and return its session id.
    pub async fn initiate(
        &self,
        store: &dyn ObjectStore,
        path: Path,
        max_bytes: u64,
    ) -> Result<Uuid, FileError> {
        let writer = WriteMultipart::new(store.put_multipart(&path).await?);
        let id = Uuid::new_v4();

        let mut sessions = self.sessions.lock().await;
        sweep(&mut sessions);
        sessions.insert(
            id,
            Session {
                path,
                writer,
                next_part: 1,
                total: 0,
                max_bytes,
                touched: Instant::now(),
            },
        );
        Ok(id)
    }

    /// Append one part. Resending the part just written is not supported — a part
    /// is consumed once accepted — so `part_number` must be exactly the next one.
    pub async fn put_part(
        &self,
        id: Uuid,
        part_number: u32,
        bytes: Bytes,
    ) -> Result<(), FileError> {
        let mut sessions = self.sessions.lock().await;
        sweep(&mut sessions);
        let session = sessions
            .get_mut(&id)
            .ok_or_else(|| FileError::UnknownUpload(id.to_string()))?;

        if part_number != session.next_part {
            return Err(FileError::PartOutOfOrder {
                got: part_number,
                expected: session.next_part,
            });
        }

        let total = session.total + bytes.len() as u64;
        if session.max_bytes != 0 && total > session.max_bytes {
            // Drop the session and its parts: the upload can never succeed.
            let mut session = sessions.remove(&id).expect("session was just borrowed");
            let _ = session.writer.abort().await;
            return Err(FileError::TooLarge {
                limit: session.max_bytes,
            });
        }

        session.writer.put(bytes);
        session.total = total;
        session.next_part += 1;
        session.touched = Instant::now();
        Ok(())
    }

    /// Finish the upload, assembling the parts into the destination object.
    pub async fn complete(&self, id: Uuid) -> Result<UploadResult, FileError> {
        let session = {
            let mut sessions = self.sessions.lock().await;
            sweep(&mut sessions);
            sessions
                .remove(&id)
                .ok_or_else(|| FileError::UnknownUpload(id.to_string()))?
        };

        session.writer.finish().await?;
        Ok(UploadResult {
            path: session.path.to_string(),
            size: session.total,
        })
    }

    /// Discard an in-progress upload and the parts already written.
    pub async fn abort(&self, id: Uuid) -> Result<(), FileError> {
        let mut session = {
            let mut sessions = self.sessions.lock().await;
            sweep(&mut sessions);
            sessions
                .remove(&id)
                .ok_or_else(|| FileError::UnknownUpload(id.to_string()))?
        };
        session.writer.abort().await?;
        Ok(())
    }
}

/// Drop sessions idle past the TTL. Their multipart uploads are left to the
/// store's own expiry: aborting here would need an await under the lock.
fn sweep(sessions: &mut HashMap<Uuid, Session>) {
    sessions.retain(|_, session| session.touched.elapsed() < SESSION_TTL);
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;
    use std::sync::Arc;

    fn store() -> Arc<dyn ObjectStore> {
        Arc::new(InMemory::new())
    }

    #[tokio::test]
    async fn parts_assemble_in_order() {
        let store = store();
        let sessions = UploadSessions::default();
        let id = sessions
            .initiate(store.as_ref(), Path::from("a/b.parquet"), 0)
            .await
            .unwrap();

        sessions
            .put_part(id, 1, Bytes::from_static(b"hello "))
            .await
            .unwrap();
        sessions
            .put_part(id, 2, Bytes::from_static(b"world"))
            .await
            .unwrap();
        let result = sessions.complete(id).await.unwrap();
        assert_eq!(result.size, 11);

        let stored = store
            .get(&Path::from("a/b.parquet"))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(stored.as_ref(), b"hello world");
    }

    #[tokio::test]
    async fn a_gap_in_part_numbers_is_rejected() {
        let store = store();
        let sessions = UploadSessions::default();
        let id = sessions
            .initiate(store.as_ref(), Path::from("a/b.parquet"), 0)
            .await
            .unwrap();

        assert!(matches!(
            sessions.put_part(id, 2, Bytes::from_static(b"x")).await,
            Err(FileError::PartOutOfOrder {
                got: 2,
                expected: 1
            })
        ));
    }

    /// The cap is enforced across parts, so a client cannot exceed it by slicing
    /// the file more finely.
    #[tokio::test]
    async fn the_size_cap_spans_parts() {
        let store = store();
        let sessions = UploadSessions::default();
        let id = sessions
            .initiate(store.as_ref(), Path::from("a/b.parquet"), 4)
            .await
            .unwrap();

        sessions
            .put_part(id, 1, Bytes::from_static(b"abc"))
            .await
            .unwrap();
        assert!(matches!(
            sessions.put_part(id, 2, Bytes::from_static(b"def")).await,
            Err(FileError::TooLarge { limit: 4 })
        ));
        // The session is gone: an upload that blew the cap cannot be completed.
        assert!(matches!(
            sessions.complete(id).await,
            Err(FileError::UnknownUpload(_))
        ));
    }

    #[tokio::test]
    async fn unknown_sessions_are_reported_not_ignored() {
        let sessions = UploadSessions::default();
        let id = Uuid::new_v4();
        assert!(matches!(
            sessions.complete(id).await,
            Err(FileError::UnknownUpload(_))
        ));
        assert!(matches!(
            sessions.abort(id).await,
            Err(FileError::UnknownUpload(_))
        ));
    }
}
