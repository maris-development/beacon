//! Dataset file management: path safety plus upload / download / delete over the
//! data lake's object store.
//!
//! These operations are how files get into and out of the lake. File mutation is
//! the highest-risk surface in the product, so the safety checks here are
//! deliberately strict and auditable:
//!
//! - [`validate_dataset_path`] is the single anti-traversal gate. Every operation
//!   routes its user-supplied path through it before touching the store.
//! - Uploads are streamed (never buffered whole) and size-capped. The file *type*
//!   is deliberately not restricted: a data lake holds whatever the operator puts
//!   in it, and beacon gains readers over time, so an extension allowlist would
//!   only block files that are legitimately there.
//! - Nothing here can read, write, or delete under [`INTERNAL_PREFIX`]; that area
//!   is owned by beacon's own machinery.
//!
//! These operate on the store directly. Anything catalog-aware — refusing to
//! delete a file a table still depends on — belongs to the caller, which can see
//! the catalog through the runtime.

use bytes::Bytes;
use futures::{Stream, StreamExt};
use object_store::{path::Path, GetResult, ObjectStore, ObjectStoreExt, WriteMultipart};

/// Prefix beacon reserves inside the datasets store for locally produced data.
/// Listing, reads and writes through this module are blocked under it.
pub const INTERNAL_PREFIX: &str = "__beacon__";

/// Failure modes for dataset file management, each mapping to a distinct HTTP
/// status at the API boundary.
#[derive(Debug, thiserror::Error)]
pub enum FileError {
    /// The supplied path was empty, absolute, contained `.`/`..` or other illegal
    /// characters, or resolved into the beacon-internal prefix. → 400.
    #[error("invalid dataset path: {0}")]
    InvalidPath(String),
    /// A file already exists at the destination and `overwrite` was not set. → 409.
    #[error("a file already exists at '{0}'; pass overwrite=true to replace it")]
    AlreadyExists(String),
    /// The streamed upload exceeded the configured size cap. → 413.
    #[error("upload exceeds the maximum allowed size of {limit} bytes")]
    TooLarge { limit: u64 },
    /// No object exists at the requested path. → 404.
    #[error("dataset not found: {0}")]
    NotFound(String),
    /// The dataset is referenced by one or more tables/crawlers. → 409.
    #[error("dataset '{path}' is in use by: {}", dependents.join(", "))]
    InUse {
        path: String,
        dependents: Vec<String>,
    },
    /// The referenced chunked-upload session does not exist (unknown id, or it
    /// already completed/expired). → 404.
    #[error("unknown or expired upload session: {0}")]
    UnknownUpload(String),
    /// A chunked-upload part arrived out of order (a gap relative to the next
    /// expected part). → 409.
    #[error("upload part {got} is out of order; expected part {expected}")]
    PartOutOfOrder { got: u32, expected: u32 },
    /// An error reading the request body stream. → 400.
    #[error("error reading upload stream: {0}")]
    Body(std::io::Error),
    /// An underlying object-store failure. → 500.
    #[error(transparent)]
    Storage(#[from] object_store::Error),
}

/// Result of a successful upload.
#[derive(Debug, Clone, serde::Serialize, utoipa::ToSchema)]
pub struct UploadResult {
    /// The normalized object key the file was written to.
    pub path: String,
    /// The number of bytes written.
    pub size: u64,
}

/// Validate and normalize a user-supplied dataset path into an object-store key.
///
/// This is the anti-traversal gate. It rejects, before any normalization:
/// - empty paths,
/// - absolute paths (leading `/` or `\`),
/// - NUL or backslash characters,
/// - any `.` or `..` segment (object_store would percent-encode these into odd
///   keys rather than error; we reject them outright for a clean, auditable
///   failure and to forbid traversal-shaped input),
/// - paths that resolve to the datasets root, and
/// - anything under [`INTERNAL_PREFIX`].
pub fn validate_dataset_path(input: &str) -> Result<Path, FileError> {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        return Err(FileError::InvalidPath("path is empty".into()));
    }
    if trimmed.starts_with('/') || trimmed.starts_with('\\') {
        return Err(FileError::InvalidPath("path must be relative".into()));
    }
    if trimmed.contains('\0') || trimmed.contains('\\') {
        return Err(FileError::InvalidPath(
            "path contains illegal characters".into(),
        ));
    }
    for segment in trimmed.split('/') {
        // Tolerate accidental double slashes (empty segments) but never dot
        // segments — those are the traversal vector.
        if segment == "." || segment == ".." {
            return Err(FileError::InvalidPath(
                "path may not contain '.' or '..' segments".into(),
            ));
        }
    }

    // object_store applies its own per-segment validation (and would otherwise
    // percent-encode anything surprising); a parse error here is a clean reject.
    let path = Path::parse(trimmed).map_err(|e| FileError::InvalidPath(e.to_string()))?;

    if path.parts().next().is_none() {
        return Err(FileError::InvalidPath(
            "path resolves to the datasets root".into(),
        ));
    }
    if is_internal(&path) {
        return Err(FileError::InvalidPath(format!(
            "path '{path}' is reserved for beacon internal use"
        )));
    }
    Ok(path)
}

/// Whether the path's first segment is the beacon-internal prefix.
fn is_internal(path: &Path) -> bool {
    path.parts()
        .next()
        .is_some_and(|first| first.as_ref() == INTERNAL_PREFIX)
}

/// Stream `body` into the store at the (already validated) `path`.
///
/// The body is streamed through a multipart writer, so memory stays bounded
/// regardless of file size. `max_bytes` caps the total (`0` = unlimited); on
/// exceed the partial upload is aborted and [`FileError::TooLarge`] returned.
/// When `overwrite` is false, an existing object at `path` is rejected rather
/// than clobbered.
pub async fn upload_dataset<S>(
    store: &dyn ObjectStore,
    path: &Path,
    overwrite: bool,
    max_bytes: u64,
    mut body: S,
) -> Result<UploadResult, FileError>
where
    S: Stream<Item = Result<Bytes, std::io::Error>> + Unpin,
{
    if !overwrite {
        match store.head(path).await {
            Ok(_) => return Err(FileError::AlreadyExists(path.to_string())),
            Err(object_store::Error::NotFound { .. }) => {}
            Err(e) => return Err(e.into()),
        }
    }

    let mut writer = WriteMultipart::new(store.put_multipart(path).await?);
    let mut total: u64 = 0;
    while let Some(chunk) = body.next().await {
        let chunk = chunk.map_err(FileError::Body)?;
        total += chunk.len() as u64;
        if max_bytes != 0 && total > max_bytes {
            // Best-effort cleanup of already-uploaded parts before failing.
            let _ = writer.abort().await;
            return Err(FileError::TooLarge { limit: max_bytes });
        }
        writer.put(chunk);
    }
    writer.finish().await?;

    Ok(UploadResult {
        path: path.to_string(),
        size: total,
    })
}

/// Open a streaming read of the dataset at the (already validated) `path`.
///
/// The caller streams the returned [`GetResult`] to the client. A missing object
/// maps to [`FileError::NotFound`].
pub async fn download_dataset(store: &dyn ObjectStore, path: &Path) -> Result<GetResult, FileError> {
    match store.get(path).await {
        Ok(result) => Ok(result),
        Err(object_store::Error::NotFound { .. }) => Err(FileError::NotFound(path.to_string())),
        Err(e) => Err(e.into()),
    }
}

/// Delete the dataset at the (already validated) `path`.
///
/// Existence is checked first so a missing file is a clean [`FileError::NotFound`]
/// rather than a silent success. The caller is responsible for any dependency
/// check (e.g. tables backed by this file) before invoking this.
pub async fn delete_dataset(store: &dyn ObjectStore, path: &Path) -> Result<(), FileError> {
    match store.head(path).await {
        Ok(_) => {}
        Err(object_store::Error::NotFound { .. }) => {
            return Err(FileError::NotFound(path.to_string()))
        }
        Err(e) => return Err(e.into()),
    }
    store.delete(path).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::stream;
    use object_store::memory::InMemory;
    use std::sync::Arc;

    fn store() -> Arc<dyn ObjectStore> {
        Arc::new(InMemory::new())
    }

    fn body(bytes: &'static [u8]) -> impl Stream<Item = Result<Bytes, std::io::Error>> + Unpin {
        stream::iter(vec![Ok(Bytes::from_static(bytes))])
    }

    #[test]
    fn rejects_traversal_and_absolute_and_internal_paths() {
        for bad in [
            "",
            "   ",
            "/etc/passwd",
            "\\windows\\system32",
            "a/../b",
            "../secret",
            "./hidden",
            "a/./b",
            "foo\0bar",
            "__beacon__/anything.parquet",
        ] {
            assert!(
                validate_dataset_path(bad).is_err(),
                "expected `{bad}` to be rejected"
            );
        }
    }

    #[test]
    fn accepts_normal_relative_paths() {
        let path = validate_dataset_path("argo/floats/a.parquet").unwrap();
        assert_eq!(path.to_string(), "argo/floats/a.parquet");
    }

    #[tokio::test]
    async fn upload_download_delete_round_trip() {
        let store = store();
        let path = validate_dataset_path("a/b.parquet").unwrap();

        let result = upload_dataset(store.as_ref(), &path, false, 0, body(b"hello"))
            .await
            .expect("upload should succeed");
        assert_eq!(result.size, 5);
        assert_eq!(result.path, "a/b.parquet");

        let got = download_dataset(store.as_ref(), &path)
            .await
            .expect("download should succeed")
            .bytes()
            .await
            .unwrap();
        assert_eq!(got.as_ref(), b"hello");

        delete_dataset(store.as_ref(), &path)
            .await
            .expect("delete should succeed");
        assert!(matches!(
            download_dataset(store.as_ref(), &path).await,
            Err(FileError::NotFound(_))
        ));
    }

    #[tokio::test]
    async fn upload_refuses_to_clobber_unless_overwrite() {
        let store = store();
        let path = validate_dataset_path("a/b.parquet").unwrap();
        upload_dataset(store.as_ref(), &path, false, 0, body(b"first"))
            .await
            .unwrap();

        assert!(matches!(
            upload_dataset(store.as_ref(), &path, false, 0, body(b"second")).await,
            Err(FileError::AlreadyExists(_))
        ));

        upload_dataset(store.as_ref(), &path, true, 0, body(b"second"))
            .await
            .expect("overwrite should replace the object");
    }

    /// The size cap is enforced against the streamed total, not a declared
    /// content length, so an oversized body fails even without one.
    #[tokio::test]
    async fn upload_enforces_the_size_cap() {
        let store = store();
        let path = validate_dataset_path("a/big.parquet").unwrap();
        assert!(matches!(
            upload_dataset(store.as_ref(), &path, false, 3, body(b"hello")).await,
            Err(FileError::TooLarge { limit: 3 })
        ));
    }

    #[tokio::test]
    async fn delete_of_a_missing_object_is_not_found() {
        let store = store();
        let path = validate_dataset_path("a/missing.parquet").unwrap();
        assert!(matches!(
            delete_dataset(store.as_ref(), &path).await,
            Err(FileError::NotFound(_))
        ));
    }
}
