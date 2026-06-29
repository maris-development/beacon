//! Admin dataset file management: path safety plus upload / download / delete
//! over the datasets store.
//!
//! These operations let a super-user get files in and out of the datasets store
//! through the admin API. File mutation is the highest-risk surface in the
//! product, so the safety checks here are deliberately strict and auditable:
//!
//! - [`validate_dataset_path`] is the single anti-traversal gate. Every operation
//!   routes its user-supplied path through it before touching the store.
//! - Uploads are streamed (never buffered whole), size-capped, and confined to an
//!   extension allowlist derived from the formats Beacon can actually read.
//! - Nothing here can read, write, or delete under the Beacon-internal
//!   [`DATASETS_WRITEABLE_PREFIX`]; that area is owned by Beacon's own machinery.
//!
//! The thin wrappers operate on a [`DatasetsStore`]; the runtime layers the
//! catalog-aware delete dependency check on top (see [`crate::runtime`]).

use beacon_object_storage::{DatasetsStore, DATASETS_WRITEABLE_PREFIX};
use bytes::Bytes;
use futures::{Stream, StreamExt};
use object_store::{path::Path, GetResult, ObjectStoreExt, WriteMultipart};

/// Failure modes for dataset file management, each mapping to a distinct HTTP
/// status at the API boundary.
#[derive(Debug, thiserror::Error)]
pub enum FileError {
    /// The supplied path was empty, absolute, contained `.`/`..` or other illegal
    /// characters, or resolved into the Beacon-internal prefix. → 400.
    #[error("invalid dataset path: {0}")]
    InvalidPath(String),
    /// The filename extension is not in the allowlist of readable formats. → 400.
    #[error("unsupported file extension '{0}'")]
    UnsupportedExtension(String),
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
/// - anything under the Beacon-internal [`DATASETS_WRITEABLE_PREFIX`].
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
            "path '{path}' is reserved for Beacon internal use"
        )));
    }
    Ok(path)
}

/// Whether the path's first segment is the Beacon-internal writeable prefix.
fn is_internal(path: &Path) -> bool {
    path.parts()
        .next()
        .is_some_and(|first| first.as_ref() == DATASETS_WRITEABLE_PREFIX)
}

/// Check the file's extension against an allowlist (case-insensitive).
///
/// `allowed` is the set of extensions Beacon's registered formats recognize, so
/// an upload can only land if the engine could subsequently read it.
pub fn validate_extension(path: &Path, allowed: &[String]) -> Result<(), FileError> {
    let ext = path
        .filename()
        .and_then(|name| name.rsplit_once('.'))
        .map(|(_, ext)| ext.to_ascii_lowercase());
    match ext {
        Some(ext) if allowed.iter().any(|a| a.eq_ignore_ascii_case(&ext)) => Ok(()),
        Some(ext) => Err(FileError::UnsupportedExtension(ext)),
        None => Err(FileError::UnsupportedExtension("(none)".into())),
    }
}

/// Stream `body` into the datasets store at the (already validated) `path`.
///
/// The body is streamed through a multipart writer, so memory stays bounded
/// regardless of file size. `max_bytes` caps the total (`0` = unlimited); on
/// exceed the partial upload is aborted and [`FileError::TooLarge`] returned.
/// When `overwrite` is false, an existing object at `path` is rejected rather
/// than clobbered.
pub async fn upload_dataset<S>(
    store: &DatasetsStore,
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
/// (including anything hidden under the internal prefix) maps to
/// [`FileError::NotFound`].
pub async fn download_dataset(store: &DatasetsStore, path: &Path) -> Result<GetResult, FileError> {
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
pub async fn delete_dataset(store: &DatasetsStore, path: &Path) -> Result<(), FileError> {
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
    use beacon_object_storage::datasets_store::local_datasets_store;
    use futures::stream;

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
            "a\\b",
            "__beacon__/iceberg/x",
            "__beacon__",
        ] {
            assert!(
                validate_dataset_path(bad).is_err(),
                "expected rejection for {bad:?}"
            );
        }
    }

    #[test]
    fn accepts_normal_nested_paths() {
        let path = validate_dataset_path("ctd/cruise42/station1.nc").expect("valid");
        assert_eq!(path.to_string(), "ctd/cruise42/station1.nc");
        // A sibling of the internal prefix is fine — only an exact first segment
        // match is reserved.
        assert!(validate_dataset_path("__beacon__extra/data.parquet").is_ok());
    }

    #[test]
    fn extension_allowlist_is_case_insensitive() {
        let allowed = vec!["nc".to_string(), "parquet".to_string()];
        let ok = validate_dataset_path("a/b.NC").unwrap();
        assert!(validate_extension(&ok, &allowed).is_ok());
        let bad = validate_dataset_path("a/b.exe").unwrap();
        assert!(matches!(
            validate_extension(&bad, &allowed),
            Err(FileError::UnsupportedExtension(_))
        ));
        let none = validate_dataset_path("a/README").unwrap();
        assert!(validate_extension(&none, &allowed).is_err());
    }

    #[tokio::test]
    async fn upload_download_delete_round_trip() {
        let dir = tempfile::tempdir().expect("temp dir");
        let store = local_datasets_store(dir.path().to_path_buf())
            .await
            .expect("store");
        let path = validate_dataset_path("a/b.parquet").unwrap();

        let body = stream::iter(vec![
            Ok(Bytes::from_static(b"hello ")),
            Ok(Bytes::from_static(b"world")),
        ]);
        let res = upload_dataset(&store, &path, false, 0, body)
            .await
            .expect("upload");
        assert_eq!(res.size, 11);
        assert_eq!(res.path, "a/b.parquet");

        let got = download_dataset(&store, &path)
            .await
            .expect("download")
            .bytes()
            .await
            .expect("bytes");
        assert_eq!(got.as_ref(), b"hello world");

        delete_dataset(&store, &path).await.expect("delete");
        assert!(matches!(
            download_dataset(&store, &path).await,
            Err(FileError::NotFound(_))
        ));
    }

    #[tokio::test]
    async fn upload_rejects_existing_without_overwrite() {
        let dir = tempfile::tempdir().expect("temp dir");
        let store = local_datasets_store(dir.path().to_path_buf())
            .await
            .expect("store");
        let path = validate_dataset_path("x.parquet").unwrap();

        let first = stream::iter(vec![Ok(Bytes::from_static(b"one"))]);
        upload_dataset(&store, &path, false, 0, first)
            .await
            .expect("first upload");

        let second = stream::iter(vec![Ok(Bytes::from_static(b"two"))]);
        assert!(matches!(
            upload_dataset(&store, &path, false, 0, second).await,
            Err(FileError::AlreadyExists(_))
        ));

        // With overwrite, it succeeds and replaces the content.
        let third = stream::iter(vec![Ok(Bytes::from_static(b"three"))]);
        upload_dataset(&store, &path, true, 0, third)
            .await
            .expect("overwrite upload");
        let got = download_dataset(&store, &path)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(got.as_ref(), b"three");
    }

    #[tokio::test]
    async fn upload_enforces_size_cap() {
        let dir = tempfile::tempdir().expect("temp dir");
        let store = local_datasets_store(dir.path().to_path_buf())
            .await
            .expect("store");
        let path = validate_dataset_path("big.parquet").unwrap();

        let body = stream::iter(vec![
            Ok(Bytes::from_static(b"aaaa")),
            Ok(Bytes::from_static(b"bbbb")),
        ]);
        // Cap of 5 bytes, payload is 8.
        assert!(matches!(
            upload_dataset(&store, &path, false, 5, body).await,
            Err(FileError::TooLarge { limit: 5 })
        ));
        // The aborted upload left nothing behind.
        assert!(matches!(
            download_dataset(&store, &path).await,
            Err(FileError::NotFound(_))
        ));
    }

    #[tokio::test]
    async fn delete_missing_is_not_found() {
        let dir = tempfile::tempdir().expect("temp dir");
        let store = local_datasets_store(dir.path().to_path_buf())
            .await
            .expect("store");
        let path = validate_dataset_path("nope.parquet").unwrap();
        assert!(matches!(
            delete_dataset(&store, &path).await,
            Err(FileError::NotFound(_))
        ));
    }
}
