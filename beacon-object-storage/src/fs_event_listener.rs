//! Filesystem [`EventListener`] backed by the [`notify`] crate.
//!
//! Watches the local datasets directory and translates filesystem
//! create/modify/remove notifications into [`ObjectEvent`]s. The
//! [`DatasetsStore`](crate::DatasetsStore) poll task drains these to keep its
//! in-memory cache warm and to fan changes out to prefix subscribers.
//!
//! Enabled by default for the local filesystem backend; controlled by the
//! `storage.enable_fs_events` Beacon config setting (env `BEACON_ENABLE_FS_EVENTS`).
//!
//! Translation (including the blocking `stat` of created/modified files) happens
//! on the `notify` watcher thread, so [`FsEventListener::poll`] only ever awaits
//! a channel and never blocks the async runtime.

use std::path::{Path as StdPath, PathBuf};

use futures::future::BoxFuture;
use notify::{EventKind, RecommendedWatcher, RecursiveMode, Watcher, recommended_watcher};
use object_store::{ObjectMeta, path::Path};

use crate::{
    datasets_store::EventListener,
    error::{StorageError, StorageResult},
    event::ObjectEvent,
};

/// An [`EventListener`] that reports filesystem changes under a watched root.
pub struct FsEventListener {
    /// Batches of translated events produced on the `notify` watcher thread.
    rx: flume::Receiver<Vec<ObjectEvent>>,
    /// The live watcher. Kept alive for the lifetime of the listener; dropping it
    /// stops the watch and closes `rx`.
    _watcher: RecommendedWatcher,
}

impl FsEventListener {
    /// Start watching `root` recursively for filesystem changes.
    ///
    /// `root` must be the directory the local object store is rooted at, so that
    /// emitted object paths are relative to it.
    pub fn new(root: PathBuf) -> StorageResult<Self> {
        // Canonicalize once so we can translate absolute event paths back to
        // store-relative object paths. Files reported by delete events may no
        // longer exist, so we avoid canonicalizing individual files here.
        let abs_root = std::fs::canonicalize(&root).map_err(|e| {
            StorageError::InitializationError(format!(
                "Failed to canonicalize datasets directory {root:?}: {e}"
            ))
        })?;

        let (tx, rx) = flume::unbounded::<Vec<ObjectEvent>>();

        let mut watcher =
            recommended_watcher(move |res: notify::Result<notify::Event>| match res {
                Ok(event) => {
                    // Chatty in production; keep at debug level.
                    tracing::debug!("Filesystem event: {:?}", event);
                    let events = translate_event(&abs_root, event);
                    if !events.is_empty() {
                        // A send error just means the listener was dropped.
                        let _ = tx.send(events);
                    }
                }
                Err(e) => tracing::error!("Error receiving filesystem event: {e}"),
            })
            .map_err(|e| {
                StorageError::InitializationError(format!(
                    "Failed to create filesystem watcher for {root:?}: {e}"
                ))
            })?;

        watcher
            .watch(&root, RecursiveMode::Recursive)
            .map_err(|e| {
                StorageError::InitializationError(format!("Failed to watch path {root:?}: {e}"))
            })?;

        tracing::info!("Filesystem event listener watching {:?}", root);
        Ok(Self {
            rx,
            _watcher: watcher,
        })
    }
}

impl EventListener for FsEventListener {
    fn poll(&self) -> BoxFuture<'_, Vec<ObjectEvent>> {
        Box::pin(async move {
            match self.rx.recv_async().await {
                Ok(events) => events,
                // Channel closed (watcher gone): park so the poll loop suspends
                // instead of spinning. In practice the owning store aborts the
                // poll task on drop before this is reached.
                Err(_) => std::future::pending().await,
            }
        })
    }
}

/// Translate a single `notify` event into object events.
///
/// Unsupported event kinds (e.g. access/metadata-only) produce no events.
fn translate_event(abs_root: &StdPath, event: notify::Event) -> Vec<ObjectEvent> {
    match event.kind {
        EventKind::Create(_) => event
            .paths
            .iter()
            .filter_map(|path| meta_for_path(abs_root, path))
            .map(ObjectEvent::Created)
            .collect(),
        EventKind::Modify(_) => event
            .paths
            .iter()
            .filter_map(|path| meta_for_path(abs_root, path))
            .map(ObjectEvent::Modified)
            .collect(),
        EventKind::Remove(_) => event
            .paths
            .iter()
            // The file is gone, so only translate its location (no `stat`).
            .filter_map(|path| location_for_path(abs_root, path))
            .map(ObjectEvent::Deleted)
            .collect(),
        _ => Vec::new(),
    }
}

/// Translate an absolute filesystem path to its store-relative object location,
/// logging and dropping the path on failure.
fn location_for_path(abs_root: &StdPath, file_path: &StdPath) -> Option<Path> {
    match object_location_for_path(abs_root, file_path) {
        Ok(location) => Some(location),
        Err(e) => {
            tracing::error!("Error translating path {file_path:?} to object location: {e}");
            None
        }
    }
}

/// Build `ObjectMeta` for an existing file, logging and dropping on failure.
///
/// Returns `None` when the file no longer exists or is a directory.
fn meta_for_path(abs_root: &StdPath, file_path: &StdPath) -> Option<ObjectMeta> {
    match object_meta_for_path(abs_root, file_path) {
        Ok(meta) => meta,
        Err(e) => {
            tracing::error!("Error translating path {file_path:?} to object meta: {e}");
            None
        }
    }
}

fn object_location_for_path(abs_root: &StdPath, file_path: &StdPath) -> StorageResult<Path> {
    // Prefer the reported path directly (works even if the file is gone). Fall
    // back to canonicalization for paths containing `..` segments.
    let rel_path = match file_path.strip_prefix(abs_root) {
        Ok(rel) => rel.to_path_buf(),
        Err(_) => {
            let abs_file_path = std::fs::canonicalize(file_path).map_err(|e| {
                StorageError::InitializationError(format!(
                    "Failed to canonicalize file path {file_path:?}: {e}"
                ))
            })?;
            abs_file_path
                .strip_prefix(abs_root)
                .map_err(|e| {
                    StorageError::InitializationError(format!(
                        "Failed to get relative path for {abs_file_path:?} with base {abs_root:?}: {e}"
                    ))
                })?
                .to_path_buf()
        }
    };

    // Build the object path from components so the platform's path separator
    // (`\` on Windows) becomes the object-store delimiter `/` rather than being
    // percent-encoded into a single segment.
    Ok(Path::from_iter(rel_path.components().filter_map(
        |component| match component {
            std::path::Component::Normal(segment) => segment.to_str(),
            _ => None,
        },
    )))
}

fn object_meta_for_path(
    abs_root: &StdPath,
    file_path: &StdPath,
) -> StorageResult<Option<ObjectMeta>> {
    let location = object_location_for_path(abs_root, file_path)?;

    let metadata = match std::fs::metadata(file_path) {
        Ok(m) => m,
        // Racy but expected: the file could be removed/renamed between the event
        // and the lookup. Treat as a non-fatal "no object".
        Err(_) => return Ok(None),
    };

    if metadata.is_dir() {
        // We do not represent directories as objects.
        return Ok(None);
    }

    let last_modified: chrono::DateTime<chrono::Utc> = metadata
        .modified()
        .map_err(|e| {
            StorageError::InitializationError(format!(
                "Failed to read modified time for {file_path:?}: {e}"
            ))
        })?
        .into();

    Ok(Some(ObjectMeta {
        location,
        size: metadata.len(),
        last_modified,
        e_tag: None,
        version: None,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn translate_create_and_remove_events() {
        let dir = tempfile::tempdir().expect("temp dir");
        let abs_root = std::fs::canonicalize(dir.path()).expect("canonicalize");

        // A created file under the root translates to a Created event whose
        // location is relative to the root.
        let file = dir.path().join("a/b.txt");
        std::fs::create_dir_all(file.parent().unwrap()).unwrap();
        std::fs::write(&file, b"hi").unwrap();

        let created = translate_event(
            &abs_root,
            notify::Event {
                kind: EventKind::Create(notify::event::CreateKind::File),
                paths: vec![file.clone()],
                attrs: Default::default(),
            },
        );
        assert_eq!(created.len(), 1);
        match &created[0] {
            ObjectEvent::Created(meta) => {
                assert_eq!(meta.location.as_ref(), "a/b.txt");
                assert_eq!(meta.size, 2);
            }
            other => panic!("unexpected event: {other:?}"),
        }

        // A remove event translates even though the file no longer needs to exist.
        let removed = translate_event(
            &abs_root,
            notify::Event {
                kind: EventKind::Remove(notify::event::RemoveKind::File),
                paths: vec![abs_root.join("a/gone.txt")],
                attrs: Default::default(),
            },
        );
        assert_eq!(removed.len(), 1);
        match &removed[0] {
            ObjectEvent::Deleted(path) => assert_eq!(path.as_ref(), "a/gone.txt"),
            other => panic!("unexpected event: {other:?}"),
        }
    }

    /// End-to-end check that the real `notify` watcher delivers a created file
    /// through `poll`. Ignored by default because delivery timing and event
    /// coalescing are OS-dependent; run explicitly with `--ignored`.
    #[tokio::test]
    #[ignore = "depends on OS filesystem-notification timing"]
    async fn watcher_delivers_created_file() {
        use std::time::Duration;

        let dir = tempfile::tempdir().expect("temp dir");
        let listener = FsEventListener::new(dir.path().to_path_buf()).expect("start watcher");

        std::fs::write(dir.path().join("new.txt"), b"hi").unwrap();

        let events = tokio::time::timeout(Duration::from_secs(5), listener.poll())
            .await
            .expect("watcher should deliver an event");
        assert!(
            events.iter().any(|e| matches!(
                e,
                ObjectEvent::Created(meta) | ObjectEvent::Modified(meta)
                    if meta.location.as_ref() == "new.txt"
            )),
            "expected an event for new.txt, got: {events:?}"
        );
    }

    #[test]
    fn translate_ignores_unsupported_event_kinds() {
        let dir = tempfile::tempdir().expect("temp dir");
        let abs_root = std::fs::canonicalize(dir.path()).expect("canonicalize");
        let events = translate_event(
            &abs_root,
            notify::Event {
                kind: EventKind::Access(notify::event::AccessKind::Read),
                paths: vec![abs_root.join("a.txt")],
                attrs: Default::default(),
            },
        );
        assert!(events.is_empty());
    }
}
