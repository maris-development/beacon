//! File system watcher that sends change events to a cache.
//!
//! The watcher is an accelerator. Network file systems send no events and
//! inotify has a watch limit, so the periodic sync stays the source of truth.

use std::path::{Component, Path as FsPath, PathBuf};
use std::time::Duration;

use notify::event::ModifyKind;
use notify::{Event, EventKind, RecommendedWatcher, RecursiveMode, Watcher};
use object_store::path::Path;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;

use crate::event::{CacheHandle, StoreEvent};

/// File system types that report no changes from other machines.
#[cfg(any(target_os = "linux", test))]
const NETWORK_FILE_SYSTEMS: &[&str] = &[
    "nfs", "nfs4", "cifs", "smb3", "smbfs", "ceph", "9p", "lustre", "afs", "gpfs", "beegfs",
    "glusterfs",
];

/// State of a [`LocalWatcher`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WatcherState {
    /// The watcher scans the folder tree. It reports no changes yet.
    Starting,
    /// The watcher reports changes.
    Running,
    /// The watcher does not run: a network file system, an error or a drop.
    Stopped,
}

/// A watcher on a store folder. Drop it to stop the watch.
#[derive(Debug)]
pub struct LocalWatcher {
    task: JoinHandle<()>,
    state: watch::Receiver<WatcherState>,
}

impl LocalWatcher {
    /// Current state of the watcher.
    pub fn state(&self) -> WatcherState {
        *self.state.borrow()
    }

    /// Wait until the start completes. True when the watcher reports changes.
    pub async fn started(&self) -> bool {
        let mut state = self.state.clone();
        let done = state.wait_for(|state| *state != WatcherState::Starting).await;
        done.is_ok_and(|state| *state == WatcherState::Running)
    }
}

impl Drop for LocalWatcher {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// Watch the folder `root` of a `LocalFileSystem` store and send its changes to `handle`.
///
/// Call it inside a tokio runtime. The start runs in the background, because
/// the scan of a large folder tree takes time. A network file system gets no
/// watcher. Events collect for `debounce` before the cache reads the source.
pub fn watch_local(handle: CacheHandle, root: &FsPath, debounce: Duration) -> LocalWatcher {
    let root = root.to_path_buf();
    let (state, state_reader) = watch::channel(WatcherState::Starting);
    let task = tokio::spawn(async move {
        let (sender, receiver) = mpsc::unbounded_channel();
        let start = tokio::task::spawn_blocking(move || start_watcher(&root, sender)).await;
        match start {
            Ok(Ok(Some((watcher, root)))) => {
                let _ = state.send(WatcherState::Running);
                tracing::info!(folder = %root.display(), "store cache: file watcher runs");
                // Changes during the start sent no event: compare the cache one time.
                handle.apply(vec![StoreEvent::Resync]).await;
                forward_events(&handle, &root, receiver, debounce).await;
                drop(watcher);
            }
            Ok(Ok(None)) => {}
            Ok(Err(error)) => {
                tracing::warn!(%error, "store cache: file watcher did not start, the sync interval stays in use");
            }
            Err(error) => {
                tracing::warn!(%error, "store cache: file watcher start stopped");
            }
        }
        let _ = state.send(WatcherState::Stopped);
    });
    LocalWatcher {
        task,
        state: state_reader,
    }
}

type RawEvents = mpsc::UnboundedSender<notify::Result<Event>>;

/// Create the watcher. `None` for a network file system. This call can block for a long time.
fn start_watcher(
    root: &FsPath,
    sender: RawEvents,
) -> notify::Result<Option<(RecommendedWatcher, PathBuf)>> {
    // `LocalFileSystem::new_with_prefix` uses the canonical path too.
    let root = std::fs::canonicalize(root).map_err(notify::Error::io)?;
    if let Some(kind) = network_file_system(&root) {
        tracing::info!(
            folder = %root.display(),
            file_system = kind,
            "store cache: no file watcher on a network file system, the sync interval stays in use"
        );
        return Ok(None);
    }
    let mut watcher = notify::recommended_watcher(move |event| {
        let _ = sender.send(event);
    })?;
    watcher.watch(&root, RecursiveMode::Recursive)?;
    Ok(Some((watcher, root)))
}

async fn forward_events(
    handle: &CacheHandle,
    root: &FsPath,
    mut receiver: mpsc::UnboundedReceiver<notify::Result<Event>>,
    debounce: Duration,
) {
    while let Some(first) = receiver.recv().await {
        let mut batch = vec![first];
        let window = tokio::time::sleep(debounce);
        tokio::pin!(window);
        loop {
            tokio::select! {
                () = &mut window => break,
                next = receiver.recv() => match next {
                    Some(event) => batch.push(event),
                    None => break,
                },
            }
        }
        let events = batch
            .into_iter()
            .flat_map(|event| to_store_events(root, event))
            .collect();
        handle.apply(events).await;
    }
}

/// Name of the network file system that holds the canonical `path`, if it is one.
///
/// Such a file system reports no changes that a different machine makes.
pub fn network_file_system(path: &FsPath) -> Option<String> {
    #[cfg(target_os = "linux")]
    {
        let mounts = std::fs::read_to_string("/proc/self/mountinfo").ok()?;
        file_system_type(&mounts, path).filter(|kind| is_network_type(kind))
    }
    #[cfg(windows)]
    {
        // A canonical path on a share or a mapped network drive has this form.
        let text = path.to_str()?;
        (text.starts_with(r"\\") && !is_local_verbatim(text)).then(|| "network share".to_string())
    }
    #[cfg(not(any(target_os = "linux", windows)))]
    {
        let _ = path;
        None
    }
}

/// True for a verbatim path on a local drive, for example `\\?\C:\data`.
#[cfg(any(windows, test))]
fn is_local_verbatim(text: &str) -> bool {
    text.strip_prefix(r"\\?\")
        .is_some_and(|rest| rest.as_bytes().get(1) == Some(&b':'))
}

#[cfg(any(target_os = "linux", test))]
fn is_network_type(kind: &str) -> bool {
    // FUSE types have the form `fuse.sshfs`.
    let name = kind.strip_prefix("fuse.").unwrap_or(kind);
    NETWORK_FILE_SYSTEMS.contains(&name) || name == "sshfs"
}

/// File system type of the mount that holds `path`, from `/proc/self/mountinfo` text.
#[cfg(any(target_os = "linux", test))]
fn file_system_type(mountinfo: &str, path: &FsPath) -> Option<String> {
    mountinfo
        .lines()
        .filter_map(|line| {
            let (mount, kind) = line.split_once(" - ")?;
            let point = unescape_mount(mount.split(' ').nth(4)?);
            let kind = kind.split(' ').next()?;
            path.starts_with(&point).then(|| (point.len(), kind.to_string()))
        })
        // The longest mount point is the mount that holds the path.
        .max_by_key(|(length, _)| *length)
        .map(|(_, kind)| kind)
}

/// Decode the octal escapes of a mount point, for example `\040` for a space.
#[cfg(any(target_os = "linux", test))]
fn unescape_mount(point: &str) -> String {
    let mut text = String::with_capacity(point.len());
    let mut rest = point;
    while let Some(at) = rest.find('\\') {
        text.push_str(&rest[..at]);
        let code = rest.get(at + 1..at + 4).and_then(|digits| u8::from_str_radix(digits, 8).ok());
        match code {
            Some(byte) => {
                text.push(char::from(byte));
                rest = &rest[at + 4..];
            }
            None => {
                text.push('\\');
                rest = &rest[at + 1..];
            }
        }
    }
    text.push_str(rest);
    text
}

fn to_store_events(root: &FsPath, event: notify::Result<Event>) -> Vec<StoreEvent> {
    let event = match event {
        Ok(event) if !event.need_rescan() => event,
        // The watcher lost events: compare the full cache with the source.
        _ => return vec![StoreEvent::Resync],
    };
    if matches!(event.kind, EventKind::Access(_)) {
        return Vec::new();
    }
    // A folder reports a content change for each file in it: only its own create or move counts.
    let folder_moved = matches!(
        event.kind,
        EventKind::Create(_) | EventKind::Modify(ModifyKind::Name(_))
    );
    event
        .paths
        .iter()
        .filter_map(|path| match fs_to_object_key(root, path) {
            None => Some(StoreEvent::Resync),
            Some(key) if path.is_dir() => folder_moved.then_some(StoreEvent::PrefixChanged { path: key }),
            Some(key) if key.is_empty() => None,
            Some(key) => Some(StoreEvent::Changed { path: key }),
        })
        .collect()
}

/// Object key of a file below the canonical store root. The root gives an empty key.
fn fs_to_object_key(root: &FsPath, path: &FsPath) -> Option<String> {
    let relative = path.strip_prefix(root).ok()?;
    let parts = relative
        .components()
        .map(|part| match part {
            Component::Normal(name) => name.to_str(),
            _ => None,
        })
        .collect::<Option<Vec<&str>>>()?;
    if parts.is_empty() {
        return Some(String::new());
    }
    let location = Path::parse(parts.join("/")).ok()?;
    Some(location.as_ref().to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    const MOUNTS: &str = "\
22 1 8:1 / / rw,relatime shared:1 - ext4 /dev/sda1 rw
40 22 0:35 / /mnt/archive rw,relatime shared:20 - nfs4 filer:/export rw,vers=4.2
41 22 0:36 / /mnt/my\\040share rw - cifs //host/share rw
42 40 8:17 / /mnt/archive/local rw - xfs /dev/sdb1 rw
43 22 0:40 / /mnt/ssh rw - fuse.sshfs user@host:/ rw";

    fn kind(path: &str) -> Option<String> {
        file_system_type(MOUNTS, FsPath::new(path)).filter(|kind| is_network_type(kind))
    }

    #[test]
    fn maps_a_file_below_the_root() {
        let root = FsPath::new("data").join("sets");
        let file = root.join("a").join("b.nc");
        assert_eq!(fs_to_object_key(&root, &file).as_deref(), Some("a/b.nc"));
        assert_eq!(fs_to_object_key(&root, &root).as_deref(), Some(""));
        assert_eq!(fs_to_object_key(&root, FsPath::new("other")), None);
    }

    #[test]
    fn finds_the_network_mount_that_holds_a_path() {
        assert_eq!(kind("/mnt/archive/argo/2024").as_deref(), Some("nfs4"));
        assert_eq!(kind("/mnt/my share/data").as_deref(), Some("cifs"));
        assert_eq!(kind("/mnt/ssh/data").as_deref(), Some("fuse.sshfs"));
        assert_eq!(kind("/data/datasets"), None, "the root disk is local");
        assert_eq!(kind("/mnt/archive/local/x"), None, "the longest mount point wins");
        assert_eq!(kind("/mnt/archive2"), None, "a mount point matches full path parts");
    }

    #[test]
    fn local_verbatim_paths_are_not_shares() {
        assert!(is_local_verbatim(r"\\?\C:\data\datasets"));
        assert!(!is_local_verbatim(r"\\?\UNC\filer\export\datasets"));
    }
}
