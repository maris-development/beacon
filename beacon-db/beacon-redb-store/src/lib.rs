//! A single-file [`object_store::ObjectStore`] backed by [redb] plus an
//! in-file blob **heap**.
//!
//! [`RedbStore`] exposes an object store whose entire state — object bytes and
//! metadata — lives inside one file. redb does not own that file; it runs on top
//! of a [`container::RegionBackend`] that shares the file with an append-only
//! blob heap ([`container::Container`]). So one `.db` file holds the transactional
//! index *and* the bulk bytes: copy it and you carry the whole dataset with you.
//!
//! # Where bytes live
//!
//! - **Small objects** (≤ [`HEAP_THRESHOLD`]) are stored inline inside redb. This
//!   suits Beacon's catalog/metadata (`table.json`, `extensions.json`, crawler
//!   definitions), which are small and rewritten often — redb reclaims their
//!   pages on overwrite.
//! - **Large objects** (Lance data files, Parquet, …) are written to the heap and
//!   referenced from redb by an [`container::Extent`]. Reads are zero-copy:
//!   [`bytes::Bytes`] pointing straight into the file's `mmap`, no allocation and
//!   no `memcpy` on the read path.
//!
//! # redb tables
//!
//! - `objects_meta` : `path -> StoredMeta` (bincode: size, etag, mtime, payload
//!   locator). Listing scans only this table.
//! - `objects_data` : `path -> bytes` — inline payloads only.
//! - `state`        : `"seq" -> u64`   — monotonic etag counter.
//!
//! # Guarantees
//!
//! Every mutation (`put`, `delete`, `copy`, `rename`) is a single redb write
//! transaction. Conditional writes ([`PutMode::Create`] / [`PutMode::Update`])
//! perform their existence/etag check *inside* that transaction — real
//! compare-and-swap. For heap objects the blob bytes are fsynced *before* the
//! redb transaction that records their extent commits, so committed metadata can
//! never point at bytes a crash could lose.
//!
//! # Concurrency
//!
//! One writer, many readers — the redb/SQLite/LMDB model, enforced by an
//! exclusive file lock. Blocking file/redb work runs on
//! [`tokio::task::spawn_blocking`] so it never stalls the async runtime.
//!
//! # Space reclamation
//!
//! Deleted or overwritten heap blobs leak their space until a compaction pass
//! reclaims it. [`RedbStore::vacuum`] with [`VacuumMode::Rewrite`] rewrites every
//! live object into a fresh file and atomically renames it back, reclaiming both
//! the dead heap blobs and redb's free pages in one shot. redb's own pages are
//! otherwise reclaimed incrementally by redb's freelist.

mod container;

use std::collections::{BTreeMap, BTreeSet};
use std::io;
use std::path::{Path as FsPath, PathBuf};
use std::sync::Arc;

use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures::{FutureExt, StreamExt, TryStreamExt, stream::BoxStream};
use object_store::{
    Attributes, CopyOptions, GetOptions, GetResult, GetResultPayload, ListResult, MultipartUpload,
    ObjectMeta, ObjectStore, PutMode, PutMultipartOptions, PutOptions, PutPayload, PutResult,
    RenameOptions, RenameTargetMode, Result as OsResult, UploadPart, path::Path,
};
use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};
use serde::{Deserialize, Serialize};

use container::{Container, Extent, RegionBackend};

/// `store` label attached to [`object_store::Error::Generic`] errors.
const STORE: &str = "redb";

/// Objects larger than this go to the heap; smaller ones stay inline in redb.
pub const HEAP_THRESHOLD: usize = 64 * 1024;

const DATA_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("objects_data");
const META_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("objects_meta");
const STATE_TABLE: TableDefinition<&str, u64> = TableDefinition::new("state");
const SEQ_KEY: &str = "seq";

/// Where an object's payload bytes live.
#[derive(Debug, Clone, Serialize, Deserialize)]
enum PayloadLoc {
    /// Bytes are in the `objects_data` redb table.
    Inline,
    /// Bytes are in the heap at this extent (zero-copy readable via mmap).
    Heap(Extent),
}

/// Per-object metadata persisted in `objects_meta` (bincode-encoded).
#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredMeta {
    size: u64,
    e_tag: String,
    last_modified_millis: i64,
    payload: PayloadLoc,
}

/// A resolved payload ready to be recorded in a redb transaction.
enum StoredPayload {
    Inline(Vec<u8>),
    Heap { extent: Extent, size: u64 },
}

/// The write semantics for a single put, derived from [`PutMode`].
enum WriteMode {
    Overwrite,
    Create,
    /// Compare-and-swap against this expected etag (`None` == "any existing").
    Update(Option<String>),
}

/// A single-file object store backed by a redb index and an in-file blob heap.
#[derive(Clone)]
pub struct RedbStore {
    inner: Arc<Inner>,
}

struct Inner {
    db: Database,
    container: Arc<Container>,
    /// The container file's path — kept so [`RedbStore::vacuum`] can rewrite the
    /// store into a sibling temp file and atomically rename it back.
    path: PathBuf,
}

/// How [`RedbStore::vacuum`] reclaims space.
#[derive(Debug, Clone, Copy)]
pub enum VacuumMode {
    /// Rewrite every live object into a fresh file, then atomically replace the
    /// original. Reclaims all space held by deleted or overwritten objects (dead
    /// heap blobs *and* redb's free pages) and produces a minimal, shippable
    /// file. Always correct, but needs ~2× the live size in free disk during the
    /// pass and exclusive access to the store (see [`RedbStore::vacuum`]).
    Rewrite,
}

impl RedbStore {
    /// Open the store at `path`, creating the file (and its tables) if absent.
    ///
    /// Takes an exclusive lock on the file for the store's lifetime.
    pub fn open(path: impl AsRef<FsPath>) -> OsResult<Self> {
        let path = path.as_ref().to_path_buf();
        let container = Container::open(&path).map_err(generic)?;
        let db = redb::Builder::new()
            .create_with_backend(RegionBackend::new(container.clone()))
            .map_err(generic)?;
        // Ensure all tables exist so reads on a fresh file don't fail.
        let txn = db.begin_write().map_err(generic)?;
        {
            txn.open_table(DATA_TABLE).map_err(generic)?;
            txn.open_table(META_TABLE).map_err(generic)?;
            txn.open_table(STATE_TABLE).map_err(generic)?;
        }
        txn.commit().map_err(generic)?;
        Ok(Self {
            inner: Arc::new(Inner {
                db,
                container,
                path,
            }),
        })
    }

    /// Compact the store, reclaiming space held by deleted or overwritten
    /// objects and shrinking the file to (roughly) its live size.
    ///
    /// Consumes the store and returns a freshly opened, compacted one:
    ///
    /// ```no_run
    /// # async fn f(store: beacon_redb_store::RedbStore) -> object_store::Result<()> {
    /// use beacon_redb_store::VacuumMode;
    /// let store = store.vacuum(VacuumMode::Rewrite).await?;
    /// # Ok(()) }
    /// ```
    ///
    /// Taking `self` by value enforces the *exclusive access* the rewrite needs:
    /// it errors if any other clone of the store is still alive. Object bytes
    /// handed out by an earlier `get` stay valid (they pin their own view of the
    /// old file), but on Windows an outstanding read can block the rename — run
    /// vacuum from a maintenance window with no reads in flight.
    ///
    /// Etags and modification times are preserved, so clients holding an etag
    /// keep working across a vacuum.
    pub async fn vacuum(self, mode: VacuumMode) -> OsResult<Self> {
        let VacuumMode::Rewrite = mode;
        let inner = Arc::into_inner(self.inner).ok_or_else(|| {
            generic(io::Error::new(
                io::ErrorKind::WouldBlock,
                "cannot vacuum: the store still has outstanding clones",
            ))
        })?;
        block(move || inner.rewrite()).await
    }

    /// Write `payload`, routing large objects to the heap (durable before the
    /// redb transaction records them) and small ones inline.
    async fn write(
        &self,
        location: &Path,
        payload: PutPayload,
        mode: WriteMode,
    ) -> OsResult<PutResult> {
        let size = payload.content_length();
        let stored = if size > HEAP_THRESHOLD {
            let chunks: Vec<Bytes> = payload.iter().cloned().collect();
            let container = self.inner.container.clone();
            let extent = block(move || container.heap_write(&chunks).map_err(generic)).await?;
            StoredPayload::Heap {
                extent,
                size: size as u64,
            }
        } else {
            let mut buf = Vec::with_capacity(size);
            for chunk in &payload {
                buf.extend_from_slice(chunk);
            }
            StoredPayload::Inline(buf)
        };
        let inner = self.inner.clone();
        let path = location.to_string();
        block(move || inner.put(&path, stored, mode)).await
    }

    /// Write pre-buffered `bytes` (used by multipart `complete`).
    async fn write_bytes(
        &self,
        location: &Path,
        bytes: Vec<u8>,
        mode: WriteMode,
    ) -> OsResult<PutResult> {
        let stored = if bytes.len() > HEAP_THRESHOLD {
            let chunks = vec![Bytes::from(bytes)];
            let size = chunks[0].len() as u64;
            let container = self.inner.container.clone();
            let extent = block(move || container.heap_write(&chunks).map_err(generic)).await?;
            StoredPayload::Heap { extent, size }
        } else {
            StoredPayload::Inline(bytes)
        };
        let inner = self.inner.clone();
        let path = location.to_string();
        block(move || inner.put(&path, stored, mode)).await
    }

    /// Fetch the full object bytes: zero-copy from the heap, or a copy from the
    /// inline table.
    async fn fetch(&self, location: &Path) -> OsResult<(Bytes, StoredMeta)> {
        let inner = self.inner.clone();
        let path = location.to_string();
        let (inline, stored) = block(move || inner.get(&path, true)).await?;
        let bytes = match &stored.payload {
            PayloadLoc::Inline => Bytes::from(inline.expect("inline payload present")),
            PayloadLoc::Heap(extent) => self
                .inner
                .container
                .read_extents(std::slice::from_ref(extent))
                .pop()
                .expect("one extent yields one slice"),
        };
        Ok((bytes, stored))
    }
}

impl Inner {
    /// Atomic put with the given [`WriteMode`], recording the (already-durable)
    /// payload locator. Runs entirely inside one redb write transaction so the
    /// CAS check and the write are indivisible.
    fn put(&self, path: &str, payload: StoredPayload, mode: WriteMode) -> OsResult<PutResult> {
        let txn = self.db.begin_write().map_err(generic)?;
        let e_tag;
        {
            let mut state = txn.open_table(STATE_TABLE).map_err(generic)?;
            let mut metas = txn.open_table(META_TABLE).map_err(generic)?;
            let mut datas = txn.open_table(DATA_TABLE).map_err(generic)?;

            let existing = metas
                .get(path)
                .map_err(generic)?
                .map(|g| decode_meta(g.value()))
                .transpose()?;
            match mode {
                WriteMode::Overwrite => {}
                WriteMode::Create => {
                    if existing.is_some() {
                        return Err(object_store::Error::AlreadyExists {
                            path: path.to_string(),
                            source: "object already exists".into(),
                        });
                    }
                }
                WriteMode::Update(expected) => match &existing {
                    None => return Err(precondition(path, "object does not exist")),
                    Some(current) => {
                        if let Some(want) = &expected
                            && &current.e_tag != want
                        {
                            return Err(precondition(path, "etag mismatch"));
                        }
                    }
                },
            }

            let seq = state
                .get(SEQ_KEY)
                .map_err(generic)?
                .map(|g| g.value())
                .unwrap_or(0)
                + 1;
            state.insert(SEQ_KEY, seq).map_err(generic)?;
            e_tag = seq.to_string();

            let (size, ploc) = match &payload {
                StoredPayload::Inline(data) => (data.len() as u64, PayloadLoc::Inline),
                StoredPayload::Heap { extent, size } => (*size, PayloadLoc::Heap(*extent)),
            };
            let meta = StoredMeta {
                size,
                e_tag: e_tag.clone(),
                last_modified_millis: Utc::now().timestamp_millis(),
                payload: ploc,
            };
            metas
                .insert(path, encode_meta(&meta)?.as_slice())
                .map_err(generic)?;
            match payload {
                StoredPayload::Inline(data) => {
                    datas.insert(path, data.as_slice()).map_err(generic)?;
                }
                // Overwriting a previously-inline object with a heap one: drop the
                // stale inline body. (The old heap blob, if any, leaks until GC.)
                StoredPayload::Heap { .. } => {
                    datas.remove(path).map_err(generic)?;
                }
            }
        }
        txn.commit().map_err(generic)?;
        Ok(PutResult {
            e_tag: Some(e_tag),
            version: None,
        })
    }

    /// Read an object's metadata and, for inline objects when `want_data`, its
    /// bytes. Heap payloads are fetched zero-copy by the caller via the extent in
    /// the returned [`StoredMeta`].
    fn get(&self, path: &str, want_data: bool) -> OsResult<(Option<Vec<u8>>, StoredMeta)> {
        let txn = self.db.begin_read().map_err(generic)?;
        let metas = txn.open_table(META_TABLE).map_err(generic)?;
        let meta = metas
            .get(path)
            .map_err(generic)?
            .map(|g| decode_meta(g.value()))
            .transpose()?
            .ok_or_else(|| not_found(path))?;
        let inline = if want_data && matches!(meta.payload, PayloadLoc::Inline) {
            let datas = txn.open_table(DATA_TABLE).map_err(generic)?;
            let guard = datas
                .get(path)
                .map_err(generic)?
                .ok_or_else(|| not_found(path))?;
            Some(guard.value().to_vec())
        } else {
            None
        };
        Ok((inline, meta))
    }

    fn delete(&self, path: &str) -> OsResult<()> {
        let txn = self.db.begin_write().map_err(generic)?;
        {
            let mut metas = txn.open_table(META_TABLE).map_err(generic)?;
            let mut datas = txn.open_table(DATA_TABLE).map_err(generic)?;
            let existed = metas.remove(path).map_err(generic)?.is_some();
            datas.remove(path).map_err(generic)?; // heap blob (if any) leaks until GC
            if !existed {
                return Err(not_found(path));
            }
        }
        txn.commit().map_err(generic)?;
        Ok(())
    }

    /// Copy (`mv == false`) or move (`mv == true`) `from` to `to`. Heap payloads
    /// are shared by reference — the destination points at the same immutable
    /// extent, no bytes are re-written.
    fn transfer(&self, from: &str, to: &str, create: bool, mv: bool) -> OsResult<()> {
        let txn = self.db.begin_write().map_err(generic)?;
        {
            let mut state = txn.open_table(STATE_TABLE).map_err(generic)?;
            let mut metas = txn.open_table(META_TABLE).map_err(generic)?;
            let mut datas = txn.open_table(DATA_TABLE).map_err(generic)?;

            let src_meta = metas
                .get(from)
                .map_err(generic)?
                .map(|g| decode_meta(g.value()))
                .transpose()?
                .ok_or_else(|| not_found(from))?;
            let src_inline = match src_meta.payload {
                PayloadLoc::Inline => Some(
                    datas
                        .get(from)
                        .map_err(generic)?
                        .ok_or_else(|| not_found(from))?
                        .value()
                        .to_vec(),
                ),
                PayloadLoc::Heap(_) => None,
            };

            if create && metas.get(to).map_err(generic)?.is_some() {
                return Err(object_store::Error::AlreadyExists {
                    path: to.to_string(),
                    source: "destination already exists".into(),
                });
            }

            let seq = state
                .get(SEQ_KEY)
                .map_err(generic)?
                .map(|g| g.value())
                .unwrap_or(0)
                + 1;
            state.insert(SEQ_KEY, seq).map_err(generic)?;
            let dst_meta = StoredMeta {
                size: src_meta.size,
                e_tag: seq.to_string(),
                last_modified_millis: Utc::now().timestamp_millis(),
                payload: src_meta.payload.clone(),
            };
            metas
                .insert(to, encode_meta(&dst_meta)?.as_slice())
                .map_err(generic)?;
            match &src_inline {
                Some(bytes) => {
                    datas.insert(to, bytes.as_slice()).map_err(generic)?;
                }
                None => {
                    datas.remove(to).map_err(generic)?;
                }
            }

            if mv {
                metas.remove(from).map_err(generic)?;
                datas.remove(from).map_err(generic)?;
            }
        }
        txn.commit().map_err(generic)?;
        Ok(())
    }

    /// Collect [`ObjectMeta`] for every object under `prefix` (segment-aware),
    /// reading only the metadata table.
    fn list(&self, prefix: Option<&Path>) -> OsResult<Vec<ObjectMeta>> {
        let txn = self.db.begin_read().map_err(generic)?;
        let metas = txn.open_table(META_TABLE).map_err(generic)?;
        let prefix = prefix.cloned().unwrap_or_default();
        let prefix_str = prefix.as_ref().to_string();

        let mut out = Vec::new();
        let iter = metas.range(prefix_str.as_str()..).map_err(generic)?;
        for entry in iter {
            let (key, value) = entry.map_err(generic)?;
            let key = key.value().to_string();
            if !prefix_str.is_empty() && !key.starts_with(&prefix_str) {
                break;
            }
            // `parse` (not `from`) reconstructs the Path from its already
            // percent-encoded raw form without re-encoding, so keys containing an
            // encoded delimiter (e.g. `b%2Fc`) round-trip exactly.
            let location = Path::parse(key.as_str()).map_err(generic)?;
            // Include only objects strictly under the prefix on a segment
            // boundary (excludes the exact-prefix object; rejects `foo` matching
            // `foo_bar`).
            let strictly_under = location
                .prefix_match(&prefix)
                .is_some_and(|mut parts| parts.next().is_some());
            if strictly_under {
                let meta = decode_meta(value.value())?;
                out.push(object_meta(location, &meta));
            }
        }
        Ok(out)
    }

    /// Rewrite every live object into a fresh sibling file and atomically rename
    /// it over the original. Dead heap blobs and redb free pages are left behind,
    /// so the new file is (roughly) the live size.
    ///
    /// Crash-safe: the original is untouched until the final `rename`, which is
    /// the single commit point. A crash mid-vacuum leaves the original intact and
    /// only orphans the temp file (cleaned up by the next vacuum).
    fn rewrite(self) -> OsResult<RedbStore> {
        let Inner {
            db,
            container,
            path,
        } = self;

        // 1. Snapshot every live object. Heap payloads are read zero-copy from the
        //    old mmap; we hold those slices only until they're copied below.
        let (prepared, max_seq, new_container, new_db) = {
            let rtxn = db.begin_read().map_err(generic)?;
            let metas = rtxn.open_table(META_TABLE).map_err(generic)?;
            let datas = rtxn.open_table(DATA_TABLE).map_err(generic)?;

            // Open the compacted target as a sibling temp file. Clear any orphan
            // from a previously-aborted vacuum first.
            let tmp = temp_sibling(&path);
            let _ = std::fs::remove_file(&tmp);
            let new_container = Container::open(&tmp).map_err(generic)?;
            let new_db = redb::Builder::new()
                .create_with_backend(RegionBackend::new(new_container.clone()))
                .map_err(generic)?;

            // Copy each object's bytes into the new file (heap blobs are fsync'd by
            // `heap_write`), capturing its new payload locator. Etags are preserved.
            let mut prepared: Vec<(String, StoredMeta, Option<Vec<u8>>)> = Vec::new();
            let mut max_seq = 0u64;
            for entry in metas.iter().map_err(generic)? {
                let (key, value) = entry.map_err(generic)?;
                let path = key.value().to_string();
                let StoredMeta {
                    size,
                    e_tag,
                    last_modified_millis,
                    payload,
                } = decode_meta(value.value())?;
                if let Ok(seq) = e_tag.parse::<u64>() {
                    max_seq = max_seq.max(seq);
                }
                let (payload, inline) = match payload {
                    PayloadLoc::Inline => {
                        let bytes = datas
                            .get(path.as_str())
                            .map_err(generic)?
                            .ok_or_else(|| not_found(&path))?
                            .value()
                            .to_vec();
                        (PayloadLoc::Inline, Some(bytes))
                    }
                    PayloadLoc::Heap(extent) => {
                        let slices = container.read_extents(std::slice::from_ref(&extent));
                        let new_extent = new_container.heap_write(&slices).map_err(generic)?;
                        (PayloadLoc::Heap(new_extent), None)
                    }
                };
                let meta = StoredMeta {
                    size,
                    e_tag,
                    last_modified_millis,
                    payload,
                };
                prepared.push((path, meta, inline));
            }
            (prepared, max_seq, new_container, new_db)
        };

        // 2. Record all metadata (and inline bodies) in one transaction, carrying
        //    the etag counter forward so future puts don't reuse an etag.
        let wtxn = new_db.begin_write().map_err(generic)?;
        {
            let mut nmetas = wtxn.open_table(META_TABLE).map_err(generic)?;
            let mut ndatas = wtxn.open_table(DATA_TABLE).map_err(generic)?;
            let mut nstate = wtxn.open_table(STATE_TABLE).map_err(generic)?;
            nstate.insert(SEQ_KEY, max_seq).map_err(generic)?;
            for (path, meta, inline) in &prepared {
                nmetas
                    .insert(path.as_str(), encode_meta(meta)?.as_slice())
                    .map_err(generic)?;
                if let Some(bytes) = inline {
                    ndatas
                        .insert(path.as_str(), bytes.as_slice())
                        .map_err(generic)?;
                }
            }
        }
        wtxn.commit().map_err(generic)?;

        // 3. Fully close both files before the rename: the new one so it can be
        //    moved, the old one so it can be replaced (and its lock released).
        let tmp = temp_sibling(&path);
        drop(new_db);
        drop(new_container);
        drop(db);
        drop(container);

        // 4. Atomically swap the compacted file in, then reopen it.
        std::fs::rename(&tmp, &path).map_err(generic)?;
        RedbStore::open(&path)
    }
}

#[async_trait::async_trait]
impl ObjectStore for RedbStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> OsResult<PutResult> {
        let mode = match opts.mode {
            PutMode::Overwrite => WriteMode::Overwrite,
            PutMode::Create => WriteMode::Create,
            PutMode::Update(v) => WriteMode::Update(v.e_tag),
        };
        self.write(location, payload, mode).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        _opts: PutMultipartOptions,
    ) -> OsResult<Box<dyn MultipartUpload>> {
        Ok(Box::new(RedbMultipart {
            store: self.clone(),
            location: location.clone(),
            parts: BTreeMap::new(),
            next_seq: 0,
        }))
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> OsResult<GetResult> {
        // `head` only needs metadata — avoid fetching the body.
        if options.head {
            let inner = self.inner.clone();
            let path = location.to_string();
            let (_none, stored) = block(move || inner.get(&path, false)).await?;
            let meta = object_meta(location.clone(), &stored);
            options.check_preconditions(&meta)?;
            let size = stored.size;
            return Ok(GetResult {
                payload: GetResultPayload::Stream(futures::stream::empty().boxed()),
                meta,
                range: 0..size,
                attributes: Attributes::default(),
            });
        }

        let (full, stored) = self.fetch(location).await?;
        let meta = object_meta(location.clone(), &stored);
        options.check_preconditions(&meta)?;

        let range = match &options.range {
            Some(r) => r.as_range(stored.size).map_err(generic)?,
            None => 0..stored.size,
        };
        // `Bytes::slice` is zero-copy — for a heap object it stays a pointer into
        // the mmap.
        let sliced = full.slice(range.start as usize..range.end as usize);
        Ok(GetResult {
            payload: GetResultPayload::Stream(
                futures::stream::once(async move { Ok(sliced) }).boxed(),
            ),
            meta,
            range,
            attributes: Attributes::default(),
        })
    }

    async fn get_ranges(
        &self,
        location: &Path,
        ranges: &[std::ops::Range<u64>],
    ) -> OsResult<Vec<Bytes>> {
        let (full, _meta) = self.fetch(location).await?;
        let len = full.len();
        let mut out = Vec::with_capacity(ranges.len());
        for r in ranges {
            let end = (r.end as usize).min(len);
            let start = (r.start as usize).min(end);
            out.push(full.slice(start..end)); // zero-copy
        }
        Ok(out)
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, OsResult<Path>>,
    ) -> BoxStream<'static, OsResult<Path>> {
        let inner = self.inner.clone();
        locations
            .then(move |location| {
                let inner = inner.clone();
                async move {
                    let location = location?;
                    let path = location.to_string();
                    block(move || inner.delete(&path)).await?;
                    Ok(location)
                }
            })
            .boxed()
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, OsResult<ObjectMeta>> {
        match self.inner.list(prefix) {
            Ok(metas) => futures::stream::iter(metas.into_iter().map(Ok)).boxed(),
            Err(e) => futures::stream::once(async move { Err(e) }).boxed(),
        }
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, OsResult<ObjectMeta>> {
        let offset = offset.clone();
        self.list(prefix)
            .try_filter(move |meta| futures::future::ready(meta.location > offset))
            .boxed()
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> OsResult<ListResult> {
        let prefix = prefix.cloned().unwrap_or_default();
        let metas = self.inner.list(Some(&prefix))?;

        let mut objects = Vec::new();
        let mut common_prefixes = BTreeSet::new();
        for meta in metas {
            let common_prefix = {
                let mut parts = match meta.location.prefix_match(&prefix) {
                    Some(parts) => parts,
                    None => continue,
                };
                let Some(first) = parts.next() else {
                    continue;
                };
                parts.next().is_some().then(|| prefix.clone().join(first))
            };
            match common_prefix {
                Some(cp) => {
                    common_prefixes.insert(cp);
                }
                None => objects.push(meta),
            }
        }

        Ok(ListResult {
            objects,
            common_prefixes: common_prefixes.into_iter().collect(),
        })
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> OsResult<()> {
        let inner = self.inner.clone();
        let from = from.to_string();
        let to = to.to_string();
        let create = matches!(options.mode, object_store::CopyMode::Create);
        block(move || inner.transfer(&from, &to, create, false)).await
    }

    async fn rename_opts(&self, from: &Path, to: &Path, options: RenameOptions) -> OsResult<()> {
        let inner = self.inner.clone();
        let from = from.to_string();
        let to = to.to_string();
        let create = matches!(options.target_mode, RenameTargetMode::Create);
        block(move || inner.transfer(&from, &to, create, true)).await
    }
}

#[cfg(test)]
impl RedbStore {
    /// Test-only: whether `location`'s payload is stored in the heap (vs inline).
    fn is_heap(&self, location: &Path) -> OsResult<bool> {
        let inner = self.inner.clone();
        let path = location.to_string();
        let (_none, meta) = inner.get(&path, false)?;
        Ok(matches!(meta.payload, PayloadLoc::Heap(_)))
    }
}

impl std::fmt::Debug for RedbStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedbStore").finish_non_exhaustive()
    }
}

impl std::fmt::Display for RedbStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "RedbStore")
    }
}

/// A buffered multipart upload. Parts are buffered in memory keyed by call order
/// (`put_part` may be called concurrently and complete out of order, but the
/// final byte order is the call order). On `complete` the parts are concatenated
/// in sequence order and written as one object (to the heap if large enough).
struct RedbMultipart {
    store: RedbStore,
    location: Path,
    parts: BTreeMap<usize, Vec<u8>>,
    next_seq: usize,
}

impl std::fmt::Debug for RedbMultipart {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedbMultipart")
            .field("location", &self.location)
            .field("parts", &self.parts.len())
            .finish()
    }
}

#[async_trait::async_trait]
impl MultipartUpload for RedbMultipart {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        let seq = self.next_seq;
        self.next_seq += 1;
        let mut buf = Vec::with_capacity(data.content_length());
        for chunk in &data {
            buf.extend_from_slice(chunk);
        }
        self.parts.insert(seq, buf);
        futures::future::ready(Ok(())).boxed()
    }

    async fn complete(&mut self) -> OsResult<PutResult> {
        let mut buf = Vec::new();
        for part in self.parts.values() {
            buf.extend_from_slice(part);
        }
        self.store
            .write_bytes(&self.location, buf, WriteMode::Overwrite)
            .await
    }

    async fn abort(&mut self) -> OsResult<()> {
        self.parts.clear();
        Ok(())
    }
}

// ---- helpers ---------------------------------------------------------------

/// Run a blocking file/redb closure on the blocking pool, flattening the join
/// error.
async fn block<T, F>(f: F) -> OsResult<T>
where
    F: FnOnce() -> OsResult<T> + Send + 'static,
    T: Send + 'static,
{
    tokio::task::spawn_blocking(f).await.map_err(generic)?
}

fn object_meta(location: Path, meta: &StoredMeta) -> ObjectMeta {
    ObjectMeta {
        location,
        last_modified: millis_to_datetime(meta.last_modified_millis),
        size: meta.size,
        e_tag: Some(meta.e_tag.clone()),
        version: None,
    }
}

fn millis_to_datetime(ms: i64) -> DateTime<Utc> {
    DateTime::<Utc>::from_timestamp_millis(ms).unwrap_or_else(Utc::now)
}

fn encode_meta(meta: &StoredMeta) -> OsResult<Vec<u8>> {
    bincode::serialize(meta).map_err(generic)
}

fn decode_meta(bytes: &[u8]) -> OsResult<StoredMeta> {
    bincode::deserialize(bytes).map_err(generic)
}

fn generic<E: std::error::Error + Send + Sync + 'static>(e: E) -> object_store::Error {
    object_store::Error::Generic {
        store: STORE,
        source: Box::new(e),
    }
}

/// The temp path a vacuum rewrites into: a hidden sibling of the container file
/// in the same directory (so `rename` stays on one filesystem and is atomic).
fn temp_sibling(path: &FsPath) -> PathBuf {
    let name = path
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("beacon.db");
    path.with_file_name(format!(".{name}.vacuum-tmp"))
}

fn not_found(path: &str) -> object_store::Error {
    object_store::Error::NotFound {
        path: path.to_string(),
        source: "no such object".into(),
    }
}

fn precondition(path: &str, msg: &'static str) -> object_store::Error {
    object_store::Error::Precondition {
        path: path.to_string(),
        source: msg.into(),
    }
}

#[cfg(test)]
mod tests;
