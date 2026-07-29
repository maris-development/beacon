# Beacon: single-file object store backed by redb

Replace Beacon's directory-of-JSON-files metadata layout with a single,
transactional `.db` file, exposed to DataFusion through the existing
`object_store::ObjectStore` trait.

Status: design / plan. Target branch: `claude/beacon-redb-object-store`.

---

## North star: run Beacon from a single `.db` file

The end state is DuckDB-like: **one file, `beacon.db`, holds everything** —
table definitions, extensions, views, crawlers, the dataset file bytes
themselves, managed-engine metadata (Delta/Iceberg/Lance), users/roles/grants,
and any audit log. You hand someone one file and they `beacon serve beacon.db`
and have the entire lake. No `data/` tree, no sidecars.

Two things to be clear-eyed about, because they define the design:

1. **Beacon's single file is a *container*, not a new columnar engine.** DuckDB's
   file is its own native columnar format. Beacon's file is a redb index plus a
   blob heap holding the formats Beacon already speaks — Parquet, NetCDF, Zarr
   chunks, Arrow IPC, Delta/Iceberg/Lance. DataFusion still reads those formats;
   they just live *inside* the file instead of loose in a directory. We are not
   reimplementing DuckDB's storage — we are packing the existing data lake into
   one addressable, transactional container. This is the right call: it reuses
   every reader Beacon already has and keeps time-travel/format semantics intact.

2. **The file holds what Beacon *owns*; it *references* everything external.**
   This is the DuckDB rule, and it dissolves the apparent "single-file vs S3"
   conflict. DuckDB's `.db` holds the tables it manages; `SELECT * FROM
   's3://…/x.parquet'` reads a file that lives outside the `.db`. Same here:

   | In the file (Beacon-owned) | Referenced, stays external |
   |---|---|
   | **Catalog / metadata for *every* table** — external, remote, managed, view, MV, crawler | The **files an external table points to** — local `datasets/` dir *or* S3, per config |
   | **Managed table data** Beacon created — Lance datasets, Iceberg managed warehouse, materialized-view Parquet | The **remote database** behind a remote/federated table (Postgres/MySQL) |
   | **Auth / RBAC** (Phase 3) | |

   So creating *any* table — external, remote, or managed — always writes its
   **metadata into `beacon.db`**. A *managed* table (e.g. Lance) also writes its
   **data into `beacon.db`** (heap storage + index entries). An *external*
   table's referenced files and a *remote* table's rows stay exactly where they
   are configured. **S3 therefore remains first-class**: it is where external
   dataset files live, alongside a `beacon.db` that still holds all metadata and
   all managed data. See D-S3 in §12 (resolved).

The plan below reaches that north star in phases; **Phase 1 (catalog only) is a
strict subset that ships value on its own** and de-risks everything after it.

---

## 0. Why this is small for Beacon

The generic version of this plan assumes you have to invent the storage
abstraction. **Beacon already has it.** Every piece of metadata Beacon persists
already flows through `object_store::ObjectStore` under a custom URL scheme:

| Scheme | Backend today | What lives there | Destination in the vision |
|---|---|---|---|
| `tables://` | `LocalFileSystem` (`data/tables/`) | `<name>/table.json`, `<name>/extensions.json`, `__crawlers__/<name>.json` — catalog for **every** table kind | **→ redb** (Phase 1) |
| `beacon-tables://` | routed through tables store (Lance `LanceWarehouse`) | managed **Lance** dataset data | **→ redb heap** (managed data, Phase 2) |
| `internal://` | datasets store, `__beacon__` prefix | materialized-view Parquet, Iceberg managed warehouse (`__beacon__/iceberg`) | **→ redb heap** (managed data, Phase 2) |
| `datasets://` (user-facing) | `LocalFileSystem` (`data/datasets/`) **or** S3 | **external** user files referenced by tables | **stays** local dir or S3 (referenced, not contained) |
| `tmp://` | `LocalFileSystem` (`data/tmp/`) | query output / scratch | **stays** outside (disposable, D2) |

Construction is centralized in one place —
[`ObjectStores::new`](../../beacon-object-storage/src/lib.rs) — and registration
in one function —
[`register_object_stores`](../../beacon-data-lake/src/lib.rs).

So the entire generic milestone list ("implement an `ObjectStore`") collapses to:
**build one `ObjectStore` implementation and swap it in behind those two
functions.** No caller changes. `SchemaPersistenceService`, `CrawlerPersistence`,
the `PersistentSchemaProvider` register/deregister loop, `init_tables` startup
scan — all of them keep calling `put`/`get`/`list`/`delete` on a
`dyn ObjectStore` and never learn the bytes now live in a redb file.

The single non-`ObjectStore` piece of on-disk state is auth/RBAC, which uses
rusqlite at `data/users/directory.db`
([`beacon-auth/src/sqlite.rs`](../../beacon-auth/src/sqlite.rs), wired at
[`runtime.rs`](../../beacon-core/src/runtime.rs)). It is already a single file;
folding it into the same `.db` is a separate, optional phase (§7).

---

## 1. Goal & non-goals

**Goal (end state).** A Beacon deployment is one file, `beacon.db`, containing
catalog + metadata + dataset bytes + managed-engine metadata + auth. `beacon
serve beacon.db` and you have the whole lake; copy the file and you've moved the
whole deployment. DataFusion reads it as a normal object store; Parquet/Arrow
reads are zero-copy off an mmap.

**Goal (Phase 1, ships first).** The *catalog and metadata* — everything in the
`tables://` store — is one file. This alone eliminates the directory-of-JSON
layout (`table.json`, `extensions.json`, `__crawlers__/*.json`), is a self-
contained deliverable, and proves the store transparently under real load before
the larger dataset bytes move into it.

**Non-goals** (these shape everything):

- **Multiple concurrent writers.** One writer process, many readers. Enforced by
  `flock(LOCK_EX)` on the file. This matches Beacon's actual deployment: one
  server owns the data dir.
- **Network filesystems.** mmap coherence and advisory locks are broken on NFS.
  Unsupported; keep `LocalFileSystem`/S3 as the fallback backend.
- **Being a general filesystem.** Objects are write-once. Overwrite = new blob +
  tombstone. This is already true of how Beacon uses the tables store (it
  rewrites whole `table.json` blobs).
- **Replacing S3.** When `S3Config` is set, datasets stay on S3. The redb store
  is a *local-backend* option, not a cloud replacement.

---

## 2. Architecture

New crate: **`beacon-redb-store`** (workspace member), or a module inside
`beacon-object-storage`. It exposes one public type, `RedbStore`, implementing
`object_store::ObjectStore`, plus an open/create constructor. Three internal
layers, each with one job — identical to the generic design:

```
  ObjectStore impl        path -> extents; get/get_ranges/put/list/delete/copy
  ------------------       -----------------------------------------------------
  redb (index)            transactions, MVCC, crash recovery, THE commit point
  ------------------       -----------------------------------------------------
  File (heap + extents)   append-only blob bytes, mmap reads, extent allocator
```

**Bulk bytes never enter redb.** redb stores only `path -> [(heap_offset, len)]`
+ etag + mtime. A large Parquet write touches redb for microseconds at commit;
everything else is `pwrite` to the heap tail.

### File layout

```
[ header slot A (4 KiB) ][ header slot B (4 KiB) ][ ... extents appended at EOF ... ]
```

- Dual-buffered, checksummed header slots (alternating write survives a torn
  write). Header holds magic, version, and the extent table.
- Extents are append-only, each tagged `Redb` or `Heap`. Both regions grow by
  appending a new extent at EOF and recording it in the table. Monotonic: a
  crash mid-append leaks space, never corrupts.

### redb via `StorageBackend`

redb doesn't need to own the file; it needs a growable, fsyncable address space.
Implement `RegionBackend: redb::StorageBackend` that maps redb's logical offsets
onto file offsets through the extent table:

```rust
impl StorageBackend for RegionBackend {
    fn len(&self) -> io::Result<u64>;                    // sum of Redb extents
    fn set_len(&self, len: u64) -> io::Result<()>;       // append extents, persist table
    fn read(&self, offset: u64, out: &mut [u8]) -> ...;  // split across extents
    fn write(&self, offset: u64, data: &[u8]) -> ...;    // split across extents
    fn sync_data(&self) -> io::Result<()>;               // fsync the whole file
}
```

Pin the `redb` version and read the `StorageBackend` trait signature from
docs.rs at that exact version before writing this — the signature has moved
across majors (3.0 changed `read` to an out-param). Record the pinned version in
`Cargo.toml` with the same "predates the no-fresh-packages window" note the repo
uses for other pins.

### The commit point

**redb's transaction is the only commit point. Do not build a second one.**

```
1. pwrite blob bytes to heap tail        (no lock, no fsync, concurrent OK)
2. file.sync_data()                       (bytes durable)  — F_FULLFSYNC on macOS
3. redb write txn:
     OBJECTS.insert(path, (extents, len, etag, mtime))
     STATE.insert("heap_len", high_water)
   txn.commit()                           (atomic; object becomes visible here)
```

Step 2 strictly before step 3 is the entire correctness argument: committed
metadata must never reference bytes that a crash could lose.

---

## 3. Data model (redb tables)

| Table | Key | Value | Purpose |
|---|---|---|---|
| `OBJECTS` | `&str` (object path) | `(Vec<Extent>, u64 len, etag, mtime)` | the object store |
| `STATE` | `&str` | `u64` | `heap_len` high-water mark, format version |
| `GARBAGE` | `(u64 off, u64 len)` | `u64 dead_at_txid` | GC tombstones |
| `SEGMENTS` | `u64 seg_id` | `(u64 live, u64 total)` | fragmentation stats |

Path keys are `&str`, so redb's B-tree gives prefix listing for free — which is
exactly what Beacon's hot paths need:

- `init_tables` lists the whole `tables://` store for `*/table.json`
  ([loading.rs](../../beacon-data-lake/src/table_runtime/loading.rs)).
- `CrawlerPersistence::load_all` scans the `__crawlers__/` prefix.
- `remove_persisted_table` lists+deletes everything under `tables://<name>/`
  ([schema_persistence.rs](../../beacon-data-lake/src/table_runtime/schema_persistence.rs)).

All three become ordered range scans. `list_with_delimiter` (used by
`PrefixStore` and DataFusion listing) is a prefix scan with delimiter grouping.

---

## 4. `ObjectStore` implementation

Beacon is on `object_store = 0.13.1` (see root `Cargo.toml`). Match Beacon's
existing `DatasetsStore`
([datasets_store.rs](../../beacon-object-storage/src/datasets_store.rs)), which
already implements the full 0.13 trait surface — use it as the template for
method signatures and error mapping.

Methods to implement (0.13 removed most default methods):

- `get_opts`, **`get_ranges`** (override explicitly — the Parquet reader hits it
  for footer + column chunks; we have no network round-trips to coalesce, so it's
  N pointer slices, no I/O), `head`
- `put_opts` (incl. `PutMode::Create` / `PutMode::Update(etag)`),
  `put_multipart_opts`
- `list`, `list_with_delimiter`
- `delete` / `delete_stream`
- `copy_opts` / `copy_if_not_exists`

Use `#[deny(clippy::missing_trait_methods)]` so a future `object_store` bump
can't silently fall back to a broken default.

### Read path — zero-copy

`get`/`get_ranges` return `Bytes` without copying, via `Bytes::from_owner`
(bytes ≥ 1.9, already in the tree) over an mmap slice that keeps the mapping
alive:

```rust
struct MmapSlice { map: Arc<Mmap>, off: usize, len: usize }
impl AsRef<[u8]> for MmapSlice {
    fn as_ref(&self) -> &[u8] { &self.map[self.off..self.off + self.len] }
}
Bytes::from_owner(MmapSlice { map: self.map.clone(), off, len })
```

A Parquet scan does no allocation and no memcpy on the read path — strictly
better than `LocalFileSystem`, which `read`s into fresh buffers.

### Write path — multipart, out-of-order parts

`put_multipart_opts` returns a `MultipartUpload`. `put_part` returns a *detached*
future; parts complete out of order but the final byte order is the order
`put_part` was *called*. Track parts in a `BTreeMap<seq, Extent>`; `complete`
collects extents in seq order, fsyncs the heap once, then does a single redb txn.
An object is therefore an **extent list, not a contiguous range**. Keep parts
large (8–64 MiB) so ranges rarely straddle an extent boundary. `PutPayload` is
already `Vec<Bytes>` — use vectored `pwrite`, don't concat.

### Conditional writes — a real upgrade for Beacon

`PutMode::Create` / `Update(etag)` map onto real redb transactions. Two things in
Beacon benefit immediately:

- The **managed-table engines** (Delta, Iceberg) need atomic
  create-if-not-exists for their commit protocol. On S3 that historically needs
  an external lock manager; on this store it's a native transaction. If the
  *datasets* store is backed by `RedbStore` (local mode), Delta/Iceberg get
  stronger guarantees than on S3. Worth advertising.
- `copy_if_not_exists` becomes trivially correct.

### Async over a sync backend

mmap page faults block the thread. Mirror `LocalFileSystem`: wrap cold reads in
`spawn_blocking`; hot page-cached reads can return ready futures.

---

## 5. Integration into Beacon (the actual wiring)

This is where the Beacon-specific work is, and it is deliberately tiny.

1. **`StorageConfig`** ([config.rs](../../beacon-object-storage/src/config.rs)):
   add a backend selector for the local stores, e.g.
   `local_backend: LocalBackend { Directory | RedbFile { path } }`, defaulting to
   `Directory` (no behavior change until opted in). Fill it in
   [`beacon-config`](../../beacon-config/src/lib.rs) from a new env var
   (`BEACON_STORE_BACKEND=redb|dir`, default `dir`).

2. **`ObjectStores::new`** ([lib.rs](../../beacon-object-storage/src/lib.rs)):
   when `redb` is selected, build `tables` (and `tmp`) as `RedbStore` instead of
   `LocalFileSystem::new_with_prefix`. One `RedbStore` per scheme, or a single
   file with a scheme prefix baked into the key — see decision D2. Nothing else in
   this function or its callers changes.

3. **Datasets (optional, D1).** `create_datasets_store`
   ([datasets_store.rs](../../beacon-object-storage/src/datasets_store.rs)) picks
   `LocalFileSystem` vs S3 today. Add `RedbStore` as a third local option. The
   `DatasetsStore` wrapper (event cache, `__beacon__` hiding, NetCDF URL
   translation) sits *above* the inner store and is backend-agnostic — but note
   the FS event listener (`FsEventListener`) watches a real directory and must be
   disabled for the redb backend (there is no directory to inotify; emit events
   from the store's own `put`/`delete` instead, or leave the cache off).

4. **Registration** ([lib.rs](../../beacon-data-lake/src/lib.rs)): unchanged.
   `register_object_stores` still registers whatever `ObjectStores` holds.

5. **Migration on first boot.** Add a one-shot importer: if `BEACON_STORE_BACKEND
   =redb` and `data/beacon.db` is absent but `data/tables/` has `*.json`, copy
   every object from the old `LocalFileSystem` tables store into the new
   `RedbStore` (a `list` + `get` + `put` loop — objects are opaque bytes, so the
   `typetag` `TableDefinition` payloads, encrypted SQL-DB credentials, etc. all
   copy verbatim with no re-serialization). Leave the old directory in place;
   don't delete user data automatically. Ship this as `beacon-datalake-cli
   migrate-store` too, for offline runs.

### What is explicitly *not* touched

- **`TableDefinition` / `typetag` contract**
  ([table_ext.rs](../../beacon-datafusion-ext/src/table_ext.rs) and the
  per-engine `definition.rs` files). The store moves *bytes*; the JSON schema of
  those bytes is unchanged. `listing_table`, `view_table`, `materialized_view`,
  `remote_table`, external-SQL definitions all round-trip untouched.
- **Encrypted credentials** for external SQL databases
  ([beacon-sql-databases/src/definition.rs](../../beacon-sql-databases/src/definition.rs))
  — still encrypted-at-rest inside the blob; the store never sees plaintext.
- **Managed-engine native metadata** (Delta `_delta_log`, Iceberg
  `__beacon__/iceberg`, Lance dataset dirs). These live in the *datasets* store
  and only move into the `.db` if D1 (datasets-on-redb) is taken.
- **Auth SQLite** unless D3 is taken (§7).

---

## 6. Space reclamation

Two independent problems, same as the generic plan:

- **redb's own pages** — reclaimed by redb's freelist; `Database::compact()`
  shrinks. Nothing to build.
- **The heap** — append-only; reclamation is ours.

Build order:

1. **Full rewrite** (`VacuumMode::Rewrite`). New file, copy live blobs, rebuild
   index, `rename()`. ~50 lines, always correct, needs 2× disk, exclusive access.
   **Build first** — it's the escape hatch, the correctness oracle, *and* the
   "hand me a small file" export path Beacon needs for distributing a dataset.
2. **Hole punching** (`VacuumMode::PunchHoles`). `F_PUNCHHOLE` (macOS),
   `fallocate(PUNCH_HOLE|KEEP_SIZE)` (Linux). Frees physical blocks without moving
   logical offsets, so the index is untouched and no reader coordination for
   relocation is needed. O(garbage), online. 4 KiB granularity — fine for
   multi-MB Parquet/Zarr. Makes the file sparse (`ls` lies, `du` tells truth);
   document it.
3. **Segmented GC** — only if `SEGMENTS` stats prove real fragmentation. Defer.

### The silent-corruption trap (read this twice)

**Never free space a live reader may still be mmapping.** A hole-punched page
under an existing mapping reads back as **zeros**, not SIGBUS — queries return
wrong answers and nothing logs. Guard rails:

- Track the oldest live read transaction's txid; only reclaim garbage whose
  `dead_at_txid` is older.
- Per-blob checksum, verified on read behind a flag — turns a silent zero-page
  into a loud error if GC is ever wrong.

### Expose as retention, not compaction

Because Delta/Iceberg time-travel *needs* old bytes, frame reclamation as
retention:

```rust
db.vacuum(VacuumOptions {
    retain:    Duration::from_secs(7 * 86400),
    mode:      VacuumMode::PunchHoles,   // or Rewrite for a shippable file
    max_bytes: Some(64 << 30),
})?;
```

One mechanism covers reader-epoch safety and time-travel semantics. Surface it
through the admin API / `beacon-datalake-cli` alongside the existing dataset
management endpoints.

---

## 7. Optional: fold auth/RBAC into the same file (phase 2+)

Auth is the one non-`ObjectStore` state:
[`SqliteStore`](../../beacon-auth/src/sqlite.rs) over `data/users/directory.db`,
holding `users`, roles, and grants. Options:

- **Leave it.** Two files (`beacon.db` + `directory.db`). Simplest; still a big
  reduction from a directory tree of JSON. Recommended default.
- **Port to redb tables** in the same file. Auth access is low-volume
  (login, GRANT/DENY) so it fits redb transactions cleanly and would make the
  deployment truly one file. Cost: rewrite `SqliteStore` against redb and a
  migration. Do this only after the object-store half is proven.

Note a pre-existing config wart to clean up while here: auth's path comes from
the legacy `lazy_static` `USERS_DIR` (hardcoded `./data/users`), which ignores
`BEACON_DATA_DIR` — see [beacon-config/src/lib.rs](../../beacon-config/src/lib.rs).
Consolidation is a natural moment to route it through the resolved data dir.

---

## 8. Invariants (violate any and you corrupt data)

1. Heap bytes fsynced **before** the redb txn that references them commits.
2. Extent table is **monotonic** — extents only ever appended. Crash between
   allocate and record leaks space; never corrupts.
3. Never truncate/shrink the file while readers hold a mapping → SIGBUS.
4. Never punch a hole in a region reachable from a live reader's snapshot.
5. Bytes below committed `heap_len` are immutable forever.
6. One writer. `flock(LOCK_EX)` on the file itself.

---

## 9. Milestones — phased path to one file

Three phases. Each ends at a shippable, releasable state; you can stop between
them. Milestone 0 is not optional and not reorderable.

### Phase 1 — catalog in one file (the engine + the proving ground)

| # | Deliverable | Rough size |
|---|---|---|
| 0 | **Crash-injection harness.** `File` wrapper that fails/reorders any write. Test: 1000 commits × crash at every write boundary → recovery always lands on a valid txid with an intact heap. | 2–3 d |
| 1 | Header + extent table + allocator (dual-slot, checksummed, monotonic) + `flock`. | 2 d |
| 2 | `RegionBackend: StorageBackend`; open a redb inside the file; run redb's own test suite against it. | 3 d |
| 3 | Heap: aligned append, mmap reads, `Bytes::from_owner` slices, remap-on-grow. | 2 d |
| 4 | `RedbStore: ObjectStore` — `get_opts`, `get_ranges`, `put_opts`, `list`, `list_with_delimiter`, `head`, `delete`/`delete_stream`, `copy_opts`. Pass the `object_store` conformance suite. | 4 d |
| 5 | `put_multipart_opts` + out-of-order parts + extent lists. | 3 d |
| 6 | **Beacon wiring:** `StorageConfig.local_backend`, `ObjectStores::new` branch, `BEACON_STORE_BACKEND`. Back `tables://` (+`tmp://`) with `RedbStore`; boot Beacon, create every table kind, restart, confirm reload. Run existing `table_runtime` + crawler tests against the redb backend. | 2 d |
| 7 | Migration importer (`data/tables/*.json` → `beacon.db`) + `beacon-datalake-cli migrate-store` / `inspect-store`. | 1–2 d |
| 8 | GC: tombstones, reachability sweep at open, `VacuumMode::Rewrite` (also the export / "ship a small file" path). | 3 d |
| 9 | `VacuumMode::PunchHoles` + reader-epoch gating + per-blob checksums. | 3 d |

At the end of Phase 1 the store engine is production-grade and the catalog is
one file. Datasets still live in `data/datasets/` (or S3). This is a coherent
release on its own.

### Phase 2 — managed data in the file (Beacon-owned bytes)

Move the **managed / Beacon-owned** stores into redb. External user datasets
(`datasets://` user-facing) are **not** touched — they keep living in the local
`datasets/` dir or S3 and are referenced, per §North star.

| # | Deliverable | Rough size |
|---|---|---|
| 10 | Back the **`internal://`** store (`__beacon__`: materialized-view Parquet + Iceberg managed warehouse) with `RedbStore` heap. This is the MV/Iceberg managed data. | 2–3 d |
| 11 | Route the **Lance** warehouse (`beacon-tables://` via `LanceWarehouse`) onto `RedbStore` so managed Lance datasets store as heap blobs with index entries. | 2–3 d |
| 12 | Validate managed engines through the in-file store: Delta/Iceberg get *stronger* commit guarantees here (real CAS via redb txns) than on S3 — add tests proving conditional-put atomicity. | 2 d |
| 13 | Batch-put API (one redb txn wrapping many puts) for partitioned / CTAS / MV output, to avoid N serialized fsyncs. | 2 d |
| 14 | Leave `datasets://` (external files) on `LocalFileSystem`/S3 **by design**; document that `CREATE EXTERNAL TABLE` metadata lands in `beacon.db` while the referenced files do not move. Confirm the `DatasetsStore` wrapper (event cache, `__beacon__` hiding, NetCDF URL translation) is unaffected. | 1 d |

At the end of Phase 2, `beacon.db` holds all metadata + all Beacon-managed data.
External files (local/S3) and remote DBs are referenced. Auth is the last piece
still outside.

### Phase 3 — the last state (truly one file)

| # | Deliverable | Rough size |
|---|---|---|
| 15 | Port auth/RBAC (`SqliteStore` → redb tables in the same file); migrate `directory.db`; fix the legacy `USERS_DIR` path wart so it honors `BEACON_DATA_DIR`. Fold in the query-audit log here too if/when it lands. | 3–4 d |
| 16 | `beacon serve beacon.db` UX: single-path invocation, self-contained bootstrap, export/import (`Rewrite`) as the shareable-file path. Docs + Docker image that mounts one file instead of a `data/` tree. | 2–3 d |

Storage engines don't fail loudly — they corrupt silently and you find out
months later when an 800 GB file won't open. If the harness (M0) is written
first, this ships. If it's written last, it never stops being debugged.

---

## 10. Testing

- **Crash injection** at every write boundary (M0) — the backbone.
- **`object_store` conformance suite** — the crate ships an `integration` module;
  it catches exactly the semantics that are easy to get subtly wrong:
  empty-prefix listing, range clamping, `NotFound` vs `Precondition` on
  conditional puts. Run it in CI (M4).
- **redb's own test suite** against `RegionBackend` (M2).
- **Beacon's existing suites against the redb backend.** Parameterize the
  `table_runtime` / `schema_persistence` / crawler tests
  ([schema_persistence.rs](../../beacon-data-lake/src/table_runtime/schema_persistence.rs),
  [loading.rs](../../beacon-data-lake/src/table_runtime/loading.rs),
  [persistent_schema_provider.rs](../../beacon-data-lake/src/table_runtime/persistent_schema_provider.rs))
  over `[InMemory, LocalFileSystem, RedbStore]` — they already run against
  `InMemory`, so this is mostly a fixture parameter. This is the cheapest, highest-
  signal proof the swap is transparent.
- **Round-trip / migration test:** build a `data/tables/` tree with several table
  kinds (listing, view, materialized-view, remote, external-SQL with an encrypted
  credential), migrate into `beacon.db`, boot, and assert every table reloads and
  queries identically.
- **Concurrent reader + writer + GC soak** with checksum verification on every
  read — the only thing that catches punch-hole-under-a-reader.
- **Fuzz `translate(offset, len)`** across extent boundaries — where the
  off-by-ones live.

---

## 11. Known limitations to document

- **Alignment does not survive multipart.** An extent starts aligned, but a
  column chunk at logical offset X lands at `extent_base + (X − extent_start)` —
  arbitrary. Fine for Parquet (arrow-rs decodes into fresh aligned buffers).
  **Not** fine for mmapping raw Arrow IPC buffers straight into SIMD kernels —
  write those through a single-shot aligned path, not multipart. Relevant to
  Beacon's Arrow-IPC / BBF formats if they ever mmap-and-hand-off.
- **Concurrent `put`s serialize at commit.** The async trait invites callers to
  assume otherwise. Say so.
- **One fsync per `put`.** A DataFusion partitioned write of 200 small Parquet
  files = 200 serialized fsyncs. Offer a batch API that wraps many puts in one
  redb txn; relevant to materialized-view / CTAS output.
- **Sparse file after `PunchHoles`.** `ls` vs `du` disagree; naive copy tools
  re-inflate. Ship `VacuumMode::Rewrite` to produce a genuinely small,
  shippable file.
- **Deleted objects don't free space until vacuum.**
- **No NFS.** Keep `LocalFileSystem`/S3 as the fallback for those deployments.
- **Opacity.** No `ls data/tables/*.json`, no external tool poking at raw files.
  Everything goes through Beacon or the export path. For Beacon this is largely
  upside (users copy one file), but the debugging workflow of "cat the
  table.json" is gone — provide `beacon-datalake-cli inspect-store`.

---

## 12. Decisions

The "run Beacon from a single `.db` file" north star resolves most of the
original forks:

- **D1 — Scope: RESOLVED = everything, phased.** Catalog (Phase 1) → dataset
  bytes (Phase 2) → auth (Phase 3). Phase 1 still ships independently.
- **D3 — Auth in or out: RESOLVED = in** (Phase 3). Single file means auth folds
  into redb; `directory.db` goes away.
- **D4 — True single file vs `heap.dat`+`index.redb`: RESOLVED = true single
  file.** The DuckDB-style "one file" property *is* the goal, so the extent-map
  `StorageBackend` (~1 week) is paid deliberately, not deferred.

- **D-S3 — RESOLVED = referenced, not contained.** The file holds Beacon-owned
  state (all metadata + all managed data + auth); external table files stay in
  the local `datasets/` dir or S3 and are *referenced*. S3 stays first-class as
  external file storage. `beacon.db` and an S3-backed external dataset coexist:
  one deployment can have both. No `redb`-vs-`s3` either/or.

Still open — decide before the phase that hits them:

- **D2 (before Phase 1 wiring) — `tmp` placement.** Even with one file for
  everything durable, query-scratch `tmp://` is disposable and high-churn;
  putting it in `beacon.db` inflates the file and feeds the GC for no benefit.
  Recommendation: keep `tmp://` on `LocalFileSystem` (or a throwaway
  `beacon-tmp.db`), *outside* the shippable file. "One file" means one file of
  *state*, not one file including scratch.
- **D-fmt (before Phase 1 ships) — format versioning.** The file becomes a
  compatibility surface (DuckDB has repeatedly paid for storage-format breaks).
  Put a version in the header from day one and decide the compat policy: refuse-
  to-open on major mismatch + an explicit `migrate` path. Cheap now, expensive
  retrofitted.

---

## 13. Why redb, not SQLite

SQLite in WAL mode is **not one file** — it spawns `-wal` and `-shm` sidecars,
defeating the entire premise. `journal_mode=DELETE` drops the sidecars but costs
concurrent readers, the thing we want. redb is copy-on-write with a dual-slot
header: no WAL, no shm, no sidecar, and its durability model is *designed to live
inside someone else's file* — the same property that makes `StorageBackend`
embedding work at all. (Beacon's auth layer uses rusqlite precisely because it
does *not* need to be embeddable — different tool, different job.)
