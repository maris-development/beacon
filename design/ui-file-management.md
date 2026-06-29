# Design: Dataset file management through the admin UI

**Status:** v1 implemented (upload + download + delete, super-user-only)
**Author:** (design discussion)
**Scope:** Let super-users upload, download, and delete dataset files from the
beacon-web admin UI, backed by new authenticated `/api/admin/datasets/*`
endpoints over the existing object-storage layer.

## Implementation status (v1)

Shipped on this branch:

- **Core** ([beacon-core/src/dataset_files.rs](../beacon-core/src/dataset_files.rs)) —
  `validate_dataset_path` (anti-traversal + internal-prefix guard), extension
  allowlist, and streamed `upload`/`download`/`delete` over the datasets store,
  with unit tests. Runtime methods (`upload_dataset`/`download_dataset`/
  `delete_dataset`) wire in the allowlist (from the format registry), the
  `BEACON_MAX_UPLOAD_BYTES` cap, and a best-effort delete dependency check
  against registered tables.
- **API** ([beacon-api/src/axum/admin/datasets.rs](../beacon-api/src/axum/admin/datasets.rs)) —
  `POST /api/admin/datasets/upload` (streaming), `GET …/datasets/download`,
  `DELETE …/datasets`, behind the existing super-user `basic_auth` gate, with
  status mapping (400/404/409/413) and end-to-end HTTP tests.
- **Config** — `BEACON_MAX_UPLOAD_BYTES` (default 5 GiB; `0` = unlimited).
- **Chunked / resumable upload** for large files — `UploadManager`
  ([beacon-core/src/dataset_uploads.rs](../beacon-core/src/dataset_uploads.rs))
  keeps one session per upload (object-store `MultipartUpload` handle), accepts
  atomic, in-order, idempotently-retryable parts, enforces the cumulative cap,
  and a background sweeper aborts idle sessions. Endpoints:
  `POST …/datasets/upload/initiate`, `PUT …/upload/part`, `POST …/upload/complete`,
  `DELETE …/upload`. Config: `BEACON_UPLOAD_PART_SIZE` (default 8 MiB),
  `BEACON_UPLOAD_SESSION_TTL_SECS` (default 1 h).
- **SDK** ([clients/beacon-ts/src/admin.ts](../clients/beacon-ts/src/admin.ts)) —
  `uploadDataset` (auto **threshold switch**: single-shot ≤ 50 MiB, chunked above,
  with per-part retry + `onProgress`) / `downloadDataset` / `deleteDataset`.
- **Web UI** ([clients/beacon-web/src/pages/datasets.tsx](../clients/beacon-web/src/pages/datasets.tsx)) —
  Upload button (uploads into the browsed folder, with a progress bar) + per-file
  Download/Delete.

Deferred to later iterations (see §7): soft-delete/trash, audit log, per-deployment
quota, Zarr-directory upload, rename/move, and per-path RBAC enforcement. Note
v1 already includes delete (with the dependency check) rather than holding it to
v2 as originally staged.

---

## 1. Motivation

Beacon discovers data files passively from the datasets store
(`DatasetsStore`, local FS or S3). There is currently **no in-product way to get
a file in or out**: operators must `scp` / `aws s3 cp` into the mounted volume
out of band. The web UI's [datasets page](../clients/beacon-web/src/pages/datasets.tsx)
is read-only browse + schema preview.

This is the missing first step of the core self-service loop the UI already
half-supports: *drop a file → `CREATE EXTERNAL TABLE` over it → query it*. The
external-table dialog and query editor already exist; only the "drop a file"
(and its inverse, "get a result file out") steps are missing.

Goals:

- **Upload** one or more dataset files into the datasets store from the UI.
- **Download** an existing dataset file from the UI.
- **Delete** a dataset file from the UI, safely.

Non-goals (this iteration):

- Folder rename/move, multi-file archive download, drag-and-drop reorganization.
- Editing file contents in place.
- Exposing these operations to non-super-user roles (see §6 — the privilege
  vocabulary is designed to allow it later, but enforcement stays super-user-only
  for v1).
- Managing the internal writeable prefix (`__beacon__`) — that stays hidden.

---

## 2. Current state (what we build on)

| Piece | Location | Relevance |
|---|---|---|
| `ObjectStores { datasets, tables, tmp }` | [beacon-object-storage/src/lib.rs](../beacon-object-storage/src/lib.rs) | `datasets` is an `Arc<DatasetsStore>` wrapping `object_store` (local or S3). `put`/`get`/`delete` are native to `object_store`. |
| `DatasetsStore` | [beacon-object-storage/src/datasets_store.rs](../beacon-object-storage/src/datasets_store.rs) | Lists/caches/watches files; hides Beacon-internal objects; has `DATASETS_WRITEABLE_PREFIX = "__beacon__"` for internal writeable objects and `is_*_writeable_prefix` guards. |
| Admin router | [beacon-api/src/axum/admin/mod.rs](../beacon-api/src/axum/admin/mod.rs) | `/api/admin/*`, gated by `basic_auth` middleware. New resource slots in next to crawlers/external-tables. |
| `basic_auth` middleware | [beacon-api/src/axum/auth.rs](../beacon-api/src/axum/auth.rs) | Rejects non-super-user (`identity.is_super_user`) with 403; 401 if no/invalid creds. |
| Privilege model | [beacon-auth/src/role.rs](../beacon-auth/src/role.rs) | `Privilege::{Read, Insert, Delete, …}` × `PrivilegeTarget` (tables **and** path-globs). Vocabulary to extend later. |
| Datasets page | [clients/beacon-web/src/pages/datasets.tsx](../clients/beacon-web/src/pages/datasets.tsx) | Folder tree, preview, schema. Host for upload/download/delete controls. |
| TS SDK admin client | [clients/beacon-ts/src/admin.ts](../clients/beacon-ts/src/admin.ts) | `admin.createCrawler()` etc. New `admin.uploadDataset()` / `downloadDataset()` / `deleteDataset()` land here. |

Key existing affordance: the object store already distinguishes a
Beacon-internal writeable prefix from user-visible datasets. User uploads must
land in the **user-visible** area, never under `__beacon__`, and the internal
prefix must remain rejected for all three operations.

---

## 3. API design

All endpoints live under the existing super-user-gated admin router and return
the admin error shape (`bad_request` → 400 with text). Paths are relative to the
datasets-store root and always validated (see §4).

### 3.1 Upload

```
POST /api/admin/datasets/upload?path=<dir/relative/path>
Content-Type: application/octet-stream   (single-file, streaming body)
```

- `path` is the **destination key** (directory + filename) relative to the
  datasets root, e.g. `ctd/cruise42/station1.nc`.
- Body is streamed straight into `datasets.put_multipart` / chunked `put` — never
  buffered whole in memory (scientific NetCDF/Zarr files are large).
- **Overwrite policy:** reject if key exists unless `?overwrite=true`. Default is
  reject, to avoid silently clobbering a file backing a live table.
- Response `201`: `{ "path": "...", "size": <bytes>, "etag": "..." }`.

> Multipart/form-data is an acceptable alternative if the frontend upload library
> needs it; the streaming raw-body form is preferred to keep memory flat. Pick
> one and keep the SDK consistent.

For Zarr (a directory of many files) the v1 answer is: upload a **zipped** Zarr
and unpack server-side into a directory key, OR require per-object uploads.
Recommend deferring Zarr-directory upload to a follow-up and documenting the
limitation; single-file formats (NetCDF, Parquet, CSV, Arrow, GeoTIFF) cover the
common case.

### 3.2 Download

```
GET /api/admin/datasets/download?path=<relative/path>
```

- Streams `datasets.get(path)` body back with
  `Content-Disposition: attachment; filename="<basename>"` and a best-effort
  `Content-Type`.
- 404 if the key does not exist or resolves under a hidden/internal prefix.

### 3.3 Delete

```
DELETE /api/admin/datasets?path=<relative/path>
```

- **Dependency check first** (see §4.3): refuse with `409 Conflict` and a list of
  dependents if any table or crawler references the file/path.
- On success: `datasets.delete(path)`, invalidate the listing cache for the
  prefix, return `200` with `{ "path": "...", "deleted": true }`.
- **Soft-delete option (recommended):** move into a `__trash__` area under the
  internal writeable prefix instead of hard-deleting, with a TTL sweep. Lets an
  operator recover from a mistaken delete. If we skip this for v1, the dependency
  check and confirmation dialog become mandatory, not optional.

### 3.4 (Reuse) Listing

The existing `GET /api/list-datasets` already powers the folder tree; no new list
endpoint is needed. After a mutation the frontend re-fetches it. Server must
invalidate the `DatasetsStore` listing cache on upload/delete so the new state is
visible immediately (the FS watcher handles eventual consistency for
out-of-band changes, but a UI mutation should reflect instantly).

---

## 4. Safety / security (the part that matters most)

File mutation is the highest-risk surface in the product. Each control below is a
hard requirement, not a nice-to-have.

### 4.1 Path confinement (anti-traversal)

- Reject any `path` containing `..`, a leading `/`, a drive/UNC prefix, or NUL.
- Normalize to a relative `object_store::path::Path` (which itself rejects
  `..` segments) and verify the canonical result is still under the datasets
  root. Never join user input onto a filesystem path with `PathBuf::push` before
  this check.
- Reject any path resolving under `DATASETS_WRITEABLE_PREFIX` (`__beacon__`) or
  any other hidden/internal prefix — these are Beacon-owned.
- On **S3** there is no real filesystem to traverse, but the same key
  normalization applies so behavior is identical across backends (and so a
  symlink on local FS can't escape — `object_store`'s local backend should be
  configured to not follow symlinks out of root).

### 4.2 Upload validation

- **Extension/format allowlist:** only accept extensions beacon can actually read
  (`.nc`, `.parquet`, `.csv`, `.arrow`/`.ipc`, `.tif`/`.tiff`, `.zip` for zarr,
  etc.). Derive from the format registry, not a hardcoded list, so it stays in
  sync with supported readers.
- **Size cap:** configurable `BEACON_MAX_UPLOAD_BYTES` (sensible default, e.g.
  some GB), enforced by counting streamed bytes and aborting + cleaning up the
  partial object on exceed.
- **Content-type is advisory only** — never trust it for authorization or routing.

### 4.3 Delete dependency check

Before deleting, resolve whether the file is referenced by:

- A registered **external table** (its location/glob covers the path).
- A **crawler**'s discovery pattern (deleting matched files mid-crawl is
  surprising; at minimum warn).

If referenced, return `409` with the dependent names so the UI can show
"`station1.nc` backs table `ctd_station1` — drop the table first." This requires
a lookup against the table catalog in [beacon-data-lake](../beacon-data-lake);
scope this lookup carefully so it's cheap.

### 4.4 AuthZ

- v1: all three endpoints require `is_super_user` (existing `basic_auth`
  middleware). No code change to authz needed beyond mounting under the admin
  router.
- Download in particular is **data exfiltration** — keep it super-user-only even
  though reads elsewhere may be granted to lesser roles.

### 4.5 Audit

Every successful (and rejected) mutation should emit an audit record:
principal, operation, path, size, result. Beacon already has a query
observability registry + Activity page — extend that surface rather than
inventing a new one. This is near-mandatory once delete exists.

---

## 5. Frontend (beacon-web)

On the [datasets page](../clients/beacon-web/src/pages/datasets.tsx):

- **Upload:** an "Upload" button per folder → file picker (multi-select), with a
  progress bar driven by the streaming upload. Show per-file success/failure;
  surface allowlist/size/overwrite errors inline.
- **Download:** a download icon on each file row → hits the download endpoint.
- **Delete:** a delete action on each file row → confirmation dialog that
  **shows the dependency-check result** (disabled/blocked with explanation if the
  file backs a table). Never a bare "are you sure".
- After any mutation, invalidate the dataset listing query (TanStack Query or
  equivalent) to refresh the tree.

SDK additions in [clients/beacon-ts/src/admin.ts](../clients/beacon-ts/src/admin.ts):
`uploadDataset(path, blob, { overwrite })`, `downloadDataset(path) → Blob/stream`,
`deleteDataset(path)`. Mirror the existing admin-method conventions (Basic auth
header, error decoding).

---

## 6. Future-proofing for RBAC

The auth model already has `Privilege::{Read, Insert, Delete}` over path-glob
targets ([beacon-auth/src/role.rs](../beacon-auth/src/role.rs)). The clean future
mapping is:

| Operation | Privilege | Target |
|---|---|---|
| Download | `Read` | path-glob |
| Upload | `Insert` | path-glob |
| Delete | `Delete` | path-glob |

So a later change can swap the blanket `is_super_user` gate for a per-path
privilege check **without changing the endpoint contracts**. v1 should still
enforce super-user-only, but write the handler so the authz decision is a single
call that's easy to replace.

---

## 7. Rollout plan

1. **v1 — Upload + Download (super-user).** Streaming upload (path-confined,
   allowlisted, size-capped) + download. Highest value, lowest risk. No delete.
2. **v2 — Delete with safety.** Dependency check + confirmation + soft-delete to
   trash. Audit records for all three operations.
3. **v3 — Hardening.** Per-deployment storage quota, Zarr-directory upload,
   optional per-path RBAC enforcement (§6), rename/move.

---

## 8. Open questions

1. **Streaming raw body vs multipart/form-data** for upload — pick based on the
   chosen frontend upload lib; keep SDK + endpoint consistent.
2. **Soft-delete vs hard-delete** for v2 — recommend soft-delete; needs a trash
   area + TTL sweep. Decide before building delete.
3. **Zarr (directory-shaped) uploads** — zip-and-unpack vs defer. Recommend defer
   + document.
4. **Overwrite semantics** — default reject; is `?overwrite=true` enough, or do we
   want a versioned write? Recommend reject-by-default for v1.
5. **Cache invalidation contract** — confirm `DatasetsStore` exposes a way to
   invalidate a prefix's cached listing synchronously after a mutation.
