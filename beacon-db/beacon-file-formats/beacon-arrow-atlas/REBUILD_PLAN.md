# Rebuild `beacon-arrow-atlas` on atlas-rust 0.16

Status: complete, phases 0 to 5. Date: 2026-09-02.
Scope: this crate, its wiring in core, server, clients, tests and docs.

| Phase | State |
| --- | --- |
| 0 — prerequisites | done: the crate is a workspace member and pins `atlas-rust = "=0.16.4"` |
| 1 — the reader | done: `config`, `store`, `compat`, `backend`, `reader`, `test_support` |
| 2 — the scan | done: `datafusion/{mod,source,options,metrics,table_function}`, `FileRead::skipped`; 74 atlas + 243 nd-array tests pass, clippy clean |
| 3 — pruning and statistics | done: `pruning`, `statistics`; 105 tests pass, clippy clean |
| 4 — wiring | done: core, functions, server, config, clients, changelog; 8 end-to-end tests in `beacon-core/tests/atlas.rs` |
| 5 — integration and docs | done: `formats/test_atlas.py`, the format page rewritten and 13 other pages updated; the docs site builds |

Sections 11 to 15 record what each phase settled that this plan had wrong.

## 1. Why a rebuild

- This crate targets atlas-rust 0.14. Version 0.14 stores a collection as a
  directory: an `atlas.json` marker plus one `.af` file per array.
- atlas-rust 0.16.4 stores a collection as one immutable file. The file is
  `<prefix>/data.atlas`. An optional `<prefix>/deleted.mask` sits beside it.
- Every atlas call this crate makes is gone in 0.16: `Atlas::open_dataset`,
  `Atlas::merged_schema`, `Atlas::pruning_index`, `StoreConfig`,
  `MergedSchema`, `ColumnKey`, `StatVal`, `PruningIndex`, `ArraySchema::codec`,
  `DatasetSchema::array_attrs` and `DatasetSchema::global_attrs`.
- The workspace excludes the crate since #371 (commit `cebecfda`). Core does
  not register it. `STORED AS ATLAS` and `read_atlas` fail today.
- The old crate predates the morsel scan. It dealt dataset names round-robin at
  plan time and streamed flat batches outside the nd spine. NetCDF, HDF5, Zarr
  and TIFF now read through `MorselSource`, `FileRead`, `NdSourceExec` and
  `NdBroadcastExec`.

Decision: rewrite the crate from scratch. Keep the crate name and path. Keep
three ideas from the old crate: the reader cache, the `atlas_*` metrics and
the per-table `OPTIONS`.

## 2. The new format, checked against the 0.16.4 source

Source: `~/.cargo/registry/src/*/atlas-rust-0.16.4/`.

| Fact | Where |
| --- | --- |
| Container: `ATLS` header, segments back to back, `zstd(msgpack(footer))`, 16-byte trailer. | `src/format/mod.rs` |
| One dataset is one segment. A segment is a complete array-format 0.12.0 file. `SegmentStore` presents it as `seg{ordinal}.af`. | `src/format/segment_store.rs` |
| An open costs one `HEAD`, one 64 KiB tail read, one more read when the footer is larger, and one `GET` of `deleted.mask`. A missing mask is fine. | `src/reader/mod.rs:57-158` |
| The footer holds every dataset name, segment range, schema, attribute value and per-array statistic. Metadata costs no I/O after the open. | `src/format/footer.rs` |
| A schema holds `dtype`, `shape`, `chunk_shape`, `dimension_names` and `fill_value` per array. Datasets with equal schemas share one pool entry. | `src/schema/array.rs` |
| Statistics per array: `min`, `max`, `null_count`, `row_count`. `null_count` counts the elements equal to the fill value. A never-written array has no entry. Lists have no `min` or `max`. | `src/format/footer.rs:305-320` |
| Attributes sit at dataset scope and at array scope. A value is a scalar, a list, or `TimestampNanoseconds`. | `src/schema/attr.rs` |
| Dtypes: `Bool`, `Int8..Int64`, `UInt8..UInt64`, `Float32`, `Float64`, `String`, `Binary`, `TimestampNs`, `List`, `FixedSizeList`. | `array-format/src/dtype.rs` |
| `read_array::<T>` needs `T: ArrayElement`. That is the numeric types, `String`, `Vec<u8>` and `TimestampNs`. There is no `bool` and no list element. | `array-format/src/array.rs` |
| `read_array` checks the dtype and the shape against the footer, opens the segment once per handle, and fetches only the chunks the region overlaps. Unwritten cells come from the fill value. | `src/reader/mod.rs:521-591` |
| Atlas merges no schema. Two datasets can declare one array name with two dtypes. | `README.md`, "Types" |
| The collection is immutable. A delete writes ordinals to `deleted.mask`. `list_datasets()` hides them. `dataset(name)` refuses them. | `src/format/mask.rs` |
| `Atlas::dataset(name)` scans the footer linearly. There is no lookup by ordinal. | `src/reader/mod.rs:394-402` |
| Each `Atlas` handle owns one `DeltaCache`: a 256 MiB block budget and a 64 MiB I/O budget. | `src/reader/mod.rs:87-90` |
| `Atlas` and `DatasetView` are `Send + Sync` and implement `Debug`. | `src/lib.rs`, `src/reader/mod.rs` |

The Python ingest (`atlas create`) shapes the data Beacon meets in practice:

- One dataset per NetCDF file. The dataset name is the file name.
- xarray opens the file with `mask_and_scale=True`. Scale and offset are
  applied. Time is `datetime64[ns]`, so it lands as `TimestampNs`.
- `_FillValue` becomes the fill value. The defaults are `NaN` for floats,
  `NaT` (`i64::MIN`) for timestamps, `""` for strings and none for integers.
- Attributes such as `units` and `calendar` stay as plain attributes.
- Python adds `_pyatlas_coords` (a JSON string), `_pyatlas_timedelta` and
  `json:`-prefixed string attributes. Beacon shows them as strings.

Dependency facts:

- atlas-rust 0.16.4 was published on 2026-09-01. That is today. The repo rule
  says: do not take a package that is less than one week old. This is the
  user's own crate, so the rule is a prompt to confirm, not a block.
- The local checkout `~/git/atlas` is at 0.15.0. It lacks the 0.16 commits.
  Fetch before any upstream work.
- Every transitive dependency fits the workspace: `object_store 0.13`,
  `ndarray 0.17`, `rkyv 0.8` (the lock pins 0.8.10), `moka 0.12`,
  `lz4_flex 0.11`, `zerocopy 0.8`, `zstd 0.13`, `rmp-serde 1`, `tempfile 3`.
  MSRV 1.85 sits under the workspace floor of 1.94.

## 3. Design

### 3.1 One dataset is one morsel

```text
CREATE EXTERNAL TABLE t STORED AS ATLAS LOCATION 'obs/**/data.atlas'

listing            obs/a/data.atlas   obs/b/data.atlas          markers
                        │                  │
create_physical_plan    │  open (cached)   │  list_datasets()
                        ▼                  ▼
entries            [a#d0 a#d1 ... a#dN] [b#d0 ... b#dM]           one PartitionedFile per dataset
                        │
repartitioned      MorselSource queue ──▶ one standing entry per partition
                        │
OpenFile::open     entry ─▶ Atlas (cached) ─▶ first open of a collection: build its pruning index
                        │                   ─▶ index row of the entry: kept, or skipped
                        │                   ─▶ DatasetView ─▶ AnyDataset (lazy backends)
                        ▼                   ─▶ FileRead::plan (chunk grid, predicate masks)
workers            pop chunk ─▶ view.read_array(start, shape) ─▶ nd-encoded batch
                        │
plan               DataSourceExec ─▶ NdSourceExec ─▶ NdBroadcastExec
```

Level 1 of the morsel scan is a dataset. Level 2 is a stored chunk of it. The
backend reports the array's `chunk_shape`, so `FileRead` cuts the dataset on
the chunk grid the writer chose. One pop then reads one stored chunk per
projected array. array-format packs chunks into 8 MiB blocks and caches the
decompressed block, so arrays that share a block cost one fetch.

### 3.2 Discovery

- The marker is `data.atlas`. The collection prefix is its parent directory.
- The factory `get_ext()` returns `atlas`. The format `get_ext()` returns
  `atlas` too. The listing filter matches by suffix, so `data.atlas` matches
  and `deleted.mask` does not. `STORED AS ATLAS` already falls back to the
  glob `*.atlas` (see `listing_table_factory_ext.rs`).
- `is_atlas_marker(obj)`: the location is `data.atlas` or ends in
  `/data.atlas`. Another `*.atlas` name is skipped with a debug log, because
  `Atlas::open` hardcodes the object name. A rename adapter is out of scope.
- `top_level_atlas_markers` keeps one marker per directory.
- `discover_datasets` emits one `DatasetMetadata` per marker with format
  `atlas`. The crawler rule "extension equals format" now admits
  `data.atlas`, so a crawler can build an atlas table. Update its test.
- `schema_units` uses `units_over_stores`. `schema_options_fingerprint`
  returns `SchemaOptions::new("atlas").finish()` when no `read_dimensions` is
  set, and `None` otherwise. That matches Zarr.
- Only the 0.16 format is read. A pre-0.16 collection is a directory of `.af`
  files with an `atlas.json` registry; nothing here recognizes one, so its
  marker is simply not a marker and a listing passes over it. There is no
  compatibility path and no migration hint.

### 3.3 Open and cache

- `open_collection(store, marker) -> Atlas` calls `Atlas::open(store, prefix)`.
- `AtlasReaderCache` is a `moka::future::Cache<CacheKey, Arc<Atlas>>` sized by
  `reader_cache_size`. The key is the marker path, its `last_modified`, its
  `size`, and a mask stamp.
- The mask stamp is one `HEAD` of `<prefix>/deleted.mask`: `None` when absent,
  else `(last_modified, size, e_tag)`. The container never changes, so this
  stamp is the only thing that can retire a cached handle. One `HEAD` per
  open-through-cache is the cost. A query pays one or two.
- `get_or_open_atlas(cache: Option<&AtlasReaderCache>, store, marker)` opens
  directly when the cache is `None`.
- Memory bound: each handle owns 320 MiB of cache budget. A cache of 32
  handles can hold 10 GiB. Keep the default at 32 and document the bound. See
  section 10 for the upstream fix.

### 3.4 Dataset to `AnyDataset`

Build one `Arc<DatasetView>` per dataset and share it across its backends.
That avoids one linear name lookup per array read.

Column model. Use the netCDF and Zarr convention:

| Atlas | Column |
| --- | --- |
| array `a` | `a` |
| array attribute `k` of `a` | `a.k` |
| dataset attribute `k` | `.k` |

The old crate used the bare key for a dataset attribute. The new convention
avoids a collision between an attribute and an array of one name. Ragged
detection reads `a.sample_dimension`, which is unchanged.

Type mapping. Put it in `compat.rs` and pin it with tests:

| Atlas dtype | `NdArrayDataType` | Note |
| --- | --- | --- |
| `Int8..Int64`, `UInt8..UInt64` | the same width | |
| `Float32`, `Float64` | `F32`, `F64` | |
| `String` | `String` | |
| `Binary` | `Binary` | |
| `TimestampNs` | `Timestamp` | `TimestampNanosecond` wraps the same `i64` |
| `Bool` array | skipped | array-format reads no `bool` |
| `List`, `FixedSizeList` | skipped | the nd model has no list |
| `Attr` scalar | rank-0 array | `Bool` attributes are kept |
| `Attr` list | skipped | |

Log a skip at `debug`, not `warn`. A collection of a million datasets would
flood the log.

Fill values. `view.array_fill_value(a)` gives a `FillValue`. Convert it with
`<T as ArrayElement>::fill_element` and report it from the backend. The nd
engine nulls the cells equal to it. Three consequences to document:

- A `NaT` timestamp fill reads as null.
- A `""` string fill reads as null. An empty string in the data reads as null
  too. That mirrors the Python ingest, which cannot store a null string.
- A `NaN` float fill leaves `NaN` cells as `NaN`, because `NaN != NaN`. That
  is the engine's rule for every format.

No CF decoding. The Python ingest applies scale and offset and decodes time
before the write. The format has a native timestamp type. Beacon therefore
reads every atlas array as stored. Update `cf-decoding.md`, which says the
opposite today. A Rust-written collection with packed integers and CF `units`
reads as integers. Document that.

Projection. `dataset_from_view(view, projected: Option<&[String]>)` builds a
backend only for a projected column. A wide dataset then pays nothing for the
columns a query never names.

Dimensions. Apply `resolve_read_dimensions` and `DatasetProjection` as Zarr's
`project_read_dimensions` does. Every array has one dimension name per axis,
so nothing is invented.

### 3.5 Array backend

```rust
pub struct AtlasArrayBackend<T> {
    view: Arc<DatasetView>,
    array: String,
    shape: Vec<usize>,
    dimensions: Vec<String>,
    chunk_shape: Vec<usize>,
    fill_value: Option<T>,
}
```

- `read_subset(subset)` calls `view.read_array::<E>(array, start, shape)` and
  converts the `ArcArray` with `into_owned()`.
- `AtlasElement` bridges `NdArrayType` and `ArrayElement`. Numeric types,
  `String` and `Vec<u8>` pass through. `TimestampNanosecond` maps element-wise
  from `TimestampNs`.
- `chunk_shape()` returns the stored chunk shape. This is what aligns level 2
  of the morsel scan with the file.
- `AttributeBackend<T>` holds one value at rank 0. Copy it from the Zarr crate.

### 3.6 Schema inference

Input: the markers of the listing.

1. Open each collection through the cache.
2. For each live dataset, compute a dedupe key: the address of
   `view.schema()` (the interned pool entry) plus the sorted list of
   `(attribute key, Attr::dtype())` for the dataset and its arrays.
3. For each new key, build the lazy `AnyDataset`, apply the dimension
   narrowing, and derive its Arrow schema with `any_dataset_to_arrow_schema`.
   Label it with `marker#dataset`.
4. Merge every labeled schema with `session_widening(state).merge_schemas`.
5. Return an empty schema for a collection with no live dataset.

A fleet of similar datasets then costs one schema per distinct shape, not one
per dataset. The pass over the footer stays O(datasets) but does no I/O.

### 3.7 Physical plan

`AtlasFormat::create_physical_plan`:

1. `reject_partition_columns("Atlas", &conf)`. A dataset has no path, so no
   partition value can come from one.
2. Collect the markers from the file groups. Reduce them to top-level markers.
3. For each marker, open the collection and list its datasets. Build one
   `PartitionedFile` per dataset, in `list_datasets()` order. Keep the
   marker's `ObjectMeta` verbatim, so the file-statistics pruner still
   recognises the store. Put the dataset name and its position in that
   listing in `extensions` as `AtlasEntry { dataset: String, position: usize }`.
   The position is the row of the dataset in the pruning index (section 3.8).
4. Encode the file schema with `beacon_datafusion_ext::nd::encoded_schema`.
5. Build `AtlasSource` with the read dimensions, the pushed projection, the
   cache and the pruning switch. Rebuild the config with the new groups.
6. Wrap the scan: `DataSourceExec` under `NdSourceExec` under
   `NdBroadcastExec`. Copy Zarr's `nd_scan_plan`.

`AtlasSource` implements `FileSource` as `ZarrSource` does:

- `repartitioned` puts every entry in one `MorselSource` through
  `morsel_scan`, unless the scan is ordered or has one partition.
- `try_pushdown_filters` folds the filters into one predicate.
- `try_pushdown_projection` merges projections.
- `create_file_opener` builds an `AtlasOpener` that holds the queue and an
  `Arc<dyn OpenFile>`.

`AtlasDatasets` implements `OpenFile`:

1. Read the `AtlasEntry`. A missing entry is an internal error.
2. Open the collection through the cache.
3. Get the collection's `CandidateFilter` from the `PruneCache`. The first
   open of a collection in a scan builds the pruning index (section 3.8).
   Every later open reads it. A pruned entry returns `FileRead::skipped()`
   and counts in `atlas_datasets_pruned`.
4. Build the `DatasetView`.
5. Build the projected `AnyDataset`, narrow the dimensions, and call
   `FileRead::plan` with the projected schema, the batch size, the predicate,
   `FilePartitions::none()` and the read metrics.

`FileRead::skipped()` does not exist yet. Add it to `beacon-nd-array` as a
public constructor for "a file the scan decided not to read": no queue and
`Output::Nothing`. It is four lines.

The single-partition path reads each entry through the same `open`, then
streams it, as Zarr's opener does.

The EXPLAIN size of a partition is the marker size times the dataset count.
That is cosmetic. Document it.

### 3.8 Pruning with a collection index

A collection can hold millions of datasets. A predicate evaluation per
dataset would cost millions of evaluations. Build one index per collection
instead, and evaluate the predicate once, over every dataset in one
vectorised pass.

**When.** The first `OpenFile::open` for an entry of a collection builds the
index. Every later open of that collection reads the result. A `PruneCache`
on `AtlasSource` memoises the result per marker path for the life of the
scan. Its `moka::future::Cache::get_with` coalesces the partitions: the first
one builds, the rest await the same future. Each partition's opener holds a
clone of the cache, and the clones share one store.

**What.** `PruningIndex` holds, in `list_datasets()` order:

- `names: Vec<String>`. The row order, and the guard of section "How an
  entry uses it".
- One `StatColumn` per referenced column: `min: ArrayRef`, `max: ArrayRef`,
  `null_count: UInt64Array`, `row_count: UInt64Array`. Every array has length
  N, one row per live dataset.

**How it is built.**

1. Take the pushed predicate and the logical projected schema. Derive that
   schema from the encoded projected schema through
   `nd::encoding::nd_value_type`, because the scan schema is nd-encoded.
   Build one `PruningPredicate`. A predicate the engine refuses gives
   `CandidateFilter::KeepAll`.
2. `collect_columns` names the referenced columns. Resolve each one:
   - An array name. Call `atlas.array_stats_by_dataset(name)`. That is one
     linear pass over the footer per column, with no view and no name lookup.
     Align its `(dataset, stats)` pairs to the rows with a
     `HashMap<&str, usize>` built once from `names`. A dataset without an
     entry stays unknown in that row.
   - An attribute, `.k` or `a.k`. The value is exact, so `min = max = value`
     and `null_count = 0`. This prunes `WHERE ".platform" = 'X'` from the
     footer alone. Atlas has no bulk attribute accessor. A value needs one
     `DatasetView`, and `Atlas::dataset(name)` is a linear scan, so the pass
     is quadratic. Build an attribute column only while N is at most
     `ATTRIBUTE_INDEX_LIMIT`, 100 000. Above it, leave the column unknown
     until the upstream lookup of section 10 lands.
   - Anything else. Unknown. The column gets no `StatColumn`, and the
     predicate cannot prune on it.
3. Pack each column into typed Arrow arrays in the table type. Use a typed
   builder per target type: `Float64Builder`, `Int64Builder`,
   `TimestampNanosecondBuilder`, `StringBuilder` and so on. Keep a
   `ScalarValue::cast_to` fallback for a type the fast path lacks. Rules per
   value:
   - Cast the dataset's native dtype to the table type. A cast failure is
     null.
   - A `NaN` bound is null. `total_cmp` sorts `NaN` last, so a `NaN` max says
     nothing about the other values.
   - `Bytes` becomes `Utf8` for a `Utf8` column when it is valid UTF-8, and
     `Binary` for a `Binary` column. Otherwise null.
   - `TimestampNs` becomes `TimestampNanosecond`.
   - A missing entry is null for `min` and `max`, and `None` for both counts.
   Run the pack on `spawn_blocking`. A million rows is CPU work, not I/O.
4. `AtlasPruningStatistics` implements `PruningStatistics` over the index.
   `num_containers` is N. Each accessor returns the column's array, or `None`
   for a column the index lacks.
5. `pruning_predicate.prune(&stats)` returns one `bool` per row. Keep it as
   `CandidateFilter::Rows { kept: BooleanBuffer, names }`.

**How an entry uses it.** `AtlasEntry` carries `position`, the dataset's row
in the plan-time listing. `CandidateFilter::keeps(position, name)` reads the
bit at `position` when `names[position] == name`. A mismatch means the
listing changed between plan and open, so the entry is kept. `KeepAll` keeps
every entry. No string is hashed per entry.

**Counts.** `row_count` is the element count of one array, not the row count
of the broadcast. The predicate uses the counts to decide "every value is
null" and "no value is null". Both hold per array before and after a
broadcast, so the counts are exact for that purpose.

**Cost.** One footer pass per referenced column, one typed pack, one
vectorised evaluation. For a million datasets and one `Float64` column the
index holds 32 MB and builds in well under a second. A pruned entry then
costs one cache lookup and one empty `FileRead` at its pop.

**Fail open.** Any error in the build gives `KeepAll`. A row the index cannot
judge stays in. The predicate runs again above the scan, so a kept dataset
that matches nothing costs a read and never a wrong row.

The pushed predicate also reaches `FileRead::plan`, which prunes chunks on the
coordinate arrays. The two levels compose.

**Later options, out of scope.**

- Keep the packed `StatColumn`s on the reader-cache entry, keyed by column
  name and table type. The collection is immutable, so a second query pays no
  pack.
- When `repartitioned` already holds the predicate, build the index there and
  queue only the candidates. That saves one pop and one prefetch task per
  pruned dataset.
- Implement `contained()` for attribute columns. Their values are exact, so an
  `IN` list prunes too.

### 3.9 Statistics for the analyzer

`FileFormat::infer_stats` folds the footer per marker, as Zarr's
`StoreRanges` does:

- An array column: the lowest `min` and the highest `max` over the live
  datasets that hold statistics for it, cast to the table type. Unknown when
  any dataset's bound is missing, `NaN`, or fails to cast.
- An attribute column: the same fold over the values.
- A dataset without the column adds nothing.

It costs no I/O beyond the open. Gate it like Zarr all the same: only
`create_for_analysis` enables it, and `enable_statistics` decides.

### 3.10 Configuration and `OPTIONS`

```rust
pub struct AtlasConfig {
    pub use_reader_cache: bool,   // true
    pub reader_cache_size: u64,   // 32
    pub use_pruning: bool,        // true
    pub enable_statistics: bool,  // true
}
```

| `OPTIONS` key | Env | Effect |
| --- | --- | --- |
| `read_dimensions` | | The dimensions the table reads |
| `use_reader_cache` | `BEACON_ATLAS_USE_READER_CACHE` | Consult the reader cache |
| `use_pruning` | `BEACON_ATLAS_USE_PRUNING` | Prune datasets on footer statistics |
| `enable_statistics` | `BEACON_ATLAS_ENABLE_STATISTICS` | Let the analyzer measure a collection |

Read the keys with `format_option()`. A key arrives `format.`-prefixed and
lowercased. Reject a bad boolean at `CREATE EXTERNAL TABLE` time.

### 3.11 Table functions

`read_atlas(glob_paths)` and `read_atlas(glob_paths, dimensions)`. A path
names a `data.atlas` or a glob such as `**/data.atlas`. The function builds
the format from the session factory with `read_dimensions`, then a
`FastObjectTable`. The `read_atlas_schema` wrapper comes for free.

### 3.12 Metrics

Keep `atlas_open_time`, `atlas_prune_time`, `atlas_dataset_build_time`,
`atlas_datasets_scanned` and `atlas_datasets_pruned`. Add
`atlas_index_builds`, the number of pruning indexes a scan built, and
`atlas_index_rows`, their row total. The partition that builds an index
records its build time in `atlas_prune_time`. Register one `ReadMetrics` per
partition, as the other nd formats do.

### 3.13 Behaviour changes versus the old crate

1. The marker is `data.atlas`, not `atlas.json`. Every `LOCATION` changes.
2. A dataset attribute column is `.k`, not `k`.
3. A dataset that lacks every projected column contributes no rows. The old
   crate null-filled its rows. NetCDF and Zarr already behave this way.
4. The scan goes through the nd spine. `NdBroadcastExec` sits above it, and
   the nd projection pushdown rule applies.
5. A partitioned atlas table is refused with a clear error.
6. The pruning index is built from the footer at the first open of a scan.
   Nothing is persisted, and nothing is read from disk to build it.
7. A pre-0.16 collection is not read at all. Its `atlas.json` is not a marker,
   so a listing passes over it rather than failing a query.
8. A column two datasets type in two families — `String` in one and `Int64` in
   another — fails schema inference, and the error names both datasets. The old
   crate took atlas's own merge, which made every such column text. Beacon now
   settles it the way it settles two files of any other format, and
   `BEACON_TYPE_WIDENING_ON_CONFLICT=keep_first` takes the first dataset's type
   instead. See section 11.

## 4. Crate layout

```text
beacon-arrow-atlas/
  Cargo.toml                       atlas-rust = "=0.16.4" via the workspace
  src/lib.rs                       crate docs, module list, re-export of `atlas`
  src/config.rs                    AtlasConfig
  src/store.rs                     markers, prefix, open, AtlasReaderCache
  src/compat.rs                    dtype, Attr and FillValue mapping; column names
  src/backend.rs                   AtlasArrayBackend<T>, AttributeBackend<T>, AtlasElement
  src/reader.rs                    dataset_from_view, collection_schema, project_read_dimensions
  src/datafusion/mod.rs            AtlasFormatFactory, AtlasFormat, nd_scan_plan
  src/datafusion/source.rs         AtlasSource, AtlasOpener, AtlasDatasets, AtlasEntry
  src/datafusion/pruning.rs        PruningIndex, StatColumn, CandidateFilter, PruneCache
  src/datafusion/statistics.rs     the infer_stats fold
  src/datafusion/options.rs        AtlasOptions
  src/datafusion/metrics.rs        AtlasScanMetrics
  src/datafusion/table_function.rs ReadAtlasFunc
  src/test_support.rs              #[cfg(test)] fixtures built with AtlasWriter
```

Every test lives beside the code it covers, including the end-to-end ones in
`datafusion/mod.rs`. The fixtures are `#[cfg(test)]`, so an integration target
under `tests/` could not reach them.

Delete every file of the old `src/` first. Nothing in it compiles against
0.16.

## 5. Work plan

Environment for every step:

```bash
export PATH="$HOME/.cargo/bin:$PATH"
source ~/.config/beacon/build-env.sh
```

The active toolchain is stable 1.98. CI also builds at 1.94. Use no feature
newer than 1.94. `cargo fmt --check` is not clean repo-wide, so format the
new crate alone.

### Phase 0: prerequisites

1. Confirm the dependency rule for atlas-rust 0.16.4 (published today).
2. Add `atlas-rust = "=0.16.4"` to `[workspace.dependencies]`.
3. Remove the `exclude` line from the workspace `Cargo.toml`. Add the crate
   to `members`.
4. Run `cargo tree -p beacon-arrow-atlas -e normal | head` after step 5 of
   phase 1 to confirm one `rkyv`, one `object_store` and one `ndarray`.

### Phase 1: the reader

1. Write `config.rs`, `store.rs`, `compat.rs`, `backend.rs`, `reader.rs`.
2. Write `test_support.rs`. Build collections in a `tempdir` with
   `AtlasWriter`: two datasets with attributes and a fill; a widening pair
   (`Int16` and `Float32`); an incompatible pair (`String` and `Int64`); a
   ranged fleet of `n` datasets; a chunked 2-D grid; a dataset with a list
   attribute; an empty collection. A `Bool` or list *array* cannot be a
   fixture: no Rust writer can produce one, so that mapping is unit-tested
   alone. A deleted dataset is made in the test that wants one, by calling
   `delete_dataset` on the open collection.
3. Unit tests: marker recognition, prefix, cache hit and miss on the mask
   stamp, every dtype mapping, fill conversion, column names, a full read, a
   window read that spans chunks, the timestamp path, the skips.
4. `cargo test -p beacon-arrow-atlas`.

### Phase 2: the scan

1. Add `FileRead::skipped()` to `beacon-nd-array`.
2. Write `datafusion/mod.rs`, `source.rs`, `options.rs`, `metrics.rs`,
   `table_function.rs`.
3. End-to-end tests in `datafusion/mod.rs`, through `ListingTable` and
   `FastObjectTable`: every row once at 1, 4 and 8 partitions; `COUNT(*)`;
   projection; the widening cast; the null fill of a missing column; the
   incompatible pair as `Utf8`; the deleted dataset absent; `read_dimensions`
   narrows the schema; the plan shape `NdBroadcastExec` over `NdSourceExec`
   over `DataSourceExec`; a chunk-pruned scan reads fewer encoded batches;
   `EXPLAIN` does not open a segment.
4. `cargo test -p beacon-arrow-atlas` and
   `cargo test -p beacon-nd-array --lib`.

### Phase 3: pruning and statistics

1. Write `pruning.rs` and `statistics.rs`.
2. Tests: the index over the ranged fleet has one row per live dataset in
   listing order; `> 45` keeps `d5..d9`; an impossible predicate prunes
   everything; a permissive one keeps everything; the mixed-dtype pair casts
   before it compares; an attribute predicate prunes from the footer; a `NaN`
   bound is null and fails open; an unknown column fails open; a deleted
   dataset has no row; a position whose name differs is kept; eight
   partitions build the index once (`atlas_index_builds` is 1); results match
   with pruning on and off; the metrics report the counts; a synthetic index
   of 200 000 rows builds and prunes in one test without a timeout;
   `infer_stats` folds a fleet and goes unknown on a mixed dtype.
3. `cargo test -p beacon-arrow-atlas`.

### Phase 4: wiring

Section 6 lists the files. Then:

```bash
cargo clippy --workspace --lib --bins --tests
cargo test --workspace --no-fail-fast --lib --bins --tests
cargo fmt -p beacon-arrow-atlas
```

`beacon-datafusion-ext` does not test standalone. Use the workspace run.

### Phase 5: integration and docs

1. `integration-tests/formats/test_atlas.py`. It needs `atlas-python`. Add it
   to `requirements-optional.txt` and skip when absent. Build a collection
   from `test_file.nc` with `atlas.create`, query it, create an external
   table, restart, check the table survives.
2. Update the docs of section 8.
3. Add a CHANGELOG entry.

## 6. Wiring outside the crate

| File | Change |
| --- | --- |
| `Cargo.toml` (workspace) | Drop the `exclude`. Add the member. Add `atlas-rust = "=0.16.4"`. |
| `beacon-db/beacon-core/Cargo.toml` | Add the crate. |
| `beacon-db/beacon-core/src/runtime_builder.rs` | `pub atlas: AtlasConfig`, `with_atlas_config`, and `AtlasFormatFactory::new(AtlasOptions::default(), builder.atlas.clone())` in `register_file_formats`. |
| `beacon-db/beacon-core/src/crawler/discovery.rs` | Update the marker test: `d/x/data.atlas` is crawlable. |
| `beacon-db/beacon-core/tests/schema_functions.rs` | Add `read_atlas_schema`. |
| `beacon-db/beacon-functions/Cargo.toml`, `src/file_formats/mod.rs` | Register `ReadAtlasFunc`. |
| `beacon-db/beacon-file-formats/beacon-nd-array/src/arrow/file_read.rs` | Add `FileRead::skipped()`. |
| `beacon-db/beacon-db-py/src/connection.rs`, `python/beacondb/_beacondb.pyi` | Add `read_atlas` and `read_atlas_schema`. |
| `beacon-server/beacon-server-config/src/lib.rs` | Re-export `AtlasConfig`. Add the four `BEACON_ATLAS_*` fields. Fill `atlas`. |
| `beacon-server/beacon-server/src/server/mod.rs` | `.with_atlas_config(config.atlas.clone())`. |
| `beacon-server/beacon-server/src/server/catalog.rs` | `"atlas" => "read_atlas"`. |
| `beacon-server/beacon-server/src/main.rs` | Add `atlas` and `array_format` to the quiet log list. |
| `beacon-clients/beacon-web/src/components/external-table-dialog.tsx` | Hint: "the `data.atlas` file". |
| `beacon-clients/beacon-web/src/pages/crawlers.tsx` | Add `{ value: "atlas", label: "Atlas" }`. |
| `integration-tests/formats/test_atlas.py`, `requirements-optional.txt` | New suite. |
| `CHANGELOG.md` | Entry. |

`beacon-file-stats` and `fast_object` mention Atlas in comments only. They
need no code change.

## 7. Tests to keep from the old crate

Port these assertions. Rewrite the fixtures with `AtlasWriter`.

- `reads_all_datasets_through_datafusion` and the `FastObjectTable` twin.
- `widened_array_dtype_is_cast_from_each_dataset`.
- `missing_column_is_null_filled_per_dataset`.
- The incompatible-dtype case, with its answer corrected: the merge is
  refused and the error names both datasets, and `keep_first` resolves it to the
  first dataset's type. See section 11.
- `pruning_matches_unpruned_results`, `pruning_on_mixed_dtype_column_end_to_end`,
  `pruning_across_many_partitions_is_correct`,
  `scan_metrics_report_pruned_and_scanned_counts`, and the `pack_column`
  tests of the old `pruning.rs`. The old index came from atlas; the new one is
  built here, but the pack and the `PruningStatistics` adapter are the same
  shape.
- `partitioned_scan_reads_every_dataset_row`.
- `cache_returns_same_arc_for_identical_marker` and
  `cache_reopens_when_last_modified_changes`. Add a mask-change case.
- `discover_datasets_emits_one_entry_per_store`.

## 8. Docs

Rewrite `docs/docs/2.0.0-rc5/formats/atlas.md`: the `data.atlas` file, the
mask, the column model, the `OPTIONS` table, footer pruning, the fill rules,
the skips, and "not readable: 0.14 collections". Then update every page that
names `atlas.json`:

- `formats/index.md` (two rows and the marker note)
- `data-sources/external-tables.md#atlas`
- `server/datasets.md`, `server/configuration.md`, `server/performance-tuning.md`
- `server/crawlers.md` (atlas is crawlable now)
- `cf-decoding.md` ("Zarr and Atlas": Atlas decodes nothing)
- `sql/table-functions.md`, `sql/table-functions-utility.md`
- `guides/speed-up-queries.md`, `guides/query-a-collection.md`, `guides/query-s3.md`
- `faq.md`, `how-it-works.md`

## 9. Risks and open points

1. Freshness. atlas-rust 0.16.4 is one day old. A 0.16.5 with an API change
   would land on this crate first. Pin exactly and accept.
2. Name lookup. `Atlas::dataset(name)` is O(datasets). A full scan of a
   million datasets makes a million lookups. That is quadratic. The pruning
   index avoids it for array columns, because `array_stats_by_dataset` is one
   linear pass. A dataset that survives pruning still pays one lookup at its
   open, and an attribute column in the index pays one per dataset. The
   rebuild therefore works as is for a selective query over a large
   collection, and for any query over a collection up to the tens of
   thousands. Above that the upstream lookup of section 10 is required.
3. Memory. Every cached handle owns 320 MiB of cache budget. Document the
   bound. The upstream shared cache removes it.
4. Mask freshness. The reader cache pays one `HEAD` per open. The schema
   cache keys on the listed objects, and a `.atlas` listing omits the mask. A
   delete can therefore leave a stale merged schema until the next listing
   change. The stale schema can only hold an extra column, which reads as
   null. Accept.
5. Chunk reads. `read_array` walks every chunk coordinate of the array per
   call to find the overlap. A long array read chunk by chunk pays
   O(chunks²) comparisons. Acceptable for now. See section 10.
6. Pruning schema. `PruningPredicate` needs the logical column types, and the
   scan's projected schema is nd-encoded. Derive the logical schema through
   `nd_value_type` and pin it with a test that a pushed `>` prunes.
7. Bool arrays and list values are invisible. The Python ingest refuses bool
   by default too. Document.
8. `EXPLAIN` sizes. See section 3.7.

## 10. Requests to atlas-rust

Not blockers. Each one lifts a limit above.

1. `Atlas::dataset_at(ordinal)` and a `HashMap<String, u32>` name index built
   at open. Removes the quadratic scan, and lets attribute columns join the
   pruning index at any size.
2. `Atlas::attribute_by_dataset(key)` and `Atlas::array_attribute_by_dataset(array, key)`,
   the attribute twins of `array_stats_by_dataset`. One footer pass per
   attribute column, with no view at all.
3. `Atlas::open_with_cache(store, prefix, Arc<DeltaCache>)`. Lets Beacon share
   one block budget across every open collection.
4. Expose the schema pool, or `Atlas::array_dtypes()`: per array name, the
   set of dtypes the live datasets declare. Makes schema inference
   O(pool + attributes).
5. In array-format `assemble_nd`, iterate the chunk coordinates that overlap
   the slice, not every coordinate of the array.

## 11. What building it settled

Phases 0 and 1 answered five questions this plan had guessed at. Each is now
pinned by a test.

**1. Two families of type are refused, not stringified.** The old crate took
atlas's own merge, where `String` absorbed everything, so a collection whose
datasets typed one array as `String` and `Int64` read back as text. The
schema of a collection now merges through the session's
`ArrowTypeWidening`, exactly as the files of every other format do, and its
default refuses that pair:

```text
Incompatible types for field 'value': Utf8 in 'sensor#a' vs Int64 in 'sensor#b'
```

The label is `{collection}#{dataset}`, so the offending dataset is named
rather than searched for. `BEACON_TYPE_WIDENING_ON_CONFLICT=keep_first` takes
the first dataset's type instead and casts the rest to it. This is a
behaviour change for a collection that holds such a column, and section 3.13
records it.

**2. An integer beside a `Float32` widens to `Float64`.** Not to `Float32`, as
the old crate's test asserted. A `Float32` holds no `Int32`, so a rule that
kept it would make the merged type depend on which dataset came first. That is
issue #377, and the session rule is what settles it.

**3. `list_datasets()` reports write order.** Not sorted order. The pruning
index of section 3.8 keys its rows on that order, and `AtlasEntry::position`
indexes into it, so both are read from the same call at plan time.

**4. A `Bool` or list array cannot be a fixture.** `array-format` implements
no element type for either, so no Rust writer can produce one and no
collection can hold one. The refusal is unit-tested at the mapping instead. A
list *attribute* is writable, and the reader drops it as designed.

**5. The marker list must sort by directory, not by path.** A path sort puts
`a/b/data.atlas` before `a/data.atlas`, because `b` sorts under `d`, so a
nested collection would be kept and its parent dropped. The old crate sorted
by path and was correct only by accident: its marker was `atlas.json`, and
`a/atlas.json` sorts before `a/b/atlas.json`. Renaming the marker broke the
assumption. `top_level_atlas_markers` now sorts on the directory.

Two Phase 0 results worth keeping:

- The dependency graph is clean. `atlas-rust 0.16.4` brings
  `array-format 0.12.0`, and the subtree resolves to one `rkyv 0.8.10` (shared
  with `beacon-file-stats`), one `object_store 0.13.2`, one `ndarray 0.17.2`
  and one `zstd 0.13.3`. Nothing was duplicated by adding the crate.
- The crate compiles clean at the workspace toolchain with no warnings of its
  own, and `cargo check --workspace --lib` still passes.

## 12. What the scan settled

Phase 2 answered four more questions, all of them about the DataFusion
surface rather than about atlas.

**1. The schema-adapter hooks are gone.** DataFusion 53 deprecates
`SchemaAdapterFactory` and gives `FileSource::with_schema_adapter_factory`
and `schema_adapter_factory` defaults that refuse and return `None`. The old
crate implemented both. The nd read adapts its own batches through
`batch_adapter_factory` inside `FileRead::plan`, so the source leaves the
deprecated pair alone and carries no adapter field.

**2. A `COUNT(*)` still has to read one array.** The projection reaches the
dataset build, so projecting nothing would build nothing, and the read would
have no grid to take a row count from — `FileRead` would then index into an
empty dataset and fail. The opener picks the widest readable array out of the
footer instead and builds that one column. A dataset with no readable array
at all falls back to building its attributes, and contributes the single row
its scalars define.

**3. A predicate column is built even when the projection omits it.** The
filter that stays above the scan forces its columns into the projection
today, so this changes nothing now. But the chunk pruning inside the read
matches columns by name, and it would stop pruning silently if that ever
stopped holding.

**4. Partition columns reach a scan through the source's `TableSchema`.**
`FileScanConfigBuilder` has no `with_table_partition_cols`; the config reads
them off the source. Only the refusal test needed to know this — an Atlas
dataset lives inside a container rather than at a path, so
`reject_partition_columns` turns such a table away before a scan is built.

The legacy format is gone from the crate entirely, by request: there is no
`atlas.json` constant, no detection, and no migration hint. A pre-0.16
collection's marker is simply not a marker, so a listing passes over it.

## 13. What pruning settled

**1. A scan is offered its filters even when it has none.** DataFusion calls
`try_pushdown_filters` with an empty list, and `conjunction` over nothing is
the literal `true`. A source that stores that becomes a source with a
predicate, and this one would then build a pruning index for every scan,
including `SELECT *`. The fix is to treat a predicate that names no column as
no predicate. The other nd formats store the same literal; it costs them
nothing, because their chunk pruning finds no column ranges in it and stops.

**2. Statistics need a count guard, and pruning does not.** A dataset that
declares an array and never writes it has no footer entry, and its cells read
back as the array's fill — or as zeros, when it declares none. In the pruning
index that dataset gets null bounds, and DataFusion keeps a container whose
statistics are null, so the answer stays right. In the statistics *fold* the
same dataset would simply be skipped, and the range would then exclude values
the collection holds. So `infer_stats` claims a bound only when every live
dataset reported one. A uniform collection — what `atlas create` writes — is
unaffected; a heterogeneous one reports unknown and is read in full.

**3. The index is what makes attribute pruning affordable at all.** An array
column costs one `array_stats_by_dataset` pass. An attribute has no bulk
accessor, so it costs one `DatasetView` per dataset and each is a linear
scan. That is capped by `ATTRIBUTE_INDEX_LIMIT`, and it is the strongest case
for `Atlas::attribute_by_dataset` upstream (section 10, item 2).

**4. `infer_stats` measures nothing during a query.** It follows Zarr: a
format built by `create()` reports unknown whatever the option says, and only
`create_for_analysis` turns the fold on. Folding a footer is cheap, but a
listing of thousands of collections would still open every one of them while
planning.

Two costs are deliberate and worth revisiting if a profile asks:

- The pack goes through `ScalarValue` per value rather than a typed builder.
  It runs on a blocking thread, once per collection per scan. A typed builder
  per target type is the optimization, and the packing is one function.
- The index is not kept between queries. The collection is immutable, so the
  packed columns could live on the reader-cache entry, keyed by column and
  target type.

## 14. What the wiring settled

**1. An Atlas collection is crawlable, and Zarr is not.** The crawler's rule is
that a file's extension must equal its format name. A Zarr store is a
directory behind a `zarr.json`, so `json != zarr` and it is skipped. A
collection is one file named `data.atlas`, so `atlas == atlas` and a crawler
builds a table over it. Tables group by directory and each collection has its
own, so a crawl of many collections makes one table each; several in one table
is what an external table over a glob is for. The discovery test that asserted
atlas was skipped is now the test that asserts it is not.

**2. `beacon-core` needed `ndarray` as a dev-dependency.** Its integration
tests write real collections, and the atlas writer takes `ndarray` views.
Beacon itself never writes a collection, so the dependency belongs in
`dev-dependencies` alone.

**3. The four settings reach a table two ways.** `BEACON_ATLAS_USE_READER_CACHE`,
`BEACON_ATLAS_READER_CACHE_SIZE`, `BEACON_ATLAS_USE_PRUNING` and
`BEACON_ATLAS_ENABLE_STATISTICS` set the runtime defaults, and every one but
the cache size is overridable per table through `OPTIONS`. An embedded caller
sets them with `RuntimeBuilder::with_atlas_config`.

### What phase 4 verified, and what it did not

Verified: `beacon-arrow-atlas` (105 tests), the whole of `beacon-core`
(45 suites, including 8 new end-to-end tests over a real runtime — the table
function, its `_schema` counterpart, a glob, `STORED AS ATLAS`, recovery
across a restart, the dimensions argument, and pruning against an unpruned
control). `cargo check` passes for `beacon-functions`, `beacon-server-config`
and `beacon-server` (lib and bins). Clippy is clean in every touched crate's
own sources.

Not run: the test suites of `beacon-server-config` and `beacon-server`, and
any build of the web client. The changes there are a config struct, three
one-line wirings and two string edits, but they are unproven.

### A note on `cargo fmt`

Do not run `cargo fmt -p <crate>` on the crates this touches. The repository is
not format-clean, so formatting a whole package rewrites files the change never
went near — one pass here reformatted 62 unrelated files across `beacon-core`
and `beacon-functions`, and they had to be reverted one by one. Format the new
crate, whose every file is new, and leave the rest alone.

## 15. What the ingest settled

The one thing no Rust test could answer: does the reader handle what `atlas
create` actually writes? It was checked directly — a collection built by
atlas-python 0.16.4 from five netCDF files, opened with this crate.

**1. Real collections carry the statistics pruning needs.** Each dataset
reported its own `min`, `max`, `null_count` and `row_count` for every array,
with the ranges the source files held. Dataset pruning therefore works on
collections built the normal way, which was the open question behind the whole
design.

**2. `atlas create` writes `{destination}/data.atlas`.** One dataset per source
file, named after the file *with its suffix* — `d0.nc`, not `d0`. A `LOCATION`
names the container inside that directory.

**3. xarray's conventions arrive intact and are handled.** A float array gets a
`NaN` fill, because xarray reads with `mask_and_scale=True`; the reader carries
it through, and `NaN` never equals itself, so such a cell reads as `NaN` rather
than as null. Python's own marker attribute lands as the column
`._pyatlas_coords`, beside `.platform` and `temperature.units`. The collection
schema came out as `._pyatlas_coords`, `.platform`, `depth`, `temperature`,
`temperature.units` — exactly the column model this plan describes.

**4. The reference writers live in `requirements.txt`.** Not
`requirements-optional.txt`, which this plan said: that file is for the Flight
SQL driver alone. The `formats/` convention is one pinned writer per format in
the main file, with each test skipping itself when its writer is absent.
`atlas-python==0.16.4` is pinned there.

### What phase 5 verified, and what it did not

Verified: the fixture half of `formats/test_atlas.py` runs against real
atlas-python; the reader handles its output; the test file imports, collects
and skips cleanly when `beacondb` is absent; the documentation site builds,
which is also a dead-link check because VitePress fails a build on one.

Not run: the body of `formats/test_atlas.py`, which needs the `beacondb`
extension built with maturin. Its SQL and its use of the embedded API follow
`formats/test_zarr.py`, and the same ground is covered in Rust by
`beacon-core/tests/atlas.rs`.
