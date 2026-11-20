# A new NetCDF scan on DataFusion morsels

Date: 2026-11-20 (revision 4; replaces the designs of 2026-10-14)
Branch: `feature/netcdf-reader-refactor-984e0d`
Status: design, for review (revision 5: no scan without a file column)

Earlier revisions planned a two-format split and a shared nd scan engine. Both are dropped. netCDF-C is gone from main, and the user wants no shared interface. This design changes one crate.

## 1. Goal

- Give `beacon-arrow-netcdf` its own scan on the DataFusion 54 morsel API.
- The scan builds its own chunks and streams nd batches. It does not use `FileRead`, `WorkQueue`, `MorselSource` or `morsel_scan` of `beacon-nd-array`.
- Keep chunk pruning and parallel reads of one file.

## 2. Decisions

| Topic | Decision |
|---|---|
| Scope | Only `beacon-arrow-netcdf`. HDF5, Zarr and TIFF keep the legacy engine. `beacon-nd-array`, `beacon-nd-execution` and `beacon-datafusion-ext` do not change. |
| Interface | No shared trait and no generic engine. All new types are concrete and private to the crate, except `NetcdfSource`. |
| Scheduling | The DataFusion morsel API. One morsel per chunk. |
| Chunks | The native chunk shape, else C order at `batch_size`. Same grid as today. |
| Chunk pruning | Kept. A filter on a 1-D coordinate drops the chunks that no row can match. |
| Large files | With fewer files than partitions, the scan cuts a file into slices. |
| No file column | A scan that projects no file column is an error. This covers `COUNT(*)` and a query of only partition columns. A row count comes from a real column: `COUNT(column)`. |
| Ragged | Not supported. A clear error at schema inference and at scan. With `read_dimensions` and `skip_unbroadcastable`, a ragged file is skipped. HDF5 still reads ragged files. |
| Unchanged | `NetcdfFormat`, the factory, the options (`read_dimensions`, `skip_unbroadcastable`, `skip_invalid_files`), the writer, the statistics, the schema fingerprint, SQL and config. |

## 3. Layout

```
beacon-arrow-netcdf/src/datafusion/
  mod.rs            NetcdfFormat builds NetcdfSource (two call sites change)
  reader.rs         fetch_schema refuses a ragged dataset
  source.rs         removed
  scan/
    mod.rs          module docs, re-export of NetcdfSource
    source.rs       NetcdfSource: the FileSource
    chunks.rs       FileScan: projection, chunk list, read of one chunk
    prune.rs        coordinate masks and the chunk test
    morsel.rs       NetcdfMorselizer, planner states, ChunkMorsel
    slices.rs       NetcdfSlice, slice_files, spread_files
    output.rs       projected columns, partition columns, the batch adapter
```

- `beacon-arrow-netcdf/Cargo.toml` adds `datafusion-datasource = { workspace = true }`. The `datafusion` crate does not re-export `morsel`.
- Only `scan/morsel.rs` and `scan/source.rs` name DataFusion morsel types. The API is experimental.

**Parts of `beacon-nd-array` that the scan uses (all public today):**
- the data model: `AnyDataset`, `Dataset`, `NdArrayD::subset`, `ArraySubset`;
- `dataset::project_read_dimensions_or_skip`;
- `arrow::array::ndarray_to_arrow_array`;
- `arrow::partition::{FilePartitions, encoded_partition_cols}`;
- `arrow::metrics::ReadMetrics`;
- `arrow::pushdown_filter::PushdownFilter` and `arrow::pushdown::{is_pushdown_candidate, mask_pushdown, compute_chunk_mask, mask_is_all_false}`.

The chunk grid and the chunk read are `pub(crate)` in `beacon-nd-array`. The scan has its own versions in `chunks.rs`.

## 4. Components

### 4.1 `NetcdfSource` (`scan/source.rs`)

```rust
#[derive(Debug, Clone)]
pub struct NetcdfSource {
    table_schema: TableSchema,           // encoded file schema + encoded partition columns
    metrics: ExecutionPlanMetricsSet,    // one shared set
    read_dimensions: Option<Vec<String>>,
    skip: SkipFiles,                     // unbroadcastable, invalid
    batch_size: usize,
    predicate: Option<Arc<dyn PhysicalExpr>>,
    projection: Option<ProjectionExprs>,
}
// new, with_skip_unbroadcastable, with_skip_invalid_files, with_projection: same as NetCDFSource today.
```

| `FileSource` method | Behavior |
|---|---|
| `create_file_opener` | `not_impl_err!("NetCDF scans use create_morselizer")`. |
| `create_morselizer` | If the projected schema holds no file column, return the error of section 4.6. Else make one `ReadMetrics` per partition and build `NetcdfMorselizer`. |
| `table_schema`, `with_batch_size`, `metrics`, `projection`, `try_pushdown_projection` | As `NetCDFSource` today. |
| `file_type` | `"netcdf"`. |
| `try_pushdown_filters` | As today: keep the conjunction as `predicate`, answer `PushedDown::No`. The `FilterExec` stays above the scan. |
| `supports_repartitioning` | `true`. |
| `repartitioned` | Section 4.5. |

### 4.2 `FileScan` (`scan/chunks.rs`)

One opened file, ready to read.

```rust
pub(super) struct FileScan { /* projected arrays, target dimensions, read schema, chunk list */ }

impl FileScan {
    /// Project `dataset` to `names`, and cut it into chunks at `batch_size`.
    pub(super) fn try_new(dataset: Dataset, names: &[String], batch_size: usize) -> Result<Self>;
    /// The chunks, in C order.
    pub(super) fn chunks(&self) -> &[ArraySubset];
    /// Rows of one chunk after the broadcast: the product of its shape.
    pub(super) fn rows(chunk: &ArraySubset) -> usize;
    /// Read one chunk into an un-broadcast nd batch.
    pub(super) async fn read(&self, chunk: &ArraySubset) -> Result<NdRecordBatch>;
}
```

- **Grid rule, same as today.** The array of the highest rank sets the target dimensions. If its chunk shape differs from its shape, the native chunk shape is the chunk shape. Else the chunk fills from the last axis up to `batch_size` elements.
- **Read.** For each projected array: map the chunk onto the axes of the array, call `NdArrayD::subset`, convert with `ndarray_to_arrow_array`, build an `NdArrowArray`. A coordinate is read one time per chunk, not one time per row.
- **No full-rank array.** When no projected array lies on every dimension of the dataset, the dimensions of the dataset are the grid. This is today's rule.

### 4.3 Pruning (`scan/prune.rs`)

```rust
/// The keep mask of each 1-D coordinate that the predicate bounds. Reads those coordinates.
pub(super) async fn coordinate_masks(scan: &FileScan, predicate: &PushdownFilter)
    -> Result<Vec<(String, Vec<bool>)>>;
/// Whether no row of `chunk` can meet the predicate.
pub(super) fn is_pruned(masks: &[(String, Vec<bool>)], dims: &[String], chunk: &ArraySubset) -> bool;
```

- The masks come from `PushdownFilter::ranges`, `is_pushdown_candidate` and `mask_pushdown`. A mask that cannot be built is left out, so its chunks are read.
- `is_pruned` uses `compute_chunk_mask` and `mask_is_all_false`.
- The masks come from the projected arrays. A filter column is always projected, because the `FilterExec` above the scan needs it.
- Pruning only skips work. The `FilterExec` above the scan applies the predicate again.

### 4.4 Planner (`scan/morsel.rs`)

```text
NetcdfMorselizer::plan_file(file)              CPU only
   file.range.is_some()  -> internal error "NetCDF scans never read byte ranges"
   otherwise             -> Unopened{file}     (slice = the file's NetcdfSlice, else {0,1})

Unopened.plan() -> Some(MorselPlan::new().with_pending_planner(open_future))

open_future  [I/O]
   open the dataset                  an error with skip_invalid_files: warn, Skipped
   project_read_dimensions_or_skip   skipped by the option: Skipped
   a ragged dataset                  NotImplemented error
   the file has none of the columns  -> Done
   FileScan::try_new
   chunks = slice range  i*n/K .. (i+1)*n/K   of the full chunk list
   masks  = coordinate_masks(..)     only when a predicate exists
   keep the chunks that is_pruned rejects; add the others to chunks_pruned / rows_pruned
   no chunk left                     -> Done
   otherwise                         -> Read{file, next: 0, ready: None}

Read{file, next, ready}.plan()                 CPU only
   morsel = ready.map(ChunkMorsel)
   io     = (next < chunks.len()).then(read_future(next))
   neither -> Ok(None);  else Some(plan with the morsel and the future)

read_future(k)  [I/O]
   FileScan::read(chunk k)           chunks_read += 1, rows_read += rows
   -> Read{file, next: k+1, ready: Some(batch)}

Skipped: files_skipped += 1 (slice 0 only), then Done
Done.plan()        -> Ok(None)

ChunkMorsel::into_stream   CPU: encode_nd_record_batch -> partition columns -> adapt to the projected schema
```

Every error gets the context `"NetCDF file {path}"`, plus `" slice i of K"` for a slice and `", chunk k of n"` for a read.

**Invariants:**
1. `plan_file` and `plan()` do no I/O. A morsel does no I/O.
2. No empty `MorselPlan`. It panics in the FileStream open timer.
3. One I/O per partition, with a read-ahead of one chunk.
4. A partition holds at most two chunks in memory.
5. The chunk list before the prune is the same in every slice. The slice cut comes first, so the K ranges cover each chunk one time, also when the predicate changes between slices.

### 4.5 Slices and `repartitioned` (`scan/slices.rs`)

```rust
/// Slice `index` of `count` of one file. It travels in PartitionedFile extensions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct NetcdfSlice { pub index: u32, pub count: u32 }
```

`repartitioned(target, _min_size, ordering, config)`:
1. Return `None` when `ordering.is_some()` or `target <= 1`.
2. Let `files` be the count of entries without a `NetcdfSlice`. If `0 < files < target`, replace each such entry with `K = ceil(target / files)` copies. Each copy gets `NetcdfSlice { index, count: K }`, the same `object_meta` and `statistics = None`.
3. Never slice an entry again that already has a `NetcdfSlice`.
4. Return exactly `target` groups. Empty groups are allowed. Put the largest entry first (weight = size / count) into the lightest group.
5. Never split by byte range.

- With `target` groups, EnforceDistribution adds no RoundRobin below `NdSourceExec`.
- DataFusion work stealing balances the unopened entries between partitions.
- Each slice opens the file, and reads the coordinates for its masks when a predicate exists.

### 4.6 Output (`scan/output.rs`)

- Each batch is nd-encoded, gets the partition columns as rank-0 arrays, and goes through the batch adapter onto the projected schema. A column that the file does not have becomes null.
- A file that has none of the wanted columns gives no batch.
- **No file column in the projection is an error.** `create_morselizer` returns: "a NetCDF scan needs at least one file column; use COUNT(column) in place of COUNT(*)". The scan has no rows-only mode and no count morsel.

A partition column shadows a file variable with the same name.

## 5. Changes outside `scan/`

| Site | Change |
|---|---|
| `datafusion/mod.rs` `create_physical_plan`, `file_source` | Build `NetcdfSource` in place of `NetCDFSource`. The plan shape stays `DataSourceExec -> NdSourceExec -> NdBroadcastExec`. |
| `datafusion/reader.rs` `fetch_schema` | After `project_read_dimensions_or_skip`, a ragged dataset gives the `NotImplemented` error. |
| `datafusion/mod.rs` `infer_stats` | A ragged file gives unknown statistics and a warning. |
| `datafusion/source.rs` | Removed. |
| `examples/scan_profile.rs` | Fix it if it names `NetCDFSource`. |
| `beacon-core/src/metrics.rs` `collect_scan_totals` | Count each distinct `object_meta.location` one time. Slices repeat a location. |
| HDF5 | No code change. Its comments that name `NetCDFSource` get an update. |

## 6. Behavior changes

1. **Ragged NetCDF files give an error:** "NetCDF file X: 'X' is a ragged dataset; the NetCDF reader does not support ragged datasets yet". WOD files read through HDF5 only.
2. **`COUNT(*)` and a query of only partition columns give an error.** Use `COUNT(column)`. The count is then the row count of the grid that the projected columns span: `COUNT(lat)` on a `time x lat x lon` file counts the `lat` values, not the grid cells.
3. **The next file does not open early.** Today a partition opens its next file while it reads the current one.
4. **A slice opens the file again.** A file on K partitions costs K header reads, and K coordinate reads when a predicate exists.
5. **Scan errors name the file in one fixed form.**

## 7. Tests

**Kept as they are:** every NetCDF read test on a regular file, also the pruning tests (`a_predicate_reaches_the_scan_and_skips_its_chunks`, the metrics test, `a_pruned_scan_counts_what_the_coordinate_says_it_should`), the partition tests, the skip-option tests and the batch-size test.

**Rewritten:**
- Every test that runs `count(*)` on a NetCDF table changes to `count(<column>)` on a full-grid column. This also applies to `partition_coverage_tests.rs` and to the `beacon-core` NetCDF tests.
- The tests that read `morsel_files()` or assert one standing entry per partition: they assert `target` groups, K slices of one file and no byte range.
- The tests that use `wod_ctd_1964.nc` as an ordinary file move to `gridded-example.nc` or to a small regular file that the test writes. This also applies to the `beacon-core` tests that read the WOD fixture through NetCDF.

**New unit tests in `scan/`:**
- The chunk grid equals the legacy grid for a native-chunk file and for a contiguous file.
- An array that does not fit the grid gives an error with its name.
- No planner state gives an empty `MorselPlan`: a skipped file, a file with none of the columns and a fully pruned file all finish.
- Slices cover each chunk one time. Only slice 0 counts `files_skipped`. The prune counts of K slices sum to the count of one scan.
- A predicate that changes between two slice opens reads no chunk twice.
- `repartitioned`: `target` groups; `None` with an ordering or `target <= 1`; zero entries; no slice of a slice; `statistics = None`; no byte range.

**New end-to-end tests:**
- `SELECT count(*)` and `SELECT <partition column>` alone fail with the error of section 4.6.
- A ragged file is refused at schema inference and at scan. A collection with one ragged file names that file.
- One file on 4 partitions returns the same rows as on 1, with and without a filter.
- `LIMIT 5` reads only the first chunks of a file with hundreds of chunks.
- A dropped result stream stops the reads.
- A file that goes away after the table exists fails with its path.
- A read from an in-memory object store.
- A physical plan that runs again after `reset_plan_states` returns the same rows.
- `beacon-core`: a sliced file counts one time in the query metrics.

**Timing check, before and after, in the PR:** the 40-file collection, and one large file on 1 and 8 partitions with and without a filter.

## 8. Docs and changelog

2.1.0-rc3 pages:
- `formats/netcdf.md`: ragged NetCDF is not supported; read such files as HDF5. `COUNT(*)` is not supported; use `COUNT(column)`.
- Pages that read a WOD file through `read_netcdf` get a regular example or a note.

`CHANGELOG.md` `[Unreleased]`:
- **Changed:** NetCDF scans run on DataFusion morsels.
- **Removed:** `COUNT(*)` and partition-only queries on NetCDF tables. Use `COUNT(column)`.
- **Removed:** ragged NetCDF support for `STORED AS NC` and `read_netcdf`.

## 9. Commit sequence

1. Tests: move the WOD-as-ordinary-file tests to regular fixtures. They stay green on the current source.
2. `scan/chunks.rs` and `scan/prune.rs`, with unit tests.
3. `scan/output.rs`, `scan/morsel.rs` and `scan/slices.rs`, with unit tests.
4. `scan/source.rs`; switch `NetcdfFormat`; refuse ragged; delete `datafusion/source.rs`; update the rewritten tests; add the end-to-end tests.
5. `beacon-core`: the metrics fix, its test, the WOD fixture moves.
6. Docs and CHANGELOG.

## 10. Out of scope

- HDF5, Zarr and TIFF on a scan of this kind.
- Ragged support in the new scan.
- `spawn_blocking` for the oxcdf decode.
- Any change to `beacon-nd-array`.

## 11. Risks

1. **Experimental API.** The DataFusion morsel API can change. Two files use it.
2. **Throughput.** One I/O per partition, and no early open of the next file. The timing check measures this.
3. **Slices.** Each slice opens the file and reads the filter coordinates again.
4. **Ragged NetCDF fails**, while HDF5 still reads the same file.
5. **`COUNT(*)` fails on NetCDF tables.** Clients and tools that send `COUNT(*)` (previews, row counters, BI tools) get the error. The same query on HDF5, Zarr and Parquet tables still works.
6. **Two copies of the chunk grid rule** exist, here and in `beacon-nd-array`, until the other formats move or the legacy engine goes away. A parity test guards the grid.
