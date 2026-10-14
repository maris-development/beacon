# NetCDF reader split and the shared nd scan engine

Date: 2026-10-14
Branch: `feature/netcdf-reader-refactor-984e0d`
Status: design, for review (revision 2, after the adversarial review)

## 1. Goal

- Split the NetCDF `FileFormat` into one format per reader: `NetcdfCFormat` (netcdf-c) and `OxcdfNetcdfFormat` (oxcdf).
- Move NetCDF reads to a new shared nd scan engine on the DataFusion 54 morsel API.
- Make the engine a generic `Dataset` trait that streams nd record batches and accepts pushdown filters. Zarr, Icechunk and GeoTIFF move to it later.
- Prepare the removal of the legacy nd-array engine (`FileRead`, `WorkQueue`, `MorselSource`, `morsel_scan`).

## 2. Decisions

| Topic | Decision |
|---|---|
| Factory | One factory, `NetCDFFormatFactory`, registered as `nc`. The option `use_rust_reader` selects the format. SQL, config and env vars do not change. |
| Formats | Two `FileFormat` types: `NetcdfCFormat` and `OxcdfNetcdfFormat`. |
| FileSource | One shared `NdFileSource` for all nd formats. Reader-specific code lives in a `DatasetOpener` and an `NdScanDataset`. |
| Scheduling | The DataFusion 54 morsel API. NetCDF does not use the legacy nd-array engine. |
| Unit of work | One morsel per chunk. Chunks come from the native chunk shape, else C order at `batch_size`. |
| Pushdown | The engine collects filters and gives them to `NdScanDataset::prune`, after the slice cut. The default `prune` does nothing. NetCDF keeps the default in this PR, so NetCDF prunes no chunks. |
| Large files | `OxcdfOpener` allows file slices: one file reads on several partitions. netcdf-c does not slice. |
| COUNT(*) | The engine counts unit rows from shapes. It reads no array data. A dataset with no arrays gives 0 rows. The legacy engine gave an error. |
| Ragged | Not supported by the NetCDF formats. A clear error at schema inference when the schema cache has no entry for the file, and always at scan. With `read_dimensions` and `skip_unbroadcastable`, a ragged file is skipped. |
| Release | This PR may land on main without ragged NetCDF support. |
| PR order | PR 1: this design. PR 2: `spawn_blocking` for netcdf-c and the oxcdf decode. |
| ext crate | This PR does not change `beacon-datafusion-ext`. |

## 3. Layout

```
beacon-nd-array/src/scan/          new, next to the legacy arrow/ engine
  mod.rs       re-exports, module docs
  dataset.rs   NdScanDataset, ScanRequest, Columns, ScanUnit, ScanPlan, Pruned, stream()
  opener.rs    DatasetOpener, NdSlice
  grid.rs      GridDataset, refuse_ragged
  source.rs    NdFileSource, nd_physical_plan, spread_files
  morsel.rs    NdMorselizer, planner states, ChunkMorsel, CountMorsel (private)
  output.rs    resolve_output, FileOutput, with_partitions, count_batch (pub(crate))
  metrics.rs   ScanMetrics
  testing.rs   MemOpener, drive() (#[cfg(test)])

beacon-arrow-netcdf/src/datafusion/
  mod.rs          NetCDFFormatFactory, NetcdfConfig, ReaderBackend, netcdf_options
  netcdf_c.rs     NetcdfCFormat, NetcdfCOpener
  oxcdf.rs        OxcdfNetcdfFormat, OxcdfOpener
  shared.rs       NetcdfCommon: option parsing, schema inference loop, schema merge, writer plan
  test_support.rs #[cfg(test)] helpers and write_regular_netcdf
  options.rs, sink.rs, statistics.rs, table_function.rs, object_meta_resolver.rs   kept
  source.rs, reader.rs (FileAccess, NetcdfInput)                                    removed
```

- The engine lives in `beacon-nd-array`, because `beacon-nd-array` already depends on `beacon-datafusion-ext`. An engine in ext would make a dependency cycle.
- `beacon-nd-array/Cargo.toml` adds `datafusion-datasource = { workspace = true }`. The `datafusion` crate does not re-export `morsel`. The crate already compiles as a dependency of `datafusion`.
- Only `scan/morsel.rs` and `scan/source.rs` name DataFusion morsel types. The API is experimental (`datafusion-datasource-54.1.0/src/morsel/mod.rs:20-21`).
- `with_partitions` and `count_batch` move from `arrow/file_read.rs` to `scan/output.rs`. `file_read.rs` imports them. No copy exists.

## 4. Engine API

### 4.1 Dataset trait (`scan/dataset.rs`)

```rust
/// What one scan wants from one opened file. The engine builds one per file.
#[derive(Debug, Clone)]
pub struct ScanRequest {
    /// The file columns to read.
    pub columns: Columns,
    /// The session batch size. A hint for the unit size.
    pub batch_size: usize,
}

#[derive(Debug, Clone)]
pub enum Columns {
    /// These names of `schema()`, in schema order. Never empty.
    Named(Arc<[String]>),
    /// No file column is wanted (COUNT(*), or only partition columns).
    /// The engine never calls `read`. It counts `ScanUnit::rows`.
    RowCount,
}

/// One unit: one planner I/O and one morsel.
#[derive(Debug, Clone)]
pub struct ScanUnit {
    /// The hyperslab on the target axes of the plan.
    pub subset: ArraySubset,
    /// Rows the unit broadcasts to. Explicit, because a later ragged unit has rows != subset.rows().
    pub rows: usize,
}
impl ScanUnit {
    /// A grid unit: rows = subset.rows().
    pub fn grid(subset: ArraySubset) -> Self;
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Pruned { pub units: usize, pub rows: usize }

/// The units one file reads for one request, in output order.
pub struct ScanPlan { /* units, pruned, state: Option<Arc<dyn Any + Send + Sync>> */ }
impl ScanPlan {
    pub fn new(units: Vec<ScanUnit>) -> Self;
    /// Attach the state that `read` needs.
    pub fn with_state<T: Any + Send + Sync>(self, state: T) -> Self;
    pub fn state<T: Any>(&self) -> Option<&T>;
    pub fn units(&self) -> &[ScanUnit];
    pub fn len(&self) -> usize;
    pub fn is_empty(&self) -> bool;
    /// Sum of `rows` over the units. No I/O.
    pub fn rows(&self) -> usize;
    /// Keep the units that `keep` accepts. Add the others to `pruned()`. Only `prune` calls this.
    pub fn retain(&mut self, keep: impl FnMut(&ScanUnit) -> bool);
    pub fn pruned(&self) -> Pruned;
    /// Keep only the units in `range`. Not counted in `pruned()`. The engine calls this for a slice.
    pub(crate) fn restrict(&mut self, range: Range<usize>);
}
// Manual Debug: len, pruned, state.is_some().

/// One opened file. Each format supplies it.
///
/// Contract:
/// - `plan` can do I/O (metadata). It never drops a unit. Its unit list depends only on
///   the file, `request.columns` and `request.batch_size`. File slices depend on this.
/// - `prune` drops units with `ScanPlan::retain`. The engine calls it after the slice cut,
///   so a dynamic filter that changes between two slices cannot read a unit twice.
///   Snapshot each dynamic filter one time per call (`snapshot_physical_expr`).
/// - `read` does all I/O and decode for one unit.
/// - The batch schema is `schema()` projected to the requested names, in schema order.
/// - Errors carry no path. The engine adds it.
#[async_trait::async_trait]
pub trait NdScanDataset: Debug + Send + Sync + 'static {
    /// Logical schema of all columns this file offers.
    fn schema(&self) -> SchemaRef;
    /// Cut `request` into units.
    async fn plan(&self, request: &ScanRequest) -> Result<ScanPlan>;
    /// Drop the units that `filters` exclude. Filters are Inexact: the FilterExec
    /// above the scan applies them again. Match columns by name. The default keeps every unit.
    async fn prune(&self, _plan: &mut ScanPlan, _filters: &[Arc<dyn PhysicalExpr>]) -> Result<()> {
        Ok(())
    }
    /// Read one unit of `plan`.
    async fn read(&self, plan: &ScanPlan, unit: &ScanUnit) -> Result<NdRecordBatch>;
}

impl dyn NdScanDataset {
    /// plan, then prune, then read each unit in order. For tests and callers outside DataFusion.
    pub fn stream(
        self: Arc<Self>,
        request: ScanRequest,
        filters: Arc<[Arc<dyn PhysicalExpr>]>,
    ) -> BoxStream<'static, Result<NdRecordBatch>>;
}
```

### 4.2 Opener (`scan/opener.rs`)

```rust
/// How one format opens one scan entry. One per scan.
#[async_trait::async_trait]
pub trait DatasetOpener: Any + Debug + Send + Sync {
    /// "netcdf", "zarr", "tiff". The value of FileSource::file_type and the prefix of error text.
    fn file_type(&self) -> &str;
    /// Open one entry. Metadata I/O only. Ok(None) skips the file (files_skipped).
    /// `store` is the store of the scan ObjectStoreUrl. A format with its own storage ignores it.
    async fn open(&self, store: &Arc<dyn ObjectStore>, file: &PartitionedFile)
        -> Result<Option<Arc<dyn NdScanDataset>>>;
    /// Let `repartitioned` cut one file into slices when there are fewer files than partitions.
    fn split_files(&self) -> bool { false }
}

/// Slice `index` of `count` of one file. It travels in PartitionedFile::with_extension.
/// An entry without an NdSlice counts as NdSlice { index: 0, count: 1 }.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NdSlice { pub index: u32, pub count: u32 }
```

### 4.3 Grid dataset (`scan/grid.rs`)

```rust
/// The stock NdScanDataset: a regular Dataset of lazy arrays.
#[derive(Debug, Clone)]
pub struct GridDataset { /* dataset: Dataset, schema: SchemaRef */ }
impl GridDataset {
    /// Wrap a regular dataset. Sorts the arrays by name. A ragged dataset gives the refuse_ragged error.
    pub fn try_new(any: AnyDataset) -> Result<Self>;
    /// project_read_dimensions_or_skip first, then try_new. Ok(None): the skip option skipped the file.
    pub fn open(any: AnyDataset, read_dimensions: Option<Vec<String>>, skip_unbroadcastable: bool)
        -> Result<Option<Self>>;
    pub fn dataset(&self) -> &Dataset;
}
impl NdScanDataset for GridDataset { /* see below; prune keeps the default */ }

/// NotImplemented for a CF ragged dataset. The text holds "ragged" and the dataset name.
pub fn refuse_ragged(any: &AnyDataset) -> Result<()>;
```

- `try_new` sorts the arrays by name. `schema()` (from `any_dataset_to_arrow_schema`) sorts the field names, and `read_nd_chunk` emits the columns in array order. Today each file reader sorts at open time. `try_new` makes this rule hold for every opener, `MemOpener` too.
- **`plan` with `Named`:**
  1. Find each name with `dataset.arrays.get_index_of(name)`.
  2. Call `Dataset::project(&indices)`, which keeps all dimensions.
  3. Call `chunk_grid(&projected, batch_size)`.
  4. Make the units with `ScanUnit::grid`.
  5. Attach a private `GridState { arrays, dims, schema }`.
- **`plan` with `RowCount`:** `chunk_grid` of the whole dataset. A dataset with no arrays gives an empty plan.
- **`read`:** `read_nd_chunk(&state.arrays, &state.dims, state.schema.clone(), unit.subset.clone())`.
- **`prune`:** the default. A NetCDF prune (`GridDataset::prune_by_coordinates`) comes later, with its first caller.

### 4.4 Source, plan helper, metrics (`scan/source.rs`, `scan/metrics.rs`)

```rust
#[derive(Debug, Clone)]
pub struct NdFileSource {
    opener: Arc<dyn DatasetOpener>,
    table_schema: TableSchema,           // encoded file schema + encoded partition columns
    metrics: ExecutionPlanMetricsSet,    // one shared set; FastObjectTable writes its file-stats counters here
    batch_size: usize,
    projection: Option<ProjectionExprs>,
    filters: Vec<Arc<dyn PhysicalExpr>>, // conjuncts, never claimed; given to NdScanDataset::prune
}
impl NdFileSource {
    pub fn new(opener: Arc<dyn DatasetOpener>, table_schema: TableSchema) -> Self;
    /// Keep a projection that was pushed into the source that create_physical_plan replaces.
    pub fn with_projection(self, projection: Option<ProjectionExprs>) -> Self;
    pub fn opener(&self) -> &Arc<dyn DatasetOpener>;
}

/// The body of create_physical_plan for each nd format: encode the file schema and the
/// partition columns, keep the pushed projection, build NdFileSource, then nd_scan_plan
/// (DataSourceExec -> NdSourceExec -> NdBroadcastExec).
pub fn nd_physical_plan(conf: FileScanConfig, opener: Arc<dyn DatasetOpener>)
    -> Result<Arc<dyn ExecutionPlan>>;

#[derive(Debug, Clone)]
pub struct ScanMetrics {
    pub chunks_read: Count,    // units this partition emitted (read, or counted in RowCount)
    pub rows_read: Count,
    pub chunks_pruned: Count,  // prune() drops in this slice's range; every slice records its own
    pub rows_pruned: Count,
    pub files_skipped: Count,  // open gave None; slice 0 only
}
impl ScanMetrics {
    /// One time per partition, in create_morselizer.
    pub fn new(set: &ExecutionPlanMetricsSet, partition: usize) -> Self;
}
```

- The metric names match the legacy names. No name starts with `output_`. No name collides with the FileStream names (`files_opened`, `files_processed`, `file_open_errors`, `file_scan_errors`, `time_elapsed_*`).
- No read timer. A timer across the read-ahead await would also count the downstream work of the previous morsel.

## 5. Morsel planner

```text
NdMorselizer::plan_file(file)                  CPU only
   file.range.is_some()  -> internal error "nd scans never read byte ranges"
   otherwise             -> Unopened{file}     (slice = file's NdSlice, else {0,1})

Unopened.plan() -> Some(MorselPlan::new().with_pending_planner(open_future))

open_future  [I/O]
   opener.open(store, file)          Err -> context "{type} file {path}[ slice i of K]"
     Ok(None)                        -> Done   (files_skipped += 1 if slice.index == 0)
     Ok(Some(dataset)):
       resolve_output(dataset.schema())
         Nothing                     -> Done
         (columns, output):
           plan = dataset.plan(&request)              Err -> context as above
           n = plan.len()
           plan.restrict(i*n/K .. (i+1)*n/K)          the slice cut, before any prune
           dataset.prune(&mut plan, &filters)         Err -> context as above
           chunks_pruned/rows_pruned += plan.pruned()
           plan.is_empty()           -> Done
           Columns::RowCount         -> Count{file}
           Columns::Named            -> Read{file, next: 0, ready: None}

Read{file, next, ready}.plan()                 CPU only
   morsel = ready.map(ChunkMorsel)
   io     = (next < plan.len()).then(read_future(next))
   step(morsel, io) -> Some(plan) | None       never an empty plan

read_future(k)  [I/O]
   dataset.read(&plan, &plan.units()[k])
     Err -> context "{type} file {path}, unit {k} of {len}"
     Ok(b) -> chunks_read += 1, rows_read += unit.rows -> Read{file, next: k+1, ready: Some(b)}

Count{file}.plan() -> Some(with_morsels([CountMorsel{file}]))      no I/O
Done.plan()        -> Ok(None)

ChunkMorsel::into_stream   CPU: compare field names and data types of the batch with the
                           read schema (internal error if not equal), then
                           encode_nd_record_batch -> with_partitions -> adapter.adapt_batch
CountMorsel::into_stream   CPU: chunks_read/rows_read += plan units/rows
                           no partition columns: 0-column batches of <= batch_size rows
                           partition columns:    count_batch of <= batch_size rows
```

`resolve_output` gives one of three results:
- **Columns:** the read schema, an encoded form of it, a `BatchAdapter` onto the projected schema, and the rank-0 partition columns. The read schema is `dataset.schema()` projected to the `Named` names, in logical (not encoded) form.
- **Rows:** COUNT(*), or only partition columns.
- **Nothing:** the query wants file columns and the file has none.

A partition column shadows a file variable with the same name. This is the legacy rule, from `file_read.rs:411-457`.

**Invariants:**
1. `plan_file` and `plan()` do no I/O. All I/O is in `open_future` and `read_future`. Each future owns only Arcs and the `PartitionedFile`, so it is `'static + Send`.
2. **No empty `MorselPlan`.** An empty plan panics in the FileStream open timer (`file_stream/scan_state.rs:274`, `metrics.rs:32`). `step()` enforces this rule, and an I/O future always resolves to a planner (`Done` means nothing is left).
3. **One I/O per partition, with a read-ahead of one unit.** `Read` returns morsel k with the future of unit k+1. ScanState polls the pending I/O before the reader (`scan_state.rs:132-160`). netcdf-c reads complete inline in the first poll until PR 2, so for netcdf-c the read-ahead gives no overlap.
4. **Order.** Morsels come before child planners (`morsel/mod.rs:115-121`). A preserve-order scan keeps the unit order.
5. **Memory.** A partition holds at most two units: one in the morsel and one in flight. COUNT(*) reads no array data.
6. **Slices.** The unit list before `prune` is the same in every slice, so the K ranges cover each unit exactly once. Each slice prunes only its own range.

## 6. `NdFileSource` as a `FileSource`

| Method | Behavior |
|---|---|
| `create_file_opener` | `not_impl_err!("{file_type} scans use create_morselizer")`, as `datafusion-datasource-54.1.0/src/file.rs:66-75` asks. |
| `create_morselizer` | Make `ScanMetrics::new(&self.metrics, partition)`. Take `config.projected_schema()?`, `config.table_partition_cols()`, the filters, the batch size and `store`. Build `NdMorselizer`. |
| `table_schema` | The encoded `TableSchema`. |
| `with_batch_size` | A clone with the new size. The metrics set is shared. |
| `metrics` | The one shared set. |
| `file_type` | `opener.file_type()`. |
| `filter` | `None`. A set filter makes DataFusion treat the scan as if it applied the filter. |
| `projection`, `try_pushdown_projection` | Merge with `try_merge`. Accept only when every expression is a plain `Column`. Else `Ok(None)`. |
| `try_pushdown_filters` | Split each filter with `split_conjunction`. Drop duplicates. Append to `filters`. Answer `PushedDown::No` for every filter. |
| `repartitioned` | Section 7. |
| `fmt_extra` | `, filters=[..]` when filters exist. |

## 7. `repartitioned` and file slices

`repartitioned(target, _min_size, ordering, config)`:
1. Return `None` when `ordering.is_some()` or `target <= 1`.
2. Let `files` be the count of entries without an `NdSlice`.
   - If `opener.split_files()` is false and the total entry count is less than `target`, return `None`. The listing groups stay, and DataFusion adds its RoundRobin below `NdSourceExec`, as today. So netcdf-c keeps its parallel work above the scan.
   - If `opener.split_files()` is true and `0 < files < target`, replace each entry without an `NdSlice` with `K = ceil(target / files)` copies. Each copy gets `NdSlice { index, count: K }`, the same `object_meta` and `statistics = None`.
3. Never slice an entry again that already has an `NdSlice`.
4. Return exactly `target` groups. Empty groups are allowed. Put the largest entry first (weight = size / count) into the lightest group.
5. Never split by byte range. Ignore `repartition_file_min_size`.

- With `target` groups, EnforceDistribution adds no RoundRobin. `add_roundrobin_on_top` checks the partition count again after `repartitioned` (`datafusion-physical-optimizer-54.1.0/src/enforce_distribution.rs:830-835`).
- Work stealing (`enable_file_stream_work_stealing`, default `true`) balances the shared queue at run time.
- A slice keeps the `object_meta` of its file for EXPLAIN, for query metrics (section 10 dedupes them) and for the size weight. FastObjectTable prunes earlier, in `TableProvider::scan`, so it never sees a slice.
- Each slice opens the file and plans the same units. It reads its range of section 5.
- A second run of one physical plan needs `reset_plan_states`. `DataSourceExec` keeps the drained file queue of the first run (`datafusion-datasource-54.1.0/src/source.rs:333, 399-405`).

**No change to `beacon-datafusion-ext` in this PR.**
- When `repartitioned` returns `None` (an ordering, `repartition_file_scans=false`, `partitioned_by_file_group`, netcdf-c with fewer files than partitions), DataFusion keeps its RoundRobin below `NdSourceExec`, as today. `NdSourceExec` decodes multi-row encoded batches (`nd/exec/source_exec.rs:146-158`), so the results stay correct. The RoundRobin coalescer then holds encoded chunks up to `batch_size` rows, as today.
- Do not override `benefits_from_input_partitioning` on `NdSourceExec` alone. Then EnforceDistribution puts the RoundRobin between `NdSourceExec` and `NdBroadcastExec`, and `NdBroadcastExec::try_new` refuses it (`nd/exec/broadcast_exec.rs:35-41`). The plan fails.

## 8. NetCDF formats

### 8.1 Openers

```rust
#[derive(Debug, Clone)]
pub struct NetcdfCOpener {
    resolver: Arc<dyn NetCDFObjectResolver>,
    read_dimensions: Option<Vec<String>>,
    skip_unbroadcastable: bool,
}
// file_type: "netcdf". split_files: false (global lock; slices only add opens).
// open: resolve the native path, crate::reader::open_dataset(path), GridDataset::open(..).

#[derive(Debug, Clone)]
pub struct OxcdfOpener {
    read_dimensions: Option<Vec<String>>,
    skip_unbroadcastable: bool,
}
// file_type: "netcdf". split_files: true.
// open: crate::oxcdf_reader::open_dataset(store.clone(), file.path().clone()), GridDataset::open(..).
```

- Each opener also has `open_any(..) -> AnyDataset`. Schema inference and statistics call it, so the scan, the schema and the statistics open a file the same way. `FileAccess` and `NetcdfInput` go away.

### 8.2 Formats

```rust
pub struct NetcdfCFormat     { common: NetcdfCommon, resolver: Arc<dyn NetCDFObjectResolver> }
pub struct OxcdfNetcdfFormat { common: NetcdfCommon, enable_statistics: bool }
struct NetcdfCommon { listing_factory: Arc<ListingFactory>, options: NetcdfOptions, output_dir: PathBuf }

impl OxcdfNetcdfFormat {
    pub fn new(listing_factory: Arc<ListingFactory>, options: NetcdfOptions) -> Self;
    pub fn with_enable_statistics(self, enable: bool) -> Self;
    pub fn with_output_dir(self, dir: PathBuf) -> Self;
    pub fn options(&self) -> &NetcdfOptions;
}
impl NetcdfCFormat {
    pub fn new(listing_factory: Arc<ListingFactory>, options: NetcdfOptions,
               resolver: Arc<dyn NetCDFObjectResolver>) -> Self;
    /// With DefaultNetCDFObjectResolver: each object fails with an error that names its path.
    pub fn unresolved(listing_factory: Arc<ListingFactory>, options: NetcdfOptions) -> Self;
    pub fn with_resolver(self, resolver: Arc<dyn NetCDFObjectResolver>) -> Self;
    pub fn with_output_dir(self, dir: PathBuf) -> Self;
    pub fn options(&self) -> &NetcdfOptions;
}
```

| `FileFormat` method | Both formats |
|---|---|
| `get_ext`, `get_ext_with_compression` | `"nc"`. The schema cache finds the factory with `get_ext` (`beacon-datafusion-ext/src/fast_object/schema.rs:206`). |
| `infer_schema` | The common loop: `buffered(meta_fetch_concurrency)` over `open_any`, then `project_read_dimensions_or_skip`, then `refuse_ragged`, then `any_dataset_to_arrow_schema`, then `session_widening(..).merge_schemas(label_by_object(..))`. A file that the skip option skips gives an empty schema. |
| `infer_stats` | netcdf-c: always `Statistics::new_unknown`, with no log. `beacon-core/src/file_stats.rs` already reports netcdf-c one time per analysis pass. oxcdf: behind `enable_statistics`, `open_any`, then `refuse_ragged`, then `statistics_for_dataset`. An error gives unknown statistics and a warning. |
| `create_physical_plan` | `nd_physical_plan(conf, Arc::new(<its opener>))`. |
| `file_source` | `Arc::new(NdFileSource::new(Arc::new(<its opener>), table_schema))`. |
| `create_writer_physical_plan` | `self.common.writer_plan(..)`. This is today's body from `mod.rs:575-642`, unchanged. Both formats write through the netcdf-c sinks. |

- `generate_statistics(NetcdfInput)` goes away. `statistics_for_dataset` stays pub, because HDF5 uses it.

### 8.3 Factory

- **`create`:** parses `read_dimensions`, `skip_unbroadcastable`, the writer keys and `enable_statistics` in `shared.rs`. Then `use_rust_reader` selects `OxcdfNetcdfFormat` or `NetcdfCFormat::unresolved`. Statistics are off for a query.
- **`default`:** the same, from the config.
- **`create_with_native_root`:** for netcdf-c, `with_resolver(create_object_resolver(&listing.native_read_root(url)?))`. Return oxcdf unchanged.
- **`create_for_analysis`:** oxcdf with statistics on when `statistics_wanted`. netcdf-c as from `create_with_native_root`.
- **`schema_options_fingerprint`:** `None` when `read_dimensions` is set. Else `SchemaOptions::new("nc").str("netcdf-c" | "oxcdf")`.
  - The bytes stay the same, so the entries of regular files stay valid.
  - A ragged file that an earlier build analyzed keeps its entry. A query then plans over it, and gets the ragged error at scan. The new analyzer writes no entry for a ragged file.
  - No version salt: a salt makes every NetCDF query infer again until `ANALYZE FILES FORCE`, and the scan error already gives the clear error.
- **`discover_datasets`, `file_extensions`, `crawlable_extensions`:** unchanged.

### 8.4 Reader identity

```rust
impl ReaderBackend {
    pub const BOTH: [ReaderBackend; 2] = [ReaderBackend::NetcdfC, ReaderBackend::Oxcdf];
    /// The reader of a NetCDF format, or None for another format.
    pub fn of(format: &dyn FileFormat) -> Option<ReaderBackend>;
}
/// The table options of a NetCDF format, on either reader.
pub fn netcdf_options(format: &dyn FileFormat) -> Option<&NetcdfOptions>;
```

### 8.5 `read_netcdf`

- `factory.create(..)`, then a downcast to `NetcdfCFormat`.
- If the downcast succeeds, call `with_resolver` with the resolver from the one `RootStore` of all glob URLs, as today (`table_function.rs:172-196`).
- An oxcdf format is used as it is.

## 9. Ragged files

- `refuse_ragged` returns `DataFusionError::NotImplemented`. The engine adds the file path. The message says that the NetCDF reader does not support ragged datasets yet.
- `GridDataset::open` and schema inference call `project_read_dimensions_or_skip` first, then `refuse_ragged`. So:
  - without `read_dimensions`, a ragged file is an error;
  - with `read_dimensions` and `skip_unbroadcastable`, a ragged file is skipped;
  - with `read_dimensions` and no skip, the dimension error of `project_read_dimensions_or_skip` applies.
- HDF5 through the netcdf-c fallback gives the same error. HDF5 through oxcdf stays on the legacy engine and still reads ragged files.

**Schema cache and file statistics:**
1. The analyzer calls `infer_schema` before `infer_stats` (`beacon-core/src/file_stats.rs:185-193`). So a ragged file fails analysis at `infer_schema`. The collector logs one warning, sets `FileState::Failed`, and takes the file off the queue (`beacon-file-stats/src/collector.rs:299-316`).
2. Ranges and schema entries from earlier passes stay while the file does not change. `ANALYZE FILES FORCE` suppresses the ranges, because the file goes to Pending and then to Failed (`registry.rs:443-446`). The schema entry stays.
3. With a warm cache, a query that never opens the ragged file can succeed, because a file prune or a LIMIT can skip it. Without the cache, the same query fails at planning. This difference is accepted, because ragged NetCDF is not supported.

## 10. Callers outside the NetCDF crate

| Site | Change |
|---|---|
| `beacon-arrow-hdf5/src/format.rs:13` | Import `ReaderBackend`, not `NetcdfFormat`. |
| `beacon-arrow-hdf5/src/format.rs:386` `writes_with_netcdf_c` | `ReaderBackend::of(writer).is_some()`. |
| `beacon-arrow-hdf5/src/format.rs:308-309, 333` | Comments that name `NetcdfFormat`. |
| `beacon-arrow-hdf5/src/format.rs:562-730` tests | `ReaderBackend::of` in place of the `NetcdfFormat` downcast. |
| `beacon-arrow-hdf5/src/source.rs:136-146`, `tests/backend_parity.rs:263` | Comments that name `NetCDFSource`. Fix the name of the test that does not exist (`only_the_rust_reader_splits_one_file` → `each_reader_serves_its_own_source`). |
| `beacon-arrow-hdf5/tests/backend_parity.rs:447` `both_backends_return_the_same_rows_for_a_ragged_file` | Register WOD on netcdf-c with a new `try_register` helper that returns `Result`. Expect `NotImplemented`, with "ragged" and the file name in the text. Keep the Rust rows check (rows > 0). |
| `beacon-arrow-hdf5/tests/backend_parity.rs:200-203` `both_backends_infer_the_same_schema` | Loop over `GRIDDED_FILE` only. The row above holds the netcdf-c WOD error. |
| `beacon-core/src/file_stats.rs:59, 135-138` | `ReaderBackend::of(format) == Some(ReaderBackend::NetcdfC)`. |
| `beacon-core/src/metrics.rs` `collect_scan_totals` | Count each distinct `object_meta.location` one time for `input_bytes` and `file_paths`. Slices repeat a location. |
| `beacon-datafusion-ext/src/listing_table_factory_ext.rs:89-92` | Comment that names `NetcdfFormat::infer_stats`. |
| `beacon-functions/src/metadata/view_dataset_statistics.rs:9, 30` | Remove the unused `reader` import. Keep the `statistics` doc link valid. |
| `beacon-arrow-netcdf/examples/scan_profile.rs` | Use `OxcdfNetcdfFormat::new(..)` in place of `FileAccess::Oxcdf`. |
| `beacon-arrow-netcdf/Cargo.toml:23-25` | Fix the comment that names `datafusion::ReaderBackend` as the runtime switch. |
| `beacon-core/tests/file_stats_service.rs` `files_in_plan` | No change. Its tests use parquet, `.h5` and zarr, and HDF5 keeps the `nd-morsel-scan` marker. |
| `beacon-core/src/query/output.rs`, `runtime_builder.rs` | No change. They use the factory only. |

## 11. Tests

### 11.1 Engine tests (`beacon-nd-array/src/scan`)

Use `testing.rs`: a `MemOpener` over in-memory datasets, built through `GridDataset::try_new`, and a `drive()` helper that builds a new physical plan for each call and runs it.
- No state gives an empty `MorselPlan`. A skipped file, a Nothing file and an empty plan all finish.
- A morsel never does I/O. A counting dataset checks that `read` runs only inside the I/O futures.
- The read-ahead gives unit k+1 in flight while morsel k is out.
- `RowCount` gives the legacy COUNT(*) row totals, compared as totals only, and calls `read` zero times. A dataset with no arrays gives 0 rows.
- Partition columns are on every batch, in Columns and in Rows modes.
- Arrays in insertion order: `GridDataset::try_new` sorts them, and the batch schema equals the read schema.
- Slices: K slices cover every unit exactly once. Only slice 0 counts `files_skipped`. The sum of the slice prune counts equals the prune count of one scan without slices, for a static filter.
- Dynamic filter and slices: a `MemOpener` dataset prunes on a `DynamicFilterPhysicalExpr`. The test updates the filter between the open of slice 0 and the open of slice 1. No unit is read twice, and each unit that its own slice keeps is read.
- `repartitioned`:
  - `target` groups when `split_files` is true;
  - `None` when `split_files` is false and there are fewer entries than `target`;
  - `None` with an ordering, and with `target <= 1`;
  - zero entries gives `target` empty groups and no error;
  - slices only with `split_files` and `0 < files < target`, and no slice of a slice;
  - `statistics = None` on slices;
  - no byte ranges.
- `plan_file` refuses a byte range.
- `try_pushdown_filters`: splits, drops duplicates, answers `No`, and gives the filters to `prune`.
- `try_pushdown_projection`: a computed expression is refused.
- Errors name the file path, the slice and the unit.
- A plan that runs again returns the same rows. Build the physical plan one time, with 2 or more partitions and work stealing on. Use at least two files and one sliced file. Collect, call `reset_plan_states(plan)`, and collect again. Compare the sorted rows. Each `ScanMetrics` total after the second run is two times the total after the first run. This test shows that `NdFileSource`, the opener and the morselizer keep no state after one run.
- Parity with the legacy `FileRead` for the grid, the encoding, COUNT(*) row totals and partition columns. These tests go away with the legacy engine.

### 11.2 NetCDF tests (`beacon-arrow-netcdf/src/datafusion/`)

New `test_support.rs` (`#[cfg(test)]`):
- `test_file`, `factory`, `format(backend)`, `analysis_format(backend)`, `register`, `register_path`, `session`, `splitting_session`;
- `ReaderBackend::BOTH` loops;
- `write_regular_netcdf(dir, name, rows)`: one small regular file with a 1-D Float64 variable of a few thousand rows (more than one chunk), a global attribute and a variable attribute. It uses `ArrowRecordBatchWriter`, as `beacon-arrow-hdf5/tests/backend_parity.rs:367-401` does.

| Test | Action |
|---|---|
| `mod.rs` `a_predicate_reaches_the_scan_and_skips_its_chunks` (:2328) | Delete. Replace with: `chunks_pruned = 0`, and the counts stay correct. |
| `mod.rs` `the_scan_reports_what_it_read_and_what_it_skipped` (:2396) | Rewrite: `chunks_read` and `rows_read` count every chunk. `files_skipped` counts a skipped file. |
| `mod.rs` `a_scan_plans_one_entry_per_partition_whatever_its_files` (:1377), `only_the_rust_reader_shares_one_file` (:1760) | Rewrite as one test for both readers. oxcdf: `target` groups, K slices of one file, no byte range. netcdf-c with fewer files than `target`: `None`. |
| `mod.rs` `an_ordered_scan_is_not_shared` (:1725) | Keep: `None` with an ordering. |
| `mod.rs` `a_large_file_splits_and_returns_the_same_rows` (:2047), `a_shared_count_star_counts_every_row_once` (:2093) | Keep. Slices give the parallel read. |
| `mod.rs` `a_short_first_axis_still_splits_into_batches` (:2497) | Keep. Chunking stays. |
| `mod.rs` `a_pruned_scan_counts_what_the_coordinate_says_it_should` (:2239), `a_pushed_down_predicate_gives_the_same_answer` (:2468) | Keep. Rename the second one and fix the doc comments. |
| `mod.rs` tests that use `WOD_FILE` as "a NetCDF file" (:772, :776, :828, :875, :920, :1263) | Move to `gridded-example.nc`, or to `write_regular_netcdf`. |
| `mod.rs` `both_readers_return_identical_values_for_every_column` (:2161) | Remove the `WOD_FILE` entry (:2173). The `GRIDDED_FILE` entry already checks a global and a variable attribute. Never run `SELECT *` on `gridded-example.nc` (:2166-2170). |
| `mod.rs` `the_skip_option_reads_past_a_file_the_dimension_list_does_not_fit` (:1497) | Keep. The ragged file is skipped (section 9). |
| `mod.rs` `each_access_resolves_an_object_its_own_way` (:1030) | Delete. `listing_factory.rs:428` covers the https native path. |
| `mod.rs` `the_default_access_cannot_resolve_and_says_so` (:1061) | Replace with `the_unresolved_netcdf_c_format_names_the_path`: build with `factory.create` and `use_rust_reader=false`, run `infer_schema` on `ObjectMeta` "dir/file.nc", and check that the error holds "dir/file.nc". |
| `reader.rs` `the_default_backend_is_the_rust_reader` (:207) | Goes with `reader.rs`. `mod.rs:1086-1090` covers the default. |
| `partition_coverage_tests.rs` `scan_reads_every_file_regardless_of_target_partitions` (:103) | Stage 32 copies of one `write_regular_netcdf` file, not `wod_ctd_1964.nc`. All copies have the same size, because the test expects `per_file * FILES`. Keep both readers, `SELECT *` and `count(*)`. Fix the "ragged fixture" doc comment. Remove the `ctx_for` copy. |
| New | `a_ragged_file_is_rejected_at_schema_and_scan` on both readers. Fingerprint for each reader, and `None` with `read_dimensions`. `create_with_native_root` and `create_for_analysis` for each reader. |
| New plan tests, at `target_partitions = 4` with an explicit `SessionConfig` | Each test plans, executes, and compares rows with `target_partitions = 1`. (a) oxcdf, one file: 4 file groups, no `RepartitionExec` between `DataSourceExec` and `NdBroadcastExec`. (b) netcdf-c, one file: the plan builds, with the RoundRobin below `NdSourceExec` as today. (c) A one-file table `WITH ORDER`, on both readers. (d) One file with `repartition_file_scans=false`, on both readers. |

### 11.3 Other crates

- `beacon-core/tests`: `read_functions.rs`, `dynamic_store.rs`, `explain_analyze_external_netcdf.rs`, `external_table_partitioned_netcdf.rs` move to a regular fixture, with new row counts. `explain_analyze_external_netcdf.rs` keeps its `output_batches` check, because FileStream registers it.
- `beacon-core/tests/schema_cache_e2e.rs`:
  1. Write a ragged file. Put an entry for it with `put_file_schemas` under the current NetCDF fingerprint, as an earlier build would. Check that `read_netcdf` plans, and that the query fails with the ragged error and the file path.
  2. Run `run_once` over the ragged file. Check that the report shows `failed = 1`, that the file state is `Failed`, and that the pass stored no schema.
- `beacon-core`: one sliced oxcdf file on 4 partitions. Check that `input_bytes` counts the file one time and `file_paths` holds its path one time.
- `beacon-arrow-hdf5`: the tests in section 10.
- Python `integration-tests/formats/test_netcdf.py`: check the classic file (`NETCDF3_CLASSIC`) and the netCDF-4 comparison. Both are regular, so they stay.

### 11.4 Timing check

Run before and after on these cases. Report the wall time and the peak memory in the PR:
- the 40-file collection test (oxcdf);
- `write_large_netcdf` on 1 and 8 partitions (oxcdf);
- netcdf-c, one file with many chunks, 8 partitions, a query with WHERE on a data column and GROUP BY;
- the same netcdf-c query on 3 files at 8 partitions.

The legacy engine opened the next file early. The new engine does not. This timing shows the cost.

## 12. Docs and changelog

2.1.0-rc1 pages:
- `formats/netcdf.md`: ragged NetCDF is not supported yet. NetCDF does not prune chunks by a filter. Only the Rust reader reads one file on several partitions. `:48-49` stays true: reads go one chunk at a time.
- `arrays-to-tables.md:84-85`, `guides/speed-up-queries.md:36` and `coming-from-xarray.md:74`: chunk skip does not apply to NetCDF.
- `server/performance-tuning.md:105-107`: a scan returns one chunk per batch, and a native chunk can hold more than `BEACON_BATCH_SIZE` rows.
- `union-by-name.md`: its WOD example reads a ragged file through `read_netcdf`. Change it to a regular collection, or add a ragged warning.
- `internals/file-statistics.md:544`: ragged ranges apply to HDF5 on the Rust reader only.
- `upgrade.md`: ragged NetCDF now gives an error. HDF5 on the Rust reader still reads it.

`CHANGELOG.md` `[Unreleased]`:
- **Changed:** NetCDF reads through a new scan engine on DataFusion morsels.
- **Changed:** chunk pruning stops for NetCDF.
- **Changed:** COUNT(*) on NetCDF reads no array data.
- **Removed:** ragged NetCDF support for `STORED AS NC` and `read_netcdf`. A schema that the cache already holds for a ragged file fails at scan, not at `CREATE EXTERNAL TABLE`.

## 13. Commit sequence

Run the full suite after commits 2, 4 and 5.
1. nd-array: move `with_partitions` and `count_batch` to `scan/output.rs`. No behavior change.
2. nd-array: the scan engine, `testing.rs` and the engine tests. No caller yet.
3. netcdf and hdf5 tests: move the tests that use WOD as an ordinary file to a regular fixture. They stay green on the legacy engine.
4. netcdf: `ReaderBackend::of`, `NetcdfCommon`, the two formats, the openers and the factory. Delete `source.rs` and `reader.rs`. Update the pruning, partition, plan and ragged tests.
5. Callers: hdf5, beacon-core (`file_stats.rs`, `metrics.rs`, tests), beacon-functions, the example, the Cargo comment.
6. Docs and CHANGELOG.

## 14. Out of scope

- `spawn_blocking` for netcdf-c (`reader::open_dataset`, `VariableBackend::read_subset`) and for the oxcdf decode. This is PR 2.
- Ragged support on the engine (`RaggedScanDataset`).
- A NetCDF chunk prune (`GridDataset::prune_by_coordinates`). It arrives with its first caller.
- Moving Zarr, Icechunk, TIFF and HDF5 (oxcdf) to the engine, then deleting `arrow/file_read.rs` and `arrow/morsel.rs`.
- The RoundRobin that coalesces encoded chunks when `repartitioned` returns `None`. The fix sets `benefits_from_input_partitioning() -> vec![false]` on `NdSourceExec`, `NdBroadcastExec`, `NdFilterExec` and `NdProjectionExec`, so a needed RoundRobin goes above `NdBroadcastExec`, on flat batches. `NdFilterPushdown` and `NdProjectionPushdown` (`optimizer.rs:60-65, 152`) must then look through that RoundRobin. Add plan tests for one small BBF file, an ordered scan and `repartition_file_scans=false`.
- `NdBroadcastExec` slices of `batch_size` rows.

## 15. Risks

1. **Experimental API.** The DataFusion morsel API can change. Only `scan/morsel.rs` and `scan/source.rs` use it.
2. **Throughput.** Each partition has one I/O and a read-ahead of one unit. The next file opens only after the current file ends (`scan_state.rs:268`). Remote stores pay latency. The timing check in 11.4 measures this.
3. **netcdf-c read-ahead.** A netcdf-c read completes inline in poll until PR 2, so the read-ahead gives no overlap for netcdf-c.
4. **Slices.** Each slice opens the file again, and a sliced file loses its statistics. An oxcdf file with fewer units than K leaves some partitions with no work. The slices live in in-memory extensions, so a future plan serializer must carry `NdSlice`.
5. **No NetCDF chunk prune.** A selective coordinate filter reads more chunks. The results stay correct.
6. **Ragged NetCDF fails.** HDF5 through netcdf-c fails on WOD files, but HDF5 through oxcdf still reads them. A cached schema of a ragged file moves the error from planning to scan (section 9).
7. **netcdf-c blocks a tokio worker** under the global lock until PR 2. This is the same as today.
8. **Error text changes.** Errors read "netcdf file X: …". Tests that match the exact old text change.
9. **DataFusion counters.** `files_processed` and `file_open_errors` count morsels and reads, not files. `ScanMetrics` is the reliable signal.
10. **Two engines exist at the same time.** Parity tests cover the overlap until the legacy engine goes away.
11. **The fallback RoundRobin stays.** When `repartitioned` returns `None`, the RoundRobin below `NdSourceExec` still holds encoded chunks up to `batch_size` rows, as today. Section 14 holds the fix.
