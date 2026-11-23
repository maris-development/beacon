# A shared ND framework: the dataset contract and the scan

Date: 2026-11-23. Status: draft, for review.
Branch: `feature/nd-framework`, from `origin/features/netcdf-reader-refactor`.

This design replaces one decision of `2026-10-14-netcdf-reader-split-design.md`:
"no shared trait and no generic engine". The user now wants one shared
framework for all ND formats.

## 1. Intent

Each ND format has its own file source, opener, dimension projection, chunk
pruning, statistics and scan. Three chunk engines do the same job:

| Engine | Formats |
|---|---|
| `FileRead` + `MorselSource` (`beacon-nd-array`) | HDF5, Zarr, GeoTIFF, Icechunk through Zarr |
| `DatasetSource` + `CollectionQueues` | Atlas |
| The DataFusion 54 morsel scan (`beacon-arrow-netcdf/src/datafusion/scan/`) | NetCDF |

A fix or a feature in one engine does not reach the other formats. This
design gives one framework. A format supplies only two things: it opens a
file, and it reads a region of a variable.

The framework has two new crates, in a new folder `beacon-db/beacon-nd/`:

1. `beacon-nd-dataset`: the dataset contract. It builds on the external crate
   `nd-arrow-array`. It does not depend on DataFusion.
2. `beacon-nd-scan`: the DataFusion scan on that contract.

## 2. Goals

- One trait, `NdDataset`, that a format implements on its file handle.
- One trait, `NdFileOpener`, that a format implements to open a file.
- One `FileSource`, one morsel planner, one chunk grid, one prune, one output
  step and one statistics function for all ND formats.
- Each concern has one module. Each module has tests that need no file
  format.
- NetCDF moves to the framework. Its query results do not change.
- The other formats can move later, one format per change.

## 3. Non-goals

- Ragged datasets. The contract does not model them in this version. HDF5,
  Zarr and Icechunk keep the legacy engine for ragged files until a later
  design.
- The CF decode. `missing_value`, the default fill, packed time, `int64` time,
  `_Unsigned` and char arrays stay as they are. NetCDF keeps the decode of
  `oxcdf_reader/compat.rs`. The switch to `nd_arrow_array::compute::decode_cf`
  is a later change, because it changes column types (section 9).
- The move of HDF5, Zarr, Icechunk, GeoTIFF, Atlas and the plugin formats.
- A move of `beacon-nd-encoding`, `beacon-nd-execution` and `beacon-nd-array`
  into `beacon-nd/`. One later commit moves these folders (user decision,
  2026-11-23).
- A change of `NdSourceExec` and `NdBroadcastExec`, except the limit hand-off
  of section 6.8.
- The `beacon.nd` encoding on the wire and in the plugin ABI.
- The writers.
- The rename of `read_dimensions` to `dimensions` in the SQL options. That is
  the design of 2026-11-22. This design gives it the typed value to use.

## 4. Anti-goals

- A NetCDF query result must not change, except that `COUNT(*)` works again
  (section 7.3).
- The scan must not get slower. The timing check of section 8.4 measures this.
- The new crates must not depend on a format crate.
- `beacon-nd-dataset` must not depend on DataFusion or on `object_store`.
- The framework must not hold a table rule that only one format needs.

## 5. Constraints

- DataFusion 54.1 morsels:
  - An empty `MorselPlan` panics in the `FileStream` open timer.
  - Never split an ND file by byte range.
  - A plan must give the same rows after `reset_plan_states`.
  - A `RepartitionExec(RoundRobinBatch)` can merge encoded batches. The scan
    emits one encoded row per chunk, and `NdSourceExec` decodes each row. Keep
    this property. This branch has no RoundRobin opt-out.
- The chunk grid spans only the dimensions that the selected variables use.
  When no selected variable lies on all of them, the grid is their
  cross-product. This is the rule of `scan/chunks.rs` `layout` today.
- `nd-arrow-array` 0.5.0 uses arrow 58.4 and ndarray 0.17. The workspace uses
  arrow `^58` and ndarray 0.17.1.
- A table without `dimensions` keeps one grid per file (user decision,
  2026-11-22). `skip_unbroadcastable` keeps its name.
- Edition 2024 and `rust-version.workspace = true` for both crates.

## 6. Approaches

### 6.1 The dataset contract

| Approach | Trade-off |
|---|---|
| A. A dataset trait and a variable trait. | The format implements both. Each format repeats the lookup. NetCDF has five backend types for this pattern today. |
| B. A dataset struct and a variable trait. | One checked container. It is the model of `beacon-nd-array` today, cleaned up. A wide file must build all metadata at each open. |
| **C. One dataset trait. Metadata and the read are its methods.** | The format implements one trait on its file handle. A wide format answers metadata from its own header. The crate cannot check the metadata in a constructor, so a shared `validate` function does it. |

Decision: C (user, 2026-11-23). The user also chose metadata as trait methods,
so that a format can implement them. `VariableMeta` stays a plain struct.

### 6.2 The base of the array model

| Approach | Trade-off |
|---|---|
| Keep `NdArrayD` and `ArrayBackend<T>` of `beacon-nd-array`. | No new dependency. It keeps the typed `ndarray` path, the fill value at conversion time, and attributes as fake arrays. |
| **Build on `nd-arrow-array`.** | Arrow arrays with a null buffer, named dimensions and attributes. Broadcast, `isel` and CF kernels exist. One more dependency, owned by the user. |

Decision: `nd-arrow-array` (user, 2026-11-23).

### 6.3 Where the framework lives

| Approach | Trade-off |
|---|---|
| Grow `beacon-nd-array`. | No new crate. The old and new models mix in one crate until the last format moves. |
| One new crate. | The contract then depends on DataFusion. |
| **Two new crates in `beacon-db/beacon-nd/`.** | The contract has no DataFusion dependency. The plugin SDK can use it later. Two crates to version. |

Decision: two crates in `beacon-nd/` (user, 2026-11-23).

## 7. Design

### 7.1 `beacon-nd-dataset`

Dependencies: `nd-arrow-array`, `arrow`, `async-trait`, `thiserror`,
`futures`. Dev: `tokio`.

`nd-arrow-array` is a git dependency with a fixed rev, in the workspace
`Cargo.toml`, as `datafusion-spatial` is today:

```toml
nd-arrow-array = { git = "https://github.com/robinskil/nd-arrow-array.git", rev = "e1208d5" }
```

Use the full commit hash of that rev. Use the crates.io release in place of
the git dependency when one exists.

```
beacon-nd-dataset/src/
  lib.rs          module list, re-exports
  error.rs        Error, Result
  region.rs       Region
  dataset.rs      NdDataset, VariableMeta, validate
  schema.rs       DatasetSchema
  memory.rs       MemoryDataset
  view.rs         GridSelection, select_grid, select_columns
  grid.rs         ChunkGrid, read_chunk
  statistics.rs   value_range
```

#### The contract (`dataset.rs`, `region.rs`)

```rust
/// A box inside one variable: a start and a length for each axis.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Region {
    pub start: Vec<usize>,
    pub shape: Vec<usize>,
}

impl Region {
    /// The whole variable.
    pub fn full(dimensions: &Dimensions) -> Self;
    /// The element count: the product of `shape`. 1 for rank 0.
    pub fn len(&self) -> usize;
}

/// What the scan must know about one variable before a read.
#[derive(Debug, Clone, PartialEq)]
pub struct VariableMeta {
    /// The Arrow type that `read` returns.
    pub data_type: DataType,
    /// The dimensions, in C order, with their sizes.
    pub dimensions: Dimensions,
    /// The native chunk shape. `None` for a variable without chunks.
    pub chunk_shape: Option<Vec<usize>>,
    pub attributes: Attributes,
}

/// One opened file of an ND format.
#[async_trait]
pub trait NdDataset: Send + Sync + std::fmt::Debug {
    /// The name for errors and logs. Usually the file path.
    fn name(&self) -> &str;
    /// Every dimension of the file, with its size.
    fn dimensions(&self) -> &Dimensions;
    /// The global attributes.
    fn attributes(&self) -> &Attributes;
    /// The variable names. The order is the same in each call.
    fn variable_names(&self) -> Vec<Arc<str>>;
    /// One variable. `None` when the file does not have it.
    fn variable(&self, name: &str) -> Option<VariableMeta>;
    /// Read one region of one variable.
    async fn read(&self, variable: &str, region: &Region) -> Result<NdArrowArray>;
}
```

`Dimensions`, `Attributes` and `NdArrowArray` are the types of
`nd-arrow-array`.

Rules of the contract:

1. `read` returns an array with the type of `VariableMeta::data_type`, and
   with the dimension names of the variable and the sizes of `region.shape`.
2. `read` returns missing data as Arrow nulls. The format applies its fill
   value. The framework knows no fill value.
3. The metadata methods do no I/O. `open` reads what they need.
4. A dimension has one size in the whole dataset. A variable that uses a
   dimension has that size on that axis.
5. A variable name is unique.
6. The framework finds a variable by name. The order of `variable_names`
   decides only two ties: the tie of `default_grid` (the first candidate
   wins) and the tie of the grid driver (the last variable wins). So the
   order must be the same at each open of the same file.
7. `read` on an unknown variable gives `Error::UnknownVariable`. A region
   outside the variable gives `Error::InvalidRegion`.
8. `open` gives `Error::Unsupported` for a valid file that this contract
   cannot model, for example a ragged file.

`validate(&dyn NdDataset) -> Result<()>` checks rules 4 and 5, and that each
chunk shape has the rank of its variable. A format calls it in its tests.
The scan calls it at open in debug builds only.

#### Errors (`error.rs`)

```rust
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("{dataset}: the file is not on the grid {grid:?}: {reason}")]
    OffGrid { dataset: String, grid: Vec<String>, reason: String },
    #[error("{dataset}: no variable '{variable}'")]
    UnknownVariable { dataset: String, variable: String },
    #[error("{dataset}: variable '{variable}': invalid region: {reason}")]
    InvalidRegion { dataset: String, variable: String, reason: String },
    #[error("{dataset}: invalid dataset: {reason}")]
    Invalid { dataset: String, reason: String },
    /// The file is valid, but the contract cannot model it.
    #[error("{dataset}: {reason}")]
    Unsupported { dataset: String, reason: String },
    #[error(transparent)]
    Nd(#[from] nd_arrow_array::NdError),
    #[error(transparent)]
    Arrow(#[from] arrow::error::ArrowError),
    /// An error of the format: I/O, decode.
    #[error("{0}")]
    Format(#[source] Box<dyn std::error::Error + Send + Sync>),
}
```

`OffGrid` is a typed error. It replaces the `anyhow` downcast to
`UnbroadcastableDataset`.

#### Ready metadata (`schema.rs`)

`DatasetSchema` holds all metadata in memory: name, dimensions, global
attributes and an `IndexMap<Arc<str>, VariableMeta>`. Its constructor runs the
checks of `validate`. It has the same metadata methods as the trait. A simple
format keeps one `DatasetSchema` and forwards the trait methods to it. A wide
format implements the methods itself.

#### A dataset in memory (`memory.rs`)

`MemoryDataset` holds a `DatasetSchema` and one `NdArrowArray` per variable.
`read` slices the array. It can report a chunk shape per variable, and it
counts its reads. The tests of both crates use it.

#### Views (`view.rs`)

A view is a struct that wraps an `Arc<dyn NdDataset>` and implements
`NdDataset`. It hides variables and dimensions. It forwards `read`.

```rust
/// The grid a table reads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GridSelection {
    /// Each file reads its own default grid.
    PerFile,
    /// The grid the table names. An empty list reads only rank-0 variables.
    Given(Vec<String>),
}

/// Keep the variables on the grid. An `OffGrid` error when the file does not fit.
pub fn select_grid(dataset: Arc<dyn NdDataset>, grid: &GridSelection)
    -> Result<Arc<dyn NdDataset>>;

/// Keep the variables with these names. A name the file does not have is ignored.
pub fn select_columns(dataset: Arc<dyn NdDataset>, names: &[&str])
    -> Arc<dyn NdDataset>;

/// The default grid of a file. `None` when all variables broadcast already.
pub fn default_grid(dataset: &dyn NdDataset) -> Option<Vec<String>>;
```

`GridSelection` is the `Dimensions` enum of the 2026-11-22 design. It has a
new name, because `nd_arrow_array::Dimensions` is a different type.

`select_grid` runs these steps:

1. Resolve the grid. `Given` gives the list. `PerFile` gives `default_grid`.
   When that is `None`, return the dataset as it is.
2. Check that the file has each dimension of the grid. If not, `OffGrid`.
3. Keep the variables whose dimensions are all on the grid. A rank-0 variable
   always stays.
4. Remove the dimensions that no kept variable uses. Keep the file order.
5. Log the removed variables at debug level, one time per file, with the
   dataset name.

`default_grid` is a port of `Dataset::default_broadcast_dimensions`
(`beacon-nd-array/src/dataset/mod.rs:164`). The port keeps the rule exactly,
also the tie rule and the invented-dimension rule. To support that rule,
`DatasetSchema` and the trait carry the set of invented dimension names:

```rust
/// The dimensions that the reader gave a name. Default: none.
fn invented_dimensions(&self) -> &[Arc<str>] { &[] }
```

Port the unit tests of `default_broadcast_dimensions` and
`project_with_dimensions` with the code.

The skip option is not in this crate. The caller sees `OffGrid` and decides.

#### The chunk grid (`grid.rs`)

```rust
/// The chunks of the selected variables of one dataset.
pub struct ChunkGrid { /* grid dimensions, chunk list */ }

impl ChunkGrid {
    /// Cut the grid of `dataset` into chunks of about `batch_size` rows.
    pub fn try_new(dataset: &dyn NdDataset, batch_size: usize) -> Result<Self>;
    /// The dimensions of the grid.
    pub fn dimensions(&self) -> &Dimensions;
    /// The chunks, in C order. Each chunk is a region of the grid.
    pub fn chunks(&self) -> &[Region];
    /// The region of `variable` that lies in `chunk`.
    pub fn variable_region(&self, variable: &VariableMeta, chunk: &Region) -> Region;
}

/// Read each variable of `dataset` in `chunk` into one un-broadcast batch.
pub async fn read_chunk(dataset: &dyn NdDataset, grid: &ChunkGrid, chunk: &Region)
    -> Result<NdRecordBatch>;
```

The rules are the rules of `scan/chunks.rs` today. Port them with their
tests:

- The grid spans the dimensions that the variables of `dataset` use.
- A variable on all grid dimensions drives the grid. With more than one, the
  variable with the most elements drives it. With none, the grid is the
  cross-product of the used dimensions.
- A grid of at most `batch_size` rows is one chunk, also with native chunks.
- Else the native chunk shape of the driver is the chunk shape. Else the
  chunk fills from the last axis up to `batch_size` rows.
- A dataset with no variable has a grid of the dataset dimensions. This case
  is the count scan of section 7.3.
- `ChunkGrid::try_new` never returns a chunk of zero rows. A grid with a
  dimension of size 0 has no chunks.

`read_chunk` reads the variables one after the other, as `FileScan::read`
does today. A coordinate is read one time per chunk. Reads at the same time
on one file handle are a later change, with its own timing check.

The grid driver tie uses the order of `variable_names`: the last variable
with the most elements wins, as `max_by_key` does in `scan/chunks.rs` today.

#### Statistics (`statistics.rs`)

```rust
/// The minimum and maximum of a rank-0 or rank-1 variable. `None` for
/// other ranks, for an empty variable and for a type with no order.
pub async fn value_range(dataset: &dyn NdDataset, variable: &str)
    -> Result<Option<(ArrayRef, ArrayRef)>>;
```

Each result is an array of length 1. The function reads the variable and uses
the Arrow `min` and `max` kernels.

### 7.2 `beacon-nd-scan`

Dependencies: `beacon-nd-dataset`, `nd-arrow-array`, `beacon-nd-encoding`,
`beacon-datafusion-ext` (`scan_adapt`, type widening), `datafusion`,
`datafusion-datasource`, `object_store`, `async-trait`, `futures`.

It does not depend on `beacon-nd-array`.

```
beacon-nd-scan/src/
  lib.rs          module list, re-exports
  opener.rs       NdFileOpener, NdScanOptions, open_for_read
  source.rs       NdSource: the FileSource
  slices.rs       NdSlice, slice_files, spread_files
  planner.rs      NdMorselizer, planner states, ChunkMorsel
  prune.rs        value ranges of a predicate, coordinate masks, the chunk test
  output.rs       encode, partition columns, adapt to the projected schema
  schema.rs       file_schema: the Arrow schema of one file
  statistics.rs   file_statistics: DataFusion statistics of one file
  metrics.rs      ScanMetrics
  tests.rs        end-to-end tests on MemoryDataset
```

Only `planner.rs` and `source.rs` name DataFusion morsel types.

#### The opener (`opener.rs`)

```rust
/// Opens one file of an ND format.
#[async_trait]
pub trait NdFileOpener: Send + Sync + std::fmt::Debug + 'static {
    /// The name for errors and EXPLAIN, for example "netcdf".
    fn format_name(&self) -> &'static str;
    /// Open one file. Read the metadata only.
    async fn open(&self, store: Arc<dyn ObjectStore>, file: &ObjectMeta)
        -> beacon_nd_dataset::Result<Arc<dyn NdDataset>>;
}

/// The table options that every ND format has.
#[derive(Debug, Clone)]
pub struct NdScanOptions {
    pub grid: GridSelection,
    pub skip_unbroadcastable: bool,
    pub skip_invalid_files: bool,
}

/// Open one file and apply the grid. `Ok(None)` when an option skips the file.
pub async fn open_for_read(opener: &dyn NdFileOpener, store: Arc<dyn ObjectStore>,
    file: &ObjectMeta, options: &NdScanOptions)
    -> Result<Option<Arc<dyn NdDataset>>>;
```

`open_for_read` is the one open path of the schema inference and the scan.
It runs:

1. `opener.open`.
   - `Error::Unsupported`: with a `Given` grid and `skip_unbroadcastable`,
     log a warning and return `None`. Else return a DataFusion
     `NotImplemented` error. `skip_invalid_files` never skips this error.
     This is the order of `open.rs` today for a ragged file.
   - Any other error with `skip_invalid_files`: log a warning, return `None`.
2. `select_grid`. On `OffGrid` with `skip_unbroadcastable`: log a warning,
   return `None`.
3. `validate`, in debug builds.

The statistics do not use `open_for_read`. They call `opener.open` with no
grid and no skip, as `open_whole` does today. So a column that the grid
removes keeps its statistics, and no query answer changes. An open error
gives unknown statistics and a warning.

A format that needs its own store ignores the `store` argument. Icechunk
needs this later.

#### The source (`source.rs`)

```rust
#[derive(Debug, Clone)]
pub struct NdSource {
    opener: Arc<dyn NdFileOpener>,
    options: NdScanOptions,
    table_schema: TableSchema,
    metrics: ExecutionPlanMetricsSet,
    batch_size: usize,
    predicate: Option<Arc<dyn PhysicalExpr>>,
    projection: Option<ProjectionExprs>,
}
```

| `FileSource` method | Behavior |
|---|---|
| `create_file_opener` | `not_impl_err!`. ND scans use `create_morselizer`. |
| `create_morselizer` | Build `NdMorselizer` with one `ScanMetrics` per partition. |
| `file_type` | `opener.format_name()`. |
| `try_pushdown_projection` | As `NetcdfSource` today. |
| `try_pushdown_filters` | Keep the conjunction as `predicate`. Answer `PushedDown::No`. The `FilterExec` stays above the scan. |
| `supports_repartitioning` | `true`. |
| `repartitioned` | Section "Slices". |

The table schema holds the `beacon.nd` encoded file schema and the partition
columns, as today.

#### Slices (`slices.rs`)

A move of `scan/slices.rs`, with `NetcdfSlice` renamed to `NdSlice`. The
rules do not change: return `None` with an ordering or `target <= 1`; no
slices with a row limit; `K = ceil(target / files)` slices per file; never a
slice of a slice; exactly `target` groups; never a byte range.

#### The planner (`planner.rs`)

A move of `scan/morsel.rs`. The states do not change:

```text
NdMorselizer::plan_file(file)                  CPU only
   file.range.is_some()  -> internal error
   otherwise             -> Unopened{file}

Unopened.plan() -> a plan with the open future

open future  [I/O]
   open_for_read                     None -> Skipped
   select_columns(projected file columns)
   no file column in the query       -> the count scan (section 7.3)
   the file has none of the columns  -> Done
   ChunkGrid::try_new
   chunks = the slice range of the full chunk list
   masks  = coordinate_masks(..)     only with a predicate
   drop the chunks that is_pruned accepts; count them
   no chunk left                     -> Done
   otherwise                         -> Read{file, next: 0, ready: None}

Read{file, next, ready}.plan()                 CPU only
   the limit is reached and ready is None -> Ok(None)
   morsel = ready.map(ChunkMorsel)
   io     = (next < chunks.len() && limit not reached).then(read future)
   neither -> Ok(None)

read future(k)  [I/O]
   read_chunk(chunk k)
   -> Read{file, next: k+1, ready: Some(batch)}

ChunkMorsel::into_stream     CPU: output.rs
```

Invariants, as today:

1. `plan_file` and `plan()` do no I/O. A morsel does no I/O.
2. No empty `MorselPlan`.
3. One I/O per partition, with a read-ahead of one chunk.
4. The chunk list before the prune is the same in each slice. The slice cut
   comes before the prune.
5. Only slice 0 counts a skipped file.

Each error gets the context `"{format_name} file {path}"`, plus the slice and
the chunk, as today.

#### Prune (`prune.rs`)

- `value_ranges(predicate) -> Vec<(column, ValueRange)>`: the bounds from the
  `column op literal` conjuncts. A port of `PushdownFilter::ranges`
  (`beacon-nd-array/src/arrow/pushdown_filter.rs`).
- `coordinate_masks(dataset, ranges)`: for each bounded variable that is
  rank 1 and has an ordered type, read it and compare it to the bounds with
  the Arrow `cmp` kernels. A mask that cannot be built is left out.
- `is_pruned(masks, grid, chunk)`: true when one mask is all false inside the
  chunk.

Pruning only skips work. The `FilterExec` above the scan applies the
predicate again. Port the tests of `scan/prune.rs` and of `pushdown.rs`.

#### Output (`output.rs`)

For each chunk:

1. Convert the `nd_arrow_array::NdRecordBatch` to a
   `beacon_nd_encoding::NdRecordBatch`. Each column keeps its values buffer;
   only the wrapper types change.
2. `encode_nd_record_batch`: one `beacon.nd` row.
3. Append each partition column as a rank-0 `beacon.nd` column, from
   `PartitionedFile::partition_values`.
4. Adapt to the projected schema with `scan_adapt::batch_adapter_factory`. A
   column that the file does not have becomes null.

A chunk of a count scan has no file column, so steps 1 to 3 do not fit. The
encoding has a carrier for this case already, and the scan uses it:

- No column at all (`COUNT(*)`): a batch with zero columns and
  `row_count = chunk rows`. `decode_nd_record_batch_row` turns it into a
  `row` axis of that size.
- Only partition columns: one encoded row. Each partition column lies on a
  `row` axis of the chunk row count. Rank-0 columns alone would decode to
  one row.

Port `FilePartitions::scalar_columns` and `row_columns`
(`beacon-nd-array/src/arrow/partition.rs`) for steps 3 and the carrier.

This crate converts to `beacon.nd` at this one place. When `NdSourceExec`
moves to `nd_arrow.array`, only this file changes.

#### Schema and statistics (`schema.rs`, `statistics.rs`)

- `file_schema(dataset) -> Schema`: one field per variable, sorted by name.
  The sort is here, not in the readers.
- `file_statistics(dataset, table_schema) -> Statistics`: `value_range` for
  each rank-0 and rank-1 variable; unknown for the others; no row count. This
  is `statistics_for_dataset` of the NetCDF crate today.

#### Metrics (`metrics.rs`)

`ScanMetrics` has the counters of `ReadMetrics` today, with the same names:
`chunks_read`, `rows_read`, `chunks_pruned`, `rows_pruned`, `files_skipped`.
`beacon-core/src/metrics.rs` reads them by name.

### 7.3 Pushdown

| Pushdown | Where | Rule |
|---|---|---|
| Columns | `planner.rs` open, `select_columns` | Only the projected file columns are read. |
| Dimensions | `open_for_read`, `select_grid` | Before the columns. Section 7.1. |
| Filters | `prune.rs` | Chunks that no row can match are not read. |
| Limit | `planner.rs` | Section "Limit". |
| Count | `planner.rs`, `grid.rs` | Section "Count". |

**Count.** A query with no file column (`COUNT(*)`, or only partition
columns) runs the same planner. After `select_columns` the dataset has no
variable. `ChunkGrid` then spans the dimensions of the dataset after
`select_grid`. `read_chunk` reads nothing and returns a batch with no
columns and the row count of the chunk. `output.rs` puts that count in the
carrier of section 7.2. So `COUNT(*)` is the row count of `SELECT *`, and it
reads no data. The planner has no count state and no second path.

**Limit.** Today no ND node hands a limit to the scan. The scan stops when
the consumer drops the stream. Keep that. In addition:

- `NdSourceExec` and `NdBroadcastExec` hand a `fetch` to their child when
  DataFusion offers it (`with_fetch`, `supports_limit_pushdown`). The builder
  checks the DataFusion 54.1 API first. If the hand-off needs more than these
  two methods on each node, leave it out and record that in the plan.
- With `FileScanConfig::limit` set, the planner counts the rows of the chunks
  it emits, per partition. It plans no more reads after the count reaches the
  limit.
- The `FileStream` limit counts encoded rows, so it counts chunks. Each chunk
  has at least one row (section 7.1), so that limit never cuts rows that the
  query needs. For the zero-column carrier it counts real rows, which is
  exact.

A limit reaches the scan only with no filter between, so chunk rows are
exact.

### 7.4 NetCDF on the framework

The NetCDF crate keeps: `factory.rs`, `format.rs`, `options.rs`,
`table_function.rs`, the sinks and `oxcdf_reader/`. It loses the `scan/`
folder, `open.rs` and the scan half of `statistics.rs`.

- **The bridge.** `beacon-nd-array` gets one new file, `src/bridge.rs`:
  `DatasetBridge` wraps a regular `beacon_nd_array::Dataset` and implements
  `NdDataset`.
  - Each array of the old dataset is one variable. The attribute arrays
    (`var.attr`, `.global`) stay rank-0 variables, so the table schema does
    not change. `VariableMeta::attributes` is empty.
  - `read` calls `NdArrayD::subset` and `ndarray_to_arrow_array`. These are
    the calls of `FileScan::read` today, so the values and the nulls do not
    change.
  - `chunk_shape` is `None` when the chunk shape equals the shape.
  - `invented_dimensions` forwards the set of the old dataset.
  - The bridge is deleted with `beacon-nd-array`.
- **The opener.** `NetcdfOpener` in the NetCDF crate implements
  `NdFileOpener`. `open` calls `oxcdf_reader::open_object`. A ragged dataset
  gives `Error::Unsupported` with the text of today. A regular dataset goes
  in a `DatasetBridge`, which keeps the array order of the old dataset.
- **The format.** `NetcdfFormat` builds an `NdSource` with a `NetcdfOpener`.
  `infer_schema` uses `open_for_read` and `file_schema`. `infer_stats` uses
  `opener.open` and `file_statistics`.
- **`beacon-core`.** The test at `beacon-core/src/metrics.rs:384` names
  `NetcdfSource`. Change it to `NdSource`. `NetcdfOptions` builds the
  `NdScanOptions`: `read_dimensions: None` is `GridSelection::PerFile`,
  `Some(list)` is `GridSelection::Given(list)`.
- **HDF5.** It uses `statistics_for_dataset` and `oxcdf_reader` of the NetCDF
  crate. Keep `statistics_for_dataset` in the NetCDF crate until HDF5 moves.

Behavior changes for NetCDF:

1. `COUNT(*)` and a query of only partition columns no longer fail. The
   count is the row count of `SELECT *`.
2. `PARTITIONED BY` stays refused in `NetcdfFormat::create_physical_plan`.
   That refusal is a decision of the format, not of the scan.
3. A file off the grid fails with the `OffGrid` text.

Nothing else changes: the plan shape, the chunk grid, the prune, the slices,
the metrics names, the options and the SQL.

### 7.5 How the later formats fit

This section is a check of the contract, not a work item.

| Format | First step | Later |
|---|---|---|
| HDF5, GeoTIFF | An opener that returns a `DatasetBridge`. Partition columns work through `output.rs`. | A native `NdDataset`. Ragged HDF5 needs its own design. |
| Zarr, Icechunk | The same. One leaf group is one file, as today. The Icechunk opener holds its version storage. | A native `NdDataset` that fetches array metadata on demand. |
| Atlas | Needs more than one dataset per container, and a prune on footer statistics. Extend `NdFileOpener` then: `open` returns a list, and a dataset can report statistics per variable. | |
| Plugins | Stay on the stream path. `NdDataset` has six methods and plain data, so a later ABI version can carry it. | |

`AttributeColumns`, a view that shows `VariableMeta::attributes` as rank-0
variables `var.attr`, comes with the first native `NdDataset`. The bridge
does not need it.

## 8. Tests

### 8.1 `beacon-nd-dataset`

- `Region`: `full`, `len`, rank 0.
- `DatasetSchema` and `validate`: a size conflict, a duplicate name, a chunk
  shape of the wrong rank.
- `MemoryDataset`: a region read, a rank-0 read, an unknown variable, a
  region outside the variable.
- `select_grid`: `Given` on and off the grid; an empty `Given` list;
  `PerFile` with a narrower grid and with a broadcast-safe file; the debug
  log. The ported tests of `default_broadcast_dimensions`.
- `select_columns`: known names, an unknown name, no names.
- `ChunkGrid`: the ported tests of `scan/chunks.rs`; the cross-product grid;
  a dataset with no variable; a dimension of size 0.
- `read_chunk`: each variable is read one time, on its own axes.
- `value_range`: rank 0, rank 1, rank 2, all nulls, a string type.

### 8.2 `beacon-nd-scan`

All tests use `MemoryDataset` behind a test opener.

- Unit: the ported tests of `scan/morsel.rs`, `slices.rs`, `prune.rs`,
  `output.rs` and `source.rs`.
- No planner state gives an empty `MorselPlan`: a skipped file, a file with
  none of the columns, a fully pruned file, a count scan.
- Count: `COUNT(*)` equals the row count of `SELECT *`, and the dataset
  counts zero reads. With slices, each row is counted one time.
- Partition columns: a value per file; a query of only partition columns.
- Limit: with a limit, the planner reads only the first chunks.
- End to end through `NdSourceExec` and `NdBroadcastExec`: one file on 1 and
  on 4 partitions, with and without a filter; a dropped stream stops the
  reads; a plan that runs again after `reset_plan_states`.
- The skips: an invalid file and a file off the grid, with and without the
  options.

### 8.3 NetCDF

- `DatasetBridge`: for each NetCDF test file, the variables, types,
  dimensions and one full read equal the old dataset.
- A schema pin: `file_schema` of each test file equals the schema on the base
  branch. Write the expected schema down before the change.
- The 25 end-to-end tests of `scan/tests.rs` move to the NetCDF crate and
  keep passing. The tests that expect the `COUNT(*)` error change to expect
  the count.
- The `beacon-core` NetCDF tests keep passing.

### 8.4 Timing check

Before and after, in the PR of the NetCDF step: the 40-file collection, and
one large file on 1 and 8 partitions, with and without a filter. Use
`examples/scan_profile.rs`.

## 9. The relation to nd-arrow-array

- **Now.** The contract uses its types: `NdArrowArray`, `Dimensions`,
  `Attributes`, `NdRecordBatch`. The framework uses none of its compute
  kernels.
- **The encoding.** The scan emits `beacon.nd`, because `NdSourceExec`, the
  plugin ABI and the wire use it. `nd_arrow.array` has the same storage with
  a different field order and an optional `attrs` field. A later change can
  move the execution nodes to `nd_arrow.array`. Then `output.rs` is the only
  file of the scan that changes.
- **The CF decode.** `decode_cf` and `decode_cf_time` differ from
  `compat.rs`:

  | Rule | `compat.rs` today | nd-arrow-array |
  |---|---|---|
  | Time | Timestamp, nanosecond | Timestamp, microsecond |
  | Packed values | Always `Float64` | The type of `scale_factor`, else `Float64` |
  | `missing_value`, valid range | Not applied | Applied |

  So that change gives new column types and new nulls. It is a later design.
  It replaces the bridge with a native `NetcdfDataset` that reads raw values
  and decodes with these kernels.

## 10. Order of work

Each step is one PR.

1. `beacon-nd-dataset`, with its tests. The workspace gets the
   `nd-arrow-array` dependency and the two members.
2. `beacon-nd-scan`, with its tests on `MemoryDataset`.
3. NetCDF: the bridge, `NetcdfOpener`, the format switch, the removal of
   `scan/` and `open.rs`, the docs and the changelog.

The first implementation plan covers step 1.

Docs for step 3, in `docs/docs/2.1.0-rc3/formats/netcdf.md`: `COUNT(*)` is
supported again. `CHANGELOG.md` `[Unreleased]`: the same, under **Changed**.

## 11. Risks

1. **Two models for a time.** `beacon-nd-array` and `beacon-nd-dataset` both
   exist until the last format moves. The bridge keeps them in step.
2. **The ported rules.** `default_grid`, the chunk grid and the prune are
   ports. A difference changes query results. The ported tests and the schema
   pin guard this.
3. **One read per variable.** A chunk of N variables is N `read` calls. They
   run one after the other, as today.
4. **The experimental morsel API.** Two files name it.
5. **An external crate in the core path.** `nd-arrow-array` has a fixed rev.
   The user owns the crate.
6. **`COUNT(*)` semantics.** The legacy engine counts the rows of the largest
   variable. The framework counts the rows of `SELECT *`. They can differ for
   a file whose default grid is not the largest variable.

## 12. Left to the builder

- Private names, and the split of a module that grows too large.
- The exact form of the limit hand-off in the execution nodes (section 7.3).
- Whether `validate` runs at open in release builds, if the timing check
  shows no cost.
- The commit order inside each step.
