# A shared ND framework: the dataset contract and the scan

Date: 2026-11-23. Status: draft, for review (revision 2).
Branch: `feature/nd-framework`, from `origin/main`.

The branch is on `main`, because the framework does not need the NetCDF
refactor (user, 2026-11-23). The sources that this design ports are on
`origin/features/netcdf-reader-refactor`: the folder
`beacon-arrow-netcdf/src/datafusion/scan/` and the designs of 2026-10-14 and
2026-11-22. Read them from that branch.

Revision 2: the variable is a trait that also reads. No format moves in this
design, and no bridge to `beacon-nd-array` exists (user, 2026-11-23).

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

This design builds the framework only. No format uses it yet. Each format
moves in its own later design.

## 2. Goals

- Two traits for a format: `NdDataset` lists what a file has, and
  `NdVariable` describes one variable and reads it.
- One trait, `NdFileOpener`, that a format implements to open a file.
- One `FileSource`, one morsel planner, one chunk grid, one prune, one output
  step and one statistics function for all ND formats.
- Each concern has one module. Each module has tests.
- The framework has full tests with no file format. A dataset in memory
  stands in for a file.
- The scan gives the same rows as the NetCDF scan of today for the same
  data, so NetCDF can move later with no change of results. One exception:
  the prune of today can lose rows behind a cast (section 7.2 "Prune"). The
  new scan keeps those rows, and it prunes for more predicates.

## 3. Non-goals

- The move of any format: NetCDF, HDF5, Zarr, Icechunk, GeoTIFF, Atlas, the
  plugin formats. No format crate changes.
- A bridge from `beacon_nd_array::Dataset` to `NdDataset`.
- Ragged datasets. The contract does not model them in this version.
- The CF decode. The framework decodes nothing. A format returns decoded
  values from `read`. Section 9 describes the relation to the kernels of
  `nd-arrow-array`.
- A move of `beacon-nd-encoding`, `beacon-nd-execution` and `beacon-nd-array`
  into `beacon-nd/`. One later commit moves these folders (user decision,
  2026-11-23).
- A change of `NdSourceExec` and `NdBroadcastExec`.
- The `beacon.nd` encoding on the wire and in the plugin ABI.
- The writers.
- The rename of `read_dimensions` to `dimensions` in the SQL options. That is
  the design of 2026-11-22. This design gives it the typed value to use.

## 4. Anti-goals

- The new crates must not depend on a format crate or on `beacon-nd-array`.
- `beacon-nd-dataset` must not depend on DataFusion or on `object_store`.
- The framework must not hold a table rule that only one format needs.
- The ported rules must not differ from their source. A difference changes
  query results when a format moves. The prune is the one exception: it is
  not a port, because the source can lose rows (section 7.2 "Prune"). It
  changes only which chunks the scan reads, not the rows of a query.
- No existing crate changes its behavior. This design adds code. It removes
  none.

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
| **A. A dataset trait and a variable trait. The variable reads.** | Each object has one concern. The scan holds a variable and reads it, with no lookup by name. The views pass variables on and forward no read. A format implements two traits. |
| B. A dataset struct and a variable trait. | One checked container. A wide file must build all metadata at each open. |
| C. One dataset trait. The variable metadata is a plain struct. | One trait for a format. Each read finds the variable by name. A format cannot give its own variable type. |

Decision: A (user, 2026-11-23). The user first chose C, then asked for the
dataset metadata as trait methods, then for the variable as a trait. With
the variable as a trait, the read belongs on the variable.

The old objection to A was the five backend types of NetCDF. They exist
because `ArrayBackend<T>` is generic over the value type. With Arrow output,
a format needs one variable type.

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

### 6.4 The source of the scan code

| Approach | Trade-off |
|---|---|
| Write the scan new. | Free design. The rules of the NetCDF scan have tests and timing checks, and a new scan loses them. |
| Move the NetCDF `scan/` folder. | No copy. NetCDF must move in the same change. |
| **Port the NetCDF `scan/` folder. Leave NetCDF as it is.** | The framework gets rules with tests. Two copies exist until NetCDF moves. |

Decision: port (user, 2026-11-23: "focus on building the framework").

## 7. Design

### 7.1 `beacon-nd-dataset`

Dependencies: `nd-arrow-array`, `arrow`, `async-trait`, `thiserror`,
`indexmap`, `tracing`. Dev: `tokio`, `tracing-test`.

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
  dataset.rs      NdDataset, NdVariable, validate
  schema.rs       DatasetSchema, VariableInfo
  memory.rs       MemoryDataset, MemoryVariable
  view.rs         GridSelection, select_grid, select_columns, default_grid
  attributes.rs   attribute_columns
  grid.rs         ChunkGrid
  read.rs         ChunkReader
  statistics.rs   value_range
```

#### The contract (`dataset.rs`, `region.rs`)

```rust
/// A box inside one variable: a start and a length for each axis.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Region { /* start and shape, with the same rank */ }

impl Region {
    /// Panics when `start` and `shape` do not have the same length.
    pub fn new(start: Vec<usize>, shape: Vec<usize>) -> Self;
    pub fn start(&self) -> &[usize];
    pub fn shape(&self) -> &[usize];
    /// The whole variable.
    pub fn full(dimensions: &Dimensions) -> Self;
    /// The element count: the product of `shape`. 1 for rank 0.
    pub fn len(&self) -> usize;
}

/// One variable of an opened file.
#[async_trait]
pub trait NdVariable: Send + Sync + std::fmt::Debug {
    fn name(&self) -> &str;
    /// The Arrow type that `read` returns.
    fn data_type(&self) -> &DataType;
    /// The dimensions, in C order, with their sizes.
    fn dimensions(&self) -> &Dimensions;
    /// The native chunk shape. `None` for a variable without chunks.
    fn chunk_shape(&self) -> Option<&[usize]>;
    fn attributes(&self) -> &Attributes;
    /// Read one region of this variable.
    async fn read(&self, region: &Region) -> Result<NdArrowArray>;
}

/// One opened file of an ND format.
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
    fn variable(&self, name: &str) -> Option<Arc<dyn NdVariable>>;
    /// The dimensions that the reader gave a name. Default: none.
    fn invented_dimensions(&self) -> &[Arc<str>] { &[] }
}
```

`Dimensions`, `Attributes` and `NdArrowArray` are the types of
`nd-arrow-array`.

Rules of the contract:

1. `read` returns an array with the type of `data_type`, with the dimension
   names of the variable and the sizes of `region.shape`.
2. `read` returns missing data as Arrow nulls. The format applies its fill
   value and its decode. The framework knows no fill value.
3. The methods of `NdDataset` and the metadata methods of `NdVariable` do no
   I/O. `open` reads what they need. Only `read` does I/O.
4. A dimension has one size in the whole dataset. A variable that uses a
   dimension has that size on that axis.
5. A variable name is unique. `variable(name)` returns a variable with that
   name. The format makes sure of this, and the framework expects it. A name
   that is not unique is a defect of the format, not a bad file. It gives
   `Error::Invalid`, and `skip_invalid_files` does not skip it.
6. The framework finds a variable by name. The order of `variable_names`
   decides only two ties: the tie of `default_grid` (the first candidate
   wins) and the tie of the grid driver (the last variable wins). So the
   order must be the same at each open of the same file.
7. A region outside the variable gives `Error::InvalidRegion`.
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
    #[error("variable '{variable}': invalid region: {reason}")]
    InvalidRegion { variable: String, reason: String },
    #[error("{dataset}: invalid dataset: {reason}")]
    Invalid { dataset: String, reason: String },
    /// The file is valid, but the contract cannot model it.
    #[error("{dataset}: {reason}")]
    Unsupported { dataset: String, reason: String },
    #[error(transparent)]
    Nd(#[from] nd_arrow_array::NdError),
    #[error(transparent)]
    Arrow(#[from] arrow::error::ArrowError),
    /// The read of one variable failed. It keeps the error of the read.
    #[error("variable '{variable}': {error}")]
    Read { variable: String, error: Box<Error> },
    /// An error of the format: I/O, decode.
    #[error(transparent)]
    Format(Box<dyn std::error::Error + Send + Sync>),
}
```

`OffGrid` is a typed error. It replaces the `anyhow` downcast to
`UnbroadcastableDataset`.

#### Ready metadata (`schema.rs`)

These types save a simple format the metadata code.

- `VariableInfo`: a plain struct with the name, the Arrow type, the
  dimensions, the chunk shape and the attributes. A format variable holds
  one and forwards the five metadata methods to it.
- `DatasetSchema`: the name, the dimensions, the global attributes, the
  invented dimensions, and an `IndexMap<Arc<str>, Arc<dyn NdVariable>>`. Its
  constructor runs the checks of `validate`. It has the methods of
  `NdDataset`. A format dataset holds one and forwards to it.

A wide format implements the trait methods itself and builds a variable only
when a caller asks for it.

#### A dataset in memory (`memory.rs`)

`MemoryVariable` holds a `VariableInfo` and one `NdArrowArray`. `read`
slices the array. It counts its reads. `MemoryDataset` holds a
`DatasetSchema` of such variables. The tests of both crates use them. They
are public, so a format crate can use them in its tests.

#### Views (`view.rs`)

A view is a struct that wraps an `Arc<dyn NdDataset>` and implements
`NdDataset`. It hides variables and dimensions. It returns the variables of
the wrapped dataset as they are, so it forwards no read.

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

/// Show each attribute as a rank-0 variable: `var.attr`, and `.attr` for a
/// global attribute.
pub fn attribute_columns(dataset: Arc<dyn NdDataset>) -> Arc<dyn NdDataset>;
```

`GridSelection` is the `Dimensions` enum of the 2026-11-22 design. It has a
new name, because `nd_arrow_array::Dimensions` is a different type.

`select_grid` runs these steps:

1. Resolve the grid. `Given` gives the list. `PerFile` gives `default_grid`.
   When that is `None`, keep all variables and go to step 4. When step 4
   then removes no dimension, return the dataset as it is.
2. Check that the file has each dimension of the grid. If not, `OffGrid`.
3. Keep the variables whose dimensions are all on the grid. A rank-0 variable
   always stays.
4. Remove the dimensions that no kept variable uses. Keep the file order.
5. Log the removed variables at debug level, one time per file, with the
   dataset name.

Step 4 runs on each path. A count scan has no variable, so its grid is the
dimensions of the dataset after `select_grid`. A dimension that no variable
uses would make `COUNT(*)` larger than the row count of `SELECT *`.

`default_grid` is a port of `Dataset::default_broadcast_dimensions`
(`beacon-nd-array/src/dataset/mod.rs:164`). The port keeps the rule exactly,
also the tie rule and the invented-dimension rule. Port the unit tests of
`default_broadcast_dimensions` and `project_with_dimensions` with the code.

The skip option is not in this crate. The caller sees `OffGrid` and decides.

`attribute_columns` keeps the Beacon table rule that an attribute is a
column. The old readers put attributes in the dataset as fake arrays. In the
contract an attribute lies on its variable, and this view makes the columns.

- It adds a variable for each attribute. Its type is the type of the
  attribute value. Its `read` returns that value. An attribute of
  `nd-arrow-array` always has one value.
- It skips an attribute with a list value, and an attribute whose column
  name is the name of a real variable.
- A format calls it in its `open` when its tables show attributes. The scan
  does not call it.

#### The chunk grid (`grid.rs`) and the chunk reader (`read.rs`)

```rust
/// The chunks of the variables of one dataset. It does no I/O.
pub struct ChunkGrid { /* grid dimensions, chunk shape, chunk counts */ }

impl ChunkGrid {
    /// Cut the grid of `dataset` into chunks of about `batch_size` rows.
    pub fn try_new(dataset: &dyn NdDataset, batch_size: usize) -> Result<Self>;
    /// The dimensions of the grid.
    pub fn dimensions(&self) -> &Dimensions;
    /// The chunk count.
    pub fn len(&self) -> usize;
    /// Chunk `index` in C order. Each chunk is a region of the grid.
    pub fn chunk(&self, index: usize) -> Region;
    /// Each chunk, in C order.
    pub fn chunks(&self) -> impl ExactSizeIterator<Item = Region> + '_;
    /// The smallest region that holds each chunk of `range`.
    pub fn span(&self, range: Range<usize>) -> Option<Region>;
}

/// The variables of one dataset, bound to one chunk grid.
pub struct ChunkReader { /* grid, schema, each variable with its grid axes */ }

impl ChunkReader {
    /// `Invalid` when a variable has a dimension that is not on the grid.
    pub fn try_new(dataset: &dyn NdDataset, grid: ChunkGrid) -> Result<Self>;
    pub fn grid(&self) -> &ChunkGrid;
    /// One nullable field for each variable, in `variable_names()` order.
    pub fn schema(&self) -> &SchemaRef;
    /// Read each variable in `chunk` into one un-broadcast batch.
    pub async fn read(&self, chunk: &Region) -> Result<NdRecordBatch>;
}
```

The reader finds each variable one time for each file. It checks rule 1 of
the contract after each read: the array must have the shape of its region.
A read error becomes `Error::Read`, which names the variable and keeps the
source error.

The rules are the rules of `scan/chunks.rs` today. Port them with their
tests:

- The grid spans the dimensions that the variables of `dataset` use.
- A variable on all grid dimensions drives the grid. With more than one, the
  last one in the order of `variable_names` wins. Each such variable has the
  same set of dimensions, so only its axis order and its native chunk shape
  can differ. The grid takes both from the winner. With none, the
  grid is the cross-product of the used dimensions.
- The grid holds no chunk list. It holds the chunk shape and the chunk count
  of each axis, and `chunk(index)` computes one chunk. So a grid with many
  chunks costs no memory, and the planner keeps only chunk numbers.
- A grid of at most `batch_size` rows is one chunk, also with native chunks.
- Else the native chunk shape of the driver is the chunk shape. Else the
  chunk fills from the last axis up to `batch_size` rows.
- A dataset with no variable has a grid of the dataset dimensions. This case
  is the count scan of section 7.3.
- `ChunkGrid::try_new` never returns a chunk of zero rows. A grid with a
  dimension of size 0 has no chunks.

`ChunkReader::read` reads the variables one after the other, as `FileScan::read`
does today. A coordinate is read one time per chunk. Reads at the same time
on one file handle are a later change, with its own timing check.

#### Statistics (`statistics.rs`)

```rust
/// The minimum and maximum of a rank-0 or rank-1 variable. `None` for
/// other ranks, for an empty variable and for a type with no order.
pub async fn value_range(variable: &dyn NdVariable)
    -> Result<Option<(ArrayRef, ArrayRef)>>;
```

Each result is an array of length 1. The function reads the variable and uses
the Arrow `min` and `max` kernels.

### 7.2 `beacon-nd-scan`

Dependencies: `beacon-nd-dataset`, `beacon-nd-encoding`,
`beacon-datafusion-ext` (`scan_adapt`, type widening), `arrow`, `datafusion`,
`datafusion-datasource`, `object_store`, `async-trait`, `futures`, `tracing`.
Dev: `beacon-nd-execution`, `tokio`. The types of `nd-arrow-array` come
through `beacon-nd-dataset`.

```
beacon-nd-scan/src/
  lib.rs          module list, re-exports
  error.rs        to_datafusion: the map from a dataset error
  opener.rs       NdFileOpener, NdScanOptions, open_for_read
  source.rs       NdSource: the FileSource; encoded_table_schema
  slices.rs       NdSlice, slice_files, spread_files
  limit.rs        RowBudget: the row limit of one partition
  planner.rs      NdMorselizer, planner states, ChunkMorsel
  prune.rs        the predicate on the coordinate values, the chunk test
  output.rs       encode, adapt to the projected schema
  schema.rs       file_schema: the Arrow schema of one file
  statistics.rs   file_statistics: DataFusion statistics of one file; keeps_values
  metrics.rs      ScanMetrics
  tests/
    fixtures.rs   MemoryOpener and fixtures, for tests only
    scan.rs       end-to-end tests on MemoryDataset
```

Only `planner.rs` and `source.rs` name DataFusion morsel types.

Each module is private. The crate root exports the public items, so each
item has one public path: `NdFileOpener`, `NdScanOptions`, `open_for_read`,
`NdSource`, `encoded_table_schema`, `file_schema`, `file_statistics` and
`to_datafusion`.

`planner.rs`, `slices.rs`, `output.rs` and `source.rs` are ports of the files
in `beacon-arrow-netcdf/src/datafusion/scan/`. `prune.rs` is new. The NetCDF
crate does not change.

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
     This is the order of the NetCDF `open.rs` today for a ragged file.
   - Any other error with `skip_invalid_files`: log a warning, return `None`.
2. `select_grid`. On `OffGrid` with `skip_unbroadcastable`: log a warning,
   return `None`.
3. `validate`, in debug builds.

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

The table schema holds the `beacon.nd` encoded file schema.
`encoded_table_schema(file_schema)` builds it. `create_morselizer` refuses a
table schema with a partition column.

`try_pushdown_projection` accepts a projection only when each expression is
a plain column.

The limit of the planner comes from `FileScanConfig::limit`. DataFusion sets
it only when no filter stays above the scan, and the scan answers
`PushedDown::No`, so a `FilterExec` stops a limit. A caller that sets the
limit by hand must make sure of the same.

#### Slices (`slices.rs`)

A port of `scan/slices.rs`, with the name `NdSlice`. These rules do not
change: return `None` with an ordering or `target <= 1`; no slices with a
row limit; never a slice of a slice; exactly `target` groups; never a byte
range.

The slice count uses the size of each file. DataFusion shares the unopened
entries between sibling partitions, so a partition that ends early takes the
next entry. This balance needs more entries than partitions.

- The aim is `A = 4 * target` entries of equal size.
- A file of `size` bytes gets `round(size * A / total)` slices, where `total`
  is the size of all unsliced files.
- The count is at least 1 and at most `A`. A count of 1 leaves the file whole.
- When `total` is 0, each file gets `ceil(A / files)` slices.

`NdSource::reorder_files` sorts the shared queue by weight, the largest entry
first. The weight of an entry is the size of its file divided by its slice
count. The large entries start first and the small entries fill the gaps at
the end.

The cut comes before the prune and does not use the filter. A slice with no
kept chunk costs one open of the file and ends.

#### The planner (`planner.rs`)

A port of `scan/morsel.rs`. The states do not change:

```text
NdMorselizer::plan_file(file)                  CPU only
   file.range.is_some()  -> internal error
   otherwise             -> Unopened{file}

Unopened.plan()
   the limit is reached  -> Ok(None)
   otherwise             -> a plan with the open future

open future  [I/O]
   open_for_read                     None -> Skipped
   select_columns(projected file columns)
   the query has file columns and
   the file has none of them         -> Done
   ChunkGrid::try_new                for a count scan: section 7.3
   ChunkReader::try_new, FileOutput::try_new
   chunks = the slice range of the chunk numbers of the grid
   masks  = chunk_masks(..)          only with a predicate and a variable
   drop the chunks that is_pruned accepts; count them
   no chunk left                     -> Done
   otherwise                         -> Read{file, next: 0, ready: None}

Read{file, next, ready}.plan()                 CPU only
   the limit is reached and ready is None -> Ok(None)
   morsel = ready.map(ChunkMorsel)
   io     = (next < chunks.len() && limit not reached).then(read future)
   neither -> Ok(None)

read future(k)  [I/O]
   reader.read(chunk k)
   -> Read{file, next: k+1, ready: Some(batch)}

ChunkMorsel::into_stream     CPU: output.rs
```

Invariants, as today:

1. `plan_file` and `plan()` do no I/O. A morsel does no I/O.
2. No empty `MorselPlan`.
3. One I/O per partition, with a read-ahead of one chunk.
4. The grid is the same in each slice. The slice cut comes before the prune.
5. Only slice 0 counts a skipped file.

Each error gets the context `"{format_name} file {path}"`, plus the slice and
the chunk, as today.

#### Prune (`prune.rs`)

The prune is not a port. It evaluates the predicate on the coordinate values
(user decision, 2026-11-23). The source collects bounds from the predicate
and compares them by hand.

- `chunk_masks(dataset, predicate, table_schema, grid, span) -> Masks`:
  1. Take the snapshot of the predicate, for a dynamic filter.
  2. Split the predicate into its conjuncts.
  3. Keep a conjunct when each of its columns is in `table_schema`, and each
     column that the file has is a rank-0 variable, or a rank-1 variable on
     one and the same dimension. A rank-0 value is repeated on each index of
     that dimension. Skip a conjunct with a volatile function, with a column of a
     nested type, or with a dimension of size 0.
  4. Read each variable of the conjunct in its file type, on the span of the
     slice. The span is the smallest region of the grid that holds each chunk
     of the slice (`ChunkGrid::span`). Each slice reads its own part. So the
     slices of a file with one dimension read each value of a variable one
     time in total. On a grid with more dimensions, a span holds each inner
     axis in full, so each slice reads the inner coordinates in full. A read
     with a wrong length gives no mask.
  5. Rewrite the conjunct with the DataFusion `DefaultPhysicalExprAdapter`,
     from `table_schema` to the file types of the coordinates.
  6. Evaluate the conjunct on a record batch of the coordinates. A null
     result becomes `false`.
  7. Combine the masks of one dimension with `AND`.

  The result is `Masks::NoRow` or `Masks::Keep(masks)`. A mask is a bit
  buffer with no null, so the type holds the rule "null is false". The first
  conjunct that rejects the file gives `NoRow` at once: the prune does not
  read the coordinates of the conjuncts after it.
- `is_pruned(masks, grid, chunk)`: true when, on one axis, the mask has no
  `true` in the range of the chunk, or when a conjunct rejects the file.

`table_schema` holds the logical type of each projected column. The source
computes it one time for each partition (`schema.rs` `logical_projection`), and
the planner keeps it as `ScanContext::filter_schema`. The encoded schema that
the scan emits is `ScanContext::output_schema`. A conjunct with a column that
is not in `table_schema` gives no mask. A projected field that is not
`beacon.nd` encoded gives a plan error when the scan starts.

The source also decides one time what each file reads:
`ReadPlan::Columns(names)` for a scan with columns, and
`ReadPlan::Count { grid_columns }` for a scan with no column.

Pruning only skips work. The `FilterExec` above the scan applies the
predicate again.

**The safety rule.** A mask must never be `false` for a row that the
predicate accepts. A pruned chunk loses rows with no error. With a doubt,
the prune builds no mask.

Why the evaluation keeps the rule:

- **The same expression.** The adapter puts the cast from the file type to
  the table type around each column. That is the cast of the scan. So the
  prune computes what the filter computes, on the same values. No rule
  about casts is necessary: `CAST(lat AS INT) = 5` is evaluated on
  `lat = 5.5`.
- **A column that the file does not have.** The scan gives nulls for it, and
  the adapter gives a null literal.
- **A whole file.** A conjunct with no rank-1 variable has one value for the
  file. Its columns are rank-0 variables, such as attribute columns, and
  columns that the file does not have. When that value is not `true`, the
  scan reads no chunk of the file. So a filter on a global attribute skips
  each file that fails it.
- **An error gives no mask.** The adapter casts strictly, and the scan
  output casts leniently. A value that does not fit gives an error in the
  prune and a null in the scan. The error gives no mask, which is the safe
  side. Each other error of the rewrite or the evaluation also gives no
  mask.
- **Each operator.** An `OR`, an `IN`, a `NOT`, a function and each type that
  is not nested are evaluated. A conjunct with columns on two dimensions
  gives no mask.
- **A nested type gives no mask.** The adapter casts a struct by field name,
  and the scan output does not. So the two casts can differ.
- **A null** gives `false`. **A NaN** follows the Arrow total order, as the
  `FilterExec` does.

One cost: a rank-1 variable is not always a small coordinate. In a file with
one dimension, each data variable has rank 1. A filter on such a variable
reads it one time for the prune, and the scan then reads the kept chunks
again.

The planner keeps the numbers of the kept chunks in a list. So a slice with
very many chunks still costs memory in the scan, 8 bytes for each chunk.

`beacon-nd-array/src/arrow/pushdown_filter.rs` on `main` has the cast defect.
The HDF5, Zarr and GeoTIFF scans use it today.

#### Output (`output.rs`)

For each chunk:

1. Convert the `nd_arrow_array::NdRecordBatch` to a
   `beacon_nd_encoding::NdRecordBatch`. Each column keeps its values buffer;
   only the wrapper types change.
2. `encode_nd_record_batch`: one `beacon.nd` row.
3. Adapt to the projected schema with
   `beacon_datafusion_ext::scan_adapt::BatchAdapter`. A column that the file
   does not have becomes null. Do not use the DataFusion
   `BatchAdapterFactory`: it fails on a source that is wider than the target
   (#675).

A chunk of a count scan (`COUNT(*)`) has no column, so these steps do not
fit. The encoding has a carrier for this case already, and the scan uses it:
a batch with zero columns and `row_count = chunk rows`.
`decode_nd_record_batch_row` turns it into a `row` axis of that size.

**No partition columns** (user decision, 2026-11-23). An ND scan does not
support partition columns for now. This keeps the output, the prune and the
count scan simple. `create_morselizer` gives a "not implemented" error for a
table schema with a partition column.

This crate converts to `beacon.nd` at this one place. When `NdSourceExec`
moves to `nd_arrow.array`, only this file changes.

#### Schema and statistics (`schema.rs`, `statistics.rs`)

- `file_schema(dataset) -> Schema`: one field per variable, sorted by name.
  The sort is here, not in the readers.
- `file_statistics(dataset, schema) -> Statistics`: `schema` is the logical
  table schema. `value_range` for each rank-0 and rank-1 variable; unknown
  for the others; no row count. This is the rule of
  `statistics_for_dataset` of the NetCDF crate today, with one addition: the
  minimum and the maximum have the type of the table field. When the step
  from the variable type to the field type does not keep values
  (`keeps_values`), the column has unknown statistics.

A format calls `file_statistics` on a dataset from `opener.open`, with no
grid and no skip, as `open_whole` does today. So a column that the grid
removes keeps its statistics.

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

**Count.** A query with no column (`COUNT(*)`) runs the same planner. The chunk grid comes from the file columns
of the table: `ChunkGrid::try_new(select_columns(dataset, table file
columns))`. That is the grid of `SELECT *`, also when the file has
variables that the table does not have. The read uses
`select_columns(dataset, &[])`, so the reader reads nothing and returns a
batch with no columns and the row count of the chunk. `output.rs` puts that
count in the carrier of section 7.2. So `COUNT(*)` is the row count of
`SELECT *`, and it reads no data. A file with none of the file columns of
the table gives no row. The planner has no count state.

A format that does not want a count, as NetCDF today, refuses the query in
its own `FileFormat`, before it builds the source.

**Limit.** Today no ND node hands a limit to the scan. The scan stops when
the consumer drops the stream. Keep that. In addition:

- With `FileScanConfig::limit` set, the planner counts the rows of the reads
  it plans, per partition. It plans no more reads after the count reaches the
  limit, and it opens no more files.
- The `FileStream` limit counts encoded rows, so it counts chunks. Each chunk
  has at least one row (section 7.1), so that limit never cuts rows that the
  query needs. For the zero-column carrier it counts real rows, which is
  exact.
- No ND node hands a `fetch` to the scan in this version. A caller that
  knows the limit sets `FileScanConfig::limit`. The hand-off through
  `NdSourceExec` and `NdBroadcastExec` is a later change.

A limit reaches the scan only with no filter between, so chunk rows are
exact.

### 7.4 How the formats fit later

This section is a check of the contract, not a work item.

| Format | What it writes |
|---|---|
| NetCDF | `NetcdfOpener`, a dataset and one variable type on `oxcdf`. Its `open` calls `attribute_columns`. It removes its `scan/` folder and `open.rs`. |
| HDF5, GeoTIFF | The same shape. The scan has no partition columns, so a format that has them today must add them to the scan first, or lose them at its move. Ragged HDF5 needs its own design. |
| Zarr, Icechunk | One leaf group is one file, as today. The dataset builds a variable on demand. The Icechunk opener holds its version storage. |
| Atlas | Needs more than one dataset per container, and a prune on footer statistics. Extend `NdFileOpener` then: `open` returns a list, and a variable can report its statistics. |
| Plugins | Stay on the stream path. A later ABI version can carry the two traits. |

What the `FileFormat` of a format writes:

| Method | Calls |
|---|---|
| `infer_schema` | `open_for_read`, then `file_schema` for each file, then a merge with the type-widening rule of the session. |
| `infer_stats` | `opener.open` with no grid and no skip, then `file_statistics`. `to_datafusion` maps a dataset error. |
| `create_physical_plan` | `encoded_table_schema`, then `NdSource::new(..).with_projection(..)`, then `nd_scan_plan`. |

## 8. Tests

### 8.1 `beacon-nd-dataset`

- `Region`: `full`, `len`, rank 0.
- `DatasetSchema` and `validate`: a size conflict, a duplicate name, a
  variable whose name differs from its key, a chunk shape of the wrong rank.
- `MemoryVariable`: a region read, a rank-0 read, a region outside the
  variable, the read count.
- `select_grid`: `Given` on and off the grid; an empty `Given` list;
  `PerFile` with a narrower grid and with a broadcast-safe file; the debug
  log. The ported tests of `default_broadcast_dimensions`.
- `select_columns`: known names, an unknown name, no names.
- `attribute_columns`: a variable attribute, a global attribute, an
  attribute with a list value, a name clash with a real variable.
- `ChunkGrid`: the ported tests of `scan/chunks.rs`; the cross-product grid;
  the driver tie; a dataset with no variable; a dimension of size 0.
- `ChunkReader`: each variable is read one time, on its own axes.
- `value_range`: rank 0, rank 1, rank 2, all nulls, a string type.

### 8.2 `beacon-nd-scan`

All tests use `MemoryDataset` behind a test opener.

- Unit: the ported tests of `scan/morsel.rs`, `slices.rs`, `output.rs` and
  `source.rs`, and the tests of the prune.
- No planner state gives an empty `MorselPlan`: a skipped file, a file with
  none of the columns, a fully pruned file, a count scan.
- Count: `COUNT(*)` equals the row count of `SELECT *`, and the variables
  count zero reads. With slices, each row is counted one time.
- Partition columns: a table schema with a partition column gives an error.
- Limit: with a limit, the planner reads only the first chunks.
- End to end through `NdSourceExec` and `NdBroadcastExec`: one file on 1 and
  on 4 partitions, with and without a filter; a dropped stream stops the
  reads; a plan that runs again after `reset_plan_states`; a read after a
  `RepartitionExec(RoundRobinBatch)`.
- The skips: an invalid file, a file off the grid and an `Unsupported` file,
  with and without the options.
- `file_schema` and `file_statistics`.

### 8.3 Equal rules

The ports must give the results of their sources. For `default_grid` and the
chunk grid, each ported test keeps the inputs and the expected numbers of the
source test. Do not change an expected number to make a ported test pass.

The prune has its own tests for the safety rule of section 7.2: a cast that
truncates, a table type narrower and wider than the file type, a literal of
another type, a null, a NaN, a cast that fails, a column that the file does
not have, an `OR`, an `IN` and a volatile function. Each test plans its
predicate with the type coercion of a query.

## 9. The relation to nd-arrow-array

- **Now.** The contract uses its types: `NdArrowArray`, `Dimensions`,
  `Attributes`, `NdRecordBatch`. The framework uses none of its compute
  kernels.
- **The encoding.** The scan emits `beacon.nd`, because `NdSourceExec`, the
  plugin ABI and the wire use it. `nd_arrow.array` has the same storage with
  a different field order and an optional `attrs` field. A later change can
  move the execution nodes to `nd_arrow.array`. Then `output.rs` is the only
  file of the scan that changes.
- **The CF decode.** A format decodes in `read`. It can use `decode_cf` and
  `decode_cf_time` of `nd-arrow-array`. These differ from the NetCDF
  `compat.rs` of today:

  | Rule | `compat.rs` today | nd-arrow-array |
  |---|---|---|
  | Time | Timestamp, nanosecond | Timestamp, microsecond |
  | Packed values | Always `Float64` | The type of `scale_factor`, else `Float64` |
  | `missing_value`, valid range | Not applied | Applied |

  So a format that moves to these kernels gets new column types and new
  nulls. The design of the NetCDF move decides that.

## 10. Order of work

Each step is one PR.

1. `beacon-nd-dataset`, with its tests. The workspace gets the
   `nd-arrow-array` dependency and the member.
2. `beacon-nd-scan`, with its tests on `MemoryDataset`.

The first implementation plan covers step 1. Neither step changes user
behavior, so the docs and the changelog do not change.

## 11. Risks

1. **No format proves the contract.** The framework has tests on a dataset
   in memory only. A real format can show a gap. Section 7.4 is the check
   against that, and the NetCDF move is the first proof.
2. **Two copies of the scan.** The NetCDF `scan/` folder and
   `beacon-nd-scan` hold the same rules until NetCDF moves. A fix in one must
   go in the other.
3. **The ported rules.** `default_grid` and the chunk grid are ports. A difference changes query results at the move of a format.
   Section 8.3 guards this.
4. **One read per variable.** A chunk of N variables is N `read` calls. They
   run one after the other, as today.
5. **The experimental morsel API.** Two files name it.
6. **An external crate in the core path.** `nd-arrow-array` has a fixed rev.
   The user owns the crate.
7. **`COUNT(*)` semantics.** The legacy engine counts the rows of the largest
   variable. The framework counts the rows of `SELECT *`. They can differ for
   a file whose default grid is not the largest variable.

## 12. Left to the builder

- Private names, and the split of a module that grows too large.
- Whether `validate` runs at open in release builds.
- The commit order inside each step.
