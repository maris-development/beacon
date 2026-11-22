# The `dimensions` option for NetCDF and HDF5

Date: 2026-11-22. Status: draft.

## Intent

A NetCDF or HDF5 table reads the variables on one grid. The option
`read_dimensions` names that grid today. The name says "read these
dimensions", but the option does two things:

1. It sets the grid.
2. It keeps only the variables whose dimensions are all on that grid.

This change:

- renames the option to `dimensions`, the same name as the argument of the
  table functions and the ERDDAP `dimensionsCSV` setting;
- makes the dimension projection one typed step that the file source owns,
  and that runs on each file at open, before the query columns.

## Goals

- One option name, `dimensions`, for NetCDF and HDF5.
- One typed value for the grid. Parse it once, from the options, into the
  format struct.
- One shared function projects each opened file, in a fixed order: first
  the dimensions, then the query columns.
- Schema inference and the scan use the same function, so they cannot
  disagree.
- Clear errors and logs: a typed error for a file off the grid, and a debug
  log that names the variables the grid removes.

## Non-goals

- One grid per table. A table without `dimensions` keeps one grid per file,
  as today (user decision, 2026-11-22). Each file gets the grid of
  `default_broadcast_dimensions`.
- Zarr, GeoTIFF, Atlas, Icechunk and the plugin formats. They keep
  `read_dimensions` until their own follow-up features. The new type is
  shared, so they can use it later.
- The CF decode findings (`missing_value`, the default fill, packed time,
  `int64` time, `_Unsigned`, char arrays). They go in the nd-arrow-array
  change.
- The crawler, the web UI and the server API examples. They keep sending
  `read_dimensions`. The alias accepts it.
- The schema cache. Nothing uses `schema_options_fingerprint` since #641.
  Leave it as it is.

## Anti-goals

- A stored table, a saved crawler definition or a client that sends
  `read_dimensions` must not break.
- A query result must not change. This change is a rename and a
  restructure.
- The scan must not get slower. The dimension step must not read data.

## How it works today

Both formats run the same steps, in different code:

| Step | NetCDF | HDF5 |
| --- | --- | --- |
| Parse the options | `NetcdfOptions::apply_format_options` | `Hdf5FormatFactory::effective_options` (`EffectiveOptions`) |
| Schema of one file | `NetcdfFormat::file_schema` → `open_for_read` | `open::fetch_schema` |
| Open one file in the scan | `scan/morsel.rs` `plan_entry` → `open_for_read` | `Hdf5Opener::open_dataset` |
| Dimension step | `project_read_dimensions_or_skip` | `project_read_dimensions_or_skip` |
| Column step | `scan/output.rs` `FileOutput::resolve` | `FileRead::plan` |

The order is already correct: the dimension step runs before the column
step. The problems are:

- The grid travels as `Option<Vec<String>>` plus a `bool` through the
  format, the source and the opener. `None` means "the grid of each file".
  `Some(vec![])` means "only scalars". The type does not show this.
- The skip check downcasts an `anyhow` error to `UnbroadcastableDataset`.
- The variables that the grid removes are logged at trace level only
  (`resolve_read_dimensions`).
- Each format parses `read_dimensions` and `skip_unbroadcastable` with its
  own code.

## Approaches

| Approach | Trade-off |
| --- | --- |
| A. Project in the reader (`read_arrays`, HDF5 `open_dataset`). | The reader must read all metadata to check the dimensions, so it saves nothing. It puts table rules into readers that other code uses. |
| B. Project in the format plan (`create_physical_plan`). | The plan does not open files. Each file can have different variables. |
| C. Project per chunk, in the morsel. | Too late. The chunk grid depends on the dimension step. |
| **D. Project in the file source, at open, with one shared function.** | Per file, before the chunk grid. The source already holds the options and the projected schema. |

Decision: D.

## Design

### 1. Option names

| Key | Old key, accepted as an alias | Value |
| --- | --- | --- |
| `dimensions` | `read_dimensions` | A list of dimension names, separated by commas. |
| `skip_unbroadcastable` | (no change) | Boolean. Skip a file that does not fit `dimensions`. |

Rules:

- `dimensions` and `read_dimensions` accept the `format.` prefix, as
  `format_option` does today.
- `dimensions` wins when the options hold both. Log a warning that names
  both keys.
- `read_dimensions` logs a deprecation warning that names `dimensions`.
  Log it once per process. The factory has no table identity, and it runs
  `create` many times per table.
- The table functions `read_netcdf` and `read_hdf5` put their `dimensions`
  argument into the `dimensions` key, and their skip argument into
  `skip_unbroadcastable`.
- `Hdf5FormatFactory::create` forwards the options to the NetCDF factory
  for the writer. The NetCDF writer ignores `read_dimensions`, so this gives no
  second warning. Check this in a test.

### 2. The typed value

Put the types in `beacon-nd-array`, in a new file
`src/dataset/file_projection.rs`:

```rust
/// The grid a table reads.
pub enum Dimensions {
    /// Each file reads its own default grid (`default_broadcast_dimensions`).
    PerFile,
    /// The grid the table names. An empty list reads only scalars.
    Given(Vec<String>),
}

/// How one opened file becomes the arrays of a scan.
pub struct FileProjection {
    pub dimensions: Dimensions,
    pub skip_unbroadcastable: bool,
    /// The query columns. `None` keeps every array on the grid.
    pub columns: Option<Vec<String>>,
}

impl FileProjection {
    /// `Ok(None)` when the file does not fit the grid and the skip is set.
    pub fn apply(&self, dataset: AnyDataset) -> Result<Option<AnyDataset>, ProjectionError>;
}

pub enum ProjectionError {
    /// The file does not hold a dimension of the grid, or its variables do
    /// not broadcast on it.
    OffGrid { file: String, missing: Vec<String>, source: anyhow::Error },
    Other(anyhow::Error),
}
```

`apply` runs these steps, in this order:

1. Resolve the grid. `Given` gives the list. `PerFile` gives
   `default_broadcast_dimensions` of this file. When that is `None`, the
   file is already broadcast-safe and step 2 does nothing.
2. A ragged dataset skips steps 3 and 4. A dimension projection is not
   defined for it, as in `AnyDataset::project` today. With `Given`, a
   ragged dataset is an error, as today.
3. Check that the file holds every dimension of the grid. If not, return
   `OffGrid`, or `Ok(None)` when the skip is set. Keep the arrays whose
   dimensions are all on the grid. Remove the dimensions that no kept
   array uses. This is `project_with_dimensions` today.
4. Log the arrays that step 3 removed, at debug level, once per file, with
   the file name.
5. When `columns` is `Some`, keep only the arrays with these names.

`apply` returns `AnyDataset`, because HDF5 reads ragged datasets. NetCDF
refuses a ragged dataset after `apply`, in `open.rs::regular`, as today.

The `OffGrid` error text names the file, the missing dimensions and the
two options: `dimensions` and `skip_unbroadcastable`.

`resolve_read_dimensions`, `project_read_dimensions` and
`project_read_dimensions_or_skip` stay in `beacon-nd-array` for Zarr,
Atlas and Icechunk. Remove them in the follow-up feature that moves the
last of these formats.

### 3. The format and the source

- `NetcdfOptions` and the HDF5 `EffectiveOptions` hold
  `dimensions: Dimensions` in place of `read_dimensions`. They keep
  `skip_unbroadcastable: bool`. The format struct keeps both, and gives
  them to the source in `create_physical_plan`.
- `NetcdfSource` and `Hdf5Source` hold `dimensions: Dimensions` in place
  of the `Option<Vec<String>>`. They keep the skip flag
  (`SkipFiles.unbroadcastable` in NetCDF).
- **NetCDF scan:** `open_for_read` builds a `FileProjection` with the
  columns of the projected schema, and calls `apply`.
  `FileOutput::resolve` no longer filters columns. It only builds the
  adapter, and returns `None` when the file has no wanted array.
- **HDF5 scan:** `Hdf5Opener::open_dataset` builds a `FileProjection` with
  `columns: None`, and calls `apply`. `FileRead::plan` keeps its own column
  filter. It needs the full projected dataset for `COUNT(*)` and to tell
  partition columns apart. Zarr and other formats share `FileRead::plan`.
- **Schema inference, both formats:** build a `FileProjection` with
  `columns: None`, and call `apply`. Inference and the scan then use the
  same rule.

### 4. Schema cache key

`schema_options_fingerprint` returns `None` when the grid is `Given`, as
it does today for `read_dimensions`. Change only the field it reads. Keep
the key of the default read, so `the_schema_cache_key_does_not_change`
still passes.

### 5. Docs

- `docs/docs/2.1.0-rc3/formats/netcdf.md` and `hdf5.md`: the new keys, the
  aliases, and the error text.
- `docs/docs/2.1.0-rc3/arrays-to-tables.md`: use the name `dimensions`.
- `CHANGELOG.md`: the rename and the aliases.
- Fix the stale cache text in the module doc of
  `beacon-arrow-hdf5/src/open.rs`.
- Write the docs in ASD-STE100.

## Tests

- Option parsing, both formats: `dimensions`, `read_dimensions`, both keys
  together, the `format.` prefix, one deprecation warning per process.
- `FileProjection::apply`:
  - `Given`, a file on the grid and a file off the grid, with and without
    the skip;
  - an empty `Given` list;
  - `PerFile`, a file that needs a narrower grid and a file that is
    already broadcast-safe;
  - a ragged dataset, with `PerFile` and with `Given`;
  - a column list;
  - the debug log of the removed arrays.
- End to end, NetCDF and HDF5:
  - a table with `dimensions` gives the same rows as the same table with
    `read_dimensions` today;
  - a file off the grid fails with the new error, and is skipped with
    `skip_unbroadcastable`;
  - `read_netcdf(path, dimensions)` and `read_hdf5(path, dimensions)` give
    the same result as the table option;
  - a table without `dimensions` gives the same rows as today.
- The existing tests that use `read_dimensions` keep passing, through the
  alias.

## Left to the builder

- The exact module and function names, if the names above clash with code
  in `beacon-nd-array`.
- The order of the commits. Recommended: the types and `apply` first, then
  the option parsing, then NetCDF, then HDF5, then the docs.
