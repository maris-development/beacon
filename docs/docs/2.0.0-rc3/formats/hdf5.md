---
description: Query HDF5 files with read_hdf5. Beacon reads HDF5 with a pure-Rust reader that adds nested groups, compound datasets and object storage. The netCDF-c library is the fallback.
---

# HDF5

```sql
SELECT * FROM read_hdf5('experiments/**/*.h5') LIMIT 10;
```

Beacon recognizes `.h5` and `.hdf5`. It finds them in the dataset store automatically.

## Two readers

A netCDF-4 file **is** an HDF5 file, and the netCDF-c library also opens plain HDF5. Beacon reads
HDF5 through the pure-Rust reader by default. Beacon reads it through that library when you set the
Rust reader to false. HDF5 behaves exactly like
[NetCDF](/docs/2.0.0-rc3/formats/netcdf): the same data model, the same
[array to table mapping](/docs/2.0.0-rc3/arrays-to-tables), the same
[CF decoding](/docs/2.0.0-rc3/cf-decoding) and the same attribute columns.

The **pure-Rust HDF5 reader** is the default. It reads the same files and gives the same answer for
a netCDF-4 file, and it adds five things over netCDF-c:

| | netCDF-c | Pure-Rust reader (default) |
| --- | --- | --- |
| Nested groups | Root group only | Every group |
| Compound datasets | Not read | One column for each member |
| Object storage | Anonymous access only | Full credential chain, no local copy |
| [File statistics](/docs/2.0.0-rc3/internals/file-statistics) | None | Per-file column ranges |
| Concurrent scans | One file at a time | In parallel |
| Writes | netCDF-c | netCDF-c |

The flag is on by default, and it is separate from `BEACON_NETCDF_USE_RUST_READER`, so you move one
format at a time. Set it for one table instead of the whole server:

```sql
CREATE EXTERNAL TABLE experiments
STORED AS HDF5
LOCATION 'experiments/'
OPTIONS ('use_rust_reader' 'false');
```

## How a dataset becomes a column

An HDF5 dataset reads as a variable. An HDF5 attribute reads as a `<dataset>.<attribute>` column,
and a file attribute as `.<attribute>`.

The pure-Rust reader adds two more shapes. Both use `/`, so quote the column name in SQL.

### A dataset inside a group

A dataset outside the root group keeps its path as its column name. Two groups can then hold the
same name without a collision.

```text
/                       .title
  station_id            station_id
  observations/         observations/.units
    temperature         observations/temperature
    qc/
      flag              observations/qc/flag
```

```sql
SELECT station_id, "observations/temperature", "observations/qc/flag"
FROM read_hdf5('nested.h5');
```

A group attribute takes the group's path and the leading dot: `observations/.units`.

### A compound dataset

An HDF5 compound dataset holds a record in each element, the way a table row holds columns. Each
member becomes its own column, named `<dataset>/<member>`. The columns share the dataset's shape,
so a query joins them by row the way it joins two ordinary datasets.

```sql
SELECT "measurements/station", "measurements/depth", "measurements/label"
FROM read_hdf5('compound.h5');
```

Beacon reads a member of a fixed-width numeric or string type. It skips a member that holds a
pointer into a heap — a variable-length string, a nested compound, an array — and logs the dataset
and every member type. A compound whose members are all of that kind gives no columns, and the log
names it.

## Read one file, a glob or a list

```sql
-- one file
SELECT * FROM read_hdf5('experiments/run-01.h5');

-- every level below a directory
SELECT * FROM read_hdf5('experiments/**/*.h5');

-- an explicit list
SELECT * FROM read_hdf5(['a.h5', 'b.hdf5']);
```

## Select the dimensions

Like NetCDF, `read_hdf5` takes an optional second argument: the dimensions to read. It sets the
grid, and Beacon returns a dataset only if the list holds all of that dataset's dimensions.

```sql
SELECT * FROM read_hdf5(['experiments/**/*.h5'], ['sample', 'channel']);
```

A netCDF-4 file names its axes with HDF5 *dimension scales*, and Beacon uses those names.

A plain HDF5 file attaches no scales, so netCDF invents a dimension for every axis. Beacon names
each one by its length, `phony_len_4` for an axis of 4 elements, over the whole file. Two datasets
in two different groups therefore share an axis of one length and broadcast together, which is what
lets one query read a payload in the root group and the description of each channel in another:

```sql
SELECT "values", "pipe/len", "pos/x"
FROM read_hdf5('folder/test.hdf5');
```

Two exceptions keep the name the reader gave them, `phony_dim_7` and the like: an axis of zero
elements, because netCDF has no fixed dimension of that length, and an axis that can grow, because
two growable axes of one length are equal by accident rather than by design.

Beacon merges two axes of one length even when they count different things. That is the only rule
available, because a plain HDF5 file records nothing else about an axis. Set
`unify_phony_dimensions` to `false` on the table, or `BEACON_HDF5_UNIFY_PHONY_DIMENSIONS=false` on
the server, to keep one dimension per length per group instead.

A file that names no dimension also picks its `SELECT *` grid by volume rather than by variable
count, so a query lands on the payload rather than on the metadata around it. See
[Arrays to tables](/docs/2.0.0-rc3/arrays-to-tables#a-file-that-names-no-dimension).

See [Arrays to tables](/docs/2.0.0-rc3/arrays-to-tables#the-dimensions-argument).

## Inspect the schema

Check the columns and the types before you write a query:

```sql
SELECT * FROM read_hdf5_schema('experiments/**/*.h5');
```

[Inspect a schema](/docs/2.0.0-rc3/formats/inspect-a-schema) compares the `_schema` functions,
`SUMMARIZE`, `DESCRIBE` and `LIMIT 0`, and says what each one costs.

## As an external table

```sql
CREATE EXTERNAL TABLE experiments
STORED AS HDF5
LOCATION 'experiments/';
```

`STORED AS H5` is accepted as a synonym.

### `OPTIONS`

`STORED AS HDF5` reads five keys. `STORED AS H5` reads the same five:

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `read_dimensions` | List of dimension names | The default grid of each file | The dimensions the table reads. Beacon returns a dataset only if the list holds every dimension of that dataset. |
| `use_rust_reader` | Boolean | `true` (`BEACON_HDF5_USE_RUST_READER`) | Read with the pure-Rust reader. Set it to `false` to read with the netCDF-C library. That library reads no nested group and no compound dataset, and it needs anonymous access to a bucket. |
| `enable_statistics` | Boolean | `true` (`BEACON_HDF5_ENABLE_STATISTICS`) | Accepted, and without effect today. Beacon rejects a value that is not a boolean, and then reads the server setting alone: `ANALYZE FILES` resolves a format per file, not per table. Set `BEACON_HDF5_ENABLE_STATISTICS` to turn the column ranges off. |
| `unify_phony_dimensions` | Boolean | `true` (`BEACON_HDF5_UNIFY_PHONY_DIMENSIONS`) | Give every unnamed axis one name per length, over the whole file, so two groups broadcast together. Set it to `false` to keep one dimension per length per group. |
| `convention` | `none` | `none` (`BEACON_HDF5_CONVENTION`) | The vendor layout the table reads on top of the container. |

The pure-Rust reader applies `unify_phony_dimensions` and `convention`. A table with
`use_rust_reader` set to `false` ignores both.

```sql
CREATE EXTERNAL TABLE das
STORED AS HDF5
LOCATION 'acquisition/*.hdf5'
OPTIONS ('unify_phony_dimensions' 'false')
```

See [`OPTIONS`](/docs/2.0.0-rc3/sql/create-external-table#options) for the rules that hold for every key.

## On object storage

On the netCDF-c reader, **HDF5 supports anonymous access only**, for the same reason as NetCDF:
that library opens a file by URL and does not go through the credential chain. For a public bucket,
set `AWS_SKIP_SIGNATURE=true`.

The default pure-Rust reader has no such limit. It reads byte ranges through the object store, so a
private S3, GCS or Azure bucket works and no local copy is made. A bucket needs no option at all:

```sql
CREATE EXTERNAL TABLE experiments
STORED AS HDF5
LOCATION 's3://bucket/experiments/';
```

See [Object Storage](/docs/2.0.0-rc3/data-sources/object-storage).

## As a query output

`COPY TO ... STORED AS HDF5` uses the netCDF-4 writer. The result is a netCDF-4 file with an HDF5
extension. Every HDF5 reader opens it. A write always uses netCDF-c, whatever the read flag says.

::: warning What does not map
The pure-Rust reader covers nested groups and compound datasets. It does not read a variable-length
or opaque type, a reference, or a region reference.

On the netCDF-c reader the netCDF data model bounds what Beacon sees. A compound datatype, a
variable-length or opaque type, a reference, and a group outside the root are not read.

One more limit is worth knowing. Beacon drops an attribute narrower than 8 bytes on a file written
with the earliest HDF5 library version — h5py's default — because the reader takes the object
header padding as part of the value. The datasets are unaffected. This is
[an upstream defect](https://github.com/robinskil/oxcdf/issues/1); a netCDF-4 file never meets it.
:::

## See also

- [NetCDF](/docs/2.0.0-rc3/formats/netcdf): the same readers, with the full detail
- [Arrays to tables](/docs/2.0.0-rc3/arrays-to-tables): the row count and the grid rule
- [CF decoding](/docs/2.0.0-rc3/cf-decoding): units, packing and fill values
- [Performance tuning](/docs/2.0.0-rc3/server/performance-tuning#hdf5-pure-rust-reader): when to
  change the reader
