---
description: Query HDF5 files with read_hdf5. Beacon reads HDF5 through the netCDF-c library, so the netCDF data model and CF decoding apply unchanged.
---

# HDF5

```sql
SELECT * FROM read_hdf5('experiments/**/*.h5') LIMIT 10;
```

Beacon recognizes `.h5` and `.hdf5`. It finds them in the dataset store automatically.

## How Beacon reads HDF5

A netCDF-4 file **is** an HDF5 file, and the netCDF-c library also opens plain HDF5. Beacon uses
that one path for both. There is no separate HDF5 reader.

This has a direct consequence: **HDF5 behaves exactly like
[NetCDF](/docs/2.0.0-rc2/formats/netcdf)**. The same data model, the same
[array to table mapping](/docs/2.0.0-rc2/arrays-to-tables), the same
[CF decoding](/docs/2.0.0-rc2/cf-decoding) and the same attribute columns.

An HDF5 dataset reads as a variable. An HDF5 attribute reads as a `<variable>.<attribute>` column.

::: warning What does not map
The netCDF data model is smaller than the HDF5 model. Beacon cannot read a file that uses these
HDF5 features:

- compound (struct) datatypes
- variable-length and opaque types
- references and region references
- nested groups beyond what netCDF-4 allows

Beacon reports an error for those files. It does not read part of a file.
:::

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
grid, and Beacon returns a variable only if the list holds all of that variable's dimensions.

```sql
SELECT * FROM read_hdf5(['experiments/**/*.h5'], ['sample', 'channel']);
```

See [Arrays to tables](/docs/2.0.0-rc2/arrays-to-tables#the-dimensions-argument).

## Inspect the schema

Check the columns and the types before you write a query:

```sql
SELECT * FROM read_hdf5_schema('experiments/**/*.h5');
```

[Inspect a schema](/docs/2.0.0-rc2/formats/inspect-a-schema) compares the `_schema` functions,
`SUMMARIZE`, `DESCRIBE` and `LIMIT 0`, and says what each one costs.

## As an external table

```sql
CREATE EXTERNAL TABLE experiments
STORED AS HDF5
LOCATION 'experiments/';
```

`STORED AS H5` is accepted as a synonym.

## On object storage

**HDF5 supports anonymous access only**, for the same reason as NetCDF: the native reader opens a
file by URL and does not go through the credential chain. For a public bucket, set
`AWS_SKIP_SIGNATURE=true`.

For a private bucket, run the server with that bucket as its dataset store. See
[Object Storage](/docs/2.0.0-rc2/data-sources/object-storage).

## As a query output

`COPY TO ... STORED AS HDF5` uses the netCDF-4 writer. The result is a netCDF-4 file with an HDF5
extension. Every HDF5 reader opens it.

## See also

- [NetCDF](/docs/2.0.0-rc2/formats/netcdf): the same reader, with the full detail
- [Arrays to tables](/docs/2.0.0-rc2/arrays-to-tables): the row count and the grid rule
- [CF decoding](/docs/2.0.0-rc2/cf-decoding): units, packing and fill values
