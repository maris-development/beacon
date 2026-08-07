---
description: Read an Icechunk repository with read_icechunk. A repository is a Zarr store with versions. Each query reads one version.
---

# Icechunk

```sql
SELECT * FROM read_icechunk('sst/repo');
```

An **Icechunk repository** is a Zarr v3 store with versions. It holds the same arrays, groups and
attributes as a plain [Zarr](/docs/2.0.0-rc2/formats/zarr) store. It adds commits, branches, tags
and snapshots.

Beacon reads a repository through the Zarr reader. The schema, the arrays and the chunk selection
stay the same.

A repository gives two properties that a plain Zarr store does not give:

- **One version for each query.** A writer adds a commit while your query runs. The query still
  reads one version.
- **The same answer each time.** Name a snapshot. The answer stays the same after later commits.

:::info Create the repository first
Beacon reads Icechunk. Beacon does not write Icechunk. Beacon adds no commit. Beacon creates no
branch. Beacon accepts no `INSERT`.

Create the repository with the Icechunk library. You can also use
[VirtualiZarr](https://github.com/zarr-developers/VirtualiZarr). Then read the repository with
Beacon.
:::

## Read the repository

```text
read_icechunk(location)
read_icechunk(location, branch)
read_icechunk(location, branch, snapshot)
read_icechunk(location, branch, snapshot, dimensions)
```

Give one `location`. The `location` is the path to the directory of the repository. The `location`
is not a glob. The `location` is not a list.

```sql
-- Read the tip of `main`
SELECT * FROM read_icechunk('sst/repo') LIMIT 100;

-- Read the tip of a different branch
SELECT count(*) FROM read_icechunk('sst/repo', 'dev');

-- Read one snapshot. Give NULL for the branch.
SELECT avg(sst) FROM read_icechunk('sst/repo', NULL, 'NNNGCAX7Z99K7XTTYK8G');

-- Read only the arrays on the `time` dimension
SELECT * FROM read_icechunk('sst/repo', NULL, NULL, ['time']);
```

A branch selects a different version than a snapshot. Give one of the two. Beacon rejects a call
that gives both.

## Persisted external table

`CREATE EXTERNAL TABLE … STORED AS ICECHUNK` adds the repository to the catalog. Beacon loads the
table again after a restart.

```sql
CREATE EXTERNAL TABLE sst
STORED AS ICECHUNK
LOCATION 'sst/repo';
```

`OPTIONS` selects the version and the arrays. Set one of `branch`, `tag` and `snapshot`. Do not set
two. Beacon reads the tip of `main` if you set none.

| Option | Function |
| --- | --- |
| `branch` | Read the tip of this branch. The tip moves after each commit. |
| `tag` | Read this tag. The tag does not move. |
| `snapshot` | Read this snapshot. The snapshot does not move. |
| `read_dimensions` | A list of dimension names. Beacon reads an array only if the list holds all dimensions of the array. |

```sql
CREATE EXTERNAL TABLE sst_v1
STORED AS ICECHUNK
LOCATION 'sst/repo'
OPTIONS ('tag' 'v1', 'read_dimensions' 'time,lat,lon');
```

A table on a branch reads new data for each query. Beacon reads the branch tip again for each scan.
A table on a tag or on a snapshot reads the same rows each time.

Beacon sets the columns when you create the table. A later commit can add a new array. Create the
table again to read that array.

## Inspect the schema

Examine the columns and the types before you write a query. `read_icechunk_schema` takes the same
arguments as `read_icechunk`. It gives one row for each column. It reads no data.

```sql
SELECT * FROM read_icechunk_schema('sst/repo');
```

[Inspect a schema](/docs/2.0.0-rc2/formats/inspect-a-schema) compares the `_schema` functions,
`SUMMARIZE`, `DESCRIBE` and `LIMIT 0`. It gives the cost of each one.

## Predicate pushdown

Predicate pushdown works as it works for a plain Zarr store. Beacon removes the chunks that the
`WHERE` clause cannot match. Beacon also cuts each coordinate dimension to the range of the clause.

```sql
SELECT time, lat, lon, sst
FROM read_icechunk('sst/repo')
WHERE lat BETWEEN 40 AND 45 AND time >= '2024-01-01';
```

[Arrays to tables](/docs/2.0.0-rc2/arrays-to-tables) shows how Beacon changes an array into rows.

## Storage

Beacon reads a repository in place. Beacon makes no local copy.

A `location` with a scheme opens through that backend. Use `s3://bucket/repo`,
`gs://bucket/repo` or `az://account/container/repo`. Beacon takes the credentials from the
environment. It reads `AWS_*`, `GOOGLE_*` and `AZURE_*`.

A `location` without a scheme resolves against the datasets store:

- A local datasets store reads from disk.
- An S3 datasets store reads over HTTP without a signature. The bucket must allow anonymous reads.
  [NetCDF](/docs/2.0.0-rc2/formats/netcdf) works the same way.
- Give an `s3://` location to read a private bucket.

## Virtual chunk references

A repository can reference a chunk inside a netCDF file or inside an HDF5 file. That file stays
outside the repository. VirtualiZarr makes this type of reference.

**Beacon does not read a virtual chunk reference.** A query that touches one fails. The error names
the container.

This read is not a read of the repository. It needs the credentials of the object store of the
referenced file. That object store is a different object store. The user holds no permission for
it. A chunk inside the repository reads the same way as any other Zarr chunk.

## Limits

- Beacon reads a repository. Beacon does not write a repository.
- Beacon does not read a virtual chunk reference.
- Beacon does not find a repository automatically. Give a `location` to a function or to a table.
- A new array from a later commit needs a new table.
