---
description: Read Icechunk repositories with read_icechunk(). A repository is a Zarr store with commits, branches and snapshots, so a query reads one fixed version.
---

# Icechunk

```sql
SELECT * FROM read_icechunk('sst/repo');
```

An **Icechunk repository** is a Zarr v3 store with version control. It holds the same arrays,
groups and attributes as a plain [Zarr](/docs/2.0.0-rc2/formats/zarr) store, plus commits,
branches and snapshots. Beacon reads a repository through the Zarr reader, so the schema, the
array handling and the chunk pruning are the same.

A repository gives you two things a plain store does not:

- **A consistent read.** A writer commits while your query runs. The query still sees one version.
- **A reproducible read.** Name a snapshot and the answer does not change, however many commits
  land later.

:::info The repository must already exist
Beacon reads Icechunk. It does not write it. There is no commit, no branch creation and no
`INSERT`. Create the repository with the Icechunk library or with
[VirtualiZarr](https://github.com/zarr-developers/VirtualiZarr), then read it here.
:::

## Read the repository

```text
read_icechunk(location)
read_icechunk(location, branch)
read_icechunk(location, branch, snapshot)
read_icechunk(location, branch, snapshot, dimensions)
```

`location` is **one path to the repository directory**. It is not a glob and not a list.

```sql
-- The tip of `main`
SELECT * FROM read_icechunk('sst/repo') LIMIT 100;

-- The tip of another branch
SELECT count(*) FROM read_icechunk('sst/repo', 'dev');

-- A fixed snapshot. Pass NULL for the branch.
SELECT avg(sst) FROM read_icechunk('sst/repo', NULL, 'NNNGCAX7Z99K7XTTYK8G');

-- Only the arrays on the `time` dimension
SELECT * FROM read_icechunk('sst/repo', NULL, NULL, ['time']);
```

A branch and a snapshot select different versions, so pass one of them. Beacon rejects a call that
gives both.

## Persisted external table

`CREATE EXTERNAL TABLE … STORED AS ICECHUNK` puts the repository in the catalog. Beacon reloads it
after a restart, like any other table.

```sql
CREATE EXTERNAL TABLE sst
STORED AS ICECHUNK
LOCATION 'sst/repo';
```

`OPTIONS` selects the version and the arrays. Set at most one of `branch`, `tag` and `snapshot`.
Without one of them the table reads the tip of `main`.

| Option | Meaning |
| --- | --- |
| `branch` | Read the tip of this branch. It moves as commits land. |
| `tag` | Read this tag. It is fixed. |
| `snapshot` | Read this snapshot id. It is fixed. |
| `read_dimensions` | A comma-separated dimension list. Beacon returns an array only if the list holds all of its dimensions. |

```sql
CREATE EXTERNAL TABLE sst_v1
STORED AS ICECHUNK
LOCATION 'sst/repo'
OPTIONS ('tag' 'v1', 'read_dimensions' 'time,lat,lon');
```

A table on a branch sees new data on every query, because Beacon re-reads the branch tip per scan.
A table on a tag or a snapshot always returns the same rows. A **new array** added by a later
commit needs the table to be created again; Beacon pins the columns when the table is registered.

## Inspect the schema

Check the columns and the types before you write a query. `read_icechunk_schema` takes the same
arguments and returns one row per column, without a read of any data:

```sql
SELECT * FROM read_icechunk_schema('sst/repo');
```

[Inspect a schema](/docs/2.0.0-rc2/formats/inspect-a-schema) compares the `_schema` functions,
`SUMMARIZE`, `DESCRIBE` and `LIMIT 0`, and says what each one costs.

## Pushdown

Predicate pushdown works as it does for a plain Zarr store. Beacon prunes chunks and slices the
coordinate dimensions from your `WHERE` clause:

```sql
SELECT time, lat, lon, sst
FROM read_icechunk('sst/repo')
WHERE lat BETWEEN 40 AND 45 AND time >= '2024-01-01';
```

See [Arrays to tables](/docs/2.0.0-rc2/arrays-to-tables) for how Beacon turns an
N-dimensional array into rows.

## Where a repository may live

Beacon reads a repository in place. It makes no local copy.

- A location that names its own storage — `s3://bucket/repo`, `gs://…`,
  `az://account/container/…` — opens through that backend. Credentials come from the environment
  (`AWS_*`, `GOOGLE_*`, `AZURE_*`), like the rest of Beacon's object stores.
- A location relative to the configured datasets store resolves against that store's root. A local
  root reads from disk. An S3 root reads over unsigned HTTP, the same way
  [NetCDF](/docs/2.0.0-rc2/formats/netcdf) does, so the bucket must allow anonymous reads. Name
  the repository with an explicit `s3://…` location to read a private one.

## Virtual chunk references

An Icechunk repository may reference chunks that stay inside a netCDF or HDF5 file outside it.
This is what VirtualiZarr produces. **Beacon does not read those.** A query that touches a virtual
reference fails, and the error names the container it would have needed.

The reason is that such a read is not a read of the repository. It needs the credentials of the
referenced file's own store. That store is a different store, outside the permissions the caller
holds on the dataset. Chunks stored inside the repository — the normal case — read like any other
Zarr chunk.

## Limitations

- Read only. No commit, no branch creation, no `INSERT`.
- No virtual chunk references. See above.
- Beacon does not find repositories automatically. Point a function or an external table at one.
- A new array added by a later commit needs the external table to be created again.
