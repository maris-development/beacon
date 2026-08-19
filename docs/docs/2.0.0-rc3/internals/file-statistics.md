---
description: Beacon records the value range of each column in each file. A query then prunes the files that cannot match.
---

# File statistics

A query with a `WHERE` clause reads a small part of your archive. Beacon opens every file to find
that part.

File statistics record the lowest and the highest value of each column in each file. Beacon compares
a query against these ranges. It then prunes the files that cannot hold a row that matches.

A server with one million netCDF files answers a narrow query. It opens fifty thousand files. It
does not open one million.

Beacon enables this feature by default. The first pass runs 15 minutes after startup.

:::warning The reader decides the range
A netCDF or HDF5 file supplies a range only through the pure-Rust reader. Both readers are the
default, so a standard server records a range. A server that sets
`BEACON_NETCDF_USE_RUST_READER=false` or `BEACON_HDF5_USE_RUST_READER=false` reads each such file
and records no range. The [Check the result](#check-the-result) section shows how to find this
condition.
:::

## What Beacon records

Beacon lists the datasets store. It reads each file one time in the background. For each column it
records:

- the lowest value and the highest value
- the row count and the null count

A query then compares its `WHERE` clause against these ranges.

```sql
SELECT * FROM read_parquet('obs/*.parquet') WHERE "TEMP" > 80;
```

```
DataSourceExec: file_groups={1 group: [[obs/hot.parquet]]}
```

Three files go in. One file comes out. The other two files record a maximum temperature below 80.
No row in them can match the query.

## The defaults

```bash
BEACON_FILE_STATS_ENABLE=true        # the default
BEACON_FILE_STATS_ON_STARTUP=false   # the default, see below
BEACON_NETCDF_USE_RUST_READER=true   # the default, netCDF ranges
BEACON_HDF5_USE_RUST_READER=true     # the default, HDF5 ranges
```

Beacon runs a pass every 15 minutes. Each pass finds new files and reads them.
[Configuration](/docs/2.0.0-rc3/server/configuration#file-statistics) lists each variable.

The **timer runs its first pass one interval after startup**, not at startup. Beacon starts the
interval again on each boot, and records no due time. A server that restarts more often than the
interval therefore never reaches a tick. `ANALYZE FILES` fills the store at a time you choose.
`BEACON_FILE_STATS_ON_STARTUP` fills it at each boot.

## Collect at every boot

```bash
BEACON_FILE_STATS_ON_STARTUP=true
```

Beacon collects as soon as the runtime is up. It finds the files, reads every one that has no
statistics, and stops when the queue is empty. The timer continues afterwards.

The collection runs in the background. It does not hold up startup, and the server answers queries
while it works. A file with no statistics yet is read in full, as it was before this feature.

```
INFO collecting file statistics at startup
INFO startup file statistics collection finished discovered=2 analyzed=2 failed=0 segments=1
```

The work is not repeated. The registry survives a restart, so the next boot reads only the files
that are new or changed. The first boot over a large archive is the expensive one.

:::tip When to set this
Set it for a server that restarts often, and for a fresh instance that must be useful at once.
Leave it off for a long-lived server over a large archive, where an unattended backfill at boot
competes with your queries. `ANALYZE FILES` covers that case, at a time you choose.
:::

:::warning The pass holds the database file
The pass keeps `beacon.db` open while it reads a batch. A process that closes a database and opens
the same file again then reports a lock error. The error continues until the pass ends. This
condition applies to an embedded caller, not to a server that exits. Leave this flag off there.
:::

Do not wait for the timer. Start a pass with SQL:

```sql
ANALYZE FILES;              -- read every file now
ANALYZE FILES 'argo/';      -- read one prefix only
ANALYZE FILES FORCE;        -- read every file again, after a reader change
```

`ANALYZE FILES` runs to completion and returns one row of counts: `discovered`, `requeued`,
`analyzed`, `failed`, `segments`, and `pending`. A second run reports `analyzed=0`, because the
files carry their statistics already. `discovered` counts every file of the listing, new or known,
so it stays above zero.

Beacon reads one million netCDF files in about 15 minutes on 8 cores. Parquet is faster. Parquet
holds its ranges in the file footer, so Beacon reads no data.

:::tip Start with one prefix
`ANALYZE FILES 'some/prefix/'` reads one part of the datasets store. Use it to see the result on
your data before you enable the timer.
:::

## Check the result

Two tables and one function show what Beacon knows. Use them. This feature fails quietly.

A format that supplies no ranges gives no error. Beacon reads the file and records nothing. Queries
continue to read every file.

The `column_count` value shows this condition. A value of zero means Beacon read the file and got
no ranges.

```sql
SELECT format,
       count(*)                                         AS files,
       sum(CASE WHEN column_count = 0 THEN 1 ELSE 0 END) AS empty
FROM beacon.system.file_stats
GROUP BY format;
```

```
netcdf  | 840000 | 840000    <- this server set BEACON_NETCDF_USE_RUST_READER=false
hdf5    |   4000 |   4000    <- this server set BEACON_HDF5_USE_RUST_READER=false
odv     |  12000 |  12000    <- ODV supplies no ranges
parquet |  50000 |      0    <- correct
```

The `beacon.system.file_stats` table holds one row for each file:

| Column | Meaning |
| --- | --- |
| `path` | The file, relative to the datasets store |
| `state` | `Pending`, `Analyzed`, `Failed`, `Stale` or `Deleted` |
| `format` | The reader that Beacon used |
| `column_count` | The columns this file supplied. **A value of zero is important.** |
| `num_rows`, `total_byte_size` | The values that the reader reported |
| `stats_epoch` | The number of times that Beacon read this file |

### One file, column by column

`column_count` counts the columns of a file. It does not show their values. The `file_statistics`
function opens the segments and reports each range that Beacon holds:

```sql
SELECT column, data_type, min, max, null_count, row_count
FROM file_statistics('obs/2025/baltic_timeseries.parquet')
ORDER BY column;
```

```
JULD | Float64 | 24837.5 | 24838.0 |   0 | 2042
PSAL | Float64 | 31.2    | 38.1    | 118 | 2042
TEMP | Float64 | -1.84   | 29.7    |  12 | 2042
```

A netCDF file reports no counts, so `null_count` and `row_count` are empty for one. The bounds are
there just the same.

A dataset is usually a directory, so the function also takes a glob. Each row keeps its `path`:

```sql
-- The columns of a dataset that hold no usable bound
SELECT path, count(*) AS columns, sum(CASE WHEN min IS NULL THEN 1 ELSE 0 END) AS no_range
FROM file_statistics('argo/2024/**')
GROUP BY path
ORDER BY path;
```

| Column | Meaning |
| --- | --- |
| `path` | The file the row belongs to |
| `column` | The column name, as the file declares it |
| `data_type` | The type of the file's own column, not of a merged table |
| `min`, `max` | The recorded bounds, in the type of that column. `NULL` means the bound is null |
| `null_count`, `row_count` | The counts the reader reported. `NULL` where it reported none |
| `segment` | The segment that holds this row |

The function needs the super-user, like the `beacon.system` tables: a range is data. It reports on
at most 1000 files in one call, so a wide glob returns an error instead of a very large result.

An unknown path is an error, not an empty result. A file with no recorded range gives zero rows.
The two conditions are different, and this keeps them apart.

Two more queries:

```sql
-- The progress of the first pass
SELECT state, count(*) FROM beacon.system.file_stats GROUP BY state;

-- The files that Beacon cannot read
SELECT path FROM beacon.system.file_stats WHERE state = 'Failed';
```

Each query also reports its own result:

```sql
EXPLAIN ANALYZE SELECT * FROM read_parquet('obs/*.parquet') WHERE "TEMP" > 80;
```

```
DataSourceExec: file_groups={1 group: [[obs/hot.parquet]]}
  metrics=[file_stats_files_considered=3, file_stats_files_pruned=2,
           file_stats_columns_used=1]
```

Read `file_stats_columns_used` first when Beacon prunes no file. A value of zero means the
statistics hold no data for your filter columns. A filter that matches every file is a different
condition.

## Formats that supply ranges

| Format | Ranges | Cost |
| --- | --- | --- |
| Parquet, GeoParquet | Yes | None. Beacon reads the file footer. |
| netCDF | Yes, through the Rust reader (the default) | Beacon opens the file and reads the coordinate variables. |
| HDF5 | Yes, through the Rust reader (the default) | Beacon opens the file and reads the one-dimensional datasets. |
| Zarr | Yes | Beacon reads the store metadata, and the coordinate arrays it does not describe. |
| CSV, Arrow IPC | No | |
| ODV, TIFF | No | |

A format that supplies no ranges costs nothing. Beacon always reads those files, as before.

:::info Zarr reads only its coordinates
A Zarr array can state its range in its metadata, with the `actual_range` attribute. Beacon uses
that attribute and reads no chunk. Beacon reads an array of rank 0 or rank 1 that does not state
one. Beacon never reads a data grid of rank 2 or higher, and that array supplies no range.

Beacon does not use `valid_min` and `valid_max`. Those attributes state which values are valid. A
store can hold a value outside them, and Beacon returns that value. A range from those attributes
would drop a file that holds a matching row.

Set `BEACON_ZARR_ENABLE_STATISTICS=false` to stop this work.
:::

:::warning netCDF and HDF5 need the Rust reader
The netCDF-C library holds one lock for each call in the process. Beacon computes the ranges through
one thread. Your core count does not change this. The work also blocks queries.

Beacon therefore computes netCDF ranges with its own Rust reader. That reader reads through the
object store and uses each core. It is the default.

With `BEACON_NETCDF_USE_RUST_READER=false`, netCDF files record `column_count = 0`. Beacon prunes
no file.

The rule applies to `.h5` and `.hdf5` files, through `BEACON_HDF5_USE_RUST_READER`.

Beacon writes the reason one time for each pass, at log level `info`. The line names the
variable to set.
:::

## Changed and deleted files

Beacon finds these changes. You do not report them.

**A file changes.** Beacon compares the size, the modification time and the etag. It does not trust
the old ranges. It reads the file again. Beacon never prunes a file on a range that describes old
content.

**A file goes.** Beacon lists the datasets store and does not find the file. It does not use
the ranges of that file.

**A file arrives.** Beacon reads it on the next pass. Before that, the file has no ranges, and
Beacon never prunes it. An incomplete first pass is safe. It makes queries faster on the files that
Beacon read. It changes nothing else.

Set `BEACON_NETCDF_USE_RUST_READER=true` (or `BEACON_HDF5_USE_RUST_READER=true`) after a pass, and
each such file has a record with no ranges. The files did not change. Only the reader changed.
Beacon does not read them again. Use `FORCE` for this condition:

```sql
ANALYZE FILES FORCE;
```

## Correctness

Beacon prunes a file only when the ranges prove that the file cannot match. Beacon keeps the file in
each other condition:

- Beacon has not read the file.
- The file changed after Beacon read it.
- The format supplied no range for the column.
- The query has a form that Beacon cannot use.

This choice costs one file read. The opposite choice removes rows from your result. Beacon therefore
keeps the file.

This feature does not change the answer to a query. It changes the time.

## Internal design

Read this section for interest or for debug work.

### Why Beacon does not use a table

A simple design holds one range for each file and for each column. A server with one million files
and 160000 column names needs 160 billion cells. Almost every cell is empty. Each file declares a
few dozen columns.

Beacon stores only the cells with data. That is about 20 million cells and 780 MB. This ratio is
8000 to 1. It controls the design.

### The three parts

| Part | Content |
| --- | --- |
| Registry | Each known file, with a number, a state and a row count |
| Segments | The ranges, in groups by column |
| Manifest | The columns that each segment holds |

Beacon holds all three parts in `beacon.db`. Copy that file and the statistics go with it. See
[Storage internals](/docs/2.0.0-rc3/internals/storage).

Beacon gives each file a number. A path is long and a number is short. 20 million records with a
200-byte path cost more than the ranges. A file keeps its number for ever. A deleted file keeps its
number, so old records stay correct.

### One column for each read

Beacon groups the ranges by column, not by file. A query on `TEMP` reads the data for `TEMP` only.
The statistics hold 3 columns or 160000 columns. The cost is the same.

For `WHERE TEMP > 6.5`, Beacon does 3 steps:

1. The manifest names the segments that hold `TEMP`. Beacon reads no segment for this step.
2. Beacon reads the `TEMP` block from each named segment. This costs 2 ranged reads for each segment.
3. Beacon compares the ranges against the query and prunes the files that cannot match.

The cost follows the column count in the query. It does not follow the column count in the
statistics.

### Groups by folder

Each pass writes one segment for each group of files. Beacon groups the files by path. Files in one
folder usually declare the same columns. A segment for one folder therefore holds few columns. A
query on a column from another folder does not read that segment.

Beacon derives the group depth from your paths. It handles `argo/f.nc` and `cmems/2024/01/15/f.nc`
at different depths. Configure nothing. `BEACON_FILE_STATS_PREFIX_DEPTH` replaces the derived value.

### Cost

| Item | Value |
| --- | --- |
| Registry, one million files | 510 MB |
| Ranges, one million files at 20 columns each | 780 MB |
| Manifest | Less than 1 MB |
| Read one million netCDF files | 15 minutes on 8 cores |
| Prune a query over one million files | 50 ms to 100 ms |

The pass uses one quarter of your cores, so it does not compete with queries. Increase
`BEACON_FILE_STATS_CONCURRENCY` above your core count for data in object storage. The pass then
waits on the network, not on the CPU.

## Limits

**Beacon prunes only on columns that many files declare.** A file with no range for a column keeps
its place in the query. A column in 10 files prunes those 10 files only. The gain comes from common
columns. Time, latitude and depth are examples. Most files declare them.

**Beacon does not reclaim old records.** Beacon reads a file again and keeps the old record. A
deleted file keeps its record. Both records are correct. The statistics grow slowly with each
change. Beacon has no compaction step.

## Related pages

- [Configuration](/docs/2.0.0-rc3/server/configuration#file-statistics) lists each variable.
- [Storage internals](/docs/2.0.0-rc3/internals/storage) shows the other content of `beacon.db`.
- [Performance tuning](/docs/2.0.0-rc3/server/performance-tuning) shows the other controls.
