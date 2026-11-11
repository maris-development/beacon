---
description: An external table can keep the value range of each column in each file. A query on the table then prunes the files that cannot match.
---

# File statistics

A query with a `WHERE` clause reads a small part of your archive. Beacon opens every file to find
that part.

File statistics record the lowest and the highest value of each column in each file. Beacon compares
a query against these ranges. It then prunes the files that cannot hold a row that matches.

An external table turns this feature on for itself. Beacon keeps one statistics table for each such
external table, in `beacon.db`.

:::warning The reader decides the range
A netCDF or HDF5 file supplies a range only through the pure-Rust reader. Both readers are the
default, so a standard server records a range. A server that sets
`BEACON_NETCDF_USE_RUST_READER=false` or `BEACON_HDF5_USE_RUST_READER=false` reads each such file
and records no range. The [Check the result](#check-the-result) section shows how to find this
condition.
:::

## Turn it on for a table

Set the option `use_statistics_pruning` on the table:

```sql
CREATE EXTERNAL TABLE obs
STORED AS PARQUET
LOCATION 'obs/'
OPTIONS ('use_statistics_pruning' 'true');
```

Beacon then creates the statistics table of `obs`, and starts the first refresh in the background.
The `CREATE` statement does not wait for it. A query on `obs` reads every file until the refresh
records its range.

```sql
SELECT * FROM obs WHERE "TEMP" > 80;
```

```
DataSourceExec: file_groups={1 group: [[obs/hot.parquet]]}
```

Three files go in. One file comes out. The other two files record a maximum temperature below 80.
No row in them can match the query.

Three table options control the statistics:

| Option | Default | Description |
| --- | --- | --- |
| `use_statistics_pruning` | `false` | Keep file statistics for this table, and prune on them. |
| `statistics_columns` | each column with a range | The columns to keep a range for, separated by a comma. |
| `statistics_refresh_interval` | `900` | The seconds between two refreshes of this table. |

A table without `use_statistics_pruning` keeps no statistics. The `read_*()` functions do not prune.

A [crawler](/docs/2.1.0-rc2/server/crawlers) copies the option to each table that it finds:

```sql
CREATE CRAWLER profiles ON 'argo/' WITH ('use_statistics_pruning' 'true');
```

`CREATE OR REPLACE EXTERNAL TABLE` keeps the statistics rows when the new table sets the option. It
deletes them when the new table does not set it.

## What Beacon records

Beacon records one row for each file of the table. The row holds:

- the path, the size and the modification time of the file
- the row count of the file
- for each statistics column: the lowest value, the highest value and the null count

A column of a number, date, time, timestamp, decimal, boolean, string or binary type holds a range. A
list, a struct or a map holds no range, so Beacon keeps no statistics for it.

### At most 1,000 columns

Beacon keeps a range for at most 1,000 columns of one table. A table with more range columns needs
`statistics_columns`. Without it, `CREATE EXTERNAL TABLE` fails:

```
the table has 1342 columns that can hold a range, more than the 1000 that statistics cover;
name the statistics columns with the option 'statistics_columns'
```

Name the columns that your queries filter on:

```sql
CREATE EXTERNAL TABLE argo
STORED AS NC
LOCATION 'argo/**/*.nc'
OPTIONS ('use_statistics_pruning' 'true', 'statistics_columns' 'JULD,LATITUDE,LONGITUDE');
```

A query on a column outside this list reads every file.

## The refresh

A refresh lists the files of the table and compares them with the stored rows. Then it:

1. deletes the rows of the files that changed or that are gone
2. reads each new or changed file
3. writes one row for each file that it read

The rows of the other files stay. A refresh over an archive that did not change reads no file.

A refresh runs at three times:

- when a table registers: at `CREATE EXTERNAL TABLE`, at startup and at a table refresh
- each `statistics_refresh_interval` after that
- when you run `ANALYZE FILES`

### Refresh now

`ANALYZE FILES` starts a refresh in the background, and returns at once. A refresh over a large
archive can run for hours, so the statement does not wait for it.

```sql
ANALYZE FILES obs;   -- refresh one table
ANALYZE FILES;       -- refresh each table with statistics
```

The result holds one row for each table:

| Column | Meaning |
| --- | --- |
| `table` | The name of the table |
| `status` | `started`: the refresh starts when a refresh slot is free. `queued`: a refresh of the table runs now, and a new refresh starts after it. |

Many `ANALYZE FILES` statements during one refresh give one more refresh, not one for each
statement. `ANALYZE FILES` on a table without `use_statistics_pruning` is an error. Only the
super-user can run `ANALYZE FILES`.

At the end of the refresh, Beacon writes a log line at level `info`:

```
INFO refreshed the file statistics on request table="obs" listed=3 analyzed=1 removed=0
```

`listed` counts the files of the table. `analyzed` counts the new and the changed files that the
refresh read. `removed` counts the rows of the changed and the deleted files. A failed refresh writes
a `warn` line, and the next refresh tries again. `table_statistics('obs')` shows the rows that the
refresh wrote.

The next periodic refresh of the table starts one `statistics_refresh_interval` after the end of
this refresh.

### Limits on the work

Many tables can keep statistics. Their refreshes share two limits:

| Variable | Default | Description |
| --- | --- | --- |
| `BEACON_FILE_STATS_CONCURRENCY` | one quarter of the cores, minimum 2 | The files that Beacon reads at the same time, over all tables. |
| `BEACON_FILE_STATS_MAX_REFRESHES` | `2` | The tables that refresh at the same time. The other tables wait for a free slot. |

The refreshes therefore do not take all cores from the queries. Increase
`BEACON_FILE_STATS_CONCURRENCY` above your core count for data in object storage. The refresh then
waits on the network, not on the CPU.

Lance writes the statistics tables through its own thread pool. `LANCE_CPU_THREADS` sets the size of
that pool.

### Restart

The statistics tables are in `beacon.db`, so they survive a restart. At startup each table registers
again, and a query on it prunes at once. The first refresh after startup reads only the files that
changed while the server was down.

### Drop a table

`DROP TABLE obs` deletes the statistics table of `obs`. Beacon does not delete the files.

### A change of the statistics columns

A refresh of the table schema, or a new `statistics_columns` value, can change the statistics
columns. Beacon then deletes the statistics table, creates it again, and reads each file again.

## Check the result

Each query reports its own result:

```sql
EXPLAIN ANALYZE SELECT * FROM obs WHERE "TEMP" > 80;
```

```
DataSourceExec: file_groups={1 group: [[obs/hot.parquet]]}
  metrics=[statistics_files_considered=3, statistics_files_pruned=2]
```

`statistics_files_considered` counts the files that the prune examined. `statistics_files_pruned`
counts the files that it dropped.

A plan holds no `statistics_files_` metric where the prune did not start. Beacon starts no prune in
these conditions:

- The table does not set `use_statistics_pruning`.
- The server sets `BEACON_FILE_STATS_ENABLE=false`.
- The query has no `WHERE` clause.
- The filter has a shape that Beacon cannot use.

### The statistics of one table

`table_statistics('<table>')` returns the statistics table of an external table. Use the name of the
`CREATE EXTERNAL TABLE` statement. Only the super-user can call it: a range is data.

```sql
SELECT _path, _row_count, "TEMP"['min'], "TEMP"['max']
FROM table_statistics('obs')
ORDER BY _path;
```

```
obs/cold.parquet | 2 | 0.0  | 5.0
obs/hot.parquet  | 2 | 90.0 | 100.0
obs/mild.parquet | 2 | 20.0 | 25.0
```

| Column | Meaning |
| --- | --- |
| `_stamp` | A hash of the store, the path, the modification time and the size of the file |
| `_path` | The file, relative to its store |
| `_size` | The size in bytes |
| `_last_modified` | The modification time |
| `_failed` | `true` when Beacon could not read the file |
| `_row_count` | The row count. `NULL` where the format reported none. |
| one struct for each statistics column | `min`, `max` and `null_count`, in the type of the table column |

A statistics column with a name that is empty, starts with `_` or holds a `.` gets the name `_c<n>`.
`<n>` is the position of the column in the table.

A `NULL` in `min` or `max` means that Beacon holds no bound. The format supplied none, or the file
does not have the column. Beacon prunes no file on a `NULL` bound.

Two more queries:

```sql
-- The files that Beacon could not read
SELECT _path FROM table_statistics('obs') WHERE _failed;

-- The files with no range for TEMP
SELECT _path FROM table_statistics('obs') WHERE "TEMP"['min'] IS NULL;
```

A format that supplies no range gives no error. Beacon reads the file and records `NULL` bounds.
Queries then read every file. The second query above finds this condition.

::: warning The views of older versions are removed
Beacon 2.1.0 removes `beacon.system.file_stats`, `beacon.system.file_stats_segments` and the
function `file_statistics(path)`. A query on them fails with "not found". Change scripts and
dashboards that read them to `table_statistics('<table>')`. The new function gives one table at a
time, and has other columns.
:::

## Formats that supply ranges

| Format | Ranges | Cost |
| --- | --- | --- |
| Parquet, GeoParquet | Yes | None. Beacon reads the file footer. |
| netCDF | Yes, through the Rust reader (the default) | Beacon opens the file and reads the coordinate variables. |
| HDF5 | Yes, through the Rust reader (the default) | Beacon opens the file and reads the one-dimensional datasets. |
| Zarr | Yes | Beacon reads the store metadata, and the coordinate arrays it does not describe. |
| CSV, Arrow IPC | No | |
| ODV, TIFF | No | |

Do not set `use_statistics_pruning` on a table of a format that supplies no range. The refresh then
costs time and prunes no file.

:::info netCDF and HDF5 range only rank 0 and rank 1
Beacon computes a range for a variable of rank 0 or rank 1. A variable of rank 2 or higher is a data
grid. A full scan of it costs too much, so that variable supplies no range. A string variable
supplies no range either.

An ARGO file therefore holds a range for `JULD`, `LATITUDE` and `LONGITUDE`. It holds none for
`TEMP`, `PSAL` and `PRES`, which have the dimensions `N_PROF` and `N_LEVELS`. A filter on `TEMP`
prunes no file of such a table.

A CF ragged file follows the same rule. Beacon ranges the instance variables, and skips the
observation variables.
:::

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

With `BEACON_NETCDF_USE_RUST_READER=false`, netCDF files record `NULL` bounds. Beacon prunes no
file. The rule applies to `.h5` and `.hdf5` files, through `BEACON_HDF5_USE_RUST_READER`.
:::

## Changed and deleted files

Beacon finds these changes. You do not report them.

**A file changes.** The stamp of a file is a hash of its store, its path, its modification time and
its size. A change gives a new stamp. The scan finds no row for the new stamp, so it keeps the file.
The next refresh reads the file again.

**A file goes.** The scan lists the files of the table, so it does not read the file. The next
refresh deletes its row.

**A file arrives.** The file has no row until the next refresh. Beacon never prunes it before that.

## Correctness

Beacon prunes a file only when the ranges prove that the file cannot match. Beacon keeps the file in
each other condition:

- Beacon has not read the file.
- The file changed after Beacon read it.
- Beacon could not read the file.
- The format supplied no range for the column.
- The query has a form that Beacon cannot use.

This choice costs one file read. The opposite choice removes rows from your result. Beacon therefore
keeps the file.

This feature does not change the answer to a query. It changes the time.

## Internal design

Read this section for interest or for debug work.

Each statistics table is a [Lance](/docs/2.1.0-rc2/internals/storage) table in the `statistics`
namespace of `beacon.db`. Copy that file and the statistics go with it.

A query on the table reads only the stamp, the row count and the columns of its filter. The other
statistics columns cost nothing. Beacon then matches the stamp of each file of the scan against the
stored rows.

A refresh deletes rows and appends rows. Many refreshes give many small Lance fragments. Beacon
compacts a statistics table when it holds more than 16 fragments.

## Limits

**A small query reads the whole statistics table.** The query reads the stamps of all files of the
table, not only of the files in its scan. On a table of one million files, a query over 1,000 files
spends about 50 ms to 110 ms on the statistics.

**A rewrite with the same size and time is not found.** The stamp holds the modification time and
the size, not the content. A tool that writes new content with the same size and the same
modification time hides the change. Beacon then prunes on the old ranges.

**Beacon prunes only on columns that many files declare.** A file with no range for a column keeps
its place in the query. A column in 10 files prunes those 10 files only. The gain comes from common
columns. Time, latitude and depth are examples. Most files declare them.

**A drop during a refresh holds `beacon.db`.** A refresh keeps the database file open until it ends.
An embedded caller that closes a database and opens the same file again during a refresh gets a lock
error. A server that exits does not see this.

## Related pages

- [Configuration](/docs/2.1.0-rc2/server/configuration#file-statistics) lists each variable.
- [CREATE EXTERNAL TABLE](/docs/2.1.0-rc2/sql/create-external-table#keys-of-each-table) lists the
  table options.
- [Storage internals](/docs/2.1.0-rc2/internals/storage) shows the other content of `beacon.db`.
- [Performance tuning](/docs/2.1.0-rc2/server/performance-tuning) shows the other controls.
