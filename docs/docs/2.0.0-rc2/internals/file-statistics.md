---
description: Beacon records the value range of every column in every file, in the background, and uses it to skip files a query cannot match.
---

# File statistics

A query with a `WHERE` clause usually needs a small part of your archive. Without help, Beacon
opens every file to find out which part.

File statistics record what each file contains — the lowest and highest value of each of its
columns — so a query can skip the files that provably cannot match. A node holding a million
netCDF files can answer a narrow question by opening fifty thousand of them instead of all of
them.

The subsystem is **off by default**. This page explains what it does, how to turn it on, and how to
tell whether it is working.

:::warning Not on by default
Set `BEACON_FILE_STATS_ENABLE=true` to use it. On a netCDF server you also need
`BEACON_NETCDF_USE_RUST_READER=true`, or it will run, succeed, and store nothing. The
[Is it working?](#is-it-working) section shows how to spot that.
:::

## What it does

Beacon lists your datasets store, reads each file once in the background, and records per column:

- the minimum and maximum value
- how many rows, and how many of them are null

When a query arrives, Beacon compares its `WHERE` clause against those ranges and drops the files
that cannot contain a matching row.

```sql
SELECT * FROM read_parquet('obs/*.parquet') WHERE "TEMP" > 80;
```

```
DataSourceExec: file_groups={1 group: [[obs/hot.parquet]]}
```

Two files went in; one came out. The other two record a maximum temperature below 80, so no row in
them can satisfy the predicate.

## Turning it on

```bash
BEACON_FILE_STATS_ENABLE=true
BEACON_NETCDF_USE_RUST_READER=true   # only if you serve netCDF
```

Beacon then wakes every 15 minutes, finds files it has not seen, and reads them. Every setting is
listed under [Configuration](/docs/2.0.0-rc2/server/configuration#file-statistics).

You do not have to wait for the timer:

```sql
ANALYZE FILES;              -- read everything now
ANALYZE FILES 'argo/';      -- one prefix, to try it before committing
```

Reading a million netCDF files takes roughly a quarter of an hour on eight cores. Parquet is much
faster, because its ranges are already in the file footer and need no scan.

:::tip Start with one prefix
`ANALYZE FILES 'some/prefix/'` reads only that part of the store. It is the cheapest way to see
what the subsystem does with your data and your layout before letting it loose on everything.
:::

## Is it working?

Two tables answer that, and they are worth knowing about because **this subsystem fails quietly**.
A format that cannot report ranges is analyzed successfully and contributes nothing. Nothing errors;
queries simply keep reading every file, exactly as they did before.

The tell is `column_count`. Zero means the file was read and gave up nothing.

```sql
SELECT format,
       count(*)                                         AS files,
       sum(CASE WHEN column_count = 0 THEN 1 ELSE 0 END) AS barren
FROM beacon.system.file_stats
GROUP BY format;
```

```
netcdf  | 840000 | 840000    <- BEACON_NETCDF_USE_RUST_READER is off
odv     |  12000 |  12000    <- ODV cannot report ranges at all
parquet |  50000 |      0    <- working
```

`beacon.system.file_stats` has one row per file:

| Column | Meaning |
| --- | --- |
| `path` | The file, relative to the datasets store |
| `state` | `Pending`, `Analyzed`, `Failed`, `Stale` or `Deleted` |
| `format` | Which reader handled it |
| `column_count` | Columns it contributed. **Zero is the interesting value.** |
| `num_rows`, `total_byte_size` | What the reader reported |
| `stats_epoch` | How many times it has been re-read |

Useful questions:

```sql
-- How far along is the first pass?
SELECT state, count(*) FROM beacon.system.file_stats GROUP BY state;

-- What could not be read?
SELECT path FROM beacon.system.file_stats WHERE state = 'Failed';
```

And per query, the scan itself reports what pruning did:

```sql
EXPLAIN ANALYZE SELECT * FROM read_parquet('obs/*.parquet') WHERE "TEMP" > 80;
```

```
DataSourceExec: file_groups={1 group: [[obs/hot.parquet]]}
  metrics=[file_stats_files_considered=3, file_stats_files_pruned=2,
           file_stats_columns_used=1]
```

`file_stats_columns_used` is the one to check when nothing is being pruned. If it is `0`, the store
holds no statistics for the columns you filtered on, which is a different problem from a filter that
genuinely matches everything.

## Which formats produce statistics

| Format | Ranges | Cost |
| --- | --- | --- |
| Parquet, GeoParquet | Yes | Free — read from the file footer |
| netCDF | Yes, **with `BEACON_NETCDF_USE_RUST_READER=true`** | Opens and scans coordinate variables |
| CSV, Arrow IPC | No | — |
| ODV, Zarr, TIFF, HDF5 | No | — |

A format that produces no ranges costs you nothing and gains you nothing: those files are always
read, exactly as before.

:::warning netCDF needs the Rust reader
The C netCDF library serialises every call in the process on a single lock. Computing statistics
through it would be single-threaded no matter how much hardware you have, and would block queries
while it ran. So Beacon only computes netCDF statistics with its own Rust reader, which reads
through the object store and parallelises properly.

With the default reader, netCDF files record `column_count = 0` and nothing is ever pruned.
:::

## Keeping up with changes

You do not have to tell Beacon when files change.

**A changed file** is noticed on the next pass by its size, modification time or etag. Its old
statistics stop being trusted immediately — a file whose contents changed must never be skipped on a
range describing what used to be there — and it is re-read.

**A deleted file** is noticed when a listing no longer reports it. Its statistics stop being used.

**A new file** is read on the next pass. Until then it has no statistics, so it is never skipped.
A half-finished first pass is safe: it makes queries faster on the files it has reached, and changes
nothing about the rest.

If you turn on `BEACON_NETCDF_USE_RUST_READER` *after* a pass has already run, every netCDF file is
recorded as read with nothing in it, and nothing will re-read them, because the files did not
change — only Beacon's ability to read them did. That is what `FORCE` is for:

```sql
ANALYZE FILES FORCE;
```

## Correctness

Pruning only ever removes a file that **provably** cannot match. Everything uncertain keeps the
file:

- a file not yet read
- a file whose contents changed since it was read
- a column the format could not report a range for
- a query the pruning engine cannot use

The cost of that choice is a file read that could have been skipped. The alternative — a file
skipped that should have been read — would silently drop rows from your results, so the subsystem
is deliberately biased the other way. Turning file statistics on cannot change the answer to a
query, only how long it takes.

## How it works

Skip this section unless you are curious or debugging.

### Why not a simple table

The obvious design is a table of files by columns, holding a range in each cell. A node with a
million files and 160 000 distinct column names between them would need 160 billion cells, and
almost all of them empty — each file declares only a few dozen columns.

Beacon stores only the cells that exist: about 20 million, roughly 780 MB. That difference, a factor
of 8000, decides the whole design.

### The three parts

| Part | What it holds | Lives in |
| --- | --- | --- |
| **Registry** | Every known file, with a short numeric id, its state and row count | `beacon.db` |
| **Segments** | The ranges themselves, grouped by column | `beacon.db` |
| **Manifest** | Which segments hold which columns | `beacon.db` |

Everything is inside `beacon.db`, so [copying that file](/docs/2.0.0-rc2/internals/storage) carries
the statistics with it.

Files get a numeric id because a path is long and a number is not. Repeating a 200-byte path across
20 million records would cost more than the statistics; the ids cost a fraction of that. An id never
changes once assigned, so a deleted file keeps its slot and old records stay meaningful.

### Reading one column

Segments are grouped by column, not by file. A query filtering on `TEMP` reads `TEMP`'s data and
nothing else — whether the store holds three columns or 160 000.

For `WHERE TEMP > 6.5`:

1. The manifest says which segments hold `TEMP` at all. Most are eliminated without being read.
2. Each surviving segment is opened and its `TEMP` block read — two ranged reads apiece.
3. The ranges are compared against the predicate, and the files that cannot match are dropped.

The cost does not grow with the number of columns in your store, only with the number the query
mentions.

### Why files are grouped by folder

Each background pass writes one segment per group of files, and groups them by their path. Files in
the same folder tend to have the same columns, so a segment covering one folder holds few columns,
and a query filtering on a column from elsewhere skips it entirely.

Beacon works the grouping out from your paths, so `argo/f.nc` and `cmems/2024/01/15/f.nc` are each
handled at their own depth. You should not need to configure this. If you do,
`BEACON_FILE_STATS_PREFIX_DEPTH` overrides it.

### What it costs to run

| | |
| --- | --- |
| Registry, one million files | ~510 MB |
| Statistics, one million files at ~20 columns each | ~780 MB |
| Manifest | under 1 MB |
| Reading a million netCDF files | ~15 minutes on eight cores |
| Pruning a query over a million files | 50–100 ms |

The background pass uses a quarter of your cores by default, so it does not compete with queries.
Raise `BEACON_FILE_STATS_CONCURRENCY` well above your core count if your data is in object storage,
where the work is waiting on the network rather than using the CPU.

## Limits

**Pruning only helps for columns most of the scanned files declare.** A file with no statistics for
a column is never skipped on it, so a column that only a handful of files carry can only ever
eliminate that handful. The gain concentrates on the columns people actually filter on — time,
latitude, depth — which are usually in every file.

**Old statistics are not yet reclaimed.** A file that is re-read leaves its previous record in place,
and a deleted file keeps its entry. Nothing is wrong with either, but the store grows slowly with
churn. Compaction is not implemented yet.

## See also

- [Configuration](/docs/2.0.0-rc2/server/configuration#file-statistics) — every setting
- [Storage internals](/docs/2.0.0-rc2/internals/storage) — what else `beacon.db` holds
- [Performance tuning](/docs/2.0.0-rc2/server/performance-tuning) — the other levers
