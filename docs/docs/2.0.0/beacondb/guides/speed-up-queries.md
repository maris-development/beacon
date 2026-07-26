---
description: Diagnose a slow BeaconDB query with EXPLAIN, then fix it using projection and predicate pushdown, better file layout, Atlas consolidation, materialized views, or managed tables.
---

# Speed Up Slow Queries

Most slow queries come down to reading more bytes than necessary. The fix is usually to help the
engine skip data rather than to make it read faster.

## 1. Find out what is happening

Start with `EXPLAIN` to see what the planner pushed down into the scan:

```sql
EXPLAIN SELECT count(*) FROM read_parquet('obs/*.parquet') WHERE depth < 50;
```

Look for the filter and the column list appearing at the scan node. If a predicate is *above* the
scan instead, it is being applied after reading, which means no pruning.

## 2. Read fewer columns

`SELECT *` on a file with hundreds of variables decodes hundreds of columns. Name only what you need
and projection pushdown does the rest:

```sql
-- reads a handful of columns instead of everything
SELECT time, latitude, temperature FROM read_netcdf('argo/**/*.nc');
```

This matters most for wide scientific files and for object storage, where every extra column is extra
network traffic.

## 3. Filter on the columns that prune

Filters on coordinate columns (time, depth, latitude, longitude) are what let readers skip row
groups, chunks, and whole files:

```sql
SELECT time, temperature
FROM read_zarr('sst/*/zarr.json')
WHERE time >= '2024-01-01' AND time < '2024-02-01';
```

Prefer plain comparisons on raw columns. A predicate wrapped in a function the reader cannot
interpret has to be evaluated after reading, so it prunes nothing.

## 4. Fix the layout

- **Partition** Hive-style (`year=2024/month=01/`) and declare the partition columns with
  `PARTITIONED BY` so whole directories are skipped. See
  [`CREATE EXTERNAL TABLE`](/docs/2.0.0/beacondb/sql/create-table).
- **Avoid very many tiny files.** Per-file overhead dominates when files are small; consolidating
  helps more than any query-side tuning.
- **Choose a cloud-optimized format** when data lives on object storage. See
  [Query Data on S3](/docs/2.0.0/beacondb/guides/query-s3).

## 5. Consolidate large array collections with Atlas

For repeated queries over big NetCDF or Zarr collections, converting to
[Atlas](/docs/2.0.0/beacondb/data-sources/formats/atlas) is usually the single largest win. Atlas
keeps per-dataset statistics, so Beacon drops whole datasets that cannot match a predicate *before
reading any array data*, then reads only the projected arrays.

```sql
CREATE EXTERNAL TABLE sensor_atlas
STORED AS ATLAS
LOCATION 'collections/*/atlas.json';
```

## 6. Compute once, query many times

If the same expensive aggregate runs repeatedly, stop recomputing it.

A [materialized view](/docs/2.0.0/beacondb/sql/create-materialized-view) caches the result and is
refreshed on demand:

```sql
CREATE MATERIALIZED VIEW monthly_means AS
SELECT date_trunc('month', time) AS month, avg(temperature) AS t
FROM read_netcdf('argo/**/*.nc')
GROUP BY 1;

REFRESH MATERIALIZED VIEW monthly_means;
```

A [managed table](/docs/2.0.0/beacondb/data-sources/internal-format) is the right choice when you
want a working subset you will also mutate:

```sql
CREATE TABLE hot_profiles AS
SELECT * FROM read_netcdf('argo/**/*.nc') WHERE temperature > 25;
```

## 7. Stream instead of collecting

A query that is fast to plan but slow to return may simply be materializing too much in the client.
Pull batches rather than collecting the whole result:

```python
for batch in con.sql("SELECT * FROM obs").record_batch(50_000):
    process(batch)
```

See [Export Query Results](/docs/2.0.0/beacondb/guides/export-results).

## Server-side tuning

Running Beacon Data Lake adds knobs for concurrency, memory and disk spilling, reader caches, and
object-store listing. Those are documented in
[Performance Tuning](/docs/2.0.0/data-lake/performance-tuning).
