---
description: Diagnose a slow Beacon query with EXPLAIN. Then fix it with pushdown, a better file layout, Atlas, a materialized view or a managed table.
---

# Speed Up Slow Queries

A slow query usually reads more bytes than necessary. Help the engine to skip data. A faster read is
not the answer.

## 1. Find the cause

Run `EXPLAIN` first. It shows what the planner pushes down into the scan:

```sql
EXPLAIN SELECT count(*) FROM read_parquet('obs/*.parquet') WHERE depth < 50;
```

Look for the filter and the column list at the scan node. A predicate *above* the scan runs after
the read. Such a predicate prunes nothing.

## 2. Read fewer columns

`SELECT *` on a file with hundreds of variables decodes hundreds of columns. Name only the columns
that you need. Projection pushdown then does the rest:

```sql
-- reads a handful of columns instead of everything
SELECT time, latitude, temperature FROM read_netcdf('argo/**/*.nc');
```

This helps most on a wide scientific file. It also helps on object storage, because each extra
column adds network traffic.

## 3. Filter on the columns that prune

A filter on a coordinate column lets a reader skip row groups, chunks and whole files. The
coordinate columns are time, depth, latitude and longitude:

```sql
SELECT time, temperature
FROM read_zarr('sst/*/zarr.json')
WHERE time >= '2024-01-01' AND time < '2024-02-01';
```

Use a plain comparison on a raw column. A predicate inside a function that the reader cannot
interpret runs after the read. It therefore prunes nothing.

## 4. Fix the layout

- **Partition** the files Hive-style, for example `year=2024/month=01/`. Declare the partition
  columns with `PARTITIONED BY`. Beacon then skips whole directories. See
  [`CREATE EXTERNAL TABLE`](/docs/2.0.0-rc2/sql/create-table).
- **Do not use very many small files.** The cost of each file dominates. A merge into fewer files
  helps more than any change to a query.
- **Use a cloud-optimized format** for data on object storage. See
  [Query Data on S3](/docs/2.0.0-rc2/guides/query-s3).

## 5. Merge large array collections with Atlas

Do you query a large NetCDF or Zarr collection often? Then convert it to
[Atlas](/docs/2.0.0-rc2/formats/atlas). This gives the largest gain. Atlas
keeps statistics for each dataset. Beacon drops the datasets that cannot match a predicate. It drops
them *before it reads any array data*. It then reads only the arrays that you select.

```sql
CREATE EXTERNAL TABLE sensor_atlas
STORED AS ATLAS
LOCATION 'collections/*/atlas.json';
```

## 6. Compute once, query many times

Does the same expensive aggregate run often? Then compute it once.

A [materialized view](/docs/2.0.0-rc2/sql/create-materialized-view) caches the result. You
refresh it on demand:

```sql
CREATE MATERIALIZED VIEW monthly_means AS
SELECT date_trunc('month', time) AS month, avg(temperature) AS t
FROM read_netcdf('argo/**/*.nc')
GROUP BY 1;

REFRESH MATERIALIZED VIEW monthly_means;
```

Use a [managed table](/docs/2.0.0-rc2/internals/storage) for a working subset
that you also change:

```sql
CREATE TABLE hot_profiles AS
SELECT * FROM read_netcdf('argo/**/*.nc') WHERE temperature > 25;
```

## 7. Stream instead of collect

A query can plan fast but return slowly. The client then holds too much data. Read batches instead
of the whole result:

```python
import adbc_driver_flightsql.dbapi as flight_sql

conn = flight_sql.connect("grpc+tls://beacon.example.com:32011")
cursor = conn.cursor()
cursor.execute("SELECT * FROM obs")

while (batch := cursor.fetch_record_batch().read_next_batch()) is not None:
    process(batch)
```

See [Export Query Results](/docs/2.0.0-rc2/guides/export-results).

## Server-side tuning

The server adds more settings. They cover concurrency, memory, disk spill, reader caches and
object store listings.
[Performance Tuning](/docs/2.0.0-rc2/server/performance-tuning) documents them.
