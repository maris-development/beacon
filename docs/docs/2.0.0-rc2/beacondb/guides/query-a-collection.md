---
description: Query a whole directory of NetCDF, Zarr, or Parquet files as a single table with BeaconDB, including schema differences, attribute columns, and when to register a table.
---

# Query a File Collection

Scientific data usually arrives as *many* files rather than one. This guide turns a directory of
files into something you can query as a single table.

## 1. Look before you query

Point a reader function at one file first and profile it, so you know the column names and types:

```python
import beacondb
con = beacondb.connect("beacon.db")

con.sql("SELECT * FROM read_netcdf('argo/2024/01/*.nc') LIMIT 5").df()
con.sql("SUMMARIZE read_netcdf('argo/2024/01/*.nc')").df()
```

[`SUMMARIZE`](/docs/2.0.0-rc2/beacondb/sql/summarize) returns one row per column with types, null counts,
and ranges. It is the fastest way to understand an unfamiliar dataset.

## 2. Widen the glob

`**` recurses, so one call can cover the whole archive:

```sql
SELECT time, latitude, longitude, temperature
FROM read_netcdf('argo/**/*.nc')
WHERE temperature > 20
  AND time >= '2024-01-01';
```

Beacon merges the files' schemas, reads them concurrently, and prunes files that cannot satisfy the
filters. Keep the `WHERE` clause as specific as you can: it is what lets the engine skip data.

You can also pass a list to combine unrelated locations in one query:

```sql
SELECT * FROM read_netcdf(['argo/**/*.nc', 'wod/**/*.nc']);
```

## 3. Handle schema differences

Files from different sources rarely have identical columns. `UNION BY NAME` aligns them by column
name instead of position, filling missing columns with nulls:

```sql
SELECT time, temperature FROM read_netcdf('argo/**/*.nc')
UNION ALL BY NAME
SELECT time, temperature FROM read_parquet('gliders/*.parquet');
```

See [UNION BY NAME](/docs/2.0.0-rc2/beacondb/sql/union-by-name).

For NetCDF collections that mix variables with incompatible dimensions, pass an explicit dimension
list so only compatible variables are returned:

```sql
SELECT * FROM read_netcdf('argo/**/*.nc', ['time', 'pressure']);
```

## 4. Use the metadata

Variable attributes are available as columns using dot notation, and file-level attributes with a
leading dot. Quote them because of the dot:

```sql
SELECT temperature, "temperature.units", ".source"
FROM read_netcdf('argo/**/*.nc')
LIMIT 1;
```

## 5. Give it a name

Once the glob is settled, register it so queries stop repeating the path:

```sql
CREATE EXTERNAL TABLE argo
STORED AS NC
LOCATION 'argo/**/*.nc';

SELECT platform, avg(temperature) FROM argo GROUP BY platform;
```

See [External Tables](/docs/2.0.0-rc2/beacondb/data-sources/external-tables).

## 6. If it is slow

A repeated scan over thousands of NetCDF or Zarr files is the classic case for consolidating into an
[Atlas](/docs/2.0.0-rc2/beacondb/data-sources/formats/atlas) collection, which prunes whole datasets
using stored statistics. See
[Speed Up Slow Queries](/docs/2.0.0-rc2/beacondb/guides/speed-up-queries).
