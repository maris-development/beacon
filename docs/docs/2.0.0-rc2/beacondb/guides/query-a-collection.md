---
description: Query a whole directory of NetCDF, Zarr or Parquet files as one table. This guide covers schema differences and attribute columns.
---

# Query a File Collection

Scientific data comes as *many* files, not as one file. This guide makes one queryable table from a
directory of files.

## 1. Look before you query

Point a reader function at one file first. Profile that file. You then know the column names and
the types:

```python
import beacondb
con = beacondb.connect("beacon.db")

con.sql("SELECT * FROM read_netcdf('argo/2024/01/*.nc') LIMIT 5").df()
con.sql("SUMMARIZE read_netcdf('argo/2024/01/*.nc')").df()
```

[`SUMMARIZE`](/docs/2.0.0-rc2/beacondb/sql/summarize) returns one row for each column. Each row gives
the type, the null count and the range. This is the fastest way to learn an unfamiliar dataset.

## 2. Widen the glob

`**` goes into every subdirectory. One call can therefore cover the whole archive:

```sql
SELECT time, latitude, longitude, temperature
FROM read_netcdf('argo/**/*.nc')
WHERE temperature > 20
  AND time >= '2024-01-01';
```

Beacon merges the schemas of the files. It reads them in parallel. It also prunes the files that
cannot match the filters. Make the `WHERE` clause as specific as possible. The clause lets the
engine skip data.

Pass a list to combine different locations in one query:

```sql
SELECT * FROM read_netcdf(['argo/**/*.nc', 'wod/**/*.nc']);
```

## 3. Handle schema differences

Files from different sources have different columns. `UNION BY NAME` aligns them by column name, not
by position. Beacon sets a missing column to null:

```sql
SELECT time, temperature FROM read_netcdf('argo/**/*.nc')
UNION ALL BY NAME
SELECT time, temperature FROM read_parquet('gliders/*.parquet');
```

See [UNION BY NAME](/docs/2.0.0-rc2/beacondb/sql/union-by-name).

Some NetCDF collections mix variables with different dimensions. Give an explicit dimension list.
Beacon then returns only the compatible variables:

```sql
SELECT * FROM read_netcdf('argo/**/*.nc', ['time', 'pressure']);
```

## 4. Use the metadata

Beacon gives the variable attributes as columns in dot notation. A file attribute uses a leading
dot. Quote these names, because they contain a dot:

```sql
SELECT temperature, "temperature.units", ".source"
FROM read_netcdf('argo/**/*.nc')
LIMIT 1;
```

## 5. Give it a name

Register the glob when it is final. Your queries then no longer repeat the path:

```sql
CREATE EXTERNAL TABLE argo
STORED AS NC
LOCATION 'argo/**/*.nc';

SELECT platform, avg(temperature) FROM argo GROUP BY platform;
```

See [External Tables](/docs/2.0.0-rc2/beacondb/data-sources/external-tables).

## 6. If it is slow

Do you scan thousands of NetCDF or Zarr files often? Then merge them into one
[Atlas](/docs/2.0.0-rc2/beacondb/data-sources/formats/atlas) collection. Atlas drops whole datasets
with its stored statistics. See
[Speed Up Slow Queries](/docs/2.0.0-rc2/beacondb/guides/speed-up-queries).
