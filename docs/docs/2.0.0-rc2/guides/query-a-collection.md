---
description: Query a whole directory of NetCDF, Zarr or Parquet files as one table. This guide covers schema differences and attribute columns.
---

# Query a File Collection

Scientific data comes as *many* files, not as one file. This guide makes one queryable table from a
directory of files.

It works through one small collection end to end, and shows the result of each step, so you can see
the shape Beacon returns before you run anything.

## The collection

```text
argo/
  2024/01/  R6901234_001.nc  R6901234_002.nc  R6901235_001.nc
  2024/02/  R6901234_003.nc  R6901236_001.nc
  2024/03/  R6901235_002.nc  R6901236_002.nc
```

Seven files, one Argo profile each. Each file holds `N_PROF = 1` and `N_LEVELS = 400`, so each one
carries 400 rows.

## 1. Look before you query

Read the columns and the types first. This opens metadata only:

```sql
SELECT * FROM read_netcdf_schema('argo/2024/01/*.nc');
```

| column_name | data_type | nullable |
|---|---|---|
| PLATFORM_NUMBER | Utf8 | true |
| JULD | Timestamp(Nanosecond) | true |
| LATITUDE | Float64 | true |
| LONGITUDE | Float64 | true |
| PRES | Float64 | true |
| TEMP | Float64 | true |
| PSAL | Float64 | true |

Then look at rows:

```sql
SELECT PLATFORM_NUMBER, JULD, LATITUDE, PRES, TEMP
FROM read_netcdf('argo/2024/01/*.nc')
LIMIT 5;
```

| PLATFORM_NUMBER | JULD | LATITUDE | PRES | TEMP |
|---|---|---|---|---|
| 6901234 | 2024-01-03 06:12:00 | 43.117 | 2.6 | 14.882 |
| 6901234 | 2024-01-03 06:12:00 | 43.117 | 6.1 | 14.879 |
| 6901234 | 2024-01-03 06:12:00 | 43.117 | 10.4 | 14.851 |
| 6901234 | 2024-01-03 06:12:00 | 43.117 | 15.2 | 14.774 |
| 6901234 | 2024-01-03 06:12:00 | 43.117 | 20.0 | 14.702 |

`PLATFORM_NUMBER`, `JULD` and `LATITUDE` repeat, because they are per-profile values broadcast
across the 400 levels. See [Arrays to tables](/docs/2.0.0-rc2/arrays-to-tables).

Check the size:

```sql
SELECT count(*) AS rows FROM read_netcdf('argo/2024/01/*.nc');
```

| rows |
|---|
| 1200 |

Three files, 400 levels each.

[`SUMMARIZE`](/docs/2.0.0-rc2/sql/summarize) profiles every column in one pass. It gives the type,
the null count and the range:

```sql
SUMMARIZE (SELECT * FROM read_netcdf('argo/2024/01/*.nc'));
```

## 2. Widen the glob

`**` goes into every subdirectory. One call can therefore cover the whole archive:

```sql
SELECT count(*) AS rows, count(DISTINCT PLATFORM_NUMBER) AS floats
FROM read_netcdf('argo/**/*.nc');
```

| rows | floats |
|---|---|
| 2800 | 3 |

All seven files, 400 levels each. Now narrow it:

```sql
SELECT PLATFORM_NUMBER, JULD, LATITUDE, LONGITUDE, TEMP
FROM read_netcdf('argo/**/*.nc')
WHERE TEMP > 20
  AND JULD >= '2024-02-01';
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

See [UNION BY NAME](/docs/2.0.0-rc2/sql/union-by-name).

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

See [External Tables](/docs/2.0.0-rc2/data-sources/external-tables).

## 6. If it is slow

Do you scan thousands of NetCDF or Zarr files often? Then merge them into one
[Atlas](/docs/2.0.0-rc2/formats/atlas) collection. Atlas drops whole datasets
with its stored statistics. See
[Speed Up Slow Queries](/docs/2.0.0-rc2/guides/speed-up-queries).
