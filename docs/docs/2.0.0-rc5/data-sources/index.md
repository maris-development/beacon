---
description: Everything that Beacon can query. Files on disk or object storage, Postgres and MySQL, other Beacons, and the internal beacon.db format.
---

# Data Sources

Beacon queries data where it lives. It reads most sources **in place**, with no import step. Point
at a path or a connection. Then query it with SQL. Beacon can also own the data. It then uses
[its own internal format](/docs/2.0.0-rc5/internals/storage).

| Source | Read with | Chapter |
| --- | --- | --- |
| Files on local disk | `read_*()` functions or external tables | [File Formats](/docs/2.0.0-rc5/formats/) |
| Datasets on an S3 bucket | the same relative paths; the store is set at startup | [Object Storage](/docs/2.0.0-rc5/data-sources/object-storage) |
| A named set of files | `CREATE EXTERNAL TABLE` | [External Tables](/docs/2.0.0-rc5/data-sources/external-tables) |
| Postgres, MySQL, ODBC | federated external tables | [SQL Databases](/docs/2.0.0-rc5/data-sources/sql-databases) |
| One table on another Beacon | `STORED AS REMOTE` | [Remote Tables](/docs/2.0.0-rc5/data-sources/remote-tables) |
| A whole remote Beacon catalog | `ATTACH` | [ATTACH](/docs/2.0.0-rc5/data-sources/attach) |
| Data that Beacon owns and can change | managed tables in `beacon.db` | [Storage internals](/docs/2.0.0-rc5/internals/storage) |

## Read files

Every supported format has a `read_*` table function. The function takes a path or a glob. Use it
directly in a `FROM` clause:

```sql
-- one file
SELECT * FROM read_parquet('profiles/2024.parquet') LIMIT 10;

-- a glob across many files
SELECT time, latitude, longitude, temperature
FROM read_netcdf('argo/**/*.nc')
WHERE temperature > 20;

-- a list of paths or globs
SELECT * FROM read_csv(['a.csv', 'b.csv']);
```

A glob (`*`, `**`) expands across directories. One query can therefore cover thousands of files.
Beacon merges their schemas. It also prunes the files that cannot match your filters. Array formats
such as [Zarr](/docs/2.0.0-rc5/formats/zarr) and
[Atlas](/docs/2.0.0-rc5/formats/atlas) use a marker file. Point at `zarr.json`
or `atlas.json`, not at the chunks.

Each format has its own chapter. The chapter covers the read behaviour, the attribute columns and
the limitations. See [File Formats](/docs/2.0.0-rc5/formats/) for the full
list. The [table functions reference](/docs/2.0.0-rc5/sql/table-functions) holds every
signature in one place.

## Functions or tables?

Both read the same files in the same way. Only the name makes the difference:

- A **`read_*()` call** fits ad-hoc queries, notebooks and exploration.
- An **[external table](/docs/2.0.0-rc5/data-sources/external-tables)** puts a stable name
  in the catalog. Use it when many queries share one source. It also lets you give a colleague a
  table name instead of a glob.

```sql
CREATE EXTERNAL TABLE ocean_profiles
STORED AS PARQUET
LOCATION 'profiles/';

SELECT * FROM ocean_profiles LIMIT 10;
```

Both ways read the bytes on demand. Beacon never copies your data.

## Combine sources

Every source becomes a table. You can therefore combine sources in one query. Join a local NetCDF
collection against a lookup table in Postgres. Or join it against a table on a remote Beacon:

```sql
SELECT o.platform, avg(o.temperature) AS t, s.station_name
FROM read_netcdf('argo/**/*.nc') o
JOIN stations s ON o.station_id = s.id
GROUP BY o.platform, s.station_name;
```

Some files share a concept but have different columns. Combine those files with
[`UNION BY NAME`](/docs/2.0.0-rc5/sql/union-by-name).

## Where paths resolve

A path points at local disk or at object storage:

- **Local disk**, for example `argo/**/*.nc`. Beacon resolves the path against its storage root.
- **The datasets store**, whether that is a local directory or an S3 bucket. Paths are relative to its root either way, for example `obs/*.parquet`. See
  [Object Storage](/docs/2.0.0-rc5/data-sources/object-storage).

Beacon holds the access keys for object stores and remote Beacons as named, scoped
[secrets](/docs/2.0.0-rc5/sql/secrets). Secrets replace scattered environment variables.
Beacon can store a secret encrypted inside the `beacon.db` file.
