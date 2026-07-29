---
description: Everything BeaconDB can query. Read external files from disk or object storage, connect to Postgres and MySQL, federate against other Beacons, or store data in BeaconDB's own beacon.db format.
---

# Data Sources

BeaconDB queries data wherever it lives. Most sources are read **in place**, with no import step: you
point at a path or a connection and query it with SQL. When you want BeaconDB to own the data
instead, it has [its own internal format](/docs/2.0.0/beacondb/data-sources/internal-format).

| Source | Read with | Chapter |
| --- | --- | --- |
| Files on local disk | `read_*()` functions or external tables | [File Formats](/docs/2.0.0/beacondb/data-sources/formats/) |
| Object storage (S3, GCS, Azure) | the same, with an `s3://` style path | [Object Storage](/docs/2.0.0/beacondb/data-sources/object-storage) |
| A named, reusable set of files | `CREATE EXTERNAL TABLE` | [External Tables](/docs/2.0.0/beacondb/data-sources/external-tables) |
| Postgres, MySQL, ODBC | federated external tables | [SQL Databases](/docs/2.0.0/beacondb/data-sources/sql-databases) |
| A single table on another Beacon | `STORED AS REMOTE` | [Remote Tables](/docs/2.0.0/beacondb/data-sources/remote-tables) |
| An entire remote Beacon catalog | `ATTACH` | [ATTACH](/docs/2.0.0/beacondb/data-sources/attach) |
| Data BeaconDB owns and can mutate | managed tables in `beacon.db` | [Internal Format](/docs/2.0.0/beacondb/data-sources/internal-format) |

## Reading files

Every supported format has a `read_*` table function that takes a path or a glob, usable directly in
a `FROM` clause:

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

Globs (`*`, `**`) expand across directories, so a single query can span thousands of files. Beacon
merges their schemas and prunes files that cannot match your filters. Array formats such as
[Zarr](/docs/2.0.0/beacondb/data-sources/formats/zarr) and
[Atlas](/docs/2.0.0/beacondb/data-sources/formats/atlas) point at their marker file (`zarr.json`,
`atlas.json`) rather than at individual chunks.

Each format has its own chapter covering read behaviour, attribute columns, and limitations. See
[File Formats](/docs/2.0.0/beacondb/data-sources/formats/) for the full list, or the
[table functions reference](/docs/2.0.0/beacondb/sql/table-functions) for every signature in one
place.

## Functions or tables?

Both read the same files the same way. The difference is only whether the source has a name:

- A **`read_*()` call** is ideal for ad-hoc queries, notebooks, and exploration.
- An **[external table](/docs/2.0.0/beacondb/data-sources/external-tables)** registers a stable,
  reusable name in the catalog, which is better when many queries share one source, or when you want
  to hand a colleague a table name instead of a glob.

```sql
CREATE EXTERNAL TABLE ocean_profiles
STORED AS PARQUET
LOCATION 'profiles/';

SELECT * FROM ocean_profiles LIMIT 10;
```

Either way, BeaconDB reads the bytes on demand and never copies your data.

## Combining sources

Because everything becomes a table, sources compose in a single query. You can join a local NetCDF
collection against a lookup table in Postgres, or against a table on a remote Beacon:

```sql
SELECT o.platform, avg(o.temperature) AS t, s.station_name
FROM read_netcdf('argo/**/*.nc') o
JOIN stations s ON o.station_id = s.id
GROUP BY o.platform, s.station_name;
```

If files share a concept but not an identical column set, combine them with
[`UNION BY NAME`](/docs/2.0.0/beacondb/sql/union-by-name).

## Where paths resolve

A path may point at local disk or at object storage:

- **Local disk**, for example `argo/**/*.nc`, resolved against BeaconDB's storage root.
- **Object storage**, for example `s3://my-bucket/obs/*.parquet`. See
  [Object Storage](/docs/2.0.0/beacondb/data-sources/object-storage).

Access keys for object stores and remote Beacons are stored as named, scoped
[secrets](/docs/2.0.0/beacondb/sql/secrets) rather than scattered environment variables, and can be
persisted encrypted inside the `beacon.db` file.
