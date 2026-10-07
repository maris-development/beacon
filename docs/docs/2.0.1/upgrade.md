---
description: Move a Beacon 1.8.0 server to 2.0.0. The catalog, the settings, the SQL and the clients that change, and what to do for each one.
---

# Upgrade from 1.8.0

A 1.8.0 server does not upgrade in place. This page lists each change that can stop a 1.8.0 setup,
and what to do for it. The [changelog](/docs/changelog/) lists all the changes.

## What does not move

A 2.0.0 server keeps its state in one file, `tables/beacon.db`, below `BEACON_DATA_DIR`. That file
holds the catalog, the managed table data, and the users, roles and grants.

A 2.0.0 server does not read the state of a 1.8.0 server:

- The table definitions in the `tables/` directory.
- The rows of the managed tables.
- The users in `users/directory.db`.

Your data files do not change. A 2.0.0 server reads them in place, from the same datasets store.

## Before you start

1. Save the SQL statements that created your external tables, views, materialized views, managed
   tables and crawlers.
2. Save the statements that created your users, roles and grants.
3. Download the rows of each managed table as Parquet, with `"output": { "format": "parquet" }`.
4. Stop the 1.8.0 server.
5. Make a copy of the data directory (`BEACON_DATA_DIR`, `./data` by default).

`GET /api/admin/table-config?table_name=<name>` on the 1.8.0 server shows the configuration of a
table. Use it if you did not save a statement.

## Start the 2.0.0 server

1. Change the image tag to `ghcr.io/maris-development/beacon:v2.0.0`.
2. Mount an empty directory at `/beacon/data/tables`. Keep the 1.8.0 directory as a backup.
3. Change the settings. See [Settings](#settings).
4. Start the server.
5. Run your saved statements again. Read [SQL](#sql) first.
6. Put each managed table back with
   [`CREATE TABLE AS SELECT`](/docs/2.0.1/sql/managed-tables#create-table-as-select) over the
   Parquet files.

## Settings

### A new name

| 1.8.0 | 2.0.0 | Note |
| --- | --- | --- |
| `BEACON_S3_DATA_LAKE` | `BEACON_S3_DATASETS` | The old name still works. Beacon logs a warning at startup. |

### New defaults

These settings are new in 2.0.0. Their defaults change how Beacon reads your files.

| Setting | Default | Effect |
| --- | --- | --- |
| `BEACON_NETCDF_USE_RUST_READER` | `true` | Beacon reads netCDF with a pure-Rust reader, not with the netCDF-C library. Set `false` to use netCDF-C. |
| `BEACON_HDF5_USE_RUST_READER` | `true` | The same for HDF5. |
| `BEACON_FILE_STATS_ENABLE` | `true` | A background task records the column ranges of each file. See [File statistics](/docs/2.0.1/internals/file-statistics). |

See [Configuration](/docs/2.0.1/server/configuration) for each new setting.

### Settings that 2.0.0 does not read

Beacon ignores these settings. It does not stop with an error. Delete them from your configuration.

| Setting | What to do |
| --- | --- |
| `BEACON_DEFAULT_TABLE_ENGINE` | Nothing. Lance is the only engine for managed tables. |
| `BEACON_ENABLE_FS_EVENTS` | Use a [crawler](/docs/2.0.1/server/crawlers) to find new files. |
| `BEACON_ENABLE_S3_EVENTS` | Use a [crawler](/docs/2.0.1/server/crawlers) to find new files. |
| `BEACON_SANITIZE_SCHEMA` | Nothing. |
| `BEACON_ST_WITHIN_POINT_CACHE_SIZE` | Nothing. The function is gone. See [Spatial functions](#spatial-functions). |
| `BEACON_NETCDF_USE_READER_CACHE` | Nothing. The schema cache (`BEACON_FILE_STATS_SCHEMA_CACHE`) does this work. |
| `BEACON_NETCDF_READER_CACHE_SIZE` | Nothing. |
| `BEACON_ATLAS_USE_READER_CACHE` | Nothing. The Atlas reader keeps one cache for each server. |
| `BEACON_ATLAS_READER_CACHE_SIZE` | Nothing. |
| `BEACON_ENABLE_BBF_SPLIT_STREAMS_SLICE` | Nothing. |
| `BEACON_UPLOAD_PART_SIZE` | Nothing. An upload part is 32 MiB. |
| `BEACON_UPLOAD_SESSION_TTL_SECS` | Nothing. An upload session lasts one hour. |

## SQL

### A name that a table holds

`CREATE EXTERNAL TABLE` and `CREATE VIEW` refuse a name that a table or a view holds. In 1.8.0,
both statements replaced the old table with no warning.

- Add [`IF NOT EXISTS`](/docs/2.0.1/sql/create-external-table#if-not-exists) to keep the old table.
- Add [`OR REPLACE`](/docs/2.0.1/sql/create-external-table#or-replace) to replace it.

### Spatial functions

2.0.0 does not have `st_within_point` and `st_geojson_as_wkt`. A query that calls one of them fails.
Use the PostGIS functions:

```sql
-- 1.8.0
st_within_point('<wkt>', lon, lat)
st_geojson_as_wkt('<geojson>')

-- 2.0.0
ST_Within(ST_Point(lon, lat), ST_GeomFromText('<wkt>'))
ST_GeomFromGeoJSON('<geojson>')
```

The GeoJSON filter of the JSON query does not change. See
[Spatial Functions](/docs/2.0.1/sql/spatial-functions).

### Atlas collections

- A `LOCATION` names the container file: `LOCATION 'obs/data.atlas'`, or a glob such as
  `'obs/**/data.atlas'`.
- 2.0.0 reads only a collection from Atlas 0.17 or later. Write an older collection again with
  `atlas create`.
- A dataset attribute is a column with a dot in front, such as `".platform"`.
- A query must name its columns. `SELECT *` and `count(*)` fail. Count a named column instead.

See [Atlas](/docs/2.0.1/formats/atlas).

### BBF files

A query must name its columns. `SELECT *` and `count(*)` fail. Count a named column instead. See
[BBF](/docs/2.0.1/formats/bbf).

### `list_datasets`

- The rows come in no fixed order. Add `ORDER BY file_name` for a sorted result.
- A listing error stops the query. In 1.8.0, a timeout ended the listing and returned a part of it.

See [`list_datasets`](/docs/2.0.1/sql/table-functions-utility#list-datasets).

## API

`GET /api/admin/table-config` returns only a notice. Use `SHOW CREATE TABLE <table>` or
`GET /api/admin/table-definition` to see the statement that created a table.

## Clients

The terminal client is now the `beacon-datalake-cli` package, with the module
`beacon_datalake_cli`. The `beacon-cli` package gets no updates. See [CLI](/docs/2.0.1/connect/cli).

## Build from source

- The minimum Rust version is 1.94.
- `ST_Transform` links PROJ. A build needs PROJ 9.6.2 or later, and `pkg-config`.
- The server binary is `beacon-server`. In 1.8.0 it was `beacon-api`. Build it with
  `cargo build --release -p beacon-server`.
