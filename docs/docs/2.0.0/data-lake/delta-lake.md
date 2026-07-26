# Delta Lake

```sql
CREATE EXTERNAL TABLE ocean_profiles
STORED AS DELTA
LOCATION 'delta/ocean_profiles'
```

A **Delta Lake table** is a directory containing a `_delta_log` transaction log alongside its Parquet data files — not a single file or a glob of files. Beacon reads the transaction log to resolve exactly which Parquet files make up the current (or a historical) version of the table, so you get consistent snapshots, **time travel**, and `INSERT` support that ordinary file-format tables don't have.

Delta tables work over both local storage and S3 / object storage, resolved against Beacon's configured dataset storage root just like every other source.

:::tip External vs managed vs Delta
- An [**external table**](./external-tables.md) (`STORED AS PARQUET`, `NETCDF`, …) reads a folder/glob of files in place; it is read-only.
- A [**managed table**](../sql/managed-tables.md) is owned by Beacon (Lance-backed by default, or Iceberg) and mutable with `INSERT` / `UPDATE` / `DELETE`.
- A **Delta table** points at an existing Delta Lake table directory. It is read in place, supports snapshot-consistent reads and time travel, and accepts `INSERT INTO` which commits a new Delta version.
:::

:::info The Delta table must already exist
Beacon registers and reads (and appends to) an **existing** Delta table. It does not yet create a brand-new, empty Delta table from a `CREATE EXTERNAL TABLE` column list, and `CREATE TABLE AS … STORED AS DELTA` is not supported — use a managed table for those. Create the Delta table with any Delta writer (delta-rs, Spark, etc.), then register it in Beacon.
:::

## Two ways to query a Delta table

### Ad-hoc with `read_delta`

Query a Delta table directly in a `FROM` clause without registering it first — useful for exploration. See the [`read_delta`](../sql/table-functions.md#read_delta) table function.

```sql
SELECT count(*) FROM read_delta('delta/ocean_profiles');
```

### Persisted external table

`CREATE EXTERNAL TABLE … STORED AS DELTA` registers the table in the catalog so it can be `SELECT`ed, `JOIN`ed, inserted into, and reloaded on restart like any other table.

DDL can be submitted through any of Beacon's SQL surfaces:

- **HTTP** — `POST /api/query` with `{ "sql": "CREATE EXTERNAL TABLE ... STORED AS DELTA ..." }`
- **Arrow Flight SQL** — any Flight SQL client (DataGrip, ADBC, DBeaver, …)

:::info
Creating an external table is admin-only DDL. SQL must be enabled (`BEACON_ENABLE_SQL=true`) to run DDL over the HTTP API; Arrow Flight SQL does not require this flag.
:::

## Defining a Delta external table

```sql
CREATE EXTERNAL TABLE <name>
STORED AS DELTA
LOCATION '<table-directory>'
OPTIONS ('version' '12')
```

### `LOCATION`

Unlike file-format tables, the `LOCATION` points at the **Delta table directory** (the folder that contains `_delta_log/`), not a folder of loose files or a glob:

```sql
CREATE EXTERNAL TABLE ocean_profiles
STORED AS DELTA
LOCATION 'delta/ocean_profiles'
```

The path is resolved relative to Beacon's configured dataset storage root (`/beacon/data/datasets` in the default Docker container, or the S3 prefix when using object storage). The table's schema is read from the transaction log — you do not declare columns.

### `OPTIONS` — time travel

Pin the table to a historical snapshot. Use **one** of:

| Option      | Description                                                                 |
| ----------- | --------------------------------------------------------------------------- |
| `version`   | A Delta version number (snapshot), e.g. `'12'`.                             |
| `timestamp` | An RFC-3339 timestamp; the latest version at or before it, e.g. `'2026-01-01T00:00:00Z'`. |

```sql
-- Register the table as it looked at version 12
CREATE EXTERNAL TABLE ocean_profiles_v12
STORED AS DELTA
LOCATION 'delta/ocean_profiles'
OPTIONS ('version' '12');

-- ...or as of a point in time
CREATE EXTERNAL TABLE ocean_profiles_jan
STORED AS DELTA
LOCATION 'delta/ocean_profiles'
OPTIONS ('timestamp' '2026-01-01T00:00:00Z');
```

Without a `version`/`timestamp` option the table always tracks the latest committed version.

## Writing: `INSERT INTO`

A Delta external table accepts `INSERT INTO`, which appends the rows as a **new Delta version** (committed atomically through the transaction log):

```sql
INSERT INTO ocean_profiles
SELECT * FROM read_parquet('staging/new_profiles.parquet');
```

The commit is written through Beacon's storage backend, so it lands in the same local-FS or S3 location as the rest of the table. On S3 the commit uses object-store conditional writes, so no external lock table is required.

:::tip
Pinning the table to a historical `version`/`timestamp` is intended for reading old snapshots; insert into a table registered at the latest version.
:::

## Storage backends

Delta tables resolve through Beacon's dataset store, so they work the same way on:

- **Local filesystem** — under the configured datasets directory.
- **S3 / object storage** — under the configured bucket/prefix.

No Delta-specific configuration is needed; the table location is interpreted exactly like other dataset paths. See [Configuration](./configuration.md) for storage setup.

## Querying and inspecting

A Delta table behaves like any other registered table:

```http
GET /api/tables
GET /api/table-schema?table_name=ocean_profiles
```

For the SQL equivalents (`SHOW TABLES`, `DESCRIBE`), see the [`CREATE EXTERNAL TABLE`](../sql/create-table.md#querying-and-inspecting) reference.

## Removing a Delta table

Dropping the table removes it from the catalog — the underlying Delta table directory and its files are **not** deleted.

```sql
DROP TABLE ocean_profiles;
```

## Limitations

- **The Delta table must already exist.** Beacon does not create an empty Delta table from a column list, and `CREATE TABLE AS … STORED AS DELTA` is not supported. Use a managed table or an external Delta writer to create one.
- **`INSERT` appends.** `UPDATE` / `DELETE` / `MERGE` on Delta tables are not exposed through Beacon's SQL; use a [managed table](../sql/managed-tables.md) when you need full row mutation.
- **One directory per table.** A Delta table maps to a single table directory containing `_delta_log/`; it is not a glob over many tables.
