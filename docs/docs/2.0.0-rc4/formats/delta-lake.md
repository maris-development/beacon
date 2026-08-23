# Delta Lake

```sql
CREATE EXTERNAL TABLE ocean_profiles
STORED AS DELTA
LOCATION 'delta/ocean_profiles'
```

A **Delta Lake table** is a directory. It holds a `_delta_log` transaction log next to its Parquet
data files. It is not one file and not a glob of files. Beacon reads the transaction log. The log
gives the exact Parquet files of the current version or of an older version. You therefore get
consistent snapshots, **time travel** and `INSERT` support. An ordinary file format table does not
give you these.

A Delta table works on local storage and on object storage. Beacon resolves the path against its
dataset storage root, like every other source.

:::tip External vs managed vs Delta

- An [**external table**](/docs/2.0.0-rc4/data-sources/external-tables) (`STORED AS
  PARQUET`, `NETCDF`, …) reads a folder or a glob of files in place. It is read-only.
- A [**managed table**](/docs/2.0.0-rc4/sql/managed-tables) belongs to Beacon. Lance holds
  the data. You can change the rows with `INSERT`, `UPDATE` and `DELETE`.
- A **Delta table** points at an existing Delta Lake table directory. Beacon reads it in place. It
  supports consistent snapshots and time travel. It accepts `INSERT INTO`, which commits a new Delta
  version.
:::

:::info The Delta table must already exist
Beacon registers, reads and appends to an **existing** Delta table. Beacon does not yet create a new,
empty Delta table from a `CREATE EXTERNAL TABLE` column list. Beacon does not support
`CREATE TABLE AS … STORED AS DELTA`. Use a managed table for those two cases. Create the Delta table
with any Delta writer, such as delta-rs or Spark. Then register it in Beacon.
:::

## Two ways to query a Delta table

### Ad-hoc with `read_delta`

Query a Delta table directly in a `FROM` clause. You register nothing first. This helps during
exploration. See the [`read_delta`](/docs/2.0.0-rc4/sql/table-functions#read-delta) table
function.

```sql
SELECT count(*) FROM read_delta('delta/ocean_profiles');
```

### Persisted external table

`CREATE EXTERNAL TABLE … STORED AS DELTA` puts the table in the catalog. You can then run `SELECT`
and `JOIN` on it. You can insert into it. Beacon reloads it after a restart, like any other table.

You can send the DDL through any SQL interface of Beacon:

- **HTTP**: `POST /api/query` with `{ "sql": "CREATE EXTERNAL TABLE ... STORED AS DELTA ..." }`
- **Arrow Flight SQL**: any Flight SQL client, such as DataGrip, ADBC or DBeaver

:::info
Only an admin can create an external table. DDL over the HTTP API needs the SQL interface. That
interface is on by default (`BEACON_ENABLE_SQL`). Arrow Flight SQL does not need this flag.
:::

## Define a Delta external table

```sql
CREATE EXTERNAL TABLE <name>
STORED AS DELTA
LOCATION '<table-directory>'
OPTIONS ('version' '12')
```

### `LOCATION`

The `LOCATION` points at the **Delta table directory**. That directory holds `_delta_log/`. This
differs from a file format table. Do not give a folder of loose files or a glob:

```sql
CREATE EXTERNAL TABLE ocean_profiles
STORED AS DELTA
LOCATION 'delta/ocean_profiles'
```

Beacon resolves the path against its dataset storage root. In the default Docker container the root
is `/beacon/data/datasets`. On object storage the root is the S3 prefix. Beacon reads the schema
from the transaction log. You declare no columns.

### `OPTIONS`

Pin the table to an older snapshot. Use **one** of these options:

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `version` | Whole number | The latest committed version | A Delta version number, for example `'12'`. |
| `timestamp` | RFC-3339 timestamp | The latest committed version | Beacon takes the last version at or before it, for example `'2026-01-01T00:00:00Z'`. |

`version` wins if you set both. See
[`OPTIONS`](/docs/2.0.0-rc4/sql/create-external-table#options) for the rules that hold for every key.

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

Without a `version` or `timestamp` option, the table always follows the latest committed version.

## Write with `INSERT INTO`

A Delta external table accepts `INSERT INTO`. Beacon appends the rows as a **new Delta version**. The
transaction log commits the version atomically:

```sql
INSERT INTO ocean_profiles
SELECT * FROM read_parquet('staging/new_profiles.parquet');
```

Beacon writes the commit through its storage backend. The commit therefore lands in the same local
disk or S3 location as the rest of the table. On S3 the commit uses a conditional write of the
object store. You need no external lock table.

:::tip
Pin a table to an older `version` or `timestamp` to read an old snapshot. Insert only into a table
that Beacon registers at the latest version.
:::

## Storage backends

Beacon resolves a Delta table through its dataset store. A Delta table therefore works the same way
on both backends:

- **Local file system**: under the configured datasets directory.
- **Object storage**: under the configured bucket and prefix.

You need no Delta configuration. Beacon reads the table location like any other dataset path. See
[Configuration](/docs/2.0.0-rc4/server/configuration) for the storage setup.

## Query and inspect

A Delta table behaves like any other registered table:

```http
GET /api/tables
GET /api/table-schema?table_name=ocean_profiles
```

The [`CREATE EXTERNAL TABLE`](/docs/2.0.0-rc4/sql/create-external-table#querying-and-inspecting)
reference gives the SQL equivalents, `SHOW TABLES` and `DESCRIBE`.

## Remove a Delta table

`DROP TABLE` removes the table from the catalog. Beacon does **not** delete the Delta table
directory or its files.

```sql
DROP TABLE ocean_profiles;
```

## Limitations

- **The Delta table must already exist.** Beacon does not create an empty Delta table from a column
  list. Beacon does not support `CREATE TABLE AS … STORED AS DELTA`. Use a managed table or an
  external Delta writer.
- **`INSERT` appends only.** Beacon does not expose `UPDATE`, `DELETE` or `MERGE` on a Delta table.
  Use a [managed table](/docs/2.0.0-rc4/sql/managed-tables) to change rows.
- **One directory per table.** A Delta table maps to one table directory with `_delta_log/`. It is
  not a glob over many tables.

## Inspect the schema

Check the columns and the types before you write a query:

```sql
SELECT * FROM read_delta('delta/ocean_profiles') LIMIT 0;
```

[Inspect a schema](/docs/2.0.0-rc4/formats/inspect-a-schema) compares the `_schema` functions,
`SUMMARIZE`, `DESCRIBE` and `LIMIT 0`, and says what each one costs.
