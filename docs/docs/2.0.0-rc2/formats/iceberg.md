---
description: Read an Apache Iceberg table in place. Register it with CREATE EXTERNAL TABLE STORED AS ICEBERG, or query it ad-hoc with read_iceberg.
---

# Apache Iceberg

```sql
CREATE EXTERNAL TABLE ocean_profiles
STORED AS ICEBERG
LOCATION 'iceberg/ocean_profiles'
```

An **Iceberg table** is a directory. It holds a `metadata` directory next to its Parquet data
files. The metadata names the exact files of a snapshot. Beacon reads that metadata. You therefore
get a consistent snapshot, file pruning from column statistics, and a schema that follows the
table.

Beacon **reads** an Iceberg table. Another system writes it. See
[Limitations](#limitations).

An Iceberg table works on local storage and on object storage. Beacon resolves the path against its
dataset storage root, like every other source.

:::tip External vs managed vs Iceberg

- An [**external table**](/docs/2.0.0-rc2/data-sources/external-tables) (`STORED AS
  PARQUET`, `NETCDF`, …) reads a folder or a glob of files in place. It is read-only.
- A [**managed table**](/docs/2.0.0-rc2/sql/managed-tables) belongs to Beacon. Lance holds the
  data. You can change the rows with `INSERT`, `UPDATE` and `DELETE`.
- An **Iceberg table** points at an existing Iceberg table directory. Beacon reads it in place, and
  follows it as the writer commits new snapshots.
:::

:::info The Iceberg table must already exist
Beacon registers and reads an **existing** Iceberg table. Beacon writes no Iceberg table. Create the
table with any Iceberg writer, such as Spark, PyIceberg or iceberg-rust. Then register it in Beacon.
:::

## Two ways to query an Iceberg table

### Ad-hoc with `read_iceberg`

Query an Iceberg table directly in a `FROM` clause. You register nothing first. This helps during
exploration. See the [`read_iceberg`](/docs/2.0.0-rc2/sql/table-functions#read-iceberg) table
function.

```sql
SELECT count(*) FROM read_iceberg('iceberg/ocean_profiles');
```

### Persisted external table

`CREATE EXTERNAL TABLE … STORED AS ICEBERG` puts the table in the catalog. You can then run `SELECT`
and `JOIN` on it. Beacon reloads it after a restart, like any other table.

You can send the DDL through any SQL interface of Beacon:

- **HTTP**: `POST /api/query` with `{ "sql": "CREATE EXTERNAL TABLE ... STORED AS ICEBERG ..." }`
- **Arrow Flight SQL**: any Flight SQL client, such as DataGrip, ADBC or DBeaver

:::info
Only an admin can create an external table. DDL over the HTTP API needs the SQL interface. That
interface is on by default (`BEACON_ENABLE_SQL`). Arrow Flight SQL does not need this flag.
:::

## Define an Iceberg external table

```sql
CREATE EXTERNAL TABLE <name>
STORED AS ICEBERG
LOCATION '<table-directory>'
OPTIONS ('snapshot_id' '3821550127947089060')
```

### `LOCATION`

The `LOCATION` points at the **Iceberg table directory**. That directory holds `metadata/`. This
differs from a file format table. Do not give the metadata file, a folder of loose files, or a glob:

```sql
CREATE EXTERNAL TABLE ocean_profiles
STORED AS ICEBERG
LOCATION 'iceberg/ocean_profiles'
```

Beacon resolves the path against its dataset storage root. In the default Docker container the root
is `/beacon/data/datasets`. On object storage the root is the S3 prefix. Beacon reads the schema
from the table metadata. You declare no columns.

Beacon finds the current metadata file itself. It reads `metadata/version-hint.text` when the writer
keeps one. Otherwise it takes the highest-numbered `*.metadata.json` in the metadata directory.

### A table written somewhere else

Iceberg metadata records **absolute** paths. Those paths name the machine or the bucket the writer
used, which is rarely how Beacon reaches the same bytes. Beacon rebases them: it strips the table
root the metadata declares, and reads the remainder under the `LOCATION` you gave. A table written to
`/warehouse/obs` and mounted at `datasets://obs` therefore reads with no rewrite of its metadata.

### `OPTIONS`, time travel

Pin the table to an older snapshot:

| Option | Description |
| ------------- | ------------------------------------------------------------------- |
| `snapshot_id` | An Iceberg snapshot id, for example `'3821550127947089060'`. |

```sql
-- Register the table as it looked at one snapshot
CREATE EXTERNAL TABLE ocean_profiles_v1
STORED AS ICEBERG
LOCATION 'iceberg/ocean_profiles'
OPTIONS ('snapshot_id' '3821550127947089060');
```

`read_iceberg` takes the same selector as a second argument:

```sql
SELECT count(*) FROM read_iceberg('iceberg/ocean_profiles', 3821550127947089060);
```

Without a `snapshot_id`, the table always follows the current snapshot.

## The table follows its writer

A registered Iceberg table re-reads the table metadata on every query. A snapshot another system
commits, **and a column it adds**, therefore show up on the next query. You restart nothing and you
re-create nothing:

```sql
-- Spark appends a snapshot and adds a `qc_flag` column...
SELECT qc_flag, count(*) FROM ocean_profiles GROUP BY qc_flag;  -- ...and Beacon sees both
```

Rows written before a column existed read as `NULL`.

## Pushdown

A `WHERE` clause reaches the Iceberg scan. Iceberg compares the predicate against the column
statistics in its manifests and drops the data files that cannot match, before any Parquet file is
opened. `EXPLAIN` shows the predicate on the scan node:

```sql
EXPLAIN SELECT * FROM ocean_profiles WHERE depth < 100;
-- IcebergTableScan projection:[...] predicate:[depth < 100]
```

A narrow `SELECT` reads fewer columns the same way.

## Storage backends

Beacon resolves an Iceberg table through its dataset store. An Iceberg table therefore works the same
way on both backends:

- **Local file system**: under the configured datasets directory.
- **Object storage**: under the configured bucket and prefix. A table on S3 reads with no local copy.

You need no Iceberg configuration and no separate credentials. Beacon reads the table location like
any other dataset path. See [Configuration](/docs/2.0.0-rc2/server/configuration) for the storage
setup.

## Query and inspect

An Iceberg table behaves like any other registered table:

```http
GET /api/tables
GET /api/table-schema?table_name=ocean_profiles
```

The [`CREATE EXTERNAL TABLE`](/docs/2.0.0-rc2/sql/create-table#querying-and-inspecting)
reference gives the SQL equivalents, `SHOW TABLES` and `DESCRIBE`.

## Remove an Iceberg table

`DROP TABLE` removes the table from the catalog. Beacon does **not** delete the Iceberg table
directory or its files.

```sql
DROP TABLE ocean_profiles;
```

## Limitations

- **Read only.** Beacon does not write an Iceberg table. `INSERT`, `UPDATE`, `DELETE`, `MERGE` and
  snapshot expiry are not supported. Use a [managed table](/docs/2.0.0-rc2/sql/managed-tables) to
  change rows, or write with Spark or PyIceberg.
- **No catalog.** A table is named by its location. A REST catalog and a Glue catalog are not
  supported yet, so a table a catalog manages must be addressed by its directory.
- **The Iceberg table must already exist.** Beacon does not create an empty Iceberg table from a
  column list, and does not support `CREATE TABLE AS … STORED AS ICEBERG`.
- **One directory per table.** An Iceberg table maps to one table directory with `metadata/`. It is
  not a glob over many tables.

## Inspect the schema

Check the columns and the types before you write a query:

```sql
SELECT * FROM read_iceberg_schema('iceberg/ocean_profiles');
```

[Inspect a schema](/docs/2.0.0-rc2/formats/inspect-a-schema) compares the `_schema` functions,
`SUMMARIZE`, `DESCRIBE` and `LIMIT 0`, and says what each one costs.
