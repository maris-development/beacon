---
description: Beacon reads an Apache Iceberg table in place. Register the table, or read it with read_iceberg.
---

# Apache Iceberg

```sql
CREATE EXTERNAL TABLE ocean_profiles
STORED AS ICEBERG
LOCATION 'iceberg/ocean_profiles'
```

An Iceberg table is a directory. The directory holds a `metadata` directory and Parquet data files.
The metadata names the exact files of one snapshot. Beacon reads that metadata. Beacon then reads
only the files that the snapshot names.

Beacon reads an Iceberg table. Another system writes the table. See [Limitations](#limitations).

An Iceberg table works on local storage and on object storage. Beacon resolves the location against
the datasets root. Every other source works the same way.

:::tip External, managed and Iceberg tables

- An [**external table**](/docs/2.0.0-rc4/data-sources/external-tables) reads a directory or a glob
  of files in place. It is read-only.
- A [**managed table**](/docs/2.0.0-rc4/sql/managed-tables) belongs to Beacon. Lance holds the data.
  Change the rows with `INSERT`, `UPDATE` and `DELETE`.
- An **Iceberg table** points to an Iceberg table directory. Beacon reads the table in place. Beacon
  reads each new snapshot.
:::

:::info The Iceberg table must exist
Beacon registers and reads a table that already exists. Beacon writes no Iceberg table. Create the
table with an Iceberg writer. Spark, PyIceberg and iceberg-rust are examples. Then register the
table in Beacon.
:::

## Two ways to read an Iceberg table

### Direct read with `read_iceberg`

Read an Iceberg table in a `FROM` clause. You register nothing first. Use this form for one query.
See the [`read_iceberg`](/docs/2.0.0-rc4/sql/table-functions#read-iceberg) table function.

```sql
SELECT count(*) FROM read_iceberg('iceberg/ocean_profiles');
```

### Registered external table

`CREATE EXTERNAL TABLE … STORED AS ICEBERG` puts the table in the catalog. You then read the table
by name. You also join the table with any other table. Beacon loads the table again after a restart.

Send the statement through any SQL interface of Beacon:

- **HTTP**: `POST /api/query` with `{ "sql": "CREATE EXTERNAL TABLE ... STORED AS ICEBERG ..." }`
- **Arrow Flight SQL**: any Flight SQL client. DataGrip, ADBC and DBeaver are examples.

:::info
Only an admin creates an external table. A statement over the HTTP API needs the SQL interface. That
interface is on by default (`BEACON_ENABLE_SQL`). Arrow Flight SQL does not need this flag.
:::

## Register an Iceberg table

```sql
CREATE EXTERNAL TABLE <name>
STORED AS ICEBERG
LOCATION '<table-directory>'
OPTIONS ('snapshot_id' '3821550127947089060')
```

### `LOCATION`

`LOCATION` gives the Iceberg table directory. That directory holds `metadata`. A file format table
takes a glob. An Iceberg table takes no glob. Give no metadata file:

```sql
CREATE EXTERNAL TABLE ocean_profiles
STORED AS ICEBERG
LOCATION 'iceberg/ocean_profiles'
```

Beacon resolves the location against the datasets root. The default Docker container uses
`/beacon/data/datasets`. An object store uses the configured bucket and prefix. Beacon reads the
columns from the table metadata. Declare no columns.

Beacon finds the current metadata file. Beacon reads `metadata/version-hint.text` first. Beacon
takes the metadata file with the highest number if that file is absent.

### A table from another system

Iceberg metadata holds absolute paths. Those paths name the machine or the bucket of the writer.
Beacon rarely reads the files at those paths. Beacon maps each path onto your location. Beacon
removes the table root from the path. Beacon reads the rest under `LOCATION`.

One example. A writer writes a table to `/warehouse/obs`. You mount the table at `datasets://obs`.
Beacon reads the table. You change no metadata.

### `OPTIONS`

Pin the table to one snapshot:

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `snapshot_id` | Whole number | The current snapshot | An Iceberg snapshot id, for example `'3821550127947089060'`. |

See [`OPTIONS`](/docs/2.0.0-rc4/sql/create-external-table#options) for the rules that hold for every key.

```sql
-- Read the table at one snapshot
CREATE EXTERNAL TABLE ocean_profiles_v1
STORED AS ICEBERG
LOCATION 'iceberg/ocean_profiles'
OPTIONS ('snapshot_id' '3821550127947089060');
```

`read_iceberg` takes the same id as a second argument:

```sql
SELECT count(*) FROM read_iceberg('iceberg/ocean_profiles', 3821550127947089060);
```

Beacon reads the current snapshot if you set no `snapshot_id`.

## Beacon reads each new snapshot

Beacon reads the table metadata for each query. Another system commits a snapshot. Another system
adds a column. The next query shows both changes. You restart nothing:

```sql
-- Spark commits a snapshot and adds a `qc_flag` column.
SELECT qc_flag, count(*) FROM ocean_profiles GROUP BY qc_flag;
```

A row from an older snapshot holds no value in a new column. Beacon reads `NULL` for that row.

## Filters and columns

Beacon sends a `WHERE` clause to the Iceberg scan. Iceberg compares the clause against the column
statistics in the manifests. Iceberg drops each data file that cannot match. Beacon then opens fewer
files. `EXPLAIN` shows the clause on the scan:

```sql
EXPLAIN SELECT * FROM ocean_profiles WHERE depth < 100;
-- IcebergTableScan projection:[...] predicate:[depth < 100]
```

A short `SELECT` list reads fewer columns.

## Storage

Beacon reads an Iceberg table through the datasets store. The table works the same way on both
backends:

- **Local file system**: under the configured datasets directory.
- **Object storage**: under the configured bucket and prefix. Beacon makes no local copy.

Beacon needs no Iceberg configuration. Beacon needs no second set of credentials. See
[Configuration](/docs/2.0.0-rc4/server/configuration) for the storage setup.

## Table information

An Iceberg table behaves like any other registered table:

```http
GET /api/tables
GET /api/table-schema?table_name=ocean_profiles
```

The [`CREATE EXTERNAL TABLE`](/docs/2.0.0-rc4/sql/create-external-table#querying-and-inspecting)
reference gives the SQL forms, `SHOW TABLES` and `DESCRIBE`.

## Remove an Iceberg table

`DROP TABLE` removes the table from the catalog. Beacon deletes no file of the table:

```sql
DROP TABLE ocean_profiles;
```

## Limitations

- **Read-only.** Beacon writes no Iceberg table. `INSERT`, `UPDATE`, `DELETE`, `MERGE` and snapshot
  expiry fail. Use a [managed table](/docs/2.0.0-rc4/sql/managed-tables) to change rows. You also
  write with Spark or PyIceberg.
- **No catalog.** A location names a table. Beacon supports no REST catalog and no Glue catalog.
  Give the directory of the table.
- **The table must exist.** Beacon creates no empty table from a column list.
  `CREATE TABLE AS … STORED AS ICEBERG` fails.
- **One directory per table.** An Iceberg table maps to one directory with `metadata`. A glob over
  many tables fails.

## Read the columns first

Read the columns and the types before you write a query:

```sql
SELECT * FROM read_iceberg_schema('iceberg/ocean_profiles');
```

[Inspect a schema](/docs/2.0.0-rc4/formats/inspect-a-schema) compares the `_schema` functions,
`SUMMARIZE`, `DESCRIBE` and `LIMIT 0`. It also gives the cost of each one.
