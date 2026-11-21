# CREATE TABLE (Managed)

```sql
CREATE TABLE observations (id BIGINT, name VARCHAR);
INSERT INTO observations VALUES (1, 'a'), (2, 'b');
SELECT * FROM observations;
```

A **managed table** is a SQL table. Beacon owns and stores its data. An
[external table](/docs/2.1.0-rc3/data-sources/external-tables) only points at existing
files. A managed table starts empty, or Beacon fills it from a query. You add rows with `INSERT`. A
managed table supports `UPDATE`, `DELETE`, schema changes with `ALTER TABLE` and secondary indexes.
The table definition and the data survive a restart.

A managed table needs write access. `CREATE`, `INSERT`, `UPDATE`, `DELETE`, `ALTER`, `CREATE INDEX`,
`DROP INDEX` and `COMPACT TABLE` need admin credentials. Anonymous access stays read-only.

::: tip Names keep their case
A table name means exactly what you write. `MyTable` and `mytable` are two different tables.
See [Identifiers and case](/docs/2.1.0-rc3/sql/identifiers).
:::

## Storage engine

**[Lance](https://lancedb.github.io/lance/)** holds a managed table. Lance is a columnar format with
versions. It supports row-level updates and deletes through deletion vectors and fragment rewrites.
It also supports scalar secondary indexes: btree, bitmap and full text. The table data and the
definitions live in the single-file `db://` store of Beacon (`beacon.db`), next to the `table.json`
of each table. The location of your datasets does not matter. S3 applies to the dataset store only.

## `CREATE TABLE`

Define the columns:

```sql
CREATE TABLE measurements (
  id     BIGINT,
  name   VARCHAR,
  value  DOUBLE
);
```

With `IF NOT EXISTS`, Beacon does nothing if the table already exists:

```sql
CREATE TABLE IF NOT EXISTS measurements (id BIGINT, name VARCHAR);
```

### `CREATE TABLE AS SELECT`

Create a table from a query and fill it (CTAS). The result of the query gives the schema:

```sql
CREATE TABLE warm_profiles AS
SELECT platform, temperature, depth
FROM read_parquet('profiles/*.parquet')
WHERE temperature > 20;
```

## `INSERT INTO`

Append rows from literal values or from a query:

```sql
INSERT INTO measurements VALUES (1, 'argo', 12.5), (2, 'glider', 9.0);

INSERT INTO measurements
SELECT id, name, value FROM staging;
```

A plain `INSERT INTO` matches the columns by position.

## `INSERT INTO ... BY NAME`

`BY NAME` matches the columns of the query to the columns of the table by name. The order of the
columns does not matter:

```sql
INSERT INTO <table> BY NAME <query>
```

```sql
CREATE EXTERNAL TABLE argo_lance (platform VARCHAR, time TIMESTAMP, temp DOUBLE)
STORED AS LANCE LOCATION 'lance/argo.lance';

INSERT INTO argo_lance BY NAME SELECT platform, time, temp, psal FROM argo_netcdf;
```

Only a Lance table adds new columns: a managed Lance table or a
[Lance external table](/docs/2.1.0-rc3/data-sources/external-tables#lance). On other table types,
`BY NAME` only matches by name. A column list together with `BY NAME` is an error. The statement
returns the number of inserted rows, the same as `INSERT INTO`. Beacon streams the rows. It does not
keep the whole query result in memory.

### Match the columns

- On a Lance table, Beacon first replaces each `.` in a query column name with `_`. See
  [Dots in column names](#dots-in-column-names).
- Beacon matches each name exactly. The match is case sensitive, so `Temp` and `temp` are two
  different columns.
- Two query columns with the same name are an error. On a Lance table, this applies to the names
  after the replacement.
- A table column that the query does not have gets `NULL`.
- A table column that is not nullable must be in the query. If not, the statement fails before a
  write.
- A `NULL` value in a column that is not nullable stops the statement with an error. Beacon writes
  no rows.

### Add new columns

A query column that is not in a Lance table becomes a new column of the table. Beacon adds all new
columns in one step, before it writes the rows. A new column is nullable. The rows that the table
holds before the insert read `NULL` in that column. In the example above, `psal` becomes a new
column.

Only a Lance table adds columns. On another table type, a query column that is not in the table is an
error. The error names the columns.

### Dots in column names

Lance does not accept a column name that contains a dot (`.`). On a Lance table, `BY NAME` replaces
each `.` in a query column name with `_`. The query column `temp.units` becomes the Lance column
`temp_units`. Beacon does not keep the old name.

- The new name is the real column name in Lance. Use it in queries on the Lance table.
- A later insert from the same source gives the same names. The columns match again.
- Two query columns can get the same name, for example `a.b` and `a_b`. This is an error. Beacon
  does not write a value to the wrong column.
- On another table type, `BY NAME` does not change the names.

### Cast the values

The session type widening rule decides each cast. Two server variables set it:

- `BEACON_TYPE_WIDENING_STRATEGY` is `default` or `numpy`.
- `BEACON_TYPE_WIDENING_ON_CONFLICT` is `fail` or `keep_first`.

See [Query engine](/docs/2.1.0-rc3/server/configuration#query-engine) for the full rule of each
value. The `type_widening` keys of an external table do not change this rule.

For each column, Beacon merges the type of the table column and the type of the query column with
the rule. The result is the widening type:

- The widening type is the table type. Beacon casts the query column to the table type.
- The widening type is wider than the table type. On a Lance table, Beacon changes the table column
  to the wider type before the insert. The rows that the table holds keep their values in the wider
  type. On another table type, the column type does not change.
- The two types are a conflict, and the rule is `fail`. The statement fails with the error of the
  rule. The table does not change. To insert the values, cast the query column to the table type
  with `CAST`.
- The two types are a conflict, and the rule is `keep_first`. The table type stays. A value that does
  not cast becomes `NULL`. This applies to all table types.

Know these rules:

- Beacon examines each type pair before the insert starts. A pair that cannot cast is an error. The
  table does not change.
- A cast is strict, except for a type pair that the rule casts leniently, as `keep_first` does. A
  value that does not cast in a strict cast stops the statement with an error.
- On a table that is not Lance, a wider query type casts strictly to the table type.
- A cast to a smaller type can lose precision. A `DOUBLE` value in a `FLOAT` column is an example.
- A literal has its own type. `SELECT 1 AS c` gives a `BIGINT`. Under the `default` rule, it widens
  an `INT` Lance column to `BIGINT`. Cast the literal to the column type to keep that type.

### Change a column type

A wider query type changes the type of the Lance column. Lance then writes all the data of that column
again. This costs I/O and storage for the full column. A large table makes this cost high.

- Lance changes a type only inside one type family. `INT` to `BIGINT` and `FLOAT` to `DOUBLE` are
  examples. Other families are time to time, text to text, binary to binary and decimal to decimal.
- A wider type of a different family is an error before a change. A `DOUBLE` query column into a
  `BIGINT` Lance column is an example. The error is `Lance cannot change column ...`.
- To prevent this error, cast the query column to the table type with `CAST` in the select list.
- A column with an index cannot change its type. The statement fails before a change. Drop the index
  with [`DROP INDEX`](#indexes) first. Create it again after the insert.
- Two inserts at the same time never make a column narrower. Beacon merges the types again under the
  write lock. A column that the other insert made wider keeps its wider type.

### Two table versions

Beacon adds the new columns first. Then it changes the types. Then it writes the rows. Each step is a
different Lance version:

- Beacon examines the indexes and the type families before all changes. The index error and the
  `Lance cannot change column` error leave the table without a change.
- If Lance fails to change the types, the new columns stay. The types do not change.
- If the write of the rows fails, the new columns and the new types stay in the table.

The new columns hold only `NULL`. Remove them with `ALTER TABLE ... DROP COLUMN` if necessary.

`EXPLAIN INSERT INTO ... BY NAME` shows how Beacon maps the columns. It shows the new columns and the
planned type changes as `planned widen=[...]`. It does not change the table. EXPLAIN does not check
if Lance accepts a type change or if the column has an index. The statement can still refuse the
change. `EXPLAIN ANALYZE` does not accept `INSERT INTO ... BY NAME`.

### NetCDF sources

A NetCDF external table merges the schemas of its files. A variable that a file does not have reads
`NULL`. This makes `BY NAME` a good way to collect many NetCDF files in one Lance table. Know these
limits:

- A query fails when its columns have disjoint dimensions. `SELECT *` can fail for this reason.
  Select the columns that share dimensions. Or set `read_dimensions` on the NetCDF table. See
  [NetCDF options](/docs/2.1.0-rc3/formats/netcdf#options).
- [Attribute columns](/docs/2.1.0-rc3/formats/netcdf#variable-attributes) (`var.attr`) have a dot
  in the name. `BY NAME` renames them in the Lance table: `temp.units` becomes `temp_units`. See
  [Dots in column names](#dots-in-column-names). Select them as usual:

  ```sql
  INSERT INTO argo_lance BY NAME
  SELECT platform, time, temp, "temp.units" FROM argo_netcdf;
  ```

- Two files can give one variable two types. The NetCDF table merges them with its own
  `type_widening` keys. A later insert can then bring a wider type. This changes the Lance column
  type. See [Change a column type](#change-a-column-type).

### Errors

| Case | Message |
| --- | --- |
| Type pair that cannot cast | `Column '<c>': cannot cast <source type> to <target type>` |
| Column that is not nullable and is not in the query | `Column '<c>' is not nullable and the source does not have it` |
| New columns on a table that is not Lance | `Table '<t>' does not have the columns <list>; only Lance tables add columns` |
| Two query columns with the same name | `The source has the column '<c>' more than one time` |
| Type change of a column with an index | `Column '<c>' has the index '<i>'; drop it before BY NAME changes the type to <type>` |
| Type change that Lance refuses | `Lance cannot change column '<c>' from <old type> to <new type>` |
| Type conflict with the rule `fail` | `Incompatible types for field '<c>': <table type> in '<t>' vs <source type> in 'source query'` |
| Lance refuses to add the columns | `Failed to add the columns ["<c>", ...] to '<uri>': <reason>` |
| Lance refuses to change the types | `Failed to change the types of the columns ["<c>", ...] of '<uri>': <reason>` |
| Write to a pinned Lance table | `Lance table '<name>' is pinned to version <n> and is read-only` |
| Column list after the table name | `INSERT ... BY NAME does not take a column list` |
| Columns added or types changed, but the table did not open again | `The table '<t>' is widened, but it did not reload: <reason>` |

The types in the conflict message use the Arrow names, for example `Int64` and `Utf8`. Under the
write lock, Beacon merges the types again. A conflict at that step gives the same message without
the table name and `'source query'`.

Only the admin can run `INSERT INTO ... BY NAME`, the same as `INSERT INTO`. The
[read rules](/docs/2.1.0-rc3/security/access-control) also apply to the tables of the query.

## `SELECT`

Query a managed table like any other table:

```sql
SELECT name, avg(value) FROM measurements GROUP BY name;
```

## `DELETE`

Delete the rows that match a predicate. Without a `WHERE` clause, Beacon deletes every row:

```sql
DELETE FROM measurements WHERE value IS NULL;

DELETE FROM measurements;        -- empties the table
```

On a **Lance** table this is a native delete with deletion vectors. Beacon does not rewrite the whole
table. On an **Iceberg** table it is copy-on-write.

## `UPDATE`

Change the column values of the matching rows. Beacon does not touch the other rows:

```sql
UPDATE measurements SET name = 'unknown' WHERE name IS NULL;

UPDATE measurements SET value = value * 1.0;   -- every row
```

On a **Lance** table Beacon rewrites only the affected fragments. On **Iceberg** it is copy-on-write.
Beacon does not support `UPDATE ... FROM` or a join in an `UPDATE`.

## `ALTER TABLE`

Change the schema. The existing rows stay readable. A new column reads `NULL`. A rename keeps the
values.

```sql
-- Add a (nullable) column
ALTER TABLE measurements ADD COLUMN quality_flag INT;

-- Rename a column
ALTER TABLE measurements RENAME COLUMN name TO platform;

-- Drop a column
ALTER TABLE measurements DROP COLUMN quality_flag;

-- Widen a column's type (safe promotions only)
ALTER TABLE measurements ALTER COLUMN id TYPE BIGINT;
```

On a **Lance** table Beacon applies a schema change directly. It does not rebuild the table. On an
**Iceberg** table Beacon allows safe type promotions only: `INT` to `BIGINT`, `FLOAT` to `DOUBLE`,
and a higher decimal precision at the same scale. Beacon rejects a narrower type and an incompatible
change.

## Indexes

::: info Lance engine only
Secondary indexes are a Lance feature. An Iceberg table does not support them.
:::

Create a scalar index on a column to make filters faster. Queries then use the index automatically.
You change no query.

```sql
-- Default (BTREE) index; auto-named <table>_<column>_idx
CREATE INDEX ON measurements (platform);

-- Named index, explicit type
CREATE INDEX value_idx  ON measurements (value)        USING btree;
CREATE INDEX flag_idx   ON measurements (quality_flag) USING bitmap;
CREATE INDEX name_idx   ON measurements (name)         USING inverted;
```

| Type | Use for |
| --- | --- |
| `btree` *(default)* | range and equality filters: `=`, `<`, `BETWEEN` and more |
| `bitmap` | a column with few distinct values |
| `inverted` | full text search over a string column |

List the indexes of a table, or drop one by name:

```sql
SHOW INDEXES ON measurements;

DROP INDEX value_idx ON measurements;
```

## `COMPACT TABLE`

::: info Lance engine only
Compaction is a Lance feature. An Iceberg table does not support it.
:::

Every write makes a new version of the table. An `INSERT` adds new fragments. A `DELETE` keeps the
rows on disk and adds a deletion file. Many small writes make many small fragments. A scan of the
table then plans more work than the row count needs.

`COMPACT TABLE` merges the small fragments into large ones. It also applies the deletions. Then it
deletes the old versions. The statement returns one report row:

```sql
COMPACT TABLE measurements;
```

| Column | Meaning |
| --- | --- |
| `fragments_removed` | fragments that the merge replaced |
| `fragments_added` | fragments that Beacon wrote |
| `files_removed` | data files and deletion files that the merge replaced |
| `files_added` | files that Beacon wrote |
| `versions_removed` | old versions that Beacon deleted |
| `bytes_removed` | disk space that Beacon released |

Beacon keeps the indexes of the table. It maps each index onto the new fragments. A table with
nothing to merge is not an error. The report is then all zeros.

### Options

```sql
COMPACT TABLE measurements WITH (
  'target_rows_per_fragment' '1048576',
  'cleanup_older_than' '7d'
);
```

| Option | Default | Meaning |
| --- | --- | --- |
| `target_rows_per_fragment` | `1048576` | Rows per fragment. Beacon merges a smaller fragment only. |
| `cleanup_older_than` | `7d` | Age limit for an old version. `never` keeps all versions. |

The merge alone releases no disk space. The old versions still point to the old files. Beacon
deletes only the versions that are older than `cleanup_older_than`. Write the age as `30s`, `15m`,
`2h` or `7d`. A number alone means seconds.

::: warning Keep the default age on a busy server
A query opens one version, and reads the files of that version until the query stops. A cleanup
with a short age can delete those files first. Use `'0s'` only when no query runs.
:::

## `DROP TABLE`

`DROP TABLE` removes a managed table. It also **deletes the data of the table**. An external table
behaves differently:

```sql
DROP TABLE measurements;

DROP TABLE IF EXISTS measurements;
```

## Notes and limitations

- **Storage**: a Lance table lives on the local file system, in the tables directory. An Iceberg
  table lives in the internal storage area of Beacon, next to the datasets, on local disk or on S3.
  You configure nothing.
- **Lance write model**: `INSERT` streams directly into the table. `DELETE` and `UPDATE` are native,
  through deletion vectors and fragment rewrites. `ALTER` needs no rebuild. Each write commits a new
  dataset version. A reader always sees a consistent snapshot. Nothing shrinks on its own. Run
  `COMPACT TABLE` to merge the fragments and to release the space.
- **Iceberg write model**: `DELETE` and `UPDATE` are copy-on-write. `ALTER` rebuilds the table. Use
  Iceberg for a table of moderate size with few schema changes, not for frequent row changes.
- **Scope**: `ALTER` supports `ADD COLUMN`, `DROP COLUMN`, `RENAME COLUMN` and `ALTER COLUMN TYPE` on
  one table. A new column is nullable. The indexes are scalar only. Beacon does not yet expose vector
  or ANN indexes.

## Query and inspect

```sql
SHOW TABLES;

DESCRIBE measurements;

SHOW INDEXES ON measurements;   -- Lance tables
```

## Show the table sizes

`beacon.system.table_sizes` has one row for each managed table. Use it to find the tables that fill
`beacon.db`. Only the admin can read it.

```sql
SELECT table_name, row_count, bytes_on_disk
FROM beacon.system.table_sizes
ORDER BY bytes_on_disk DESC;
```

| Column | Value |
| --- | --- |
| `table_name` | The table name. |
| `engine` | The storage engine. The value is `lance`. |
| `row_count` | The rows in the latest version. |
| `fragment_count` | The fragments in the latest version. |
| `version_count` | The versions that the table holds. |
| `bytes_on_disk` | The bytes of all files of the table. |

`bytes_on_disk` includes the old versions, the indexes and the manifests. `COMPACT TABLE` deletes
the old versions and decreases this value.

The table also shows the internal tables of Beacon. Their names start with `__beacon_`. For example,
`__beacon_query_metrics` holds the recorded query metrics.

Beacon measures each table when you query `beacon.system.table_sizes`. The query time increases
with the number of tables and files.
