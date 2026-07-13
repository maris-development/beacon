# CREATE TABLE (Managed)

```sql
CREATE TABLE observations (id BIGINT, name VARCHAR);
INSERT INTO observations VALUES (1, 'a'), (2, 'b');
SELECT * FROM observations;
```

A **managed table** is a SQL table whose data Beacon owns and stores itself. Unlike an [external table](../data-lake/external-tables.md) — which only points at existing files — a managed table is created empty (or from a query) and populated with `INSERT`. It supports `UPDATE`, `DELETE`, schema evolution with `ALTER TABLE`, and secondary `INDEX`es. Table definitions and data survive restarts.

Managed tables are an authenticated, write capability: `CREATE`, `INSERT`, `UPDATE`, `DELETE`, `ALTER`, and `CREATE/DROP INDEX` require admin credentials. Anonymous access remains read-only.

## Storage engine

Managed tables are backed by **[Lance](https://lancedb.github.io/lance/)**: a columnar, versioned format with native row-level updates/deletes (deletion vectors / fragment rewrite) and scalar secondary indexes (btree, bitmap, full-text). Table data and definitions live in Beacon's single-file `db://` store (`beacon.db`) alongside each table's `table.json`, regardless of where your datasets are stored (S3 only ever applies to the datasets store).

## `CREATE TABLE`

Define columns explicitly:

```sql
CREATE TABLE measurements (
  id     BIGINT,
  name   VARCHAR,
  value  DOUBLE
);
```

`IF NOT EXISTS` makes creation a no-op when the table already exists:

```sql
CREATE TABLE IF NOT EXISTS measurements (id BIGINT, name VARCHAR);
```

### `CREATE TABLE AS SELECT`

Create and populate a table from a query (CTAS). The table's schema is the schema of the query result:

```sql
CREATE TABLE warm_profiles AS
SELECT platform, temperature, depth
FROM read_parquet('profiles/*.parquet')
WHERE temperature > 20;
```

## `INSERT INTO`

Append rows from literal values or a query:

```sql
INSERT INTO measurements VALUES (1, 'argo', 12.5), (2, 'glider', 9.0);

INSERT INTO measurements
SELECT id, name, value FROM staging;
```

## `SELECT`

Query a managed table like any other table:

```sql
SELECT name, avg(value) FROM measurements GROUP BY name;
```

## `DELETE`

Remove rows matching a predicate, or all rows when no `WHERE` is given:

```sql
DELETE FROM measurements WHERE value IS NULL;

DELETE FROM measurements;        -- empties the table
```

On **Lance** tables this is a native delete (deletion vectors) — it does not rewrite the whole table. On **Iceberg** tables it is copy-on-write.

## `UPDATE`

Change column values for matching rows; unmatched rows are untouched:

```sql
UPDATE measurements SET name = 'unknown' WHERE name IS NULL;

UPDATE measurements SET value = value * 1.0;   -- every row
```

On **Lance** tables this rewrites only the affected fragments; on **Iceberg** it is copy-on-write. `UPDATE ... FROM` / joins are not supported.

## `ALTER TABLE`

Evolve the schema. Existing rows keep reading correctly: added columns read `NULL`, renames preserve values.

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

On **Lance** tables schema changes are applied natively (no table rebuild). On **Iceberg** tables, only safe type promotions are allowed: `INT → BIGINT`, `FLOAT → DOUBLE`, and decimal precision increases at the same scale; narrowing or incompatible changes are rejected.

## Indexes

::: info Lance engine only
Secondary indexes are a feature of the **Lance** engine. They are not available on Iceberg-backed tables.
:::

Create a scalar index on a column to speed up filters. Once created, queries use it automatically — no query changes are needed.

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
| `btree` *(default)* | range / equality filters (`=`, `<`, `BETWEEN`, …) |
| `bitmap` | low-cardinality columns (few distinct values) |
| `inverted` | full-text search over string columns |

List a table's indexes, or drop one by name:

```sql
SHOW INDEXES ON measurements;

DROP INDEX value_idx ON measurements;
```

## `DROP TABLE`

Remove a managed table. Unlike an external table, this **deletes the table's data**:

```sql
DROP TABLE measurements;

DROP TABLE IF EXISTS measurements;
```

## Notes & limitations

- **Storage**: Lance tables live on the local filesystem (the tables directory); Iceberg tables live in Beacon's internal storage area alongside the datasets (local or S3). There is nothing extra to configure.
- **Lance write model**: `INSERT` streams directly into the table; `DELETE`/`UPDATE` are native (deletion vectors / fragment rewrite); `ALTER` is applied without a rebuild. Each write commits a new dataset version, and readers always see a consistent snapshot.
- **Iceberg write model**: `DELETE`/`UPDATE` are copy-on-write and `ALTER` rebuilds the table; best suited to moderate-sized tables and occasional schema changes rather than high-frequency row churn.
- **Scope**: `ALTER` supports single-table `ADD` / `DROP` / `RENAME COLUMN` and `ALTER COLUMN TYPE`; added columns are nullable. Indexes are scalar only (vector/ANN indexes are not yet exposed).

## Querying and inspecting

```sql
SHOW TABLES;

DESCRIBE measurements;

SHOW INDEXES ON measurements;   -- Lance tables
```
