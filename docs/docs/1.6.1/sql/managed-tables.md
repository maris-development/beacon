# CREATE TABLE (Managed)

```sql
CREATE TABLE observations (id BIGINT, name VARCHAR);
INSERT INTO observations VALUES (1, 'a'), (2, 'b');
SELECT * FROM observations;
```

A **managed table** is a SQL table whose data Beacon owns and stores itself, backed by [Apache Iceberg](https://iceberg.apache.org/). Unlike an [external table](../data-lake/external-tables.md) — which only points at existing files — a managed table is created empty (or from a query) and populated with `INSERT`. It supports `UPDATE`, `DELETE`, and schema evolution with `ALTER TABLE`, and its data lives inside Beacon's storage. Table definitions and data survive restarts.

Managed tables are an authenticated, write capability: `CREATE`, `INSERT`, `UPDATE`, `DELETE`, and `ALTER` require admin credentials. Anonymous access remains read-only.

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

## `UPDATE`

Change column values for matching rows; unmatched rows are untouched:

```sql
UPDATE measurements SET name = 'unknown' WHERE name IS NULL;

UPDATE measurements SET value = value * 1.0;   -- every row
```

`UPDATE ... FROM` / joins are not supported.

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

Only safe type promotions are allowed: `INT → BIGINT`, `FLOAT → DOUBLE`, and decimal precision increases at the same scale. Narrowing or incompatible changes are rejected.

## `DROP TABLE`

Remove a managed table. Unlike an external table, this **deletes the table's data**:

```sql
DROP TABLE measurements;

DROP TABLE IF EXISTS measurements;
```

## Notes & limitations

- **Storage**: data and metadata live in Beacon's internal area of the configured storage (local filesystem or S3), alongside the datasets — there is nothing extra to configure.
- **Write model**: `DELETE` and `UPDATE` are copy-on-write (the affected data is rewritten), and `ALTER TABLE` rebuilds the table under the new schema. These operations rewrite table data and do not preserve previous Iceberg snapshots, so they are best suited to moderate-sized tables and occasional schema changes rather than high-frequency row churn.
- **Scope**: `ALTER` supports single-table `ADD` / `DROP` / `RENAME COLUMN` and `ALTER COLUMN TYPE`; added columns are nullable.

## Querying and inspecting

```sql
SHOW TABLES;

DESCRIBE measurements;
```
