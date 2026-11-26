# INSERT

`INSERT INTO` adds rows to a table. Use it on a
[managed table](/docs/2.1.0-rc3/sql/managed-tables) and on an external table that accepts writes,
for example a Delta table. A read-only external table, for example a NetCDF table, refuses the
statement with an error.

`INSERT INTO` needs admin credentials. The
[read rules](/docs/2.1.0-rc3/security/access-control) also apply to the tables of the query.

## `INSERT INTO`

Add rows from literal values or from a query:

```sql
INSERT INTO measurements VALUES (1, 'argo', 12.5), (2, 'glider', 9.0);

INSERT INTO measurements
SELECT id, name, value FROM staging;
```

A plain `INSERT INTO` matches the columns by position. The first query column goes into the first
table column, and so on. The statement returns the number of inserted rows.

## `INSERT INTO ... BY NAME`

`BY NAME` matches the columns of the query to the columns of the table by name. The order of the
columns does not matter:

```sql
INSERT INTO <table> BY NAME <query>
```

```sql
CREATE TABLE argo (platform VARCHAR, time TIMESTAMP, temp DOUBLE, psal DOUBLE);

INSERT INTO argo BY NAME SELECT time, platform, temp FROM argo_netcdf;
```

`BY NAME` does not change the table. It does not add columns, rename columns or change column types.
It works on each table type that accepts `INSERT INTO`. A column list together with `BY NAME` is an
error. The statement returns the number of inserted rows, the same as `INSERT INTO`. Beacon streams
the rows. It does not keep the whole query result in memory.

### Match the columns

- Beacon matches each name exactly. The match is case sensitive, so `Temp` and `temp` are two
  different columns.
- A query column that the table does not have is an error. The error names all such columns. Remove
  them from the query, or rename them with `AS`.
- Two query columns with the same name are an error.
- A table column that the query does not have gets `NULL`. In the example above, `psal` gets `NULL`.
- A table column that is not nullable must be in the query. If not, the statement fails before a
  write.
- A `NULL` value in a column that is not nullable stops the statement with an error. Beacon writes
  no rows.

### Cast the values

The type of the table column always stays. Beacon casts each query column to the type of its table
column. The session type widening rule decides how. Two server variables set it:

- `BEACON_TYPE_WIDENING_STRATEGY` is `default` or `numpy`.
- `BEACON_TYPE_WIDENING_ON_CONFLICT` is `fail` or `keep_first`.

See [Query engine](/docs/2.1.0-rc3/server/configuration#query-engine) for the full rule of each
value. The `type_widening` keys of an external table do not change this rule.

For each column, Beacon merges the type of the table column and the type of the query column with
the rule:

- The rule accepts the two types. Beacon casts the query column to the table type. The cast is
  strict: a value that does not cast stops the statement with an error. This is also true when the
  query type is wider than the table type.
- The two types are a conflict, and the rule is `fail`. The statement fails with the error of the
  rule. To insert the values, cast the query column to the table type with `CAST`.
- The two types are a conflict, and the rule is `keep_first`. Beacon casts leniently. A value that
  does not cast becomes `NULL`.

Know these rules:

- Beacon examines each type pair before the insert starts. A pair that cannot cast is an error.
- A cast to a smaller type can lose precision. A `DOUBLE` value in a `FLOAT` column is an example.
- A `BIGINT` value that is too large for an `INT` column stops the statement with an error.

`EXPLAIN INSERT INTO ... BY NAME` shows how Beacon maps the columns, for example
`cast=[c: Int32->Int64], null=[d]`. It also shows each mapping error. It does not write.
`EXPLAIN ANALYZE` does not accept `INSERT INTO ... BY NAME`.

### NetCDF sources

A NetCDF external table merges the schemas of its files. A variable that a file does not have reads
`NULL`. With `BY NAME`, you can collect many NetCDF files in one table. Create the table with all
the variables first. Know these limits:

- A query fails when its columns have disjoint dimensions. `SELECT *` can fail for this reason.
  Select the columns that share dimensions. Or set `read_dimensions` on the NetCDF table. See
  [NetCDF options](/docs/2.1.0-rc3/formats/netcdf#options).
- [Attribute columns](/docs/2.1.0-rc3/formats/netcdf#variable-attributes) (`var.attr`) have a dot
  in the name. A Lance table does not accept a column name with a dot. So `SELECT *` fails when the
  source has attribute columns. Leave them out of the select list. Or add a column for them to the
  table, and rename them with `AS`:

  ```sql
  ALTER TABLE argo ADD COLUMN temp_units VARCHAR;

  INSERT INTO argo BY NAME
  SELECT platform, time, temp, "temp.units" AS temp_units FROM argo_netcdf;
  ```

### Errors

| Case | Message |
| --- | --- |
| Query columns that the table does not have | `Table '<t>' does not have the columns '<c>', ...` |
| Two query columns with the same name | `The source has the column '<c>' more than one time` |
| Column that is not nullable and is not in the query | `Column '<c>' is not nullable and the source does not have it` |
| Type pair that cannot cast | `Column '<c>': cannot cast <source type> to <target type>` |
| Type conflict with the rule `fail` | `Incompatible types for field '<c>': <table type> in '<t>' vs <source type> in 'source query'` |
| Column list after the table name | `INSERT ... BY NAME does not take a column list` |

The types in the conflict message use the Arrow names, for example `Int64` and `Utf8`.
