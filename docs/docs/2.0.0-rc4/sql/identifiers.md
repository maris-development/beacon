---
description: Beacon keeps the case of every table, column and alias name. MyTable and mytable are two different names. Keywords and built-in functions stay case-insensitive.
---

# Identifiers and case

Beacon keeps the case of every identifier. A table name, a column name and an alias mean exactly
what you write. `MyTable` and `mytable` are two different names.

```sql
CREATE EXTERNAL TABLE MyTable STORED AS CSV LOCATION 'obs.csv';

SELECT * FROM MyTable   -- 2 rows
SELECT * FROM mytable   -- Error during planning: table 'beacon.public.mytable' not found
```

Most SQL databases fold an unquoted name to one case. PostgreSQL folds to lowercase. Oracle folds
to uppercase. Beacon folds nothing.

::: tip Pick one spelling
Lowercase names with underscores need no quotes and no thought. Use `ocean_profiles`, not
`OceanProfiles`.
:::

## Quotes change nothing

A quoted name and an unquoted name follow the same rule. `"MyTable"` and `MyTable` are the same
table.

Use quotes only for a name that the parser cannot read as one word: a name with a space, a name
that starts with a digit, or a reserved word.

```sql
CREATE EXTERNAL TABLE "My Table" STORED AS CSV LOCATION 'obs.csv';

SELECT * FROM "My Table"
```

## Columns keep the case of the source

A column name comes from the file, not from Beacon. A netCDF variable `TEMP` stays `TEMP`. A CSV
header `Depth` stays `Depth`.

```sql
SELECT TEMP FROM MyTable   -- 2 rows
SELECT temp FROM MyTable   -- Schema error: No field named temp
```

`DESCRIBE <table>` prints the exact spelling of every column:

```sql
DESCRIBE MyTable
```

| column_name | data_type |
| ----------- | --------- |
| Depth       | Int64     |
| TEMP        | Float64   |
| value       | Int64     |

An alias follows the same rule:

```sql
SELECT TEMP AS Celsius FROM MyTable ORDER BY Celsius   -- 2 rows
SELECT TEMP AS Celsius FROM MyTable ORDER BY celsius   -- Schema error: No field named celsius
```

## Every statement follows the rule

`DROP TABLE`, `ALTER TABLE`, `INSERT`, `UPDATE`, `DELETE`, `REFRESH`, `CREATE INDEX` and
`SET EXTENSION` all name the table the same way as `SELECT`:

```sql
DROP TABLE mytable   -- Table 'mytable' does not exist
DROP TABLE MyTable   -- dropped
```

A grant names one exact table too:

```sql
GRANT SELECT ON TABLE mytable TO ROLE reader   -- does not cover MyTable
```

## What stays case-insensitive

| Element | Rule | Example |
| ------- | ---- | ------- |
| Keywords | Case-insensitive | `select`, `SELECT`, `SeLeCt` |
| Format after `STORED AS` | Case-insensitive | `STORED AS csv`, `STORED AS CSV` |
| Built-in functions | Case-insensitive | `count(*)`, `COUNT(*)` |
| [Table functions](/docs/2.0.0-rc4/sql/table-functions) | **Case-sensitive.** Always lowercase | `read_csv(…)`, never `READ_CSV(…)` |
| Table, column and alias names | **Case-sensitive** | `MyTable` ≠ `mytable` |

## Find the exact name

The catalog holds the spelling. Ask it before you guess:

```sql
SHOW TABLES              -- every table name
DESCRIBE ocean_profiles  -- every column name
```
