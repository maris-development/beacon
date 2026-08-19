# CREATE VIEW

```sql
CREATE VIEW north_atlantic AS
    SELECT * FROM ocean_profiles
    WHERE latitude BETWEEN 0 AND 70
```

A view is a saved `SELECT` statement. It behaves like a table. It holds no data. Beacon runs the
query each time that you reference the view. Beacon stores a view. It survives a restart.

## Syntax

```sql
CREATE [OR REPLACE] VIEW <view_name> AS
    <select_statement>
```

## `OR REPLACE`

Define an existing view again. You do not drop it first:

```sql
CREATE OR REPLACE VIEW north_atlantic AS
    SELECT * FROM ocean_profiles
    WHERE latitude BETWEEN 0 AND 70
      AND longitude BETWEEN -80 AND 0
```

## Query over a table function

A view works over a [table function](/docs/2.0.0-rc3/sql/table-functions) and over an
external table. Use it to give a set of files a table name:

```sql
CREATE VIEW argo_2024 AS
    SELECT *
    FROM read_netcdf(['argo/2024/**/*.nc'])
    WHERE time >= '2024-01-01'
```

## Combine datasets with `UNION ALL BY NAME`

A view can show several datasets with different schemas as one table. See
[UNION ALL BY NAME](/docs/2.0.0-rc3/sql/union-by-name) for the column matching and the type
widening.

```sql
CREATE VIEW all_profiles AS
    SELECT * FROM read_netcdf(['argo/**/*.nc'])
    UNION ALL BY NAME
    SELECT * FROM read_netcdf(['wod/**/*.nc'])
```

## `DROP TABLE`

`DROP TABLE` removes a view from the catalog:

```sql
DROP TABLE north_atlantic

DROP TABLE IF EXISTS north_atlantic
```

:::info
`DROP TABLE` removes an external table and a view. There is no separate `DROP VIEW` statement.
:::
