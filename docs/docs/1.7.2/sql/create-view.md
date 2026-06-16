# CREATE VIEW

```sql
CREATE VIEW north_atlantic AS
    SELECT * FROM ocean_profiles
    WHERE latitude BETWEEN 0 AND 70
```

A view is a saved `SELECT` statement that behaves like a table. It holds no data — Beacon runs the query on the fly whenever the view is referenced. Views are persisted and survive restarts.

## Syntax

```sql
CREATE [OR REPLACE] VIEW <view_name> AS
    <select_statement>
```

## `OR REPLACE`

Redefine an existing view without dropping it first:

```sql
CREATE OR REPLACE VIEW north_atlantic AS
    SELECT * FROM ocean_profiles
    WHERE latitude BETWEEN 0 AND 70
      AND longitude BETWEEN -80 AND 0
```

## Querying over a table function

Views work over [table functions](./table-functions.md) as well as external tables — useful when you want to expose a specific file set as a named table:

```sql
CREATE VIEW argo_2024 AS
    SELECT *
    FROM read_netcdf(['argo/2024/**/*.nc'])
    WHERE time >= '2024-01-01'
```

## Harmonizing datasets with `UNION ALL BY NAME`

Use a view to expose multiple datasets with different schemas as a single unified table. See [UNION ALL BY NAME](./union-by-name.md) for how column matching and type widening work.

```sql
CREATE VIEW all_profiles AS
    SELECT * FROM read_netcdf(['argo/**/*.nc'])
    UNION ALL BY NAME
    SELECT * FROM read_netcdf(['wod/**/*.nc'])
```

## `DROP TABLE`

Remove a view from the catalog:

```sql
DROP TABLE north_atlantic

DROP TABLE IF EXISTS north_atlantic
```

:::info
`DROP TABLE` removes both external tables and views — there is no separate `DROP VIEW` statement.
:::
