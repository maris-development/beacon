# Views

```sql
CREATE VIEW north_atlantic AS
    SELECT * FROM ocean_profiles
    WHERE latitude BETWEEN 0 AND 70
```

A view is a saved `SELECT` statement that behaves like a table. It holds no data — Beacon runs the query on the fly whenever the view is referenced. Views are persisted and survive restarts.

## Creating a view

```sql
CREATE VIEW <view_name> AS
    <select_statement>
```

## Replacing a view

```sql
CREATE OR REPLACE VIEW <view_name> AS
    <select_statement>
```

## Dropping a view

```sql
DROP TABLE <view_name>
```

:::info
`DROP TABLE` removes both external tables and views — there is no separate `DROP VIEW` statement.
:::
