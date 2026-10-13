# Views

```sql
CREATE VIEW north_atlantic AS
    SELECT * FROM ocean_profiles
    WHERE latitude BETWEEN 0 AND 70
```

A view is a saved `SELECT` statement. It behaves like a table. It holds no data. Beacon runs the
query each time that you reference the view. Beacon stores a view. It survives a restart.

## Create a view

```sql
CREATE VIEW <view_name> AS
    <select_statement>
```

## Replace a view

```sql
CREATE OR REPLACE VIEW <view_name> AS
    <select_statement>
```

## Drop a view

```sql
DROP TABLE <view_name>
```

:::info
`DROP TABLE` removes an external table and a view. There is no separate `DROP VIEW` statement.
:::
