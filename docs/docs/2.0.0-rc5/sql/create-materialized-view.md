# CREATE MATERIALIZED VIEW

```sql
CREATE MATERIALIZED VIEW monthly_sales AS
    SELECT
        customer_id,
        date_trunc('month', order_date) AS month,
        SUM(amount) AS total_amount
    FROM orders
    GROUP BY customer_id, date_trunc('month', order_date)
```

A materialized view runs its query **once**, at creation time. It stores the result as one Parquet
file. A query on the view reads that Parquet file. Beacon does not run the original query again. Use
a materialized view for an expensive, repeated or aggregate query.

The query can read any source that Beacon knows. Sources include registered tables,
[external tables](/docs/2.0.0-rc5/data-sources/external-tables),
[views](/docs/2.0.0-rc5/sql/create-view) and table functions such as `read_netcdf()`,
`read_zarr()` and `read_atlas()`.

A regular [view](/docs/2.0.0-rc5/sql/create-view) runs its query on every reference. A
materialized view changes only when you run [`REFRESH`](#refresh).

## Syntax

```sql
CREATE MATERIALIZED VIEW <view_name> AS
    <select_statement>
```

The statement makes Beacon do four steps:

1. Beacon stores the materialized view definition in the catalog as a table provider.
2. Beacon runs the query at once.
3. Beacon writes the result as one `part.parquet` file. Each refresh gets its own subdirectory. The
   subdirectories live under the reserved `__beacon__/<view_name>/` prefix in the dataset store.
4. Beacon serves later reads from the stored Parquet result.

The catalog holds the view name, the original SQL query, the output schema and the storage location.
It also holds the creation time and the time of the last refresh.

## Query the view

```sql
SELECT * FROM monthly_sales
```

This scans the stored Parquet result. It uses columnar projection and predicate pushdown. It does
**not** run the original query again.

## REFRESH

```sql
REFRESH monthly_sales
```

A refresh runs the original query again. It replaces the stored Parquet data with the new result.
This is a full refresh. Beacon writes the new data to a new directory. It then swaps the catalog
pointer atomically. A failed refresh therefore keeps the previous result. You can still query it.

::: info
This version supports **full refresh** only. Beacon plans incremental refresh, scheduled refresh and
dependency-based invalidation for a later release.
:::

### Errors

A refresh of a name that is not a materialized view gives a clear error:

```text
Materialized view 'unknown_view' does not exist
```

```text
Object 'orders' is not a materialized view
```

## DROP

```sql
DROP TABLE monthly_sales

DROP TABLE IF EXISTS monthly_sales
```

`DROP TABLE` removes a materialized view from the catalog. It also deletes the stored Parquet data.

## Example

```sql
CREATE MATERIALIZED VIEW top_customers AS
    SELECT
        customer_id,
        SUM(total) AS lifetime_value
    FROM orders
    GROUP BY customer_id
    ORDER BY lifetime_value DESC
    LIMIT 100;

SELECT * FROM top_customers;

REFRESH top_customers;
```
