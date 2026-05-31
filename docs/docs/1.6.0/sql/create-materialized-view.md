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

A materialized view runs its defining query **once**, at creation time, and persists the result
set as Parquet files. Querying the view reads straight from the persisted Parquet instead of
recomputing the original query — useful for expensive, repeated, or aggregation-heavy queries.

Unlike a regular [view](./create-view.md) (which recomputes on every reference), a materialized
view only changes when you explicitly [`REFRESH`](#refresh) it.

## Syntax

```sql
CREATE MATERIALIZED VIEW <view_name> AS
    <select_statement>
```

When the statement runs, Beacon:

1. Stores the materialized view definition in the catalog as a table provider.
2. Executes the defining query immediately.
3. Writes the result as one or more Parquet files under the reserved
   `__beacon__/<view_name>/` prefix in the datasets object store.
4. Serves future reads from the persisted Parquet result.

The catalog records the view name, original SQL query, output schema, storage location, creation
timestamp, and last-refresh timestamp.

## Querying

```sql
SELECT * FROM monthly_sales
```

This scans the Parquet-backed result and benefits from columnar projection and predicate
pushdown — it does **not** re-run the original query.

## REFRESH

```sql
REFRESH monthly_sales
```

A refresh recomputes the original query and replaces the stored Parquet data with the new result
(full refresh). The new data is written to a fresh directory and the catalog pointer is swapped
atomically, so a failed refresh leaves the previous result intact and queryable.

::: info
Only **full refresh** is supported in this version. Incremental refresh, scheduled refresh, and
dependency-based invalidation are planned for later releases.
:::

### Errors

Refreshing a name that is not a materialized view fails clearly:

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

Dropping a materialized view removes it from the catalog and deletes its persisted Parquet data.

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
