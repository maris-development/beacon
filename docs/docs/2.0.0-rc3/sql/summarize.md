---
description: SUMMARIZE profiles a table or a query. It returns one row per column with the minimum, maximum, distinct count, average and null share.
---

# SUMMARIZE

`SUMMARIZE` profiles a table or a query. It returns **one row for each column**. Run it first on a
new dataset.

```sql
SUMMARIZE obs;
```

| column_name | column_type | min | max | distinct | avg | std | count | null_percentage |
|---|---|---|---|---|---|---|---|---|
| temperature | Float64 | 10.0 | 30.0 | 3 | 20.0 | 10.0 | 3 | 25.0 |
| depth | Int64 | 0 | 100 | 3 | 62.5 | 47.9 | 4 | 0.0 |
| platform | Utf8 | A | B | 2 | | | 4 | 0.0 |

Beacon returns the columns in the order of the source.

## Forms

```sql
SUMMARIZE obs;                                   -- a table
SUMMARIZE public.obs;                            -- schema-qualified
SUMMARIZE SELECT depth FROM obs WHERE depth > 0; -- any query
SUMMARIZE (SELECT * FROM read_parquet('obs/*.parquet'));

::: warning A table function needs a query
`SUMMARIZE` takes a **name** or a **query**. A bare
[table function](/docs/2.0.0-rc3/sql/table-functions) is neither, so this does not parse:

```sql
SUMMARIZE read_netcdf('argo/*.nc');   -- error
```

Wrap it in a query instead:

```sql
SUMMARIZE (SELECT * FROM read_netcdf('argo/*.nc'));
```
:::
```

## What each column means

- **min / max**: for columns with an order, such as numbers, strings and timestamps. Beacon returns them as text.
- **distinct**: the exact number of distinct non-null values.
- **avg / std**: for numeric columns only. Other columns get `NULL`.
- **count**: the number of non-null values.
- **null_percentage**: the share of `NULL` values, from `0` to `100`.

## Notes

`SUMMARIZE` becomes an ordinary aggregate query with one pass. It therefore works on a
read-only connection. It needs no special
privileges. It scans the source once.
