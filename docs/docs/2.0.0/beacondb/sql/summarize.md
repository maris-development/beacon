---
description: SUMMARIZE profiles a table or query, one row per column with min/max, distinct count, avg/std, non-null count and null percentage.
---

# SUMMARIZE

`SUMMARIZE` produces a **one-row-per-column profile** of a table or query, the first thing to run on
a new dataset.

```sql
SUMMARIZE obs;
```

| column_name | column_type | min | max | distinct | avg | std | count | null_percentage |
|---|---|---|---|---|---|---|---|---|
| temperature | Float64 | 10.0 | 30.0 | 3 | 20.0 | 10.0 | 3 | 25.0 |
| depth | Int64 | 0 | 100 | 3 | 62.5 | 47.9 | 4 | 0.0 |
| platform | Utf8 | A | B | 2 | | | 4 | 0.0 |

Columns are returned in the source's column order.

## Forms

```sql
SUMMARIZE obs;                                   -- a table
SUMMARIZE public.obs;                            -- schema-qualified
SUMMARIZE SELECT depth FROM obs WHERE depth > 0; -- any query
SUMMARIZE (SELECT * FROM read_parquet('obs/*.parquet'));
```

## What each column means

- **min / max**: for orderable columns (numbers, strings, temporal), shown as text.
- **distinct**: exact number of distinct non-null values.
- **avg / std**: for numeric columns only (`NULL` otherwise).
- **count**: number of non-null values.
- **null_percentage**: share of `NULL`s, `0`–`100`.

## Notes

`SUMMARIZE` lowers to an ordinary single-pass aggregate query, so it works on a
[read-only](/docs/2.0.0/beacondb/python/getting-started#read-only) database and needs no special privileges.
It scans the source once.
