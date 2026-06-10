# SQL Guide

Beacon embeds [Apache DataFusion](https://datafusion.apache.org/) as its query engine, giving you a broad standard SQL dialect — `SELECT`, `WHERE`, `GROUP BY`, `JOIN`, `ORDER BY`, window functions, and more.

## Running SQL

| Surface | How | Requirement |
| ------- | --- | ----------- |
| **HTTP API** | `POST /api/query` with `{ "sql": "..." }` | `BEACON_ENABLE_SQL=true` |
| **Arrow Flight SQL** | Any Flight SQL client (DataGrip, ADBC, DBeaver) | Enabled by default |

## What you can query

**Registered tables** — any [External Table](../data-lake/external-tables.md) or [View](../data-lake/view.md):

```sql
SELECT * FROM ocean_profiles LIMIT 100
```

**Files on the fly** — using [Table Functions](./table-functions.md) without a persistent table:

```sql
SELECT * FROM read_netcdf(['argo/**/*.nc']) LIMIT 100
```
