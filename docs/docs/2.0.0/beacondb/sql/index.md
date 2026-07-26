---
description: Query Beacon with standard SQL (Apache DataFusion) — SELECT, WHERE, GROUP BY, JOIN, table functions like read_netcdf(), DDL, and a full function reference.
---

# SQL Guide

Beacon embeds [Apache DataFusion](https://datafusion.apache.org/) as its query engine, giving you a broad standard SQL dialect — `SELECT`, `WHERE`, `GROUP BY`, `JOIN`, `ORDER BY`, window functions, and more. **This dialect is the same in both products** — [beacondb](/docs/2.0.0/beacondb/) (embedded) and beacon-datalake (server).

## Running SQL

| Surface | How |
| ------- | --- |
| **beacondb** (embedded) | `con.sql("…")` / `con.execute("…")` in Python |
| **HTTP API** (server) | `POST /api/query` with `{ "sql": "..." }` (needs `BEACON_ENABLE_SQL=true`) |
| **Arrow Flight SQL** (server) | Any Flight SQL client — DataGrip, ADBC, DBeaver (enabled by default) |

## What you can query

**Registered tables** — any [External Table](../../data-lake/external-tables.md) or [View](../../data-lake/view.md):

```sql
SELECT * FROM ocean_profiles LIMIT 100
```

**Files on the fly** — using [Table Functions](./table-functions.md) without a persistent table:

```sql
SELECT * FROM read_netcdf(['argo/**/*.nc']) LIMIT 100
```

## Reference

- Query: [SELECT](./select.md) · [WHERE](./where.md) · [GROUP BY](./group-by.md) · [JOIN](./join.md) · [UNION BY NAME](./union-by-name.md)
- Tables & views: [CREATE TABLE](./create-table.md) · [CREATE VIEW](./create-view.md) · [CREATE MATERIALIZED VIEW](./create-materialized-view.md) · [Managed tables](./managed-tables.md) · [Remote tables & `ATTACH`](./remote-tables.md)
- Functions: [Table functions](./table-functions.md) · [Utility table functions](./table-functions-utility.md) · [Function reference](./function-reference.md) · [GeoParquet](./geoparquet.md)
- Secrets & profiling: [`CREATE SECRET`](./secrets.md) · [`SUMMARIZE`](./summarize.md)

**Friendly SQL.** Beacon also supports DuckDB-style conveniences from DataFusion: `SELECT * EXCLUDE (col)` / `REPLACE (…)`, `GROUP BY ALL`, `QUALIFY`, `UNION BY NAME`, `FROM`-first (`FROM t SELECT …`), `DESCRIBE`, `SHOW TABLES` / `SHOW COLUMNS`, list literals, and trailing commas.
