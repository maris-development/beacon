---
description: Query Beacon with standard SQL (Apache DataFusion), SELECT, WHERE, GROUP BY, JOIN, table functions like read_netcdf(), DDL, and a full function reference.
---

# SQL Guide

Beacon embeds [Apache DataFusion](https://datafusion.apache.org/) as its query engine, giving you a broad standard SQL dialect, `SELECT`, `WHERE`, `GROUP BY`, `JOIN`, `ORDER BY`, window functions, and more. **This dialect is the same either way**: [BeaconDB](/docs/2.0.0/beacondb/) (embedded) and Beacon Data Lake (served).

## Running SQL

| Surface | How |
| ------- | --- |
| **BeaconDB** (embedded) | `con.sql("…")` / `con.execute("…")` in Python |
| **HTTP API** (server) | `POST /api/query` with `{ "sql": "..." }` (SQL interface on by default) |
| **Arrow Flight SQL** (server) | Any Flight SQL client, DataGrip, ADBC, DBeaver (enabled by default) |

## What you can query

**Registered tables**: any [External Table](/docs/2.0.0/beacondb/data-sources/external-tables) or [View](/docs/2.0.0/data-lake/view):

```sql
SELECT * FROM ocean_profiles LIMIT 100
```

**Files on the fly**: using [Table Functions](/docs/2.0.0/beacondb/sql/table-functions) without a persistent table:

```sql
SELECT * FROM read_netcdf(['argo/**/*.nc']) LIMIT 100
```

## Reference

- Query: [SELECT](/docs/2.0.0/beacondb/sql/select) · [WHERE](/docs/2.0.0/beacondb/sql/where) · [GROUP BY](/docs/2.0.0/beacondb/sql/group-by) · [JOIN](/docs/2.0.0/beacondb/sql/join) · [UNION BY NAME](/docs/2.0.0/beacondb/sql/union-by-name)
- Tables & views: [CREATE TABLE](/docs/2.0.0/beacondb/sql/create-table) · [CREATE VIEW](/docs/2.0.0/beacondb/sql/create-view) · [CREATE MATERIALIZED VIEW](/docs/2.0.0/beacondb/sql/create-materialized-view) · [Managed tables](/docs/2.0.0/beacondb/sql/managed-tables) · [Remote tables & `ATTACH`](/docs/2.0.0/beacondb/sql/remote-tables)
- Functions: [Table functions](/docs/2.0.0/beacondb/sql/table-functions) · [Utility table functions](/docs/2.0.0/beacondb/sql/table-functions-utility) · [Function reference](/docs/2.0.0/beacondb/sql/function-reference)
- Secrets & profiling: [`CREATE SECRET`](/docs/2.0.0/beacondb/sql/secrets) · [`SUMMARIZE`](/docs/2.0.0/beacondb/sql/summarize)

**Friendly SQL.** Beacon also supports these ergonomic extensions from DataFusion: `SELECT * EXCLUDE (col)` / `REPLACE (…)`, `GROUP BY ALL`, `QUALIFY`, `UNION BY NAME`, `FROM`-first (`FROM t SELECT …`), `DESCRIBE`, `SHOW TABLES` / `SHOW COLUMNS`, list literals, and trailing commas.
