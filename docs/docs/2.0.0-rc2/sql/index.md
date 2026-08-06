---
description: Query Beacon with standard SQL on Apache DataFusion. SELECT, WHERE, GROUP BY, JOIN, table functions such as read_netcdf(), DDL and a function reference.
---

# SQL Guide

Beacon uses [Apache DataFusion](https://datafusion.apache.org/) as its query engine. It gives you a
broad standard SQL dialect with `SELECT`, `WHERE`, `GROUP BY`, `JOIN`, `ORDER BY`, window functions
and more. Every transport below speaks the same dialect.

## Run SQL

| Interface | How |
| ------- | --- |
| **HTTP API** (server) | `POST /api/query` with `{ "sql": "..." }`. The SQL interface is on by default. |
| **Arrow Flight SQL** (server) | Any Flight SQL client, such as DataGrip, ADBC or DBeaver. It is on by default. |

## What you can query

**Registered tables**: any [external table](/docs/2.0.0-rc2/data-sources/external-tables) or
[view](/docs/2.0.0-rc2/server/view):

```sql
SELECT * FROM ocean_profiles LIMIT 100
```

**Files on demand**: use a [table function](/docs/2.0.0-rc2/sql/table-functions). You
register no table:

```sql
SELECT * FROM read_netcdf(['argo/**/*.nc']) LIMIT 100
```

## Reference

- Query: [SELECT](/docs/2.0.0-rc2/sql/select) · [WHERE](/docs/2.0.0-rc2/sql/where) · [GROUP BY](/docs/2.0.0-rc2/sql/group-by) · [JOIN](/docs/2.0.0-rc2/sql/join) · [UNION BY NAME](/docs/2.0.0-rc2/sql/union-by-name)
- Tables and views: [CREATE TABLE](/docs/2.0.0-rc2/sql/create-table) · [CREATE VIEW](/docs/2.0.0-rc2/sql/create-view) · [CREATE MATERIALIZED VIEW](/docs/2.0.0-rc2/sql/create-materialized-view) · [Managed tables](/docs/2.0.0-rc2/sql/managed-tables) · [Remote tables and `ATTACH`](/docs/2.0.0-rc2/sql/remote-tables)
- Functions: [Table functions](/docs/2.0.0-rc2/sql/table-functions) · [Utility table functions](/docs/2.0.0-rc2/sql/table-functions-utility) · [Function reference](/docs/2.0.0-rc2/sql/function-reference)
- Secrets and profiles: [`CREATE SECRET`](/docs/2.0.0-rc2/sql/secrets) · [`SUMMARIZE`](/docs/2.0.0-rc2/sql/summarize)

**Friendly SQL.** Beacon also supports these DataFusion extensions: `SELECT * EXCLUDE (col)`,
`REPLACE (…)`, `GROUP BY ALL`, `QUALIFY`, `UNION BY NAME`, `FROM` first (`FROM t SELECT …`),
`DESCRIBE`, `SHOW TABLES`, `SHOW COLUMNS`, list literals and trailing commas.
