---
description: Register the data a Beacon node serves. Datasets and formats, tables and views, crawlers, and sources outside the node.
---

# Server setup

Your server runs. This chapter is the next step. Turn a directory or a bucket of files into a
catalog. Your users then query it by name.

No step here copies data. Each step makes a definition. Beacon stores the definition and resolves it
at query time. A managed table is the one exception. Beacon owns those rows.

## The three jobs

| | What it covers |
|---|---|
| **[Datasets & formats](/docs/2.0.0-rc5/server/datasets)** | Which files Beacon recognizes, and what each format supports |
| **[Tables & views](/docs/2.0.0-rc5/data-sources/)** | Giving files names: external tables, views, materialized views, crawlers, managed tables |
| **[Other sources](/docs/2.0.0-rc5/data-sources/object-storage)** | Data the node does not hold: a bucket, a Postgres database, another Beacon node |

Before this, [deploy the node](/docs/2.0.0-rc5/getting-started) and
[configure it](/docs/2.0.0-rc5/server/configuration). After it,
[decide who may read what](/docs/2.0.0-rc5/security/access-control) and
[point clients at it](/docs/2.0.0-rc5/connect/python).

## Core concepts

- **Datasets**: single files or stores, for example `.nc`, `.zarr` and `.parquet`. You query a
  dataset directly. It is the smallest unit in Beacon.
- **External tables**: a registered name over one or more files. Give a folder or a glob pattern.
  Beacon merges the schemas. You query the files as one table. See
  [External Tables](/docs/2.0.0-rc5/data-sources/external-tables).
- **Managed tables**: tables that Beacon owns. You change them with `INSERT`, `UPDATE` and
  `DELETE`. The Lance engine holds them by default. Iceberg is the other option. See
  [Managed Tables](/docs/2.0.0-rc5/sql/managed-tables).
- **Views**: a saved query that behaves like a table. See
  [Views](/docs/2.0.0-rc5/server/view).
- **Metadata and schema**: Beacon reads the dataset metadata and builds the schemas. You can
  therefore see the available columns before you write a query.
- **Pushdown and partitions**: Beacon pushes filters and projections down. This reduces the I/O and
  makes a query over large data faster.
<!-- MCP is unreleased. Restore on release:
- **MCP server**: give your tables to AI agents such as Claude, as read-only tools over the Model
  Context Protocol. The agents find the tables and run `SELECT` queries. See
  [MCP Server](/docs/2.0.0-rc5/mcp).
-->

## How it works at a glance

1. **Register or copy the datasets** into the configured data directories or object store.
2. **Inspect the schemas** through the API. You then know the available columns.
3. **Query a dataset or a table** with SQL or with the JSON query DSL.
<!-- MCP is unreleased. Restore on release:
4. **Serve it to AI agents**, optional. Enable the `mcp` extension of a table. The agents then query
   the catalog over MCP. See [MCP Server](/docs/2.0.0-rc5/mcp).
-->

For the full detail, see the [SQL query docs](/docs/2.0.0-rc5/api/querying/sql) and the
[JSON query docs](/docs/2.0.0-rc5/api/querying/json).

<!-- MCP is unreleased. Restore this whole section on release:

## Serve your tables to AI agents (MCP)

Register your datasets and tables first. Beacon can then give them to AI agents such as Claude and
GitHub Copilot. It uses its built-in [MCP Server](/docs/2.0.0-rc5/mcp). The catalog and the access
control stay the same. Beacon serves the tables as **read-only** tools over the Model Context
Protocol. An agent finds your tables, inspects their schemas and runs `SELECT` queries. You deploy
no extra service.

You enable each table with its `mcp` [extension](/docs/2.0.0-rc5/server/extensions). An agent
therefore sees only the tables and columns that you choose:

```sql
SET EXTENSION 'mcp' FOR obs TO '{
  "enabled": true,
  "title": "Ocean observations",
  "description": "Argo float profiles: temperature and salinity by location, depth and time.",
  "exposed_columns": ["lat", "lon", "depth", "temperature"]
}';
```

Each enabled table becomes its own tool. The tool covers the exposed columns only. Beacon also gives
the generic tools `list_tables`, `describe_table` and `run_sql`. The same identity and role grants
control the access. See [Access control](/docs/2.0.0-rc5/security/access-control).

The full [MCP Server guide](/docs/2.0.0-rc5/mcp) shows how to connect a client, how to restrict
authentication and how to serve a large result. An agent runs ordinary Beacon SQL. The
[SQL Guide](/docs/2.0.0-rc5/sql/) gives the full dialect.

-->
