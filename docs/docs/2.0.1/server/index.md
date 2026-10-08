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
| **[Datasets & formats](/docs/2.0.1/server/datasets)** | Which files Beacon recognizes, and what each format supports |
| **[Tables & views](/docs/2.0.1/data-sources/)** | Giving files names: external tables, views, materialized views, crawlers, managed tables |
| **[Other sources](/docs/2.0.1/data-sources/object-storage)** | Data the node does not hold: a bucket, a Postgres database, another Beacon node |

Before this, [deploy the node](/docs/2.0.1/getting-started) and
[configure it](/docs/2.0.1/server/configuration). After it,
[decide who may read what](/docs/2.0.1/security/access-control) and
[point clients at it](/docs/2.0.1/connect/python).

## Core concepts

- **Datasets**: single files or stores, for example `.nc`, `.zarr` and `.parquet`. You query a
  dataset directly. It is the smallest unit in Beacon.
- **External tables**: a registered name over one or more files. Give a folder or a glob pattern.
  Beacon merges the schemas. You query the files as one table. See
  [External Tables](/docs/2.0.1/data-sources/external-tables).
- **Managed tables**: tables that Beacon owns. You change them with `INSERT`, `UPDATE` and
  `DELETE`. The Lance engine holds them by default. Iceberg is the other option. See
  [Managed Tables](/docs/2.0.1/sql/managed-tables).
- **Views**: a saved query that behaves like a table. See
  [Views](/docs/2.0.1/server/view).
- **Metadata and schema**: Beacon reads the dataset metadata and builds the schemas. You can
  therefore see the available columns before you write a query.
- **Pushdown and partitions**: Beacon pushes filters and projections down. This reduces the I/O and
  makes a query over large data faster.

## How it works at a glance

1. **Register or copy the datasets** into the configured data directories or object store.
2. **Inspect the schemas** through the API. You then know the available columns.
3. **Query a dataset or a table** with SQL or with the JSON query DSL.

For the full detail, see the [SQL query docs](/docs/2.0.1/api/querying/sql) and the
[JSON query docs](/docs/2.0.1/api/querying/json).
