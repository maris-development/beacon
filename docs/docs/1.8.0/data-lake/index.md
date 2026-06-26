# Data lake

Beacon provides a lightweight data lake model that makes scientific datasets easy to discover, query, and serve through a single API. It supports array and tabular formats (NetCDF, Zarr, Parquet, ODV, CSV) and exposes them through Arrow + DataFusion for fast columnar reads.

## Core concepts

- **Datasets**: Individual files or stores (for example `.nc`, `.zarr`, `.parquet`). Datasets can be queried directly and are the smallest unit in Beacon.
- **External tables**: A registered name over one or more files (a folder or glob pattern) with a merged schema, queryable as one logical table. See [External Tables](./external-tables.md).
- **Managed tables**: tables Beacon owns and can mutate with `INSERT` / `UPDATE` / `DELETE`, backed by the Lance engine (default) or Iceberg. See [Managed Tables](../sql/managed-tables.md).
- **Views**: A saved query exposed as a table. See [Views](./view.md).
- **Metadata & schema**: Beacon inspects dataset metadata and builds schemas so you can discover available columns before running queries.
- **Pushdown & partitioning**: Filters and projections are pushed down to reduce IO and speed up queries over large data.

## How it works at a glance

1. **Register or place datasets** in the configured data directories or object store.
2. **Inspect schemas** through the API to understand available columns.
3. **Query datasets or tables** using SQL or the JSON query DSL.

For detailed guidance, see the [SQL query docs](../api/querying/sql.md) and [JSON query docs](../api/querying/json.md).
