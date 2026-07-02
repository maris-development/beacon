# Data lake

Beacon provides a lightweight data lake model that makes scientific datasets easy to discover, query, and serve through a single API. It supports array and tabular formats (NetCDF, Zarr, Parquet, ODV, CSV) and exposes them through Arrow + DataFusion for fast columnar reads.

## Core concepts

- **Datasets**: Individual files or stores (for example `.nc`, `.zarr`, `.parquet`). Datasets can be queried directly and are the smallest unit in Beacon.
- **External tables**: A registered name over one or more files (a folder or glob pattern) with a merged schema, queryable as one logical table. See [External Tables](./external-tables.md).
- **Managed tables**: tables Beacon owns and can mutate with `INSERT` / `UPDATE` / `DELETE`, backed by the Lance engine (default) or Iceberg. See [Managed Tables](../sql/managed-tables.md).
- **Views**: A saved query exposed as a table. See [Views](./view.md).
- **Metadata & schema**: Beacon inspects dataset metadata and builds schemas so you can discover available columns before running queries.
- **Pushdown & partitioning**: Filters and projections are pushed down to reduce IO and speed up queries over large data.
- **MCP server**: Expose your lakehouse to AI agents (e.g. Claude) as read-only tools over the Model Context Protocol, so they can discover tables and run `SELECT` queries. See [MCP Server](../mcp.md).

## How it works at a glance

1. **Register or place datasets** in the configured data directories or object store.
2. **Inspect schemas** through the API to understand available columns.
3. **Query datasets or tables** using SQL or the JSON query DSL.
4. **Serve it to AI agents** (optional): opt tables in via their `mcp` extension and let agents query the lakehouse over MCP. See [MCP Server](../mcp.md).

For detailed guidance, see the [SQL query docs](../api/querying/sql.md) and [JSON query docs](../api/querying/json.md).

## Serve the lakehouse to AI agents (MCP)

Once your datasets and tables are registered, Beacon can expose them to AI agents (e.g. Claude, GitHub Copilot) through its built-in [MCP Server](../mcp.md) — the same lakehouse, the same access control, served as **read-only** tools over the Model Context Protocol. Agents can discover your tables, inspect their schemas, and run `SELECT` queries without any extra service.

Exposure is opt-in per table via the table's `mcp` [extension](./extensions.md), so agents only ever see the tables and columns you choose:

```sql
SET EXTENSION 'mcp' FOR obs TO '{
  "enabled": true,
  "title": "Ocean observations",
  "description": "Argo float profiles: temperature and salinity by location, depth and time.",
  "exposed_columns": ["lat", "lon", "depth", "temperature"]
}';
```

Each opted-in table becomes a dedicated tool (restricted to its exposed columns), alongside generic `list_tables`, `describe_table`, and `run_sql` tools. Access is governed by the same identity and role grants as the rest of Beacon — see [Access control](../security/access-control.md).

To connect a client, lock down authentication, or expose large results, see the full [MCP Server guide](../mcp.md). The queries agents run are ordinary Beacon SQL — see the [SQL Guide](../sql/index.md) for what they can express.
