# MCP server

Beacon ships an [MCP](https://modelcontextprotocol.io) server over the
streamable-HTTP transport at `POST/GET/DELETE /mcp`, so MCP clients (e.g. Claude)
can discover tables and run read-only queries.

## Tools

Generated dynamically from the runtime:

- `list_tables` — registered tables and their MCP exposure status.
- `describe_table` — a table's column schema plus its extensions (MCP descriptor, presets).
- `run_sql` — run a read-only `SELECT` and get JSON rows.
- **one tool per table** whose `mcp` extension is enabled (see below). Its inputs are
  derived from the extension: `select` (restricted to `exposed_columns`), `preset`
  (an enum of the table's preset names, expanded to filters), and `limit`.

The MCP surface is **strictly read-only**: every tool call executes with
`is_super_user` cleared, so the query planner rejects any DDL/DML (`CREATE`,
`INSERT`, `UPDATE`, `DELETE`, `SET EXTENSION`, …) regardless of who connects —
only `SELECT` runs. The caller's roles are preserved, so per-user read grants
(RBAC) still apply. Results are capped (1000 rows) to keep tool output bounded.

## Exposing a table to MCP

Use the table-extensions surface (SQL or REST):

```sql
SET EXTENSION 'mcp' FOR obs TO '{
  "enabled": true,
  "tool_name": "query_obs",
  "title": "Ocean observations",
  "description": "Argo float profiles: temperature and salinity by location, depth and time.",
  "exposed_columns": [
    {"name": "lat", "description": "latitude in decimal degrees"},
    {"name": "lon", "description": "longitude in decimal degrees"},
    {"name": "depth", "description": "measurement depth in meters"},
    "temperature"
  ]
}';
SET EXTENSION 'preset' FOR obs TO '{"presets":[{"name":"shallow","filters":[{"column":"depth","op":"<=","value":10}]}]}';
```

`query_obs` then appears as an MCP tool with a `preset: "shallow"` option.

The `mcp` descriptor maps to the MCP `Tool` standard: `tool_name` → `Tool.name`
(validated to MCP/Anthropic rules — 1–64 chars of `[A-Za-z0-9_-]`; the generated
default is sanitized), `title` → `Tool.title`, and `description` describes **what
the table means** → `Tool.description`. `exposed_columns` constrain the generated
`inputSchema`; each entry is either a bare name or `{"name", "description"}` — the
per-column meanings are folded into the `select` parameter help and returned by
`describe_table`, so the model knows what each field represents. Every tool carries
`annotations.readOnlyHint: true`. Payloads are parsed strictly
(`deny_unknown_fields`), so unknown keys are rejected rather than ignored.

## Connecting Claude

**HTTP (Claude Code / API):** point the client at `http://<host>:<port>/mcp`.

**Claude Desktop** (via an HTTP-capable MCP entry):

```json
{ "mcpServers": { "beacon": { "url": "http://localhost:5001/mcp" } } }
```

> The endpoint rides the same `resolve_identity` middleware as the client API:
> requests authenticate via `Authorization` (resolving to that user's roles), or
> the anonymous principal when enabled, or a role-less identity otherwise. It is
> read-only regardless. Gate it with `BEACON_MCP_ENABLED=false` to disable.
