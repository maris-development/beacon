---
description: Table extensions attach optional, named behavior to a registered table — the `mcp` extension exposes it to AI agents as a read-only tool, and the `preset` extension defines reusable named filter sets.
---

# Table Extensions

An **extension** is a named JSON descriptor attached to a registered table that turns on optional behavior without changing the table's data or schema. Extensions are persisted with the table and survive restarts.

Two extensions ship today:

- **`mcp`** — expose the table to AI agents as a read-only [MCP](../mcp.md) tool.
- **`preset`** — define named, predefined filter sets that other features (e.g. the `mcp` tool) can reference by name.

## Managing extensions

Extensions are set, inspected, and removed with SQL DDL (or the equivalent admin REST API). The value is a JSON object whose shape depends on the extension.

```sql
-- Attach or replace an extension
SET EXTENSION '<name>' FOR <table> TO '<json>';

-- List the extensions on a table
SHOW EXTENSIONS FOR <table>;

-- Remove an extension
DROP EXTENSION '<name>' FOR <table>;
```

Payloads are parsed strictly — unknown keys and invalid values are rejected rather than ignored, so a typo fails loudly instead of silently disabling the feature.

## The `mcp` extension

Enabling the `mcp` extension makes the table discoverable to AI agents and generates a dedicated per-table tool on Beacon's [MCP Server](../mcp.md). The tool is **read-only** and constrained to the columns you expose.

```sql
SET EXTENSION 'mcp' FOR obs TO '{
  "enabled": true,
  "tool_name": "query_obs",
  "title": "Ocean observations",
  "description": "Argo float profiles: temperature and salinity by location, depth and time.",
  "exposed_columns": [
    {"name": "lat",   "description": "latitude in decimal degrees"},
    {"name": "depth", "description": "measurement depth in meters"},
    "temperature"
  ]
}';
```

| Field | Purpose |
|---|---|
| `enabled` | Turns the tool on. Set `false` to hide the table without dropping the extension. |
| `tool_name` | The generated tool's name (`[A-Za-z0-9_-]`, 1–64 chars). Defaults to `query_<table>`. |
| `title` | Human-readable label for the tool. |
| `description` | What the table means — shown to the agent so it picks the right tool. |
| `exposed_columns` | Whitelist of columns the agent may `select`. Each entry is a bare name (`"temperature"`) or `{"name", "description"}`; the descriptions help the model understand each field. Omit to expose all columns. |
| `guardrails` | Optional free-form map of advisory hints (see below). |

### Advisory guard rails

The optional `guardrails` map holds arbitrary key/value hints that Beacon surfaces to the agent (appended to the tool description and returned by `describe_table`) but does **not** enforce:

```sql
SET EXTENSION 'mcp' FOR obs TO '{
  "enabled": true,
  "guardrails": {
    "recommended_row_limit": 10000,
    "note": "Always filter by time range; use export_query for full extracts."
  }
}';
```

These are nudges only. Actual result-size limits come from the built-in `run_sql` preview cap — see the [MCP Server guide](../mcp.md) for the full tool set, connecting a client, and authenticating agents.

## The `preset` extension

A **preset** is a named, predefined filter set stored on the table. Presets give agents (and other callers) a curated shorthand for common query shapes, so they can pick `shallow` instead of re-deriving the filter each time. When a table has both the `mcp` and `preset` extensions, its generated MCP tool exposes the preset names as an enum.

```sql
SET EXTENSION 'preset' FOR obs TO '{
  "presets": [
    {
      "name": "shallow",
      "description": "Surface layer only",
      "filters": [{"column": "depth", "op": "<=", "value": 10}]
    },
    {
      "name": "north_atlantic",
      "description": "North Atlantic basin",
      "filters": [
        {"column": "lat", "op": ">=", "value": 0},
        {"column": "lat", "op": "<=", "value": 70}
      ]
    }
  ]
}';
```

Each preset has a `name`, a `description` (what it selects), and a list of `filters`. A filter is `{"column", "op", "value"}`; multiple filters in one preset are combined with `AND`.

## See also

- [MCP Server](../mcp.md) — connect an agent, authenticate it, and handle large results.
- [External Tables](./external-tables.md) — registering the tables you attach extensions to.
- [Access control](../security/access-control.md) — the identity and role grants that govern what agents can read.
