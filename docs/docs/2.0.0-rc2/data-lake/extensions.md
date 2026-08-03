---
description: A table extension adds optional, named behaviour to a registered table. The preset extension defines named filter sets.
# MCP is unreleased. Restore the original description on release:
# description: A table extension adds optional, named behaviour to a registered table. The mcp extension gives it to AI agents. The preset extension defines named filter sets.
---

# Table Extensions

An **extension** is a named JSON descriptor on a registered table. It switches optional behaviour
on. It changes neither the data nor the schema of the table. Beacon stores an extension with the
table. It survives a restart.

<!-- MCP is unreleased. Restore on release (and drop the single-extension wording below):

Beacon has two extensions today:

- **`mcp`**: give the table to AI agents as a read-only [MCP](/docs/2.0.0-rc2/mcp) tool.
- **`preset`**: define named filter sets. Other features, such as the `mcp` tool, use them by name.
-->

Beacon has one extension today:

- **`preset`**: define named filter sets. Other features use them by name.

## Manage extensions

Set, inspect and remove an extension with SQL DDL. The admin REST API gives the same functions. The
value is a JSON object. Each extension has its own shape.

```sql
-- Attach or replace an extension
SET EXTENSION '<name>' FOR <table> TO '<json>';

-- List the extensions on a table
SHOW EXTENSIONS FOR <table>;

-- Remove an extension
DROP EXTENSION '<name>' FOR <table>;
```

Beacon parses the payload strictly. It rejects an unknown key and an invalid value. It does not
ignore them. A spelling error therefore gives an error. It does not switch the feature off in
silence.

<!-- MCP is unreleased. Restore this whole section on release:

## The `mcp` extension

Enable the `mcp` extension to give the table to AI agents. Beacon then generates a tool for that
table on its [MCP Server](/docs/2.0.0-rc2/mcp). The tool is **read-only**. It covers the columns
that you expose.

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
| `enabled` | Switches the tool on. Set `false` to hide the table. The extension stays. |
| `tool_name` | The name of the tool. Use 1 to 64 characters from `[A-Za-z0-9_-]`. The default is `query_<table>`. |
| `title` | A label for a human reader. |
| `description` | What the table means. Beacon shows it to the agent, so the agent picks the correct tool. |
| `exposed_columns` | The columns that the agent can `select`. An entry is a bare name such as `"temperature"`, or an object with `name` and `description`. A description helps the model to understand the field. Omit the key to expose every column. |
| `guardrails` | An optional free-form map with hints. See below. |

### Advisory guard rails

The optional `guardrails` map holds any key and value hint. Beacon adds the map to the tool
description. `describe_table` also returns it. Beacon does **not** enforce the map:

```sql
SET EXTENSION 'mcp' FOR obs TO '{
  "enabled": true,
  "guardrails": {
    "recommended_row_limit": 10000,
    "note": "Always filter by time range; use export_query for full extracts."
  }
}';
```

These values are hints only. The built-in `run_sql` preview limit controls the result size. The
[MCP Server guide](/docs/2.0.0-rc2/mcp) gives the full tool set. It also shows how to connect a
client and how to authenticate an agent.

-->

## The `preset` extension

<!-- MCP is unreleased. Restore this paragraph on release (it replaces the one below):

A **preset** is a named filter set on the table. A preset gives an agent or another caller a short
name for a common query. The caller picks `shallow` instead of the full filter. A table with both
the `mcp` and the `preset` extension gets an MCP tool. That tool shows the preset names as an enum.
-->

A **preset** is a named filter set on the table. A preset gives a caller a short name for a common
query. The caller picks `shallow` instead of the full filter.

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

Each preset has a `name`, a `description` and a list of `filters`. The description says what the
preset selects. A filter is `{"column", "op", "value"}`. Beacon combines the filters of one preset
with `AND`.

## See also

<!-- MCP is unreleased. Restore on release:
- [MCP Server](/docs/2.0.0-rc2/mcp): connect an agent, authenticate it and handle a large result.
-->
- [External Tables](/docs/2.0.0-rc2/beacondb/data-sources/external-tables): register the tables that take an extension.
- [Access control](/docs/2.0.0-rc2/security/access-control): the identity and role grants that control what an agent reads.
