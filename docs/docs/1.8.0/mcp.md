---
description: Beacon's built-in MCP server lets AI agents (e.g. Claude) discover tables and run read-only queries over the Model Context Protocol — with per-table tools generated from table extensions, per-user auth, and a large-result export path.
---

# MCP Server

Beacon ships a built-in [MCP](https://modelcontextprotocol.io) server so AI agents
(e.g. Claude) can discover your tables and run **read-only** queries over the
Model Context Protocol. It is served over the streamable-HTTP transport at
`POST/GET/DELETE /mcp`, alongside the REST API.

The tool set is generated from your data: a few generic tools plus one tool per
table you opt in via the table's `mcp` extension.

## Enabling & configuration

The endpoint is mounted by default. Relevant environment variables:

| Variable | Default | Effect |
|---|---|---|
| `BEACON_MCP_ENABLED` | `true` | Mount `/mcp`. Set `false`/`0`/`off` to disable. |
| `BEACON_AUTH_ANONYMOUS_ENABLED` | `true` | Unauthenticated requests resolve to the anonymous principal. |
| `BEACON_AUTH_ENFORCE` | `false` | Apply per-role read grants at query time. |

With the defaults, `/mcp` is on and open (anonymous, read-only). To lock it down,
set `BEACON_AUTH_ENFORCE=true` and `BEACON_AUTH_ANONYMOUS_ENABLED=false`, then
give each agent a credential (see [Authenticating an agent](#authenticating-an-agent)).

## Tools

Generated dynamically on every `tools/list`:

- **`list_tables`** — registered tables and their MCP exposure status.
- **`describe_table`** — a merged per-column view (`name`, `data_type`,
  `nullable`, `description`), scoped to `exposed_columns` when set or all columns
  otherwise, plus the table's extensions.
- **`run_sql`** — run a read-only `SELECT` and get JSON rows. A **bounded
  preview** (capped at 1000 rows); larger results are truncated and steer you to
  `export_query`.
- **`export_query`** — for **large** results: returns a recipe (an `/api/query`
  request + a Python snippet) to fetch the result as a Parquet/Arrow/CSV file. See
  [Large results](#large-results).
- **one tool per table** whose `mcp` extension is enabled, generated from the
  extension: `select` (restricted to the exposed columns), `preset` (an enum of
  the table's named filter sets), and `limit`.

The MCP surface is **strictly read-only**: every tool call runs with super-user
privileges cleared, so the planner rejects any DDL/DML (`CREATE`, `INSERT`,
`UPDATE`, `DELETE`, `SET EXTENSION`, …) regardless of who connects. Each tool
carries `annotations.readOnlyHint: true`.

## Exposing a table to MCP

A table becomes an MCP tool when its `mcp` [extension](/docs/1.8.0/data-lake/extensions)
is enabled. Set it via SQL (`SET EXTENSION`) or the admin REST API. An optional
`preset` extension adds named, predefined filter sets.

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

SET EXTENSION 'preset' FOR obs TO '{
  "presets": [
    {"name": "shallow", "description": "Surface layer",
     "filters": [{"column": "depth", "op": "<=", "value": 10}]}
  ]
}';
```

Read back or remove with `SHOW EXTENSIONS FOR obs` / `DROP EXTENSION 'mcp' FOR obs`.

### Fields → the MCP `Tool` standard

| Extension field | MCP `Tool` | Notes |
|---|---|---|
| `tool_name` | `name` | Validated to 1–64 chars of `[A-Za-z0-9_-]`; the generated default (`query_<table>`) is sanitized. |
| `title` | `title` | Human-readable label. |
| `description` | `description` | What the **table** means. |
| `exposed_columns` | `inputSchema` | Constrains `select`; per-column meanings feed its help + `describe_table`. |
| — | `annotations.readOnlyHint` | Always `true`. |

`exposed_columns` entries are a bare name (`"lat"`) or `{"name", "description"}`.
The per-column meanings are folded into the tool's `select` help and returned by
`describe_table`, so the model knows what each field represents. Payloads parse
strictly (unknown keys and invalid operators are rejected).

### Advisory guard rails

The `mcp` descriptor may carry a free-form `guardrails` map — arbitrary key/value
pairs that beacon surfaces to the agent (appended to the tool's description and
returned by `describe_table`) but does **not** enforce. Use it to nudge model
behavior:

```sql
SET EXTENSION 'mcp' FOR obs TO '{
  "enabled": true,
  "guardrails": {
    "recommended_row_limit": 10000,
    "note": "Always filter by time range; use export_query for full extracts."
  }
}';
```

Any keys are allowed — these are hints only. Enforcement of result size is the
separate, built-in `run_sql` preview cap (below).

## Large results

`run_sql` inlines rows into the model's context and is capped (1000 rows) — for
previews and reasoning, not bulk data. When a result exceeds the cap it is marked
`"truncated": true` with `guidance` steering the model to `export_query`, so a
partial preview is never mistaken for the full result.

`export_query` returns a **fetch recipe** rather than data: the model gets a small
JSON blob, and a Python script fetches the file directly from `/api/query` (which
streams the Parquet/Arrow/CSV in one response). Calling it with
`{"sql": "SELECT …", "format": "parquet"}` yields a `request` (POST body for
`/api/query`) and a runnable `python` snippet:

```python
import io, requests, pandas as pd
resp = requests.post(f"{BEACON_URL}/api/query",
    headers={"Authorization": AUTH},
    json={"sql": "SELECT … FROM obs WHERE …", "output": {"format": "parquet"}})
resp.raise_for_status()
df = pd.read_parquet(io.BytesIO(resp.content))
```

Formats: `parquet` (default), `arrow` (IPC), `csv`. Only read-only `SELECT`/`WITH`
queries are accepted; the query runs when the script runs, under whatever
credential the script sends.

## Authenticating an agent

`/mcp` authenticates via the HTTP `Authorization` header (the same identity
resolution as the [client API](/docs/1.8.0/security/access-control)):

- **Basic** — `Authorization: Basic base64(user:pass)` → a beacon user's roles.
- **Bearer** — `Authorization: Bearer <token>` → an OIDC/OAuth2 JWT.
- **No header** → the anonymous principal (if enabled), else no access.

MCP is read-only regardless of identity; the identity only decides *which reads*
are allowed (when `BEACON_AUTH_ENFORCE=true`). Create a read-only user for an
agent (as a super-user, via SQL or the admin API):

```sql
CREATE USER agent WITH PASSWORD 's3cret';
GRANT SELECT ON obs TO ROLE readers;   -- when enforcing
GRANT ROLE readers TO USER agent;
```

::: tip
Beacon's built-in super-user is **config-only** (`BEACON_ADMIN_*`) and is not a
client identity, so those admin credentials do **not** authenticate on `/mcp`.
Use a `CREATE USER` account or an OIDC token.
:::

## Connecting a client

**Claude Code (CLI):**

::: code-group

```bash [Anonymous]
claude mcp add --transport http beacon http://localhost:5001/mcp
```

```bash [Authenticated]
# Basic auth
claude mcp add --transport http beacon https://your-host/mcp \
  --header "Authorization: Basic $(printf 'agent:s3cret' | base64)"

# Bearer token
claude mcp add --transport http beacon https://your-host/mcp \
  --header "Authorization: Bearer <token>"
```

:::

**GitHub Copilot CLI** — add Beacon as an MCP server via `gh copilot`:

::: code-group

```bash [Anonymous]
gh copilot mcp add --name beacon --type http --url http://localhost:5001/mcp
```

```bash [Authenticated]
# Basic auth
gh copilot mcp add --name beacon --type http --url https://your-host/mcp \
  --header "Authorization: Basic $(printf 'agent:s3cret' | base64)"

# Bearer token
gh copilot mcp add --name beacon --type http --url https://your-host/mcp \
  --header "Authorization: Bearer <token>"
```

:::

**VS Code (CLI)** — register the server with `code --add-mcp`:

::: code-group

```bash [Anonymous]
code --add-mcp "{\"name\":\"beacon\",\"type\":\"http\",\"url\":\"http://localhost:5001/mcp\"}"
```

```bash [Authenticated]
# Basic auth
code --add-mcp "{\"name\":\"beacon\",\"type\":\"http\",\"url\":\"https://your-host/mcp\",\"headers\":{\"Authorization\":\"Basic $(printf 'agent:s3cret' | base64)\"}}"

# Bearer token
code --add-mcp "{\"name\":\"beacon\",\"type\":\"http\",\"url\":\"https://your-host/mcp\",\"headers\":{\"Authorization\":\"Bearer <token>\"}}"
```

:::

**Claude Desktop** — bridge a static token with `mcp-remote`:

```json
{
  "mcpServers": {
    "beacon": {
      "command": "npx",
      "args": ["mcp-remote", "https://your-host/mcp",
               "--header", "Authorization: Bearer <token>"]
    }
  }
}
```

For an open/anonymous local instance, point straight at the URL:
`{ "mcpServers": { "beacon": { "url": "http://localhost:5001/mcp" } } }`.

**Programmatic (MCP SDKs)** — set the header on the streamable-HTTP transport:

```ts
new StreamableHTTPClientTransport(new URL("https://your-host/mcp"), {
  requestInit: { headers: { Authorization: "Bearer <token>" } },
});
```

The transport attaches the header to every request — beacon authenticates per
request, even within a long-lived session.

### Quick check

```bash
curl -s -X POST http://127.0.0.1:5001/mcp \
  -H "Content-Type: application/json" \
  -H "Accept: application/json, text/event-stream" \
  -H "Authorization: Basic $(printf 'agent:s3cret' | base64)" \
  -d '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"c","version":"0"}}}'
```

A `200` with an `initialize` result means the credential was accepted; `401` means
it was rejected.

## How it works

The MCP server is a thin protocol adapter in front of beacon's query runtime — it
adds no query engine of its own. Every tool call becomes a normal beacon query, so
MCP inherits the planner, catalog, metrics, and access control.

- **Transport** — an `rmcp` streamable-HTTP service mounted at `/mcp`, behind the
  `BEACON_MCP_ENABLED` flag and the same identity-resolution middleware as the
  client API.
- **`tools/list`** — rebuilt per call: the generic tools plus one per enabled
  table, generated from the `mcp`/`preset` extensions (so newly-exposed tables
  appear without a restart).
- **`tools/call`** — resolves the caller's identity, **clears super-user** (MCP is
  read-only), and dispatches: `run_sql`/table tools build a `SELECT` and run it;
  `export_query` returns a fetch recipe; `describe_table`/`list_tables` read the
  catalog. Per-table tools expand the chosen `preset` into a `WHERE` clause with
  safely-rendered values — the model never supplies raw SQL through them.
- **Results** — capped and returned as JSON tool content; errors come back as MCP
  tool errors (`isError: true`) so the model can react.
