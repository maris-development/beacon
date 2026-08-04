---
description: Beacon has a built-in MCP server. AI agents discover tables and run read-only queries over the Model Context Protocol, with per-table tools and per-user auth.
# Unreleased: kept out of the local search index. Also listed in
# .vitepress/config.mts UNRELEASED_PAGES (sitemap + noindex). Remove both to release.
search: false
---

# MCP Server

Beacon has a built-in [MCP](https://modelcontextprotocol.io) server. AI agents such as Claude
discover your tables through it. They then run **read-only** queries over the Model Context
Protocol. The server uses the streamable-HTTP transport at `POST/GET/DELETE /mcp`. It runs next to
the REST API.

Beacon generates the tool set from your data. It gives a few generic tools. It adds one tool for
each table with an enabled `mcp` extension.

## Enable and configure

Beacon mounts the endpoint by default. These environment variables control it:

| Variable | Default | Effect |
|---|---|---|
| `BEACON_MCP_ENABLED` | `true` | Mount `/mcp`. Set `false`, `0` or `off` to switch it off. |
| `BEACON_AUTH_ANONYMOUS_ENABLED` | `true` | Beacon maps a request without credentials to the anonymous principal. |
| `BEACON_AUTH_ENFORCE` | `false` | Beacon applies the read grants of each role at query time. |

The defaults keep `/mcp` on and open. Access is anonymous and read-only. To restrict access, set
`BEACON_AUTH_ENFORCE=true` and `BEACON_AUTH_ANONYMOUS_ENABLED=false`. Then give each agent a
credential. See [Authenticate an agent](#authenticate-an-agent).

## Tools

Beacon builds this list on every `tools/list` call:

- **`list_tables`**: returns the registered tables and their MCP status.
- **`describe_table`**: returns one row per column with `name`, `data_type`, `nullable` and
  `description`. It returns the `exposed_columns` if you set them. If not, it returns all columns.
  It also returns the extensions of the table.
- **`run_sql`**: runs a read-only `SELECT` and returns JSON rows. It is a **bounded preview** with a
  limit of 1000 rows. Beacon truncates a larger result and points you to `export_query`.
- **`export_query`**: for **large** results. It returns a recipe: an `/api/query` request and a
  Python snippet. The recipe reads the result as a Parquet, Arrow or CSV file. See
  [Large results](#large-results).
- **one tool for each table** with an enabled `mcp` extension. Beacon builds the tool from the
  extension. The tool takes `select`, limited to the exposed columns. It takes `preset`, an enum of
  the named filter sets of the table. It also takes `limit`.

The MCP interface is **read-only**. Every tool call runs without super-user privileges. The planner
therefore rejects `CREATE`, `INSERT`, `UPDATE`, `DELETE`, `SET EXTENSION` and every other DDL or DML
statement. This holds for every caller. Each tool carries `annotations.readOnlyHint: true`.

## Expose a table to MCP

A table becomes an MCP tool when you enable its
`mcp` [extension](/docs/2.0.0-rc2/server/extensions). Set the extension with SQL
(`SET EXTENSION`) or with the admin REST API. The optional `preset` extension adds named filter
sets.

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

Use `SHOW EXTENSIONS FOR obs` to read the extensions. Use `DROP EXTENSION 'mcp' FOR obs` to delete
one.

### Fields → the MCP `Tool` standard

| Extension field | MCP `Tool` | Notes |
|---|---|---|
| `tool_name` | `name` | Beacon accepts 1 to 64 characters from `[A-Za-z0-9_-]`. Beacon cleans the default name, `query_<table>`. |
| `title` | `title` | A label for a human reader. |
| `description` | `description` | What the **table** means. |
| `exposed_columns` | `inputSchema` | Limits `select`. The column descriptions go into the tool help and into `describe_table`. |
|-| `annotations.readOnlyHint` | Always `true`. |

An `exposed_columns` entry is a bare name such as `"lat"`. It can also be an object with `name` and
`description`. Beacon adds the column descriptions to the `select` help of the tool.
`describe_table` also returns them. The model then knows what each field means. Beacon parses the
payload strictly. It rejects unknown keys and invalid operators.

### Advisory guard rails

The `mcp` descriptor can hold a free-form `guardrails` map. The map takes any key and value pair.
Beacon adds the map to the description of the tool. `describe_table` also returns it. Beacon does
**not** enforce the map. Use it to guide the model:

```sql
SET EXTENSION 'mcp' FOR obs TO '{
  "enabled": true,
  "guardrails": {
    "recommended_row_limit": 10000,
    "note": "Always filter by time range; use export_query for full extracts."
  }
}';
```

Beacon allows any key. The values are hints only. The built-in `run_sql` preview limit controls the
result size. See the next section.

## Large results

`run_sql` puts the rows into the context of the model. The limit is 1000 rows. Use it for previews,
not for bulk data. Beacon marks a larger result with `"truncated": true`. It adds a `guidance` field
that points the model to `export_query`. The model therefore never treats a partial preview as the
full result.

`export_query` returns a **recipe**, not data. The model gets a small JSON object. A Python script
then reads the file from `/api/query`. That endpoint streams the Parquet, Arrow or CSV file in one
response. Call the tool with `{"sql": "SELECT …", "format": "parquet"}`. It returns a `request`
field, the POST body for `/api/query`. It also returns a `python` field with a runnable snippet:

```python
import io, requests, pandas as pd
resp = requests.post(f"{BEACON_URL}/api/query",
    headers={"Authorization": AUTH},
    json={"sql": "SELECT … FROM obs WHERE …", "output": {"format": "parquet"}})
resp.raise_for_status()
df = pd.read_parquet(io.BytesIO(resp.content))
```

The formats are `parquet` (default), `arrow` (IPC) and `csv`. Beacon accepts read-only `SELECT` and
`WITH` queries only. The query runs when the script runs. It runs under the credential that the
script sends.

## Authenticate an agent

`/mcp` authenticates with the HTTP `Authorization` header. It resolves the identity in the same way
as the [client API](/docs/2.0.0-rc2/security/access-control):

- **Basic**: `Authorization: Basic base64(user:pass)` gives the roles of a Beacon user.
- **Bearer**: `Authorization: Bearer <token>` takes an OIDC or OAuth2 JWT.
- **No header** gives the anonymous principal. If anonymous access is off, the caller gets no access.

MCP stays read-only for every identity. The identity decides *which reads* Beacon allows. This
applies when `BEACON_AUTH_ENFORCE=true`. Create a read-only user for the agent. Use SQL or the admin
API as a super-user:

```sql
CREATE USER agent WITH PASSWORD 's3cret';
GRANT SELECT ON obs TO ROLE readers;   -- when enforcing
GRANT ROLE readers TO USER agent;
```

::: tip
The built-in super-user of Beacon comes from the configuration only (`BEACON_ADMIN_*`). It is not a
client identity. Those admin credentials do **not** authenticate on `/mcp`. Use a `CREATE USER`
account or an OIDC token.
:::

## Connect a client

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

**GitHub Copilot CLI**: add Beacon as an MCP server with `gh copilot`:

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

**VS Code (CLI)**: register the server with `code --add-mcp`:

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

**Claude Desktop**: pass a static token with `mcp-remote`:

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

For an open local server, use the URL alone:
`{ "mcpServers": { "beacon": { "url": "http://localhost:5001/mcp" } } }`.

**Programmatic (MCP SDKs)**: set the header on the streamable-HTTP transport:

```ts
new StreamableHTTPClientTransport(new URL("https://your-host/mcp"), {
  requestInit: { headers: { Authorization: "Bearer <token>" } },
});
```

The transport adds the header to every request. Beacon authenticates each request. This holds inside
a long session too.

### Quick check

```bash
curl -s -X POST http://127.0.0.1:5001/mcp \
  -H "Content-Type: application/json" \
  -H "Accept: application/json, text/event-stream" \
  -H "Authorization: Basic $(printf 'agent:s3cret' | base64)" \
  -d '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"c","version":"0"}}}'
```

A `200` response with an `initialize` result means Beacon accepts the credential. A `401` response
means Beacon rejects it.

## How it works

The MCP server is a thin protocol adapter in front of the Beacon query runtime. It adds no query
engine. Every tool call becomes a normal Beacon query. MCP therefore uses the same planner, catalog,
metrics and access control.

- **Transport**: an `rmcp` streamable-HTTP service at `/mcp`. The `BEACON_MCP_ENABLED` flag controls
  it. It uses the same identity middleware as the client API.
- **`tools/list`**: Beacon builds the list on every call. It returns the generic tools and one tool
  for each enabled table. It reads the `mcp` and `preset` extensions. A new table appears without a
  restart.
- **`tools/call`**: Beacon resolves the identity of the caller. It then **clears super-user**,
  because MCP is read-only. `run_sql` and the table tools build a `SELECT` and run it.
  `export_query` returns a recipe. `describe_table` and `list_tables` read the catalog. A table tool
  expands the chosen `preset` into a `WHERE` clause with safe values. The model never sends raw SQL
  through a table tool.
- **Results**: Beacon limits the rows and returns them as JSON tool content. Beacon returns an error
  as an MCP tool error with `isError: true`. The model can then react.
