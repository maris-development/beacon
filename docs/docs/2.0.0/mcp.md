---
description: Beacon has a built-in MCP server. AI agents discover tables and run read-only queries over the Model Context Protocol, with table comments as context and per-user auth.
# Unreleased: kept out of the local search index. Also listed in
# .vitepress/config.mts UNRELEASED_PAGES (sitemap + noindex). Remove both to release.
search: false
---

# MCP Server

Beacon has a built-in [MCP](https://modelcontextprotocol.io) server. AI agents such as Claude
discover your tables through it. They then run **read-only** queries over the Model Context
Protocol. The server uses the streamable-HTTP transport at `POST/GET/DELETE /mcp`. It runs next to
the REST API.

Beacon gives four fixed tools. An agent sees every table that its identity can read. The
[comments](/docs/2.0.0/sql/comment-on) on your tables and columns tell the agent what the data holds.

## Enable and configure

Beacon mounts the endpoint by default. These environment variables control it:

| Variable | Default | Effect |
|---|---|---|
| `BEACON_MCP_ENABLED` | `true` | Mount `/mcp`. Set `false`, `0` or `off` to switch it off. |
| `BEACON_MCP_ALLOWED_HOSTS` | empty | Extra `Host` values for `/mcp`, comma-separated, such as `beacon.example.org`. Set `*` to accept every host. |
| `BEACON_AUTH_ANONYMOUS_ENABLED` | `true` | Beacon maps a request without credentials to the anonymous principal. |
| `BEACON_AUTH_ENFORCE` | `false` | Beacon applies the read grants of each role at query time. |

The defaults keep `/mcp` on and open. Access is anonymous and read-only. To restrict access, set
`BEACON_AUTH_ENFORCE=true` and `BEACON_AUTH_ANONYMOUS_ENABLED=false`. Then give each agent a
credential. See [Authenticate an agent](#authenticate-an-agent).

`/mcp` accepts only the loopback hosts `localhost`, `127.0.0.1` and `::1`. This check stops DNS
rebinding attacks. A server with a public name refuses a request with
`403 Forbidden: Host header is not allowed`. Add the public name to `BEACON_MCP_ALLOWED_HOSTS`.
Behind a trusted reverse proxy, you can set `*`.

## Tools

`tools/list` always returns the same four tools:

- **`list_tables`**: returns each table that the caller can read, with its table comment as
  `description`.
- **`describe_table`**: returns the table comment as `description`, and one row per column with
  `name`, `data_type`, `nullable` and `description`. The column `description` is the column comment.
  If the column has no comment, Beacon uses the `description` metadata of the file format.
- **`run_sql`**: runs a read-only `SELECT` and returns JSON rows. It is a **bounded preview** with a
  limit of 1000 rows. Beacon truncates a larger result and points you to `export_query`.
- **`export_query`**: for **large** results. It returns a recipe: an `/api/query` request and a
  Python snippet. The recipe reads the result as a Parquet, Arrow or CSV file. See
  [Large results](#large-results).

The MCP interface is **read-only**. Every tool call runs without super-user privileges. The planner
therefore rejects `CREATE`, `INSERT`, `UPDATE`, `DELETE`, `COMMENT ON` and every other DDL or DML
statement. This holds for every caller. Each tool carries `annotations.readOnlyHint: true`.

## Make a table ready for MCP

A table is ready for MCP when two conditions are true:

1. The identity of the agent can read the table. See [Authenticate an agent](#authenticate-an-agent).
2. The table and its columns have comments. The agent reads them to write correct SQL.

```sql
COMMENT ON TABLE obs IS 'Argo float profiles: temperature and salinity by location, depth and time.';
COMMENT ON COLUMN obs.lat IS 'Latitude in decimal degrees';
COMMENT ON COLUMN obs.depth IS 'Measurement depth in meters';
```

See [COMMENT ON](/docs/2.0.0/sql/comment-on) for the full syntax.

Standard SQL replaces the per-table settings of earlier builds:

| Goal | Use |
|---|---|
| Hide a table from the agent | Do not grant `SELECT` on the table to the role of the agent. |
| Show only some columns | Create a view with those columns. Comment the view. |
| A named filter set | Create a view with a `WHERE` clause. Comment the view. |
| A hint for the agent | Write the hint in the table comment, for example "Filter by time first." |

```sql
CREATE VIEW obs_shallow AS SELECT lat, lon, depth, temperature FROM obs WHERE depth <= 10;
COMMENT ON TABLE obs_shallow IS 'Surface layer only. Filter by time first.';
```

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
as the [client API](/docs/2.0.0/security/access-control):

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
- **`tools/list`**: returns the four fixed tools. It reads no table.
- **`tools/call`**: Beacon resolves the identity of the caller. It then **clears super-user**,
  because MCP is read-only. `run_sql` runs the `SELECT` of the agent. `export_query` returns a
  recipe. `list_tables` and `describe_table` read the catalog and the Arrow schema of each table,
  which carries the comments. A new table or comment shows without a restart.
- **Results**: Beacon limits the rows and returns them as JSON tool content. Beacon returns an error
  as an MCP tool error with `isError: true`. The model can then react.
