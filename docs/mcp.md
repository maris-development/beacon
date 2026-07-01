# MCP server

Beacon ships an [MCP](https://modelcontextprotocol.io) server so MCP clients
(e.g. Claude) can discover beacon's tables and run **read-only** queries against
them. It is served over the streamable-HTTP transport at `POST/GET/DELETE /mcp`
by the `beacon-mcp` crate, mounted alongside the REST API.

For how it works internally, see [mcp-architecture.md](mcp-architecture.md).

## Enabling & configuration

The endpoint is mounted by default. Relevant environment variables:

| Variable | Default | Effect |
|---|---|---|
| `BEACON_MCP_ENABLED` | `true` | Mount `/mcp`. Set `false`/`0`/`off` to disable. |
| `BEACON_AUTH_ANONYMOUS_ENABLED` | `true` | Unauthenticated requests resolve to the anonymous principal. |
| `BEACON_AUTH_ENFORCE` | `false` | Apply per-role read grants at query time. |

With the defaults, `/mcp` is on and open (anonymous, read-only). To lock it down,
set `BEACON_AUTH_ENFORCE=true` and `BEACON_AUTH_ANONYMOUS_ENABLED=false`, then
issue each agent a credential (below).

## Tools

The tool set is generated dynamically from the runtime on every `tools/list`:

- **`list_tables`** — registered tables and their MCP exposure status.
- **`describe_table`** — a merged per-column view (`name`, `data_type`,
  `nullable`, `description`), scoped to `exposed_columns` when set or all columns
  otherwise, plus the raw extensions. Descriptions come from the extension,
  falling back to the Arrow field's `description`/`comment` metadata.
- **`run_sql`** — run a read-only `SELECT` and get JSON rows (for previews; capped).
- **`export_query`** — for **large** results: returns a *recipe* (the exact
  `/api/query` request + a ready-to-run Python snippet) to fetch the result as a
  Parquet/Arrow/CSV file. It does **not** run the query or return rows, so the
  bytes never enter the model's context. See [Large results](#large-results).
- **one tool per table** whose `mcp` extension is enabled. Inputs are derived from
  the extension: `select` (restricted to `exposed_columns`; its help lists each
  column as `name (type): meaning`), `preset` (an enum of the table's preset
  names, expanded to filters at query time), and `limit`.

The MCP surface is **strictly read-only**: every tool call executes with
`is_super_user` cleared, so the query planner rejects any DDL/DML (`CREATE`,
`INSERT`, `UPDATE`, `DELETE`, `SET EXTENSION`, …) regardless of who connects —
only `SELECT` runs. The caller's roles are preserved, so per-user read grants
(RBAC) still apply. Every tool carries `annotations.readOnlyHint: true`. Results
are capped (1000 rows) to keep tool output bounded.

## Exposing a table to MCP

Tables become MCP tools via the table-extensions surface (SQL or the admin REST
API). The `mcp` extension describes the table; an optional `preset` extension
adds named, predefined filter sets.

```sql
SET EXTENSION 'mcp' FOR obs TO '{
  "enabled": true,
  "tool_name": "query_obs",
  "title": "Ocean observations",
  "description": "Argo float profiles: temperature and salinity by location, depth and time.",
  "exposed_columns": [
    {"name": "lat",   "description": "latitude in decimal degrees"},
    {"name": "lon",   "description": "longitude in decimal degrees"},
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

`query_obs` then appears as an MCP tool with a `preset: "shallow"` option. Read
back or remove with `SHOW EXTENSIONS FOR obs` / `DROP EXTENSION 'mcp' FOR obs`.

### How the `mcp` fields map to the MCP `Tool` standard

| Extension field | MCP `Tool` | Notes |
|---|---|---|
| `tool_name` | `name` | Validated to 1–64 chars of `[A-Za-z0-9_-]`; the generated default (`query_<table>`) is sanitized. |
| `title` | `title` | Human-readable label. |
| `description` | `description` | What the **table** means. |
| `exposed_columns` | `inputSchema` | Constrains `select`; per-column meanings feed its help + `describe_table`. |
| — | `annotations.readOnlyHint` | Always `true`. |

`exposed_columns` entries are either a bare name (`"lat"`) or
`{"name": ..., "description": ...}`. Payloads parse strictly
(`deny_unknown_fields`, typed operators), so typos/extra keys are rejected rather
than silently dropped. See the table-extensions docs for the full schema.

## Large results

`run_sql` inlines rows into the model's context and is capped (1000 rows) — it's
for previews and reasoning, not bulk data. For large exports, `export_query`
returns a **fetch recipe** instead of the data: the model gets a small JSON blob,
and a Python script fetches the file directly from `/api/query` (which streams the
Parquet/Arrow/CSV in one response). The bytes never pass through the model.

Calling `export_query` with `{"sql": "SELECT ... FROM obs WHERE ...", "format": "parquet"}`
returns something like:

```json
{
  "note": "This does not run the query. POST `request.body` to <BEACON_URL>/api/query; the response body IS the file.",
  "format": "parquet",
  "request": {
    "method": "POST", "path": "/api/query",
    "headers": {"Content-Type": "application/json", "Authorization": "<same as MCP; omit if anonymous>"},
    "body": {"sql": "SELECT ... FROM obs WHERE ...", "output": {"format": "parquet"}}
  },
  "python": "import io, requests, pandas as pd\nBEACON_URL = \"http://localhost:5001\"\n..."
}
```

The `python` field is runnable as-is (fill in `BEACON_URL`/`AUTH`):

```python
import io, requests, pandas as pd
resp = requests.post(f"{BEACON_URL}/api/query",
    headers={"Authorization": AUTH},
    json={"sql": "SELECT ... FROM obs WHERE ...", "output": {"format": "parquet"}})
resp.raise_for_status()
df = pd.read_parquet(io.BytesIO(resp.content))
```

Formats: `parquet` (default; typed, columnar), `arrow` (IPC), `csv`. Only
read-only `SELECT`/`WITH` queries are accepted. The query runs when the script
runs, under whatever credential the script sends (same read-only rules as MCP).

## Authenticating an agent

`/mcp` authenticates via the HTTP `Authorization` header (the same
`resolve_identity` path as the client API):

- **Basic** — `Authorization: Basic base64(user:pass)` → a beacon user's roles.
- **Bearer** — `Authorization: Bearer <token>` → an OIDC/OAuth2 JWT.
- **No header** → the anonymous principal (if enabled), else no access.

MCP is read-only regardless of identity; the identity only decides *which reads*
are allowed (when `BEACON_AUTH_ENFORCE=true`).

Create a read-only user to hand to an agent (as a super-user, via SQL or the
admin API):

```sql
CREATE USER agent WITH PASSWORD 's3cret';
-- when enforcing, grant reads and assign a role:
GRANT SELECT ON obs TO ROLE readers;
GRANT ROLE readers TO USER agent;
```

> Beacon's built-in super-user is **config-only** (`BEACON_ADMIN_*`) and is not a
> client identity, so those admin credentials do **not** authenticate on `/mcp`.
> Use a `CREATE USER` account or an OIDC token.

## Connecting a client

**Claude Code (CLI)** — streamable HTTP with an auth header:

```bash
claude mcp add --transport http beacon https://your-host/mcp \
  --header "Authorization: Basic $(printf 'agent:s3cret' | base64)"
# or: --header "Authorization: Bearer <token>"
```

**Claude Desktop** — for a static token, bridge with `mcp-remote`:

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

(For an open/anonymous local instance you can point a client straight at the URL
with no header: `{ "mcpServers": { "beacon": { "url": "http://localhost:5001/mcp" } } }`.)

**Programmatic (MCP SDKs)** — set the header on the streamable-HTTP transport:

```ts
new StreamableHTTPClientTransport(new URL("https://your-host/mcp"), {
  requestInit: { headers: { Authorization: "Bearer <token>" } },
});
```

```python
streamablehttp_client("https://your-host/mcp",
                      headers={"Authorization": "Bearer <token>"})
```

The transport attaches the header to every request, which is what beacon needs —
it authenticates per request, even within a long-lived MCP session.

## Quick check

```bash
curl -s -X POST http://127.0.0.1:5001/mcp \
  -H "Content-Type: application/json" \
  -H "Accept: application/json, text/event-stream" \
  -H "Authorization: Basic $(printf 'agent:s3cret' | base64)" \
  -d '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"c","version":"0"}}}'
```

A `200` with an `initialize` result means the credential was accepted; `401`
means it was rejected.
