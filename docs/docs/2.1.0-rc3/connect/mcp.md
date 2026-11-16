---
description: Beta. Connect an AI agent to Beacon over the Model Context Protocol (MCP). Describe the tables with COMMENT ON, and let the AI Agent query them.
---

# MCP server <Badge type="warning" text="beta" />

::: warning Beta. In development
The MCP server is in development. The tools, their arguments and their output can change in a
new release without notice. Do not build production workflows on it yet. Report problems in the
[Beacon issue tracker](https://github.com/maris-development/beacon/issues).
:::

Beacon is queryable by agents over the Model Context Protocol (MCP). An AI agent can directly connect to Beacon's MCP Server, discover the tables and
run read-only SQL.

The agent knows only what the comments tell it. The procedure thus has two parts:

1. [Prepare the server](#prepare-the-server): add comments and give the agent context.
2. [Connect an agent](#connect-an-agent): add the endpoint to the MCP client.

## Prepare the server

### 1. Describe the tables

Add a comment to each table and each column that provides the agent of context. A table needs no other MCP
configuration.

```sql
COMMENT ON TABLE ctd IS 'CTD casts in the North Sea, 2024. One row per depth level. Source: RV Pelagia';
COMMENT ON COLUMN ctd.depth IS 'Depth below the sea surface. Unit: m';
COMMENT ON COLUMN ctd.temp IS 'Sea water temperature (ITS-90). Unit: degC';
COMMENT ON COLUMN ctd.qc IS 'Quality flag (SeaDataNet L20): 1 good, 4 bad';
```

Only the super-user can set a comment. The agent reads the comments as follows:

- The table comment tells the agent what the table consists of.
- The column comment tells the agent what a column holds. Give the unit and the code list.
- A column without a comment shows the `description` metadata of the source file, if the file has
  it.

Create a view with its own comment to give the agent a subset of a table. See
[`COMMENT ON`](/docs/2.1.0-rc3/sql/comment-on) for the full syntax and for more guidelines.

### 2. Allow the public host name

Beacon serves MCP at `/mcp` with the streamable HTTP transport. With a base path, the endpoint is
`{BEACON_BASE_PATH}/mcp`.

By default, `/mcp` accepts only requests with a loopback `Host` header: `localhost`, `127.0.0.1`
and `::1`. Another `Host` gets `403 Forbidden`. This protects against DNS rebinding.

Add the public name of the server to `BEACON_MCP_ALLOWED_HOSTS`:

```bash
BEACON_MCP_ALLOWED_HOSTS=beacon.example.org,beacon.example.org:8443
```

| Variable | Default | Description |
| --- | --- | --- |
| `BEACON_MCP_ENABLED` | `true` | Serve the MCP endpoint. `false`, `0` or `off` turns it off. |
| `BEACON_MCP_ALLOWED_HOSTS` | _(empty)_ | Comma-separated `Host` values that `/mcp` accepts in addition to the loopback hosts. An entry is a host or a `host:port`. `*` accepts every host. |

## Connect an agent

MCP uses the same authentication as the HTTP API. Send the credentials of the agent user in an
`Authorization` header:

- `Basic <base64 of username:password>`
- `Bearer <token>`, for example an OIDC token

### Claude Code

```bash
claude mcp add --transport http beacon https://beacon.example.org/mcp --header "Authorization: Basic <base64>"
```

A local server with anonymous access needs no header:

```bash
claude mcp add --transport http beacon http://localhost:5001/mcp
```

### Other clients

Most clients take a JSON configuration. Give the URL and, if necessary, the header:

```json
{
  "mcpServers": {
    "beacon": {
      "type": "http",
      "url": "https://beacon.example.org/mcp",
      "headers": { "Authorization": "Bearer <token>" }
    }
  }
}
```

The field names are different in each client. Refer to the documentation of your client.

### Do a test of the connection

Ask the agent which tables it can read. The agent calls `list_tables` and shows each table with
its comment.

- An empty list means that the user has no read grant.
- A table without a description means that the table has no comment.

## What the agent can do

The tool set is fixed. Each tool is read-only and reads only the tables in the default schema.

| Tool | Arguments | Result |
| --- | --- | --- |
| `list_tables` | _(none)_ | The tables that the caller can read, each with its table comment. |
| `describe_table` | `table_name` | The table comment and the columns. Each column has a name, a data type, nullability and a description. |
| `run_sql` | `sql` | The rows as JSON. A result with more than 1000 rows stops at 1000 rows and has `"truncated": true`. |
| `export_query` | `sql`, `format` | A `POST /api/query` request and a Python script that download the full result as a file. The tool does not run the query. |

`export_query` accepts a `SELECT` or `WITH` statement. `format` is `parquet` (the default),
`arrow` or `csv`. Large results thus go into a file and not into the context of the agent.

A tool error returns as a tool result with the error text, so the agent can read it.
