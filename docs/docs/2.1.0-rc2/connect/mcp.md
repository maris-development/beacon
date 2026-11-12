---
description: Beta. Connect an AI agent to Beacon over the Model Context Protocol (MCP). The agent lists tables, reads their comments and runs read-only SQL.
---

# MCP server <Badge type="warning" text="beta" />

::: warning Beta. In development
The MCP server is in development. The tools, their arguments and their output can change in a
new release without notice. Do not build production workflows on it yet. Report problems in the
[Beacon issue tracker](https://github.com/maris-development/beacon/issues).
:::

Beacon is a Model Context Protocol (MCP) server. An MCP client, for example Claude Code or Claude
Desktop, connects an AI agent to Beacon. The agent then finds the tables, reads what each table
and column holds, and runs read-only SQL.

The agent learns the meaning of the data from the table and column comments. Add comments with
[`COMMENT ON`](/docs/2.1.0-rc2/sql/comment-on) before you connect an agent. A table needs no other
MCP configuration.

## Endpoint

Beacon serves MCP at `/mcp` with the streamable HTTP transport. With a base path, the endpoint is
`{BEACON_BASE_PATH}/mcp`.

```
http://localhost:5001/mcp
```

The endpoint is on by default. Set `BEACON_MCP_ENABLED=false` to turn it off.

## Tools

The tool set is fixed. Each tool is read-only.

| Tool | Arguments | Result |
| --- | --- | --- |
| `list_tables` | _(none)_ | The tables that the caller can read, each with its table comment. |
| `describe_table` | `table_name` | The table comment and the columns. Each column has a name, a data type, nullability and a description. |
| `run_sql` | `sql` | The rows as JSON. Maximum 1000 rows. |
| `export_query` | `sql`, `format` | A recipe to download the full result as a file. The tool does not run the query. |

### Column descriptions

`describe_table` gives the column comment as the description. A column without a comment gives
the `description` metadata of the source file, if the file has it.

### Row limit of `run_sql`

`run_sql` returns a preview. A result with more than 1000 rows stops at 1000 rows. The response
then is an object with `"truncated": true`, the rows and a hint to use `export_query`.

### Recipe of `export_query`

`export_query` accepts a statement that starts with `SELECT` or `WITH`. `format` is `parquet`
(the default), `arrow` or `csv`. The tool returns:

- The `POST /api/query` request that gives the file.
- A Python script that sends the request and reads the file into a pandas DataFrame.

The script has the placeholders `BEACON_URL` and `AUTH`. Replace them with the server address and
your credentials. Large results thus go into a file and not into the context of the agent.

## Access control

MCP uses the same authentication as the HTTP API. Send an `Authorization` header:

- `Basic <base64 username:password>`
- `Bearer <token>`, for example an OIDC token

A request without credentials runs as the `anonymous` user when anonymous access is on.

Each tool call runs as the caller. The roles of the caller set which tables `list_tables` shows and
which rows the queries read. MCP is always read-only. The super-user also cannot run DDL or DML
over MCP.

The super-user has no roles. With `BEACON_AUTH_ENFORCE=true`, the super-user therefore reads no
table over MCP. Use a user with read grants for an AI agent. See
[Access Control](/docs/2.1.0-rc2/security/access-control#mcp-is-read-only).

## Allowed hosts

Against DNS rebinding, `/mcp` accepts only requests with a loopback `Host` header: `localhost`,
`127.0.0.1` and `::1`. A request with another `Host` gets `403 Forbidden`.

Behind a public name, add the name to `BEACON_MCP_ALLOWED_HOSTS`:

```bash
BEACON_MCP_ALLOWED_HOSTS=beacon.example.org,beacon.example.org:8443
```

- An entry is a host or a `host:port`.
- The loopback hosts stay allowed.
- The entry `*` accepts every host. This turns the check off.

## Connect a client

### Claude Code

```bash
claude mcp add --transport http beacon http://localhost:5001/mcp
```

Add a header for a server that needs credentials:

```bash
claude mcp add --transport http beacon https://beacon.example.org/mcp --header "Authorization: Bearer <token>"
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

## Configuration

| Variable | Default | Description |
| --- | --- | --- |
| `BEACON_MCP_ENABLED` | `true` | Serve the MCP endpoint. `false`, `0` or `off` turns it off. |
| `BEACON_MCP_ALLOWED_HOSTS` | _(empty)_ | Comma-separated `Host` values that `/mcp` accepts in addition to the loopback hosts. `*` accepts every host. |

## Limits

- The tools read only the tables in the default schema.
- The Beacon home page does not show the MCP endpoint.
- A tool error returns as a tool result with the error text, so the agent can read it.
