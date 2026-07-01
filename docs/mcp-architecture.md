# How the MCP server works

This explains the internals of beacon's MCP server — the request path, how tools
are generated, how identity and read-only enforcement work, and how a tool call
becomes a query. For usage (enabling, exposing tables, connecting clients) see
[mcp.md](mcp.md).

## Overview

The MCP server is a thin **protocol adapter** in front of the existing
`Runtime`. It adds no query engine of its own: every tool call is translated into
a normal `Runtime::run_query`, so MCP inherits beacon's planner, catalog,
metrics, and RBAC. The tool set is not hard-coded — it is generated from the live
catalog and each table's `mcp`/`preset` extension, so curators shape the MCP
surface with SQL/REST, no code changes.

```
Claude ──MCP JSON-RPC over streamable HTTP──▶ beacon-api  /mcp
                                                  │  resolve_identity (per request)
                                                  ▼
                                         beacon-mcp (rmcp ServerHandler)
                                          list_tools / call_tool
                                                  │
                                                  ▼
                            Runtime ── get_table_extensions / list_table_schema_view
                                    ── run_query(Query, identity[super-user cleared])
```

## Crate layout (`beacon-mcp`)

| File | Responsibility |
|---|---|
| `server.rs` | The rmcp `ServerHandler`: `get_info`, `list_tools`, `call_tool`, and identity extraction. |
| `catalog.rs` | Tool generation (`resolve_columns`, per-table tools), preset→SQL, argument→query mapping. |
| `result.rs` | Runs a query and serializes the rows to JSON. |
| `lib.rs` | `streamable_http_service(runtime)` — builds the tower service mounted at `/mcp`. |

It depends on `rmcp` (the official Rust MCP SDK, pinned `=1.8.0`) for the protocol
and transport, and on `beacon-core` for `Runtime`, the extension types, and
`AuthIdentity`.

## Transport & mounting

`beacon_mcp::streamable_http_service(runtime)` returns an rmcp
`StreamableHttpService` (a `tower::Service`) wrapping a fresh `BeaconMcpServer`
per session. `beacon-api`'s `router.rs` mounts it:

```
route_service("/mcp", beacon_mcp::streamable_http_service(runtime))
  .layer(from_fn_with_state(runtime, resolve_identity))   // same as client API
```

behind a `BEACON_MCP_ENABLED` check. Streamable HTTP uses one endpoint for the
whole session (POST for requests, GET for the SSE stream, DELETE to end it); rmcp
tracks sessions with an in-memory `LocalSessionManager`.

## Request lifecycle

1. **`initialize`** — client and server negotiate; `get_info` advertises the
   `tools` capability and server instructions.
2. **`tools/list`** — `list_tools` builds the current tool set (see below). It is
   rebuilt per call, so newly-exposed tables appear without a restart.
3. **`tools/call`** — `call_tool` resolves the caller's identity, dispatches by
   tool name, executes, and returns the result as MCP text content (`isError` set
   on failure).

## Tool generation (`list_tools`)

Two groups are assembled:

**Generic tools** — always present: `list_tables`, `describe_table`, `run_sql`.

**Per-table tools** — for each table whose `mcp` extension has `enabled: true`,
one tool is generated from the extension metadata:

- `name` ← `tool_name` (or a sanitized `query_<table>`), `title`, `description`.
- `inputSchema` is built from `resolve_columns` + presets:
  - `select` — an array whose `enum` is the exposed column names; its description
    lists each column as `name (type): meaning`.
  - `preset` — an `enum` of the table's preset names (if any).
  - `limit` — integer, default 100.
- `annotations.readOnlyHint = true`.

`resolve_columns` merges the table's Arrow schema (types) with the extension's
per-column descriptions, scoped to `exposed_columns` (in order) when set or all
columns otherwise, with a fallback to the Arrow field's `description`/`comment`
metadata. The same function feeds `describe_table`, so the tool schema and the
description tool agree.

## Executing a call (`call_tool`)

```
call_tool(request, context):
    identity = identity_from_context(context)   # super-user cleared
    dispatch(runtime, request.name, request.arguments, identity):
        "list_tables"    -> catalog listing (JSON)
        "describe_table" -> merged columns + extensions (JSON)
        "run_sql"        -> run_query(SELECT, identity) -> JSON rows
        <table tool>     -> build_table_sql(...) -> run_query(..., identity) -> JSON rows
```

**Per-table tools → SQL.** `build_table_sql` turns the validated arguments into a
`SELECT`: `select` (checked against `exposed_columns`, identifiers quoted), the
chosen `preset` expanded to a `WHERE` from its stored filters, and `limit`.
Preset operators are the typed `PresetOp` enum; values are rendered safely
(scalars/arrays escaped) — the model never supplies raw SQL through these tools.

## Identity & read-only enforcement

The `resolve_identity` middleware authenticates each HTTP request (Basic/Bearer →
a user's roles, or the anonymous principal, or an empty identity) and inserts the
`AuthIdentity` into the request extensions. The streamable-HTTP transport injects
the request `http::request::Parts` into the MCP `RequestContext`, so
`identity_from_context` recovers that `AuthIdentity`.

Before use it **clears `is_super_user`**:

```rust
let mut identity = /* from request parts, or AuthIdentity::empty() */;
identity.is_super_user = false;   // MCP is read-only, always
```

This is defense-in-depth: the query planner gates DDL/DML on super-user, so a
non-super identity can only run `SELECT`. The caller's `roles` are preserved, so
when `BEACON_AUTH_ENFORCE=true` per-user read grants still apply. (Beacon's
super-user is config-only and never a client identity, so this only matters if
that ever changes.)

## Results

Rows are collected from the query stream, capped at 1000, and serialized to JSON
(`result.rs`) as the tool's text content. Errors are returned as MCP tool errors
(`isError: true`) with the message, rather than failing the JSON-RPC call, so the
model can read and react to them.

## Extending it

- **New generic tool** — add a builder in `catalog.rs`, list it in `list_tools`,
  and add a `dispatch` arm.
- **Richer per-table inputs** (e.g. server-side filters, sort) — extend the
  per-table `inputSchema` and `build_table_sql`.
- **New extension-driven behavior** — add fields to the `mcp` extension in
  `beacon-core`'s `extensions` module; they flow here via `get_table_extensions`.
