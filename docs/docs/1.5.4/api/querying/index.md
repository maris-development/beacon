# Querying

Beacon’s API exposes a single query execution surface that can return streamed Arrow results or downloadable files. This chapter explains the two ways to express a query and the supporting endpoints you’ll typically use when building clients and UIs.

## Why query via the API

Use the query API when you want to:

- Fetch a subset of rows/columns (projection + filtering) instead of downloading whole files.
- Build interactive UIs (schemas → query builder → validate/explain → run → show metrics).
- Export results in a client-friendly format (Arrow IPC stream, CSV, Parquet, NetCDF, GeoParquet, ...).

## Two query styles

- **JSON query DSL** ([json.md](json.md))
  - Best for programmatic query builders.
  - Request is a typed object: `select`, optional `from`, optional `filters`, optional `output`.

- **SQL** ([sql.md](sql.md))
  - Best for power users and ad-hoc analysis.
  - Send `{ "sql": "SELECT ..." }` to the same endpoint.

- **Examples** ([examples.md](examples.md))
  - Copy/paste query patterns for JSON and SQL.

## Core endpoints

## Default response: Arrow IPC stream

By default, `/api/query` returns an Apache Arrow IPC stream (content type `application/vnd.apache.arrow.stream`).

If you set `output`, Beacon returns a downloadable file instead (for example CSV/Parquet/NetCDF).

Client libraries and docs for reading Arrow IPC streams:

- Rust: [docs.rs/arrow-ipc StreamReader](https://docs.rs/arrow-ipc/latest/arrow_ipc/reader/struct.StreamReader.html)
- Python: [PyArrow IPC streaming (open_stream / RecordBatchStreamReader)](https://arrow.apache.org/docs/python/ipc.html)
- C++: [Arrow C++ IPC stream reading (RecordBatchStreamReader)](https://arrow.apache.org/docs/cpp/ipc.html)

Run a query:

```http
POST /api/query
Content-Type: application/json

{ "select": ["time"], "limit": 1 }
```

Validate a query without running it:

```http
POST /api/parse-query
Content-Type: application/json

{ "select": ["time"], "limit": 1 }
```

Explain the planned query (useful for debugging and performance work):

```http
POST /api/explain-query
Content-Type: application/json

{ "select": ["time"], "limit": 1 }
```

Fetch metrics for a completed query (query id is returned via the `x-beacon-query-id` response header):

```http
GET /api/query/metrics/{query_id}
```

## Choosing sources

Most deployments query a **registered table** (collection) by name (for example `"from": "default"`).

Some formats can also be queried directly by specifying a `from` object with a format and paths (see [json.md](json.md) and [sql.md](sql.md) for examples).

