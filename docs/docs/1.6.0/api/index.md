# REST API

Beacon exposes an HTTP API for querying datasets, inspecting schemas, and managing the data lake. All surfaces speak JSON over HTTP.

## OpenAPI reference

Beacon generates an OpenAPI spec at runtime. When the server is running, open one of these URLs:

| UI | URL |
| -- | --- |
| Swagger UI | `/swagger` |
| Scalar UI | `/scalar/` |
| Raw spec (JSON) | `/openapi.json` |

## Base URL

All endpoints in these docs are shown as relative paths (e.g. `GET /api/health`). Send requests to your Beacon base URL — by default `http://localhost:5001`. If you run behind a reverse proxy, use that URL instead.

## Health check

```http
GET /api/health
```

Returns `200 OK` when Beacon is up and ready.

## What's in the API

| Section | Description |
| ------- | ----------- |
| [Exploring the Data Lake](./exploring-data-lake.md) | Discover datasets, tables, and schemas |
| [Querying](./querying/index.md) | Run queries (JSON DSL or SQL) and receive results |
| [JSON Query DSL](./querying/json.md) | Structured query format for programmatic clients |
| [SQL](./querying/sql.md) | Full SQL via DataFusion |
| [Examples](./querying/examples.md) | Copy-paste query patterns |
