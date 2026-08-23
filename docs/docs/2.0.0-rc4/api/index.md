# REST API

Beacon gives an HTTP API. Use it to query datasets, to inspect schemas and to manage the server.
Every endpoint uses JSON over HTTP.

## OpenAPI reference

Beacon generates an OpenAPI specification at run time. Start the server. Then open one of these
URLs:

| UI | URL |
| -- | --- |
| Swagger UI | `/swagger` |
| Scalar UI | `/scalar/` |
| Raw spec (JSON) | `/openapi.json` |

## Base URL

This documentation shows every endpoint as a relative path, for example `GET /api/health`. Send your
request to the base URL of your Beacon server. The default is `http://localhost:5001`. Behind a
reverse proxy, use the URL of that proxy.

## Admin path alias

Beacon serves each endpoint on two paths. The second path has the prefix `/admin`:

| Endpoint | Alias |
| -------- | ----- |
| `POST /api/query` | `POST /admin/api/query` |
| `GET /api/tables` | `GET /admin/api/tables` |
| `GET /api/admin/crawlers` | `GET /admin/api/admin/crawlers` |
| `GET /api/health` | `GET /admin/api/health` |

The two paths run the same handler.

Each path below `/admin` needs the admin Basic auth credentials. This also applies
to the endpoints that answer any caller on `/api/*`. `GET /api/info` is open.
`GET /admin/api/info` is not.

Use the alias when a proxy in front of Beacon protects `/api/*`. Your proxy keeps
control of `/api/*`. The [admin web UI](/docs/2.0.0-rc4/connect/web-admin-ui) calls
the alias only, so the UI stays in service.

```bash
curl -u beacon-admin:beacon-password http://localhost:5001/admin/api/tables
```

The OpenAPI specification lists each endpoint one time. It shows the path without
the prefix.

## Health check

```http
GET /api/health
```

Returns `200 OK` when Beacon runs and is ready.

## What's in the API

| Section | Description |
| ------- | ----------- |
| [Exploring the catalog](/docs/2.0.0-rc4/api/exploring-data) | Find datasets, tables and schemas |
| [Querying](/docs/2.0.0-rc4/api/querying/) | Run a query with the JSON DSL or with SQL, and get the results |
| [JSON Query DSL](/docs/2.0.0-rc4/api/querying/json) | A structured query format for a client program |
| [SQL](/docs/2.0.0-rc4/api/querying/sql) | Full SQL through DataFusion |
| [Examples](/docs/2.0.0-rc4/api/querying/examples) | Query patterns that you can copy |
