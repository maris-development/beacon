# API

Beacon exposes an HTTP API for:

- Discovering datasets and tables (collections)
- Inspecting available columns (Arrow schemas)
- Running queries (JSON DSL or SQL)
- Downloading results in formats like Arrow IPC, CSV, Parquet, NetCDF, GeoParquet, ODV

::: tip
The API is fully documented via OpenAPI. When Beacon is running, open:

- Swagger UI: `/swagger`
- Scalar UI: `/scalar/`
- OpenAPI JSON: `/openapi.json`
:::

## Base URL

Examples in these docs are shown as raw HTTP request templates (e.g. `GET /api/health`).

Send them to your Beacon base URL (by default `http://localhost:5001`).

If you run behind a reverse proxy, use that URL instead.

## Quick health check

```http
GET /api/health
```

## Next

- Explore datasets/tables: [exploring-data-lake.md](exploring-data-lake.md)
- Query with JSON: [querying/json.md](querying/json.md)
- Query with SQL: [querying/sql.md](querying/sql.md)
- Query examples: [querying/examples.md](querying/examples.md)
