# SQL

Beacon can execute SQL through DataFusion. SQL queries are submitted to the same endpoint as JSON queries:

```http
POST /api/query
Content-Type: application/json

{ "sql": "SELECT time, temperature FROM default LIMIT 10" }
```

:::warning
SQL is disabled by default. Enable it by setting the environment variable `BEACON_ENABLE_SQL=true`. Requests with `"sql": "..."` return an error when SQL is disabled.
:::

## Query a registered table

List available tables:

```http
GET /api/tables
```

Run a SQL query against a table:

```http
POST /api/query
Content-Type: application/json

{
  "sql": "SELECT time, latitude, longitude, temperature FROM default WHERE temperature > 5 LIMIT 1000",
  "output": { "format": "csv" }
}
```

Inspect the table schema:

```http
GET /api/table-schema?table_name=default
```

## Query files directly

Table functions let you query files without registering a table first. Paths are resolved relative to Beacon's dataset root and support glob patterns.

List available table functions:

```http
GET /api/table-functions
```

### NetCDF

```http
POST /api/query
Content-Type: application/json

{
  "sql": "SELECT time, latitude, longitude FROM read_netcdf(['argo/**/*.nc']) LIMIT 100",
  "output": { "format": "csv" }
}
```

### Zarr

```http
POST /api/query
Content-Type: application/json

{
  "sql": "SELECT time, sst FROM read_zarr(['sst/*/zarr.json']) LIMIT 100",
  "output": { "format": "csv" }
}
```

Supply `statistics_columns` as a second argument to enable 1D slice pushdown for large coordinate dimensions:

```http
POST /api/query
Content-Type: application/json

{
  "sql": "SELECT time, latitude, longitude, sst FROM read_zarr(['sst/*/zarr.json'], ['time', 'latitude', 'longitude']) WHERE time >= '2025-01-01' LIMIT 1000",
  "output": { "format": "csv" }
}
```

### Parquet

```http
POST /api/query
Content-Type: application/json

{
  "sql": "SELECT * FROM read_parquet(['obs/**/*.parquet']) LIMIT 100",
  "output": { "format": "csv" }
}
```

Other table functions: `read_arrow`, `read_csv`, `read_odv_ascii`, `read_bbf`, `read_tiff`. See [Reading Files](../../sql/table-functions.md) for full signatures.

## Output formats

See [Querying — Output formats](./index.md#output-formats) for the full list. Add `output` alongside `sql` in the same request body:

```http
POST /api/query
Content-Type: application/json

{
  "sql": "SELECT longitude, latitude, time, temperature FROM default LIMIT 100000",
  "output": {
    "format": {
      "geoparquet": { "longitude_column": "longitude", "latitude_column": "latitude" }
    }
  }
}
```

## Explain and metrics

Explain the physical plan for a SQL query:

```http
POST /api/explain-query
Content-Type: application/json

{ "sql": "SELECT * FROM default LIMIT 10" }
```

Fetch metrics after execution (query ID comes from the `x-beacon-query-id` response header):

```http
GET /api/query/metrics/{query_id}
```
