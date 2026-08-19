# SQL

Beacon runs SQL through DataFusion. A SQL query goes to the same endpoint as a JSON query:

```http
POST /api/query
Content-Type: application/json

{ "sql": "SELECT time, temperature FROM default LIMIT 10" }
```

:::warning
SQL is on by default. Set the environment variable `BEACON_ENABLE_SQL=false` to switch it off. A
request with `"sql": "..."` then returns an error.
:::

## Query a registered table

List the available tables:

```http
GET /api/tables
```

Run a SQL query on a table:

```http
POST /api/query
Content-Type: application/json

{
  "sql": "SELECT time, latitude, longitude, temperature FROM default WHERE temperature > 5 LIMIT 1000",
  "output": { "format": "csv" }
}
```

Inspect the schema of the table:

```http
GET /api/table-schema?table_name=default
```

## Query files directly

A table function queries files. You register no table first. Beacon resolves a path against its
dataset root. A path also takes a glob pattern.

The [table function reference](/docs/2.0.0-rc3/sql/table-functions) holds every table
function and its signature. `GET /api/table-functions` is deprecated. It returns an empty list.

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

Predicate pushdown is automatic. Beacon prunes the chunks and slices the coordinate dimensions. It
uses your `WHERE` clause. You configure no extra argument:

```http
POST /api/query
Content-Type: application/json

{
  "sql": "SELECT time, latitude, longitude, sst FROM read_zarr(['sst/*/zarr.json']) WHERE time >= '2025-01-01' LIMIT 1000",
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

The other table functions are `read_arrow`, `read_csv`, `read_odv_ascii`, `read_bbf` and
`read_tiff`. See [Read Files](/docs/2.0.0-rc3/sql/table-functions) for the full
signatures.

## Output formats

See [Querying, Output formats](/docs/2.0.0-rc3/api/querying/#output-formats) for the full list. Put
`output` next to `sql` in the same request body:

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

Get the physical plan of a SQL query:

```http
POST /api/explain-query
Content-Type: application/json

{ "sql": "SELECT * FROM default LIMIT 10" }
```

Fetch the metrics after the query. The query ID comes from the `x-beacon-query-id` response header:

```http
GET /api/query/metrics/{query_id}
```
