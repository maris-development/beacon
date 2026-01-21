
# Querying with SQL

Beacon can execute SQL queries through DataFusion.

SQL queries are submitted to the same endpoint as JSON queries:

```http
POST /api/query
```

All paths are shown as relative URLs. Send them to your Beacon base URL.

## Enable SQL

SQL is disabled by default. Enable it by setting `BEACON_ENABLE_SQL=true`.

If SQL is disabled, `/api/query` requests with `{ "sql": "..." }` will return an error.

## Query a collection/registered table

List tables:

```http
GET /api/tables
```

Run a SQL query:

```http
POST /api/query
Content-Type: application/json

{

  "sql": "SELECT * FROM default LIMIT 10",
  "output": { "format": "csv" }
}
```

## Discover columns

For a quick schema view:

```http
GET /api/table-schema?table_name=default
```

## Query datasets directly using table functions

Beacon registers table functions for common formats (useful when you do not want to pre-create a collection/table).

List them:

```http
GET /api/table-functions
```

Examples:

### Using globs and multiple paths

Most `read_*` table functions accept an array of path strings. Those strings can be:

- A single dataset path
- A glob pattern (for example `"**/*.nc"` or `"some-folder/*.parquet"`)
- Multiple dataset paths and/or multiple glob patterns in the same array

NetCDF with a single glob:

```http
POST /api/query
Content-Type: application/json

{
  "sql": "SELECT time, latitude, longitude FROM read_netcdf(['**/*.nc']) LIMIT 10",
  "output": { "format": "csv" }
}
```

NetCDF with multiple globs:

```http
POST /api/query
Content-Type: application/json

{
  "sql": "SELECT time, latitude, longitude FROM read_netcdf(['argo/*.nc', 'wod/**/*.nc']) LIMIT 10",
  "output": { "format": "csv" }
}
```

NetCDF with explicit dataset paths:

```http
POST /api/query
Content-Type: application/json

{
  "sql": "SELECT time, latitude, longitude FROM read_netcdf(['datasets/a.nc', 'datasets/b.nc']) LIMIT 10",
  "output": { "format": "csv" }
}
```

### NetCDF

```http
POST /api/query
Content-Type: application/json

{
  "sql": "SELECT time, latitude, longitude FROM read_netcdf(['test-files/gridded-example.nc']) LIMIT 10",
  "output": { "format": "csv" }
}
```

### Parquet

```http
POST /api/query
Content-Type: application/json

{
  "sql": "SELECT * FROM read_parquet(['some-folder/*.parquet']) LIMIT 10",
  "output": { "format": "csv" }
}
```

Parquet with multiple globs (or a mix of globs and explicit paths):

```http
POST /api/query
Content-Type: application/json

{
  "sql": "SELECT * FROM read_parquet(['some-folder/*.parquet', 'other-folder/**/*.parquet', 'one-file.parquet']) LIMIT 10",
  "output": { "format": "csv" }
}
```

### Zarr

```http
POST /api/query
Content-Type: application/json

{
  "sql": "SELECT * FROM read_zarr(['some-zarr-dataset/zarr.json']) LIMIT 10",
  "output": { "format": "csv" }
}
```

`read_zarr` also supports an optional second argument `statistics_columns` (a list of column names). Supplying these can improve performance for large Zarr datasets by enabling predicate-based pruning and 1D slice pushdown.

```http
POST /api/query
Content-Type: application/json

{
  "sql": "SELECT * FROM read_zarr(['some-zarr-dataset/zarr.json'], ['valid_time', 'latitude', 'longitude']) WHERE valid_time >= '2025-01-01' LIMIT 100",
  "output": { "format": "csv" }
}
```

## Output formats

If `output` is omitted, the response is streamed back (Apache Arrow IPC stream (content type `application/vnd.apache.arrow.stream`)).

To download a single file instead, set `output.format`.

Client libraries and docs for reading Arrow IPC streams:

- Rust: [docs.rs/arrow-ipc StreamReader](https://docs.rs/arrow-ipc/latest/arrow_ipc/reader/struct.StreamReader.html)
- Python: [PyArrow IPC streaming (open_stream / RecordBatchStreamReader)](https://arrow.apache.org/docs/python/ipc.html)
- C++: [Arrow C++ IPC stream reading (RecordBatchStreamReader)](https://arrow.apache.org/docs/cpp/ipc.html)

To download a single file instead, set `output.format` in the same request body as your SQL.

### Simple formats

```http
POST /api/query
Content-Type: application/json

{
  "sql": "SELECT * FROM default LIMIT 1000",
  "output": { "format": "parquet" }
}
```

Supported simple values include: `csv`, `ipc` (alias: `arrow`), `parquet`, `netcdf`.

### Formats with options

GeoParquet (requires lon/lat columns):

```http
POST /api/query
Content-Type: application/json

{
  "sql": "SELECT longitude, latitude, time, temp FROM default LIMIT 100000",
  "output": {
    "format": { "geoparquet": { "longitude_column": "longitude", "latitude_column": "latitude" } }
  }
}
```

NetCDF with explicit dimensions:

```http
POST /api/query
Content-Type: application/json

{
  "sql": "SELECT time, depth, temp FROM default",
  "output": { "format": { "nd_netcdf": { "dimension_columns": ["time", "depth"] } } }
}
```

## Explain and metrics

Explain (planned query):

```http
POST /api/explain-query
Content-Type: application/json

{ "sql": "SELECT * FROM default LIMIT 10" }
```

Metrics (use the `x-beacon-query-id` response header from `/api/query`):

```http
GET /api/query/metrics/{query_id}
```
