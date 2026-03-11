
# Querying with JSON

Beacon supports a structured JSON query format. This is useful when you want to build queries programmatically (without generating SQL), because the query is expressed as a typed object: select columns, apply filters, choose a data source, and pick an output format.

JSON queries are sent to:

```http
POST /api/query
```

All paths are shown as relative URLs. Send them to your Beacon base URL.

::: tip
Discover columns first:

- Default table schema: `GET /api/default-table-schema`
- Per-dataset schema: `GET /api/dataset-schema?file=...`
:::

## Request shape

At the top level, the request body is a `Query`.

- In JSON mode, you send a JSON query body directly.
- You can optionally include `output` to download a file instead of [streaming Arrow IPC](#output-formats).

### Minimal JSON query

This queries a NetCDF file directly (useful when no table/default table is configured) and returns the first 10 rows as a CSV download.

Raw HTTP:

```http
POST /api/query
Content-Type: application/json

{
  "from": { "netcdf": { "paths": ["test-files/gridded-example.nc"] } },
  "select": ["time", "latitude", "longitude"],
  "limit": 10,
  "output": { "format": "csv" }
}
```

Notes:

- `select` is required.
- If `from` is omitted, Beacon uses the configured default table (if one is configured).
- If you have not created any tables yet, specify a file source via `from` (for example `{"netcdf":{"paths":["...nc"]}}`).
- If `output` is omitted, Beacon streams Arrow IPC (`application/vnd.apache.arrow.stream`).

## Selecting columns

`select` accepts multiple forms:

- Column name string: `"temp"`
- Column with alias:

```http
POST /api/query
Content-Type: application/json

{
  "from": { "netcdf": { "paths": ["obs-example.nc"] } },
  "select": [{ "column": "sea_surface_temperature", "alias": "sst" }],
  "limit": 10,
  "output": { "format": "csv" }
}
```

For compatibility with older clients, `column_name` is accepted as an alias of `column`:

```http
POST /api/query
Content-Type: application/json

{
  "from": { "netcdf": { "paths": ["argo-*.nc"] } },
  "select": [{ "column_name": "TEMP", "alias": "temperature" }],
  "limit": 10,
  "output": { "format": "csv" }
}
```

- Function call:

```http
POST /api/query
Content-Type: application/json

{
  "from": { "netcdf": { "paths": ["argo.nc", "argo-2.nc"] } },
  "select": [{ "function": "round", "args": ["temp"], "alias": "temp_rounded" }],
  "limit": 10,
  "output": { "format": "csv" }
}
```

## Choosing the data source (`from`)

You can query either a registered table or a set of files as an ad-hoc source.

### Query a table

Use `GET /api/tables` to discover the table names available.

```http
POST /api/query
Content-Type: application/json

{
  "from": "default",
  "select": ["time", "temp"],
  "limit": 10,
  "output": { "format": "csv" }
}
```

### Query files directly

`from` also supports a format+paths shape.

NetCDF example:

```http
POST /api/query
Content-Type: application/json

{
  "from": { "netcdf": { "paths": ["test-files/gridded-example.nc"] } },
  "select": ["time", "latitude", "longitude"],
  "limit": 100,
  "output": { "format": "csv" }
}
```

Zarr example:

```http
POST /api/query
Content-Type: application/json

{
  "from": { "zarr": { "paths": ["some-zarr-dataset/zarr.json"] } },
  "select": ["time", "latitude", "longitude"],
  "limit": 100,
  "output": { "format": "csv" }
}
```

Zarr statistics (predicate pruning):

If you frequently filter on a small set of “coordinate-like” columns (for example time/lat/lon), provide `statistics_columns` so Beacon can prune Zarr groups and push down 1D slicing.

```http
POST /api/query
Content-Type: application/json

{
  "from": {
    "zarr": {
      "paths": ["some-zarr-dataset/zarr.json"],
      "statistics_columns": ["valid_time", "latitude", "longitude"]
    }
  },
  "select": ["valid_time", "latitude", "longitude"],
  "filters": [{ "column": "valid_time", "min": "2025-01-01" }],
  "limit": 100,
  "output": { "format": "csv" }
}
```

Zarr with glob paths and multiple datasets:

```http
POST /api/query
Content-Type: application/json

{
  "from": {
    "zarr": {
      "paths": [
        "datasets/**/*.zarr/zarr.json",
        "test-files/gridded-example.zarr/zarr.json",
        "other-zarr-dataset/zarr.json"
      ]
    }
  },
  "select": ["time", "latitude", "longitude"],
  "limit": 100,
  "output": { "format": "csv" }
}
```

Parquet example:

```http
POST /api/query
Content-Type: application/json

{
  "from": { "parquet": { "paths": ["some-folder/*.parquet"] } },
  "select": ["time", "latitude", "longitude"],
  "limit": 100,
  "output": { "format": "csv" }
}
```

Other supported `from` formats include `zarr`, `parquet`, `csv`, `arrow`, and `odv`.

## Filters

Use `filters` (recommended) to constrain results.

`filters` is an array so you can apply multiple filters (even multiple filters on the same column). Filters are combined with AND by default.

::: tip
Beacon also accepts a legacy single-object `filter` property, but the docs use `filters` for clarity.
:::

### Range filter (between)

```http
POST /api/query
Content-Type: application/json

{
  "select": ["time", "temp"],
  "filters": [{ "column": "temp", "min": 2, "max": 10 }],
  "limit": 100,
  "output": { "format": "csv" }
}
```

For compatibility with older clients, `for_query_parameter` is accepted as an alias of `column`.

### Equality filter

```http
POST /api/query
Content-Type: application/json

{
  "select": ["time", "platform"],
  "filters": [{ "column": "platform", "eq": "SHIP" }],
  "limit": 100,
  "output": { "format": "csv" }
}
```

### Combine filters (and/or)

```http
POST /api/query
Content-Type: application/json

{
  "select": ["time", "latitude", "longitude", "temp"],
  "filters": [
    { "column": "temp", "min": 2, "max": 10 },
    { "column": "latitude", "min": -10, "max": 10 }
  ],
  "limit": 10000,
  "output": { "format": "csv" }
}
```

To express OR conditions, wrap them in an `or` filter:

```http
POST /api/query
Content-Type: application/json

{
  "select": ["time", "platform"],
  "filters": [
    {
      "or": [
        { "column": "platform", "eq": "SHIP" },
        { "column": "platform", "eq": "BUOY" }
      ]
    }
  ],
  "limit": 100,
  "output": { "format": "csv" }
}
```

### GeoJSON spatial filter

This filter checks whether a point (lon/lat columns) falls within a provided GeoJSON geometry.

```http
POST /api/query
Content-Type: application/json

{
  "select": ["time", "longitude", "latitude", "temp"],
  "filters": [
    {
      "longitude_column": "longitude",
      "latitude_column": "latitude",
      "geometry": {
        "type": "Polygon",
        "coordinates": [
          [
            [4.0, 52.0],
            [6.0, 52.0],
            [6.0, 54.0],
            [4.0, 54.0],
            [4.0, 52.0]
          ]
        ]
      }
    }
  ],
  "limit": 10000,
  "output": { "format": "csv" }
}
```

## Sorting, limits, offsets

- `limit`: maximum rows returned
- `offset`: skip N rows
- `sort_by`: sort expressions

::: warning
The current JSON encoding for `sort_by` uses enum keys and is case-sensitive.

Use `[{"Asc":"time"}]` or `[{"Desc":"time"}]`.
:::

## Output formats

### Streaming Arrow IPC (default)

If `output` is omitted, `/api/query` returns an Apache Arrow IPC stream (content type `application/vnd.apache.arrow.stream`).

To download a single file instead, set `output.format`.

Client libraries and docs for reading Arrow IPC streams:

- Rust: [docs.rs/arrow-ipc StreamReader](https://docs.rs/arrow-ipc/latest/arrow_ipc/reader/struct.StreamReader.html)
- Python: [PyArrow IPC streaming (open_stream / RecordBatchStreamReader)](https://arrow.apache.org/docs/python/ipc.html)
- C++: [Arrow C++ IPC stream reading (RecordBatchStreamReader)](https://arrow.apache.org/docs/cpp/ipc.html)

To download a single file, set `output.format`.

Simple formats:

```http
POST /api/query
Content-Type: application/json

{
  "select": ["time", "temp"],
  "output": { "format": "csv" }
}
```

Supported simple values include: `csv`, `ipc` (alias: `arrow`), `parquet`, `netcdf`.

Formats with options:

```http
POST /api/query
Content-Type: application/json

{
  "select": ["longitude", "latitude", "time", "temp"],
  "output": {
    "format": { "geoparquet": { "longitude_column": "longitude", "latitude_column": "latitude" } }
  }
}
```

```http
POST /api/query
Content-Type: application/json

{
  "select": ["time", "depth", "temp"],
  "output": { "format": { "nd_netcdf": { "dimension_columns": ["time", "depth"] } } }
}
```

## Validating and explaining queries

Validate a query body:

```http
POST /api/parse-query
Content-Type: application/json

{ "select": ["time"], "limit": 1 }
```

Explain the planned query:

```http
POST /api/explain-query
Content-Type: application/json

{ "select": ["time"], "limit": 1 }
```

## Query metrics

Beacon returns a query id via the `x-beacon-query-id` response header.

```http
GET /api/query/metrics/{query_id}
```

::: tip
If you are building a UI, metrics are useful for showing row counts, bytes output, and timing breakdowns.
:::
