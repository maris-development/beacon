# Querying with SQL

SQL queries are sent to Beacon using `POST /api/query` with a JSON body containing a `sql` string.

::: tip
If your node has SQL disabled, enable it by setting `BEACON_ENABLE_SQL=true`.
:::

::: tip
If you are querying from Python (notebooks/pipelines), using the Beacon Python SDK is recommended: [Beacon Python SDK docs](https://maris-development.github.io/beacon-py/)
:::

## Minimal example

The most common pattern is to use a **table function** in the `FROM` clause. This lets you query one file, or many files at once, using glob patterns.

```http
POST {beacon-host}/api/query
Content-Type: application/json

{
  "sql": "SELECT * FROM read_parquet(['**/*.parquet']) LIMIT 10"
}
```

If you omit the `output` field, Beacon streams results back as Arrow IPC (`application/vnd.apache.arrow.stream`).

## Download as a file (recommended for most clients)

To receive a single downloadable file, set `output.format`.

```http
POST {beacon-host}/api/query
Content-Type: application/json

{
  "sql": "SELECT time, latitude, longitude, temp FROM read_netcdf(['**/*.nc']) WHERE temp BETWEEN 2 AND 10 LIMIT 10000",
  "output": {
    "format": "parquet"
  }
}
```

Beacon will respond with:

- `Content-Disposition: attachment; filename="output.parquet"`
- `x-beacon-query-id: <uuid>`

## Common patterns

### Discover available table functions

Beacon registers a set of table-valued functions you can use in SQL (for example `read_netcdf`, `read_zarr`, `read_parquet`, ...).

- List table functions: `GET /api/table-functions`

### Inspect available tables and schema

- List tables: `GET /api/tables`
- Default table schema (if you use it): `GET /api/default-table-schema`
- Table schema: `GET /api/table-schema?table_name=...`

### Query a registered table

If your Beacon node has tables configured (collections), you can query them directly:

Use `GET /api/tables` to discover the table names available on your server.

```http
POST {beacon-host}/api/query
Content-Type: application/json

{
  "sql": "SELECT time, latitude, longitude, temp FROM some_table WHERE temp IS NOT NULL LIMIT 1000",
  "output": { "format": "csv" }
}
```

## Read from files using table functions

All file-reading functions take a **list of glob paths** as their first argument, for example `['**/*.nc']`.

### NetCDF (`read_netcdf`)

```http
POST {beacon-host}/api/query
Content-Type: application/json

{
  "sql": "SELECT TEMP, PSAL, LONGITUDE, LATITUDE FROM read_netcdf(['**/*.nc']) WHERE TEMP IS NOT NULL LIMIT 100000",
  "output": { "format": "parquet" }
}
```

### Zarr (`read_zarr`)

```http
POST {beacon-host}/api/query
Content-Type: application/json

{
  "sql": "SELECT TEMP, PSAL, LONGITUDE, LATITUDE FROM read_zarr(['datasets.zarr/zarr.json']) LIMIT 100000",
  "output": { "format": "parquet" }
}
```

```http
POST {beacon-host}/api/query
Content-Type: application/json

{
  "sql": "SELECT TEMP, PSAL, LONGITUDE, LATITUDE FROM read_zarr(['*/zarr.json']) LIMIT 100000",
  "output": { "format": "parquet" }
}
```

`read_zarr` also supports an optional second argument `statistics_columns` (a list of column names). Supplying these can improve performance for large Zarr datasets by enabling pushdown based on precomputed statistics.

```http
POST {beacon-host}/api/query
Content-Type: application/json

{
  "sql": "SELECT TEMP, PSAL, LONGITUDE, LATITUDE FROM read_zarr(['datasets.zarr/zarr.json'], ['LONGITUDE', 'LATITUDE']) WHERE LONGITUDE > 10 AND LATITUDE < 50",
  "output": { "format": "parquet" }
}
```

### Parquet (`read_parquet`)

```http
POST {beacon-host}/api/query
Content-Type: application/json

{
  "sql": "SELECT * FROM read_parquet(['*.parquet']) WHERE quality_flag = 0 LIMIT 100000",
  "output": { "format": "parquet" }
}
```

### CSV (`read_csv`)

```http
POST {beacon-host}/api/query
Content-Type: application/json

{
  "sql": "SELECT * FROM read_csv(['1.csv', '2.csv']) LIMIT 10000",
  "output": { "format": "parquet" }
}
```

### Arrow IPC files (`read_arrow`)

```http
POST {beacon-host}/api/query
Content-Type: application/json

{
  "sql": "SELECT * FROM read_arrow(['*.arrow', '*.ipc']) LIMIT 10000",
  "output": { "format": "parquet" }
}
```

### Beacon Binary Format (`read_bbf`)

```http
POST {beacon-host}/api/query
Content-Type: application/json

{
  "sql": "SELECT * FROM read_bbf(['*.bbf']) WHERE time >= '2020-01-01' AND time < '2021-01-01'",
  "output": { "format": "parquet" }
}
```

### Inspect schemas without reading data (`read_schema`)

If you want to quickly inspect schemas (especially for many files) without running a full scan, use `read_schema`:

```http
POST {beacon-host}/api/query
Content-Type: application/json

{
  "sql": "SELECT * FROM read_schema(['**/*.nc'], 'netcdf')"
}
```

### Explain a query plan

Use `POST /api/explain-query` with the same body you would send to `/api/query`.

### Get query metrics

Use the `x-beacon-query-id` response header from `/api/query`, then request:

`GET /api/query/metrics/{query_id}`

## Output formats

For file output, supported `output.format` values include:

- `csv`
- `parquet`
- `ipc` (alias: `arrow`)
- `netcdf`
- `nd_netcdf` (requires `dimension_columns`)
- `geoparquet`
- `odv`

Examples of formats with options:

```json
{
  "sql": "SELECT longitude, latitude, time, temp FROM read_parquet(['*.parquet']) LIMIT 10000",
  "output": {
    "format": { "geoparquet": { "longitude_column": "longitude", "latitude_column": "latitude" } }
  }
}
```

```json
{
  "sql": "SELECT time, depth, temp FROM read_netcdf(['**/*.nc']) LIMIT 10000",
  "output": {
    "format": { "nd_netcdf": { "dimension_columns": ["time", "depth"] } }
  }
}
```
