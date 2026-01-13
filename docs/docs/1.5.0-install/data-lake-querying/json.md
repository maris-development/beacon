# Querying with JSON

Beacon also supports a structured JSON query format. This is useful when you want to build queries programmatically (without generating SQL), because the query is expressed as a typed object: select columns, apply filters, sort, and choose an output format.

JSON queries are sent to the same endpoint as SQL:

`POST /api/query`

::: tip
If you are querying from Python (notebooks/pipelines), using the Beacon Python SDK is recommended: [Beacon Python SDK docs](https://maris-development.github.io/beacon-py/)
:::

## Request shape

At the top level, the request is a `Query` object:

- Either `{ "sql": "..." }` (SQL mode)
- Or a JSON query body (JSON mode)

In both cases you can optionally include `output`.

### Minimal JSON query

```json
{
  "select": ["time", "latitude", "longitude", "temp"],
  "limit": 10
}
```

Notes:

- `select` is required.
- If `from` is omitted, Beacon uses the configured default table.
- If `output` is omitted, Beacon streams Arrow IPC (`application/vnd.apache.arrow.stream`).

## Selecting columns

`select` accepts multiple forms:

- Column name string: `"temp"`
- Column with alias:

```json
{ "column": "sea_surface_temperature", "alias": "sst" }
```

- Function call:

```json
{ "function": "round", "args": ["temp"], "alias": "temp_rounded" }
```

## Choosing the data source (`from`)

You can query either a registered table or a set of files as an ad-hoc source.

### Query a table

Use `GET /api/tables` to discover the table names available on your server.

```json
{
  "from": "some_table",
  "select": ["time", "temp"],
  "limit": 10
}
```

### Query files directly

`from` also supports a format+paths shape (example with NetCDF):

```json
{
  "from": { "netcdf": { "paths": ["my-file.nc"] } },
  "select": ["time", "temp"],
  "limit": 100
}
```

Other supported `from` formats include `zarr`, `parquet`, `csv`, `arrow`, and `odv`.

## Filters

Use the `filter` field to constrain results.

### Range filter (between)

```json
{
  "select": ["time", "temp"],
  "filter": { "column": "temp", "min": 2, "max": 10 }
}
```

### Equality filter

```json
{
  "select": ["time", "platform"],
  "filter": { "column": "platform", "eq": "SHIP" }
}
```

### Combine filters (and/or)

```json
{
  "select": ["time", "latitude", "longitude", "temp"],
  "filter": {
    "and": [
      { "column": "temp", "min": 2, "max": 10 },
      { "column": "latitude", "min": -10, "max": 10 }
    ]
  },
  "limit": 10000
}
```

### GeoJSON spatial filter

This filter checks whether a point (lon/lat columns) falls within a provided GeoJSON geometry.

```json
{
  "select": ["time", "longitude", "latitude", "temp"],
  "filter": {
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
  },
  "limit": 10000
}
```

## Sorting, limits, offsets

- `sort_by`: `[{"asc": "time"}]` or `[{"desc": "temp"}]`
- `limit`: maximum rows returned
- `offset`: skip N rows

Example:

```json
{
  "select": ["time", "temp"],
  "sort_by": [{ "desc": "time" }],
  "limit": 100
}
```

## Distinct

Use `distinct` to return distinct combinations.

```json
{
  "distinct": {
    "on": ["platform"],
    "select": ["platform"]
  },
  "select": ["platform"]
}
```

## Output formats

To download a single file, set `output.format`.

Simple formats:

```json
{
  "select": ["time", "temp"],
  "output": { "format": "csv" }
}
```

Formats with options:

```json
{
  "select": ["longitude", "latitude", "time", "temp"],
  "output": {
    "format": { "geoparquet": { "longitude_column": "longitude", "latitude_column": "latitude" } }
  }
}
```

```json
{
  "select": ["time", "depth", "temp"],
  "output": { "format": { "nd_netcdf": { "dimension_columns": ["time", "depth"] } } }
}
```

## Tips

- Discover columns with `GET /api/default-table-schema`.
- Validate query bodies with `POST /api/parse-query`.
- Use the `x-beacon-query-id` header to fetch metrics from `GET /api/query/metrics/{query_id}`.
