# Query examples

This chapter contains end-to-end examples you can copy/paste and adapt.

- All examples are shown as raw HTTP request templates.
- Examples use `output.format: "csv"` so the response is a single downloadable file.
  - If you omit `output`, Beacon streams Arrow IPC instead.

::: tip
Start by discovering columns:

- Default table schema: `GET /api/default-table-schema`
- Per-dataset schema: `GET /api/dataset-schema?file=...`
:::

## JSON examples

### 1) Select + alias + computed column

- Select a few columns
- Add an aliased computed column using a function
- Use a literal function argument

```http
POST /api/query
Content-Type: application/json

{
  "from": "default",
  "select": [
    "time",
    "latitude",
    "longitude",
    {
      "function": "round",
      "args": ["temp", { "value": 2 }],
      "alias": "temp_rounded"
    }
  ],
  "limit": 100,
  "output": { "format": "csv" }
}
```

### 2) Multiple filters (AND by default)

Use `filters` to apply multiple constraints.

```http
POST /api/query
Content-Type: application/json

{
  "from": "default",
  "select": ["time", "latitude", "longitude", "temp"],
  "filters": [
    { "column": "temp", "min": 2, "max": 10 },
    { "column": "latitude", "min": -10, "max": 10 }
  ],
  "limit": 1000,
  "output": { "format": "csv" }
}
```

### 3) OR conditions

Wrap OR logic inside a single `or` filter object.

```http
POST /api/query
Content-Type: application/json

{
  "from": "default",
  "select": ["time", "platform", "temp"],
  "filters": [
    {
      "or": [
        { "column": "platform", "eq": "SHIP" },
        { "column": "platform", "eq": "BUOY" }
      ]
    }
  ],
  "limit": 1000,
  "output": { "format": "csv" }
}
```

### 4) Sort + offset + limit

`sort_by` is case-sensitive and uses enum keys.

```http
POST /api/query
Content-Type: application/json

{
  "from": "default",
  "select": ["time", "temp"],
  "sort_by": [{ "Desc": "time" }],
  "offset": 100,
  "limit": 50,
  "output": { "format": "csv" }
}
```

### 5) DISTINCT ON

Use `distinct` to keep one row per key.

```http
POST /api/query
Content-Type: application/json

{
  "from": "default",
  "distinct": {
    "on": ["platform"],
    "select": ["platform", "time", "temp"]
  },
  "sort_by": [{ "Desc": "time" }],
  "limit": 100,
  "output": { "format": "csv" }
}
```

### 6) Query a dataset directly (ad-hoc source)

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

### 7) Zarr pruning with `statistics_columns`

If you frequently filter on coordinate-like arrays, provide `statistics_columns` so Beacon can prune Zarr groups and push down 1D slicing.

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
  "select": ["valid_time", "latitude", "longitude", "temp"],
  "filters": [
    { "column": "valid_time", "min": "2025-01-01" },
    { "column": "latitude", "min": -10, "max": 10 }
  ],
  "limit": 1000,
  "output": { "format": "csv" }
}
```

## SQL examples

SQL queries are submitted to the same endpoint as JSON queries.

::: warning
SQL is disabled by default; enable it with `BEACON_ENABLE_SQL=true`.
:::

### 1) Select + computed columns + literals

```http
POST /api/query
Content-Type: application/json

{
  "sql": "SELECT time, latitude, longitude, round(temp, 2) AS temp_rounded FROM default LIMIT 100",
  "output": { "format": "csv" }
}
```

### 2) WHERE with multiple conditions

```http
POST /api/query
Content-Type: application/json

{
  "sql": "SELECT time, latitude, longitude, temp FROM default WHERE temp BETWEEN 2 AND 10 AND latitude BETWEEN -10 AND 10 LIMIT 1000",
  "output": { "format": "csv" }
}
```

### 3) Aggregation + GROUP BY

```http
POST /api/query
Content-Type: application/json

{
  "sql": "SELECT platform, avg(temp) AS avg_temp, count(*) AS n FROM default GROUP BY platform ORDER BY n DESC LIMIT 100",
  "output": { "format": "csv" }
}
```

### 4) Read Zarr directly + enable pruning

```http
POST /api/query
Content-Type: application/json

{
  "sql": "SELECT valid_time, latitude, longitude, temp FROM read_zarr(['some-zarr-dataset/zarr.json'], ['valid_time', 'latitude', 'longitude']) WHERE valid_time >= '2025-01-01' LIMIT 1000",
  "output": { "format": "csv" }
}
```

## Validate and explain (works for JSON and SQL)

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
