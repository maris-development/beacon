# Query Examples

Copy-paste examples for both query styles. All examples use `"output": { "format": "csv" }` — omit `output` to receive a streaming Arrow IPC response instead.

## JSON DSL

### Select with computed column

```http
POST /api/query
Content-Type: application/json

{
  "from": "default",
  "select": [
    "time",
    "latitude",
    "longitude",
    { "function": "round", "args": ["temperature", { "value": 2 }], "alias": "temperature_rounded" }
  ],
  "limit": 100,
  "output": { "format": "csv" }
}
```

### Range filter on multiple columns

```http
POST /api/query
Content-Type: application/json

{
  "from": "default",
  "select": ["time", "latitude", "longitude", "temperature"],
  "filters": [
    { "column": "temperature", "min": 2, "max": 10 },
    { "column": "latitude", "min": -10, "max": 10 }
  ],
  "limit": 1000,
  "output": { "format": "csv" }
}
```

### OR filter

```http
POST /api/query
Content-Type: application/json

{
  "from": "default",
  "select": ["time", "platform", "temperature"],
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

### Sort, offset, and limit

```http
POST /api/query
Content-Type: application/json

{
  "from": "default",
  "select": ["time", "temperature"],
  "sort_by": [{ "Desc": "time" }],
  "offset": 100,
  "limit": 50,
  "output": { "format": "csv" }
}
```

### DISTINCT ON

```http
POST /api/query
Content-Type: application/json

{
  "from": "default",
  "distinct": {
    "on": ["platform"],
    "select": ["platform", "time", "temperature"]
  },
  "sort_by": [{ "Desc": "time" }],
  "limit": 100,
  "output": { "format": "csv" }
}
```

### GeoJSON spatial filter

```http
POST /api/query
Content-Type: application/json

{
  "from": "default",
  "select": ["time", "longitude", "latitude", "temperature"],
  "filters": [
    {
      "longitude_column": "longitude",
      "latitude_column": "latitude",
      "geometry": {
        "type": "Polygon",
        "coordinates": [[[4.0, 52.0], [6.0, 52.0], [6.0, 54.0], [4.0, 54.0], [4.0, 52.0]]]
      }
    }
  ],
  "limit": 10000,
  "output": { "format": "csv" }
}
```

### Query a NetCDF file directly

```http
POST /api/query
Content-Type: application/json

{
  "from": { "netcdf": { "paths": ["argo/**/*.nc"] } },
  "select": ["time", "latitude", "longitude", "temperature"],
  "limit": 100,
  "output": { "format": "csv" }
}
```

### Query Zarr with a coordinate range

Predicate pushdown is automatic — Beacon prunes chunks and slices coordinate dimensions from your `filters`:

```http
POST /api/query
Content-Type: application/json

{
  "from": {
    "zarr": {
      "paths": ["sst/*/zarr.json"]
    }
  },
  "select": ["time", "latitude", "longitude", "sst"],
  "filters": [
    { "column": "time", "min": "2025-01-01" },
    { "column": "latitude", "min": -10, "max": 10 }
  ],
  "limit": 1000,
  "output": { "format": "csv" }
}
```

---

## SQL

:::warning
SQL is enabled by default; it can be disabled with `BEACON_ENABLE_SQL=false`.
:::

### Select with a computed column

```http
POST /api/query
Content-Type: application/json

{
  "sql": "SELECT time, latitude, longitude, round(temperature, 2) AS temperature_rounded FROM default LIMIT 100",
  "output": { "format": "csv" }
}
```

### WHERE with multiple conditions

```http
POST /api/query
Content-Type: application/json

{
  "sql": "SELECT time, latitude, longitude, temperature FROM default WHERE temperature BETWEEN 2 AND 10 AND latitude BETWEEN -10 AND 10 LIMIT 1000",
  "output": { "format": "csv" }
}
```

### GROUP BY aggregation

```http
POST /api/query
Content-Type: application/json

{
  "sql": "SELECT platform, avg(temperature) AS avg_temp, count(*) AS n FROM default GROUP BY platform ORDER BY n DESC LIMIT 100",
  "output": { "format": "csv" }
}
```

### Read Zarr directly with 1D pushdown

```http
POST /api/query
Content-Type: application/json

{
  "sql": "SELECT time, latitude, longitude, sst FROM read_zarr(['sst/*/zarr.json'], ['time', 'latitude', 'longitude']) WHERE time >= '2025-01-01' LIMIT 1000",
  "output": { "format": "csv" }
}
```

### Export as GeoParquet

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

---

## Validate and explain

Both styles support validate and explain. The request body is identical to a normal query body.

```http
POST /api/parse-query
Content-Type: application/json

{ "select": ["time", "temperature"], "limit": 1 }
```

```http
POST /api/explain-query
Content-Type: application/json

{ "sql": "SELECT * FROM default LIMIT 10" }
```
