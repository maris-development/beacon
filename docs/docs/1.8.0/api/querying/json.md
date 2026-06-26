# JSON Query DSL

The JSON DSL lets you express queries as a typed object — no SQL string building required. It is the preferred interface for programmatic clients and query builders.

```http
POST /api/query
Content-Type: application/json
```

:::tip
Discover available columns before querying:

- Default table: `GET /api/table-schema?table_name=default`
- From a file glob: `GET /api/dataset-schema?file=argo/**/*.nc`

:::

## Request shape

| Field | Required | Description |
| ----- | -------- | ----------- |
| `select` | Yes | Columns (and expressions) to return |
| `from` | No | Data source — table name or inline file source |
| `filters` | No | Row filters, combined with AND by default |
| `sort_by` | No | Sort expressions |
| `limit` | No | Maximum rows to return |
| `offset` | No | Rows to skip |
| `distinct` | No | DISTINCT ON expression |
| `output` | No | Output format (default: Arrow IPC stream) |

## Selecting columns

### Plain column

```json
{ "select": ["time", "latitude", "longitude"] }
```

### Column with alias

```json
{
  "select": [
    { "column": "sea_surface_temperature", "alias": "sst" }
  ]
}
```

### Function call

```json
{
  "select": [
    { "function": "round", "args": ["temperature", { "value": 2 }], "alias": "temperature_rounded" }
  ]
}
```

`args` entries are either a column name string or a literal `{ "value": … }` object.

## Choosing the data source (`from`)

### Query a registered table

```http
POST /api/query
Content-Type: application/json

{
  "from": "default",
  "select": ["time", "temperature"],
  "limit": 100,
  "output": { "format": "csv" }
}
```

Use `GET /api/tables` to list available table names. When `from` is omitted, Beacon uses the configured default table.

### Query files directly

Pass a format key with a `paths` array. Paths are resolved relative to Beacon's dataset root and support glob patterns.

**NetCDF:**

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

**Zarr:**

```http
POST /api/query
Content-Type: application/json

{
  "from": { "zarr": { "paths": ["sst/*/zarr.json"] } },
  "select": ["time", "sst"],
  "limit": 100,
  "output": { "format": "csv" }
}
```

Predicate pushdown is automatic for large Zarr stores — Beacon prunes chunks and slices coordinate dimensions from your `filters`, with no extra options to configure:

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
  "filters": [{ "column": "time", "min": "2025-01-01" }],
  "limit": 1000,
  "output": { "format": "csv" }
}
```

**Parquet:**

```http
POST /api/query
Content-Type: application/json

{
  "from": { "parquet": { "paths": ["obs/**/*.parquet"] } },
  "select": ["time", "latitude", "longitude"],
  "limit": 100,
  "output": { "format": "csv" }
}
```

Other supported format keys: `csv`, `arrow`, `odv`, `tiff`, `bbf`.

## Filters

`filters` is an array of filter objects. Multiple entries are combined with AND. A filter can be placed on any column in the schema.

### Range (min / max)

```json
{ "filters": [{ "column": "temperature", "min": 2, "max": 10 }] }
```

Either `min` or `max` can be omitted for an open-ended range.

### Equality

```json
{ "filters": [{ "column": "platform", "eq": "SHIP" }] }
```

### AND (multiple filters)

```http
POST /api/query
Content-Type: application/json

{
  "select": ["time", "latitude", "longitude", "temperature"],
  "filters": [
    { "column": "temperature", "min": 2, "max": 10 },
    { "column": "latitude", "min": -10, "max": 10 }
  ],
  "limit": 10000,
  "output": { "format": "csv" }
}
```

### OR

Wrap OR branches in a single `or` filter object:

```http
POST /api/query
Content-Type: application/json

{
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

### GeoJSON spatial filter

Tests whether a point (lon/lat columns) falls within a GeoJSON geometry:

```http
POST /api/query
Content-Type: application/json

{
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

### Filter operator reference

Every leaf filter names a column with `column` (alias `for_query_parameter`) plus
one operator key. The full set:

| Operator | Key(s) | Example |
| --- | --- | --- |
| Equal | `eq` | `{ "column": "platform", "eq": "SHIP" }` |
| Not equal | `neq` (aliases `not_eq`, `not_equal`) | `{ "column": "platform", "neq": "BUOY" }` |
| Greater than | `gt` | `{ "column": "depth", "gt": 0 }` |
| Greater or equal | `gt_eq` (alias `min`) | `{ "column": "depth", "gt_eq": 0 }` |
| Less than | `lt` | `{ "column": "depth", "lt": 100 }` |
| Less or equal | `lt_eq` (alias `max`) | `{ "column": "depth", "lt_eq": 100 }` |
| Range (between) | `gt_eq` + `lt_eq` (aliases `min`/`low` and `max`/`high`) | `{ "column": "temp", "min": 2, "max": 10 }` |
| Is null | `is_null` | `{ "is_null": { "column": "qc_flag" } }` |
| Is not null | `is_not_null` (aliases `skip_fill_values`, `skip_missing`) | `{ "is_not_null": { "column": "qc_flag" } }` |
| All of | `and` | `{ "and": [ … ] }` |
| Any of | `or` | `{ "or": [ … ] }` |
| Point-in-geometry | `longitude_column` + `latitude_column` + `geometry` | see [GeoJSON spatial filter](#geojson-spatial-filter) |

The `min` / `max` keys used elsewhere on this page are aliases of `gt_eq` / `lt_eq`.

## Sorting and pagination

| Field | Description |
| ----- | ----------- |
| `sort_by` | Array of `{"Asc": "col"}` or `{"Desc": "col"}` objects |
| `limit` | Maximum number of rows |
| `offset` | Number of rows to skip |

:::warning
`sort_by` enum keys are case-sensitive: `"Asc"` and `"Desc"`, not `"asc"` / `"desc"`.
:::

```http
POST /api/query
Content-Type: application/json

{
  "select": ["time", "temperature"],
  "sort_by": [{ "Desc": "time" }],
  "offset": 100,
  "limit": 50,
  "output": { "format": "csv" }
}
```

## DISTINCT ON

Return one row per unique combination of the `on` columns:

```http
POST /api/query
Content-Type: application/json

{
  "distinct": {
    "on": ["platform"],
    "select": ["platform", "time", "temperature"]
  },
  "sort_by": [{ "Desc": "time" }],
  "limit": 100,
  "output": { "format": "csv" }
}
```

## Output formats

See [Querying — Output formats](./index.md#output-formats) for the full list. The `output` field is identical for JSON DSL and SQL queries.
