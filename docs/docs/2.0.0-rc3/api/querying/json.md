# JSON Query DSL

The JSON DSL gives a query as a typed object. You build no SQL string. Use this interface in a
client program and in a query builder.

```http
POST /api/query
Content-Type: application/json
```

:::tip
Find the available columns before you write a query:

- Default table: `GET /api/table-schema?table_name=default`
- From a file glob: `GET /api/dataset-schema?file=argo/**/*.nc`

:::

## Request shape

| Field | Required | Description |
| ----- | -------- | ----------- |
| `select` | Yes | Columns (and expressions) to return |
| `from` | No | Data source, table name or inline file source |
| `filters` | No | Row filters, combined with AND by default |
| `sort_by` | No | Sort expressions |
| `limit` | No | Maximum rows to return |
| `offset` | No | Rows to skip |
| `distinct` | No | DISTINCT ON expression |
| `output` | No | Output format (default: Arrow IPC stream) |

## Select columns

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

An `args` entry is a column name string, or a literal object `{ "value": … }`.

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

Use `GET /api/tables` to list the table names. Without a `from` field, Beacon uses the default
table.

### Query files directly

Give a format key with a `paths` array. Beacon resolves a path against its dataset root. A path also
takes a glob pattern.

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

Predicate pushdown is automatic on a large Zarr store. Beacon prunes the chunks and slices the
coordinate dimensions. It uses your `filters`. You configure no extra option:

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

The other format keys are `csv`, `arrow`, `odv`, `tiff` and `bbf`.

## Filters

`filters` is an array of filter objects. Beacon combines the entries with `AND`. A filter works on
any column of the schema.

### Range (min / max)

```json
{ "filters": [{ "column": "temperature", "min": 2, "max": 10 }] }
```

Omit `min` or `max` for a range with one limit.

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

Put the `OR` branches in one `or` filter object:

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

Tests if a point lies inside a GeoJSON geometry. The point comes from a longitude column and a
latitude column:

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

The filter plans this expression:

```sql
ST_Within(
    ST_Point(longitude, latitude),
    ST_GeomFromGeoJSON('{"type":"Polygon","coordinates":[[[4,52],[6,52],[6,54],[4,54],[4,52]]]}')
)
```

The three functions come from the [spatial
functions](/docs/2.0.0-rc3/sql/spatial-functions) of the SQL path. A JSON filter and a SQL `WHERE`
clause therefore state the same test under the same names. Any GeoJSON geometry type works, such
as `Polygon`, `MultiPolygon` and `Point`.

### Filter operator reference

Every leaf filter names a column with `column`. The alias is `for_query_parameter`. Each filter also
takes one operator key. This is the full set:

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

The `min` and `max` keys on this page are aliases of `gt_eq` and `lt_eq`.

## Sort and paginate

| Field | Description |
| ----- | ----------- |
| `sort_by` | Array of `{"Asc": "col"}` or `{"Desc": "col"}` objects |
| `limit` | Maximum number of rows |
| `offset` | Number of rows to skip |

:::warning
The `sort_by` enum keys use exact case. Write `"Asc"` and `"Desc"`. Do not write `"asc"` or
`"desc"`.
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

Returns one row for each unique combination of the `on` columns:

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

See [Querying, Output formats](/docs/2.0.0-rc3/api/querying/#output-formats) for the full list. The
`output` field is the same for a JSON DSL query and a SQL query.
