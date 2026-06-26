# Querying

All queries go through a single endpoint:

```http
POST /api/query
Content-Type: application/json
```

The request body selects between two query styles:

| Style | When to use | Body key |
| ----- | ----------- | -------- |
| [JSON DSL](./json.md) | Programmatic clients, query builders | `select`, `from`, `filters`, … |
| [SQL](./sql.md) | Power users, ad-hoc analysis | `sql` |

Both styles share the same `output` field and the same supporting endpoints.

## Supporting endpoints

### Validate

Parse and type-check a query without executing it:

```http
POST /api/parse-query
Content-Type: application/json

{ "select": ["time", "temperature"], "limit": 1 }
```

### Explain

Return the physical query plan — useful for debugging and performance work:

```http
POST /api/explain-query
Content-Type: application/json

{ "select": ["time", "temperature"], "limit": 1 }
```

### Explain Analyze

**Run** the query and return the physical plan annotated with per-operator
runtime metrics (rows, bytes, and time per node) — the analog of SQL `EXPLAIN
ANALYZE`. Because it executes the query, it is subject to the same SQL gating as
`/api/query` (disabled when `BEACON_ENABLE_SQL=false` for `sql` bodies):

```http
POST /api/explain-analyze-query
Content-Type: application/json

{ "select": ["time", "temperature"], "limit": 1 }
```

### Query metrics

Beacon returns a query ID via the `x-beacon-query-id` response header. Use it to fetch timing and row-count metrics after the query completes:

```http
GET /api/query/metrics/{query_id}
```

## Default response: Arrow IPC stream

When `output` is omitted, `/api/query` returns an [Apache Arrow IPC stream](https://arrow.apache.org/docs/format/Columnar.html#ipc-streaming-format) (`application/vnd.apache.arrow.stream`). This is the most efficient format for downstream processing.

Client libraries:

- Python: [PyArrow `open_stream` / `RecordBatchStreamReader`](https://arrow.apache.org/docs/python/ipc.html)
- Rust: [`arrow-ipc` `StreamReader`](https://docs.rs/arrow-ipc/latest/arrow_ipc/reader/struct.StreamReader.html)
- C++: [`RecordBatchStreamReader`](https://arrow.apache.org/docs/cpp/ipc.html)

## Output formats

Add an `output` field to download a single file instead of streaming Arrow IPC.

### Simple formats

Set `output.format` to one of: `csv`, `parquet`, `netcdf`, `ipc` (alias: `arrow`).

```http
POST /api/query
Content-Type: application/json

{
  "select": ["time", "temperature"],
  "output": { "format": "csv" }
}
```

### GeoParquet

Requires longitude and latitude columns:

```http
POST /api/query
Content-Type: application/json

{
  "select": ["longitude", "latitude", "time", "temperature"],
  "output": {
    "format": {
      "geoparquet": {
        "longitude_column": "longitude",
        "latitude_column": "latitude"
      }
    }
  }
}
```

### N-dimensional NetCDF

Reconstructs a multi-dimensional NetCDF file from the result, using the specified columns as dimension axes:

```http
POST /api/query
Content-Type: application/json

{
  "select": ["time", "depth", "temperature"],
  "output": {
    "format": {
      "nd_netcdf": {
        "dimension_columns": ["time", "depth"]
      }
    }
  }
}
```

### ODV (Ocean Data View)

Exports the result as an Ocean Data View collection, returned as a **ZIP
archive**. ODV needs to know which columns carry the station coordinates and
which are data vs. metadata, so the format is configured with an options object:

```http
POST /api/query
Content-Type: application/json

{
  "select": ["cruise", "longitude", "latitude", "depth", "time", "temperature"],
  "output": {
    "format": {
      "odv": {
        "longitude_column": { "column_name": "longitude" },
        "latitude_column": { "column_name": "latitude" },
        "depth_column": { "column_name": "depth" },
        "time_column": { "column_name": "time" },
        "key_column": "cruise",
        "qf_schema": "SEADATANET",
        "data_columns": [{ "column_name": "temperature" }],
        "meta_columns": []
      }
    }
  }
}
```

Each `*_column` / `data_columns` / `meta_columns` entry identifies a result
column (and may carry ODV-specific attributes such as units and quality-flag
column). `key_column` groups rows into ODV stations and `qf_schema` selects the
quality-flag scheme. Compression and archiving behavior are configurable; the
response is always a ZIP.

## Data sources

Most queries target a **registered table** by name:

```json
{ "from": "default", "select": ["time"] }
```

Both styles also support querying files directly without a registered table — see the [JSON DSL `from` reference](./json.md#choosing-the-data-source-from) and the [SQL table functions](./sql.md#query-files-directly).
