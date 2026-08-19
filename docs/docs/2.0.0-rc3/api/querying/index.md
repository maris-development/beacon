# Querying

Every query goes to one endpoint:

```http
POST /api/query
Content-Type: application/json
```

The request body chooses one of two query styles:

| Style | When to use | Body key |
| ----- | ----------- | -------- |
| [JSON DSL](/docs/2.0.0-rc3/api/querying/json) | A client program or a query builder | `select`, `from`, `filters`, … |
| [SQL](/docs/2.0.0-rc3/api/querying/sql) | An expert user or an ad-hoc analysis | `sql` |

Both styles use the same `output` field. They also use the same support endpoints.

## Support endpoints

### Validate

Check that a query body is correct. Beacon parses it into a valid query. Beacon does not run it.
This is a check of the structure. It does not check the column types:

```http
POST /api/parse-query
Content-Type: application/json

{ "select": ["time", "temperature"], "limit": 1 }
```

### Explain

Returns the query plan. Beacon does not run the query. Use this to debug and to tune performance:

```http
POST /api/explain-query
Content-Type: application/json

{ "select": ["time", "temperature"], "limit": 1 }
```

### Explain Analyze

**Runs** the query. It then returns the physical plan with the runtime metrics of each operator. The
metrics give the rows, the bytes and the time of each node. This matches `EXPLAIN ANALYZE` in SQL.
The endpoint runs the query. The same SQL rules as `/api/query` therefore apply. A `sql` body fails
when `BEACON_ENABLE_SQL=false`:

```http
POST /api/explain-analyze-query
Content-Type: application/json

{ "select": ["time", "temperature"], "limit": 1 }
```

### Query metrics

Beacon returns a query ID in the `x-beacon-query-id` response header. Use the ID after the query to
fetch the timing and the row count:

```http
GET /api/query/metrics/{query_id}
```

## Default response: Arrow IPC stream

Without an `output` field, `/api/query` returns an
[Apache Arrow IPC stream](https://arrow.apache.org/docs/format/Columnar.html#ipc-streaming-format).
The content type is `application/vnd.apache.arrow.stream`. This is the fastest format for a later
process.

Client libraries:

- Python: [PyArrow `open_stream` / `RecordBatchStreamReader`](https://arrow.apache.org/docs/python/ipc.html)
- Rust: [`arrow-ipc` `StreamReader`](https://docs.rs/arrow-ipc/latest/arrow_ipc/reader/struct.StreamReader.html)
- C++: [`RecordBatchStreamReader`](https://arrow.apache.org/docs/cpp/ipc.html)

## Output formats

Add an `output` field to download one file. Beacon then does not stream Arrow IPC.

### Simple formats

Set `output.format` to `csv`, `parquet`, `netcdf` or `ipc`. The alias of `ipc` is `arrow`.

```http
POST /api/query
Content-Type: application/json

{
  "select": ["time", "temperature"],
  "output": { "format": "csv" }
}
```

### GeoParquet

This format needs a longitude column and a latitude column:

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

Builds a multi-dimensional NetCDF file from the result. The named columns become the dimension
axes:

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

Exports the result as an Ocean Data View collection. Beacon returns a **ZIP archive**. ODV needs the
columns of the station coordinates. It also needs to know the data columns and the metadata columns.
An options object gives this:

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

Each entry in `*_column`, `data_columns` and `meta_columns` names a result column. An entry can also
hold ODV attributes, such as the units and the quality flag column. `key_column` groups the rows
into ODV stations. `qf_schema` selects the quality flag scheme. You can configure the compression
and the archive. The response is always a ZIP file.

## Data sources

Most queries use the name of a **registered table**:

```json
{ "from": "default", "select": ["time"] }
```

Both styles also query files directly, without a registered table. See the
[JSON DSL `from` reference](/docs/2.0.0-rc3/api/querying/json#choosing-the-data-source-from) and the
[SQL table functions](/docs/2.0.0-rc3/api/querying/sql#query-files-directly).
