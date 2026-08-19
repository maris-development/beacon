---
description: Write Beacon results to Parquet, CSV, NetCDF, GeoParquet or ODV. Stream a large result to disk, or read it with pandas and Arrow.
---

# Export Query Results

A query result goes to a dataframe or to a file. The size of the result decides your choice.

## To a dataframe

Use a dataframe for a result that fits in memory:

```python
from beacon_api import Client

client = Client("https://beacon.example.com")

query = client.sql_query(
    "SELECT * FROM read_parquet('obs/*.parquet') WHERE depth < 50"
)

df = query.to_pandas_dataframe()
gdf = query.to_geo_pandas_dataframe("lon", "lat")
```

Beacon runs nothing until you call a `to_*` method. Build the query first. Then check it with
[`EXPLAIN`](/docs/2.0.0-rc3/sql/select).

## To a file

Write straight to disk. Python holds no copy of the data:

```python
query.to_parquet("out.parquet")
query.to_csv("out.csv")
```

A geospatial or multi-dimensional target needs the columns that give its structure:

```python
query.to_geoparquet("pts.parquet", "lon", "lat")
query.to_nd_netcdf("grid.nc", dimension_columns=["depth"])
```

`to_nd_netcdf` needs Beacon 1.5.0 or later on the server.

## Results too large for memory

`to_pandas_dataframe()` collects every row. For a large export, ask the server for a file format.
Then stream the response to disk. The server encodes during the scan. Its memory use stays flat:

```bash
curl -X POST https://beacon.example.com/api/query \
  -H "Content-Type: application/json" \
  -d '{
    "sql": "SELECT * FROM read_parquet(\"huge/*.parquet\")",
    "output": { "format": "parquet" }
  }' \
  --output huge.parquet
```

The same works for `csv`, `netcdf` and `ipc`. See
[output formats](/docs/2.0.0-rc3/api/querying/#output-formats).

For a streaming read in Python instead of a file, connect over Arrow Flight SQL and pull record
batches. See [Python ADBC](/docs/2.0.0-rc3/connect/python-adbc).

## Keep results on the server

Do you query the result again, instead of a hand-over to another tool? Then store the result. Do not
export it:

```sql
CREATE TABLE hot_profiles AS
SELECT * FROM read_netcdf('argo/**/*.nc') WHERE temperature > 25;
```

This creates a [managed table](/docs/2.0.0-rc3/sql/managed-tables) inside `beacon.db`. Does the
source data change? Do you want a refresh on demand? Then use a
[materialized view](/docs/2.0.0-rc3/sql/create-materialized-view).

## Output formats

| Format | `output.format` | Client method |
|---|---|---|
| Parquet | `parquet` | `to_parquet()` |
| GeoParquet | `parquet` with `geo` options | `to_geoparquet()` |
| CSV | `csv` | `to_csv()` |
| NetCDF | `netcdf` | none |
| N-dimensional NetCDF | `netcdf` with `dimensions` | `to_nd_netcdf()` |
| Arrow IPC | `ipc` | none |
| ODV ASCII | `odv` | none |

See [output formats](/docs/2.0.0-rc3/api/querying/#output-formats) for the full option set of each
one.
