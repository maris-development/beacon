---
description: Write BeaconDB query results to Parquet, CSV, NetCDF, HDF5, GeoParquet, or ODV, stream large results in batches, or hand them to pandas, Polars, and Arrow.
---

# Export Query Results

A query result can go straight to a file, into a dataframe, or out in batches. Which you choose
mostly depends on how big the result is.

## To a dataframe

For results that fit in memory:

```python
import beacondb
con = beacondb.connect("beacon.db")

rel = con.sql("SELECT * FROM read_parquet('obs/*.parquet') WHERE depth < 50")

rel.df()      # pandas
rel.arrow()   # pyarrow.Table
```

Relations are lazy: nothing executes until you ask for results, so you can build a query up in steps
without paying for intermediate passes.

## To a file

Write directly from a relation, without materializing it in Python first:

```python
rel.to_parquet("out.parquet")
rel.to_csv("out.csv")
rel.to_netcdf("out.nc")                       # a real NetCDF-4 file
rel.to_hdf5("out.h5")
```

Geospatial and multi-dimensional targets take the columns that define their structure:

```python
rel.to_geoparquet("pts.parquet", longitude="lon", latitude="lat")
rel.to_nd_netcdf("grid.nc", ["depth"])        # multi-dimensional NetCDF
rel.to_odv("out.zip", longitude="lon", latitude="lat",
           depth="pres", time="juld", key="platform")
```

:::warning Local destinations only
File sinks currently write to local paths. A `scheme://` destination such as `s3://…` raises
`NotSupportedError`.
:::

## Results too large for memory

`.df()` and `.arrow()` collect everything. For a large export, pull batches instead. The engine
produces them on demand and releases the GIL while doing so:

```python
for batch in con.sql("SELECT * FROM read_parquet('huge/*.parquet')").record_batch():
    process(batch)

con.sql("SELECT * FROM obs").record_batch(50_000)   # about 50k rows per batch
```

This keeps memory flat regardless of result size, and pairs well with writing incrementally to your
own sink.

## Keeping results in the database

When the result is something you will query again rather than hand to another tool, store it instead
of exporting it:

```sql
CREATE TABLE hot_profiles AS
SELECT * FROM read_netcdf('argo/**/*.nc') WHERE temperature > 25;
```

That creates a [managed table](/docs/2.0.0-rc2/beacondb/data-sources/internal-format) inside `beacon.db`.
If the underlying data changes and you want the result refreshed on demand, use a
[materialized view](/docs/2.0.0-rc2/beacondb/sql/create-materialized-view) instead.

## Over the server

Beacon Data Lake exposes the same conversions over HTTP: set `output.format` to `csv`, `parquet`,
`netcdf`, or `ipc` on a query request, with options for GeoParquet, N-dimensional NetCDF, and ODV.
See [querying output formats](/docs/2.0.0-rc2/api/querying/#output-formats).
