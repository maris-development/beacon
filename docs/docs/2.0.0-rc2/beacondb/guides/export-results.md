---
description: Write BeaconDB results to Parquet, CSV, NetCDF, HDF5, GeoParquet or ODV. Stream a large result in batches, or read it with pandas, Polars and Arrow.
---

# Export Query Results

A query result goes to a file, to a dataframe or out in batches. The size of the result decides your
choice.

## To a dataframe

Use a dataframe for a result that fits in memory:

```python
import beacondb
con = beacondb.connect("beacon.db")

rel = con.sql("SELECT * FROM read_parquet('obs/*.parquet') WHERE depth < 50")

rel.df()      # pandas
rel.arrow()   # pyarrow.Table
```

A relation is lazy. Beacon runs nothing until you ask for the results. You can therefore build a
query in steps. Beacon makes no intermediate pass.

## To a file

Write directly from a relation. Python holds no copy of the data:

```python
rel.to_parquet("out.parquet")
rel.to_csv("out.csv")
rel.to_netcdf("out.nc")                       # a real NetCDF-4 file
rel.to_hdf5("out.h5")
```

A geospatial or multi-dimensional target needs the columns that give its structure:

```python
rel.to_geoparquet("pts.parquet", longitude="lon", latitude="lat")
rel.to_nd_netcdf("grid.nc", ["depth"])        # multi-dimensional NetCDF
rel.to_odv("out.zip", longitude="lon", latitude="lat",
           depth="pres", time="juld", key="platform")
```

:::warning Local destinations only
A file sink writes to a local path. A `scheme://` destination such as `s3://…` raises
`NotSupportedError`.
:::

## Results too large for memory

`.df()` and `.arrow()` collect everything. For a large export, read batches instead. The engine
makes them on demand. It releases the GIL during each pull:

```python
for batch in con.sql("SELECT * FROM read_parquet('huge/*.parquet')").record_batch():
    process(batch)

con.sql("SELECT * FROM obs").record_batch(50_000)   # about 50k rows per batch
```

The memory then stays flat for any result size. This also fits a write to your own sink, step by
step.

## Keep results in the database

Do you query the result again, instead of a hand-over to another tool? Then store the result. Do not
export it:

```sql
CREATE TABLE hot_profiles AS
SELECT * FROM read_netcdf('argo/**/*.nc') WHERE temperature > 25;
```

This creates a [managed table](/docs/2.0.0-rc2/beacondb/data-sources/internal-format) inside
`beacon.db`. Does the source data change? Do you want a refresh on demand? Then use a
[materialized view](/docs/2.0.0-rc2/beacondb/sql/create-materialized-view).

## Over the server

Beacon Data Lake gives the same conversions over HTTP. Set `output.format` on a query request to
`csv`, `parquet`, `netcdf` or `ipc`. There are also options for GeoParquet, N-dimensional NetCDF and
ODV. See [output formats](/docs/2.0.0-rc2/api/querying/#output-formats).
