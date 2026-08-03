---
description: Writing SQL against beacondb — lazy execution, the catalog-driven readers with keyword options, streaming results, file sinks, and EXPLAIN.
---

# Querying

## SQL in, results out

`sql()`, `query()`, `table()`, and `view()` return a **relation**: a query that has been built but
not yet run. You shape it in SQL, and nothing touches the engine until a terminal method:

```python
rel = con.sql("""
    SELECT user_id, count(*) AS n
    FROM events
    WHERE kind = 'click'
    GROUP BY user_id
    ORDER BY n DESC
    LIMIT 10
""")

rel.sql          # inspect the SQL — runs nothing
rel.explain()    # logical + physical plan; rel.explain(analyze=True) runs it with metrics
rel.df()         # now it runs
```

Terminals are `fetchall`/`fetchmany`/`fetchone`, `arrow`/`df`/`pl`, `record_batch`, `show`,
`create`/`create_view`, and the `to_*` sinks. Everything before one of those is free, so
`rel.sql`, `rel.columns` and `rel.types` are cheap ways to check a query before paying for it.

::: info No method chaining
BeaconDB does not currently expose relational composition (`.filter()`, `.aggregate()`,
`.join()`, …). Write the SQL instead — it is the same engine, and one statement reads back more
clearly than the equivalent chain.
:::

## Reading files

The readers are Beacon's table functions surfaced as methods; every one returns a lazy relation:

```python
con.read_parquet("obs/*.parquet").df()
con.read_hdf5("data.h5").df()          # netCDF-4 is HDF5; plain HDF5 reads too

# to filter or aggregate, call the reader as a table function in SQL
con.sql("SELECT * FROM read_parquet('obs/*.parquet') WHERE depth <= 100").df()
con.sql("""
    SELECT platform, avg(temperature) AS t
    FROM read_netcdf('argo/float.nc')
    GROUP BY platform
""").df()
con.read_csv("stations.csv"); con.read_zarr(...); con.read_delta(...); con.list_datasets()
```

Every table function Beacon registers is a method, `con.table_functions()` lists them, and
`con.read(fn, *args)` is the general form.

**Reader options.** Pass format options positionally or by keyword (matched to the reader's declared
parameters), plus a universal `columns=[...]` projection:

```python
con.read_csv("stations.csv", delimiter=";")
con.read_parquet("obs/*.parquet", columns=["depth", "temp"])
```

## Streaming large results

`.arrow()`/`.df()` collect the whole result. For a result too big for memory, `.record_batch()`
returns a **`pyarrow.RecordBatchReader`** that pulls batches from the engine on demand (the GIL is
released during each pull):

```python
for batch in con.read_parquet("huge/*.parquet").record_batch():
    process(batch)

con.sql("SELECT * FROM obs").record_batch(50_000)   # ~50k rows per batch
```

## Writing files

```python
rel.to_parquet("out.parquet")
rel.to_csv("out.csv")
rel.to_netcdf("out.nc")                       # a real NetCDF-4 file
rel.to_hdf5("out.h5")
rel.to_nd_netcdf("grid.nc", ["depth"])        # multi-dimensional
rel.to_geoparquet("pts.parquet", longitude="lon", latitude="lat")
rel.to_odv("out.zip", longitude="lon", latitude="lat",       # Ocean Data View archive
           depth="pres", time="juld", key="platform")
```

Local paths only for now; a `scheme://` destination raises `NotSupportedError`.

## Data profiling

`SUMMARIZE` gives a one-row-per-column profile, the first thing to run on a new dataset:

```python
con.sql("SUMMARIZE read_parquet('obs/*.parquet')").df()
```

See [SUMMARIZE](/docs/2.0.0-rc2/beacondb/sql/summarize).

## Beyond SQL

```python
con.json_query({ "select": ["depth", "temperature"], "from": "obs",
                 "filter": {"column": "depth", "gt_eq": 50}, "limit": 5 }).df()
con.list_tables(); con.functions(); con.metrics()
```
