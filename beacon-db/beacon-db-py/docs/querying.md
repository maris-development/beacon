---
description: Write SQL against beacondb. Lazy execution, catalog-driven readers with keyword options, result streams, file sinks and EXPLAIN.
---

# Querying

## SQL in, results out

`sql()`, `query()`, `table()` and `view()` return a **relation**. A relation is a query that Beacon
builds but does not run. You shape it in SQL. Nothing reaches the engine until you call a terminal
method:

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

The terminal methods are `fetchall`, `fetchmany`, `fetchone`, `arrow`, `df`, `pl`, `record_batch`,
`show`, `create`, `create_view` and the `to_*` sinks. Everything before a terminal method costs
nothing. `rel.sql`, `rel.columns` and `rel.types` are therefore cheap ways to check a query first.

::: info No method chaining
BeaconDB does not yet give relational composition, such as `.filter()`, `.aggregate()` and
`.join()`. Write the SQL instead. It uses the same engine. One statement also reads better than the
equal chain.
:::

## Reading files

The readers are the table functions of Beacon as methods. Each one returns a lazy relation:

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

Every table function of Beacon is also a method. `con.table_functions()` lists them.
`con.read(fn, *args)` is the general form.

**Reader options.** Give the format options by position or by keyword. Beacon matches a keyword to a
declared parameter of the reader. Every reader also takes a `columns=[...]` projection:

```python
con.read_csv("stations.csv", delimiter=";")
con.read_parquet("obs/*.parquet", columns=["depth", "temp"])
```

## Stream large results

`.arrow()` and `.df()` collect the whole result. For a result that does not fit in memory, use
`.record_batch()`. It returns a **`pyarrow.RecordBatchReader`**. The reader pulls batches from the
engine on demand. Beacon releases the GIL during each pull:

```python
for batch in con.read_parquet("huge/*.parquet").record_batch():
    process(batch)

con.sql("SELECT * FROM obs").record_batch(50_000)   # ~50k rows per batch
```

## Write files

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

A sink writes to a local path only. A `scheme://` destination raises `NotSupportedError`.

## Data profiling

`SUMMARIZE` returns one row for each column. Run it first on a new dataset:

```python
con.sql("SUMMARIZE read_parquet('obs/*.parquet')").df()
```

See [SUMMARIZE](/docs/2.0.0-rc4/beacondb/sql/summarize).

## Beyond SQL

```python
con.json_query({ "select": ["depth", "temperature"], "from": "obs",
                 "filter": {"column": "depth", "gt_eq": 50}, "limit": 5 }).df()
con.list_tables(); con.functions(); con.metrics()
```
