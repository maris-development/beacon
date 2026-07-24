# beacondb

Python bindings for **beacon-db**: an embeddable, DuckDB-class database for scientific data.

```python
import beacondb

con = beacondb.connect("beacon.db")
con.sql("SELECT 1 AS a").fetchall()          # [(1,)]
con.sql("SELECT * FROM read_parquet('obs/*.parquet')").df()
```

One file holds everything beacon *owns* — the catalog and the data it manages. Everything
else is referenced from it: netCDF/Zarr/Parquet files on disk or in S3, Delta and Iceberg
tables, remote Postgres/MySQL. Copy `beacon.db` and the managed lake travels with you.

This is the engine linked **in-process**. There is no server and no HTTP; for talking to a
running Beacon server, use [`beacon-datalake-cli`](../../clients/beacon-datalake-cli/) or
[`@beacon/client`](../../clients/beacon-ts/).

## Install

```bash
pip install beacondb                 # core
pip install "beacondb[pandas]"       # + .df()
pip install "beacondb[polars]"       # + .pl()
```

Nothing is required at runtime: results cross into Python over the Arrow PyCapsule
protocol (`__arrow_c_stream__`), so any Arrow consumer can read them with no dependency of
ours. pyarrow/pandas/polars are only needed by the methods that return their types.

## Auth is off by default

Opening a database locally needs no credentials and permits everything — the same contract
as SQLite and DuckDB: **possession of the file is full control.**

```python
con = beacondb.connect("beacon.db")
con.whoami()
# {'username': 'local', 'roles': [], 'is_super_user': True, 'auth': False}
```

Pass `auth=True` to switch on beacon's RBAC. A session is then the anonymous, read-only
principal until credentials are supplied:

```python
anon = beacondb.connect("beacon.db", auth=True,
                        admin_username="admin", admin_password=...)
anon.whoami()["username"]              # 'anonymous'
anon.execute("CREATE TABLE t (a INT)") # NotPermittedError

analyst = anon.connect_as(username="analyst", password=...)
analyst.sql("SELECT * FROM t").df()    # allowed if granted
```

Supplying credentials with `auth=False` is an error, not a no-op — a connection that looks
restricted but is not would be worse than a clear refusal.

RBAC written into a database is **not** enforced when it is opened with `auth=False`. It is
a boundary for *served* access (the `beacon-datalake` HTTP / Flight SQL transports), not
against local possession of the file.

## One file, one handle

The container file is held under an exclusive lock, so one process opens one `beacon.db`.
A second `connect()` to the same path in the same process returns the same underlying
database (sharing its lock); from another process it raises `OperationalError`. Use
`cursor()` for an independent result slot, and `connect_as()` for a different identity.

## Lazy relations

`sql()`, `query()`, `table()` and `view()` return a **relation**: a query you compose
without running it. Relational methods chain; nothing touches the engine until a terminal
method:

```python
rel = (con.table("events")
          .filter("kind = 'click'")
          .aggregate("user_id, count(*) AS n", "user_id")
          .order("n desc")
          .limit(10))

rel.sql        # inspect the SQL — runs nothing
rel.df()       # now it runs
```

The builder keeps `ORDER BY` and `LIMIT` in one `SELECT`, so `.order(...).limit(n)` is a
correct top-N rather than an unspecified inner-`ORDER BY`. Relations are immutable, so a
relation is safe to branch into two derived queries.

## Reading files, and beacon's own formats

The readers are beacon's table functions, surfaced as methods. Every one returns a lazy
relation you can compose further:

```python
con.read_parquet("obs/*.parquet").filter("depth <= 100").df()
con.read_netcdf("argo/float.nc").aggregate("platform, avg(temperature) AS t", "platform").df()
con.read_csv("stations.csv"); con.read_zarr(...); con.read_delta(...); con.list_datasets()
```

They are resolved from the catalog (`beacon.system.table_functions`), so *any* table
function beacon registers is a method — `con.table_functions()` lists them, and new ones
appear with no client update. `con.read(fn, *args)` is the general form.

## Writing files

```python
rel.to_parquet("out.parquet")
rel.to_csv("out.csv")
rel.to_arrow_ipc("out.arrow")
rel.to_netcdf("out.nc")                      # a real NetCDF-4 file
rel.to_nd_netcdf("grid.nc", ["depth"])       # multi-dimensional
rel.to_geoparquet("pts.parquet", longitude="lon", latitude="lat")
```

Local paths only for now; a `scheme://` destination raises `NotSupportedError`.

## Status

Working today: `connect()` with both auth modes; the DB-API path (`execute`/`fetchone`/
`fetchmany`/`fetchall`, `description`/`rowcount`, `cursor`); `connect_as`/`as_anonymous`/
`whoami`; context managers; the Arrow PyCapsule protocol with `.arrow()`/`.df()`/`.pl()`;
the lazy relation (`filter`/`project`/`aggregate`/`order`/`limit`/`distinct`/`join`/`union`/
`count`/`sum`/`min`/`max`/`mean`/`query`, terminals `fetch*`/`arrow`/`df`/`pl`/
`record_batch`/`explain`/`show`/`create`/`create_view`, metadata `sql`/`columns`/`types`/
`shape`/`__len__`); the catalog-driven `read_*` readers; the `to_parquet`/`to_csv`/
`to_arrow_ipc`/`to_netcdf`/`to_nd_netcdf`/`to_geoparquet` sinks; and bound parameters —
`execute(sql, params)` / `executemany(sql, rows)` with `?` or `$1` placeholders, bound
(never interpolated) so they are injection-safe.

Not yet: `register()` of pandas/Arrow objects, streaming results, `read_only=True`, `to_odv`,
and multi-statement transactions. See
[plans/python-interface-requirements.md](../../plans/python-interface-requirements.md).
