---
description: BeaconDB is an embeddable SQL engine for scientific data. It reads NetCDF, Zarr, Parquet, GeoTIFF and more in place from disk or object storage, and stores what it owns in one portable beacon.db file.
---

# BeaconDB

BeaconDB is an **embeddable SQL engine for scientific data**. It runs inside your process, reads
your existing files where they already live, and answers SQL over them. There is no server to
operate and no import step.

```python
import beacondb

con = beacondb.connect("beacon.db")
con.sql("SELECT platform, avg(temperature) AS t "
        "FROM read_netcdf('argo/**/*.nc') GROUP BY platform").df()
```

The model is deliberately simple: one process, one file, full SQL. What sets it apart is what it
reads. BeaconDB speaks the formats scientific data actually ships in, including multi-dimensional
array formats that general-purpose engines cannot open.

## What it does

- **Reads files in place.** NetCDF, Zarr, Parquet/GeoParquet, CSV, Arrow, GeoTIFF, Atlas, Delta, ODV,
  and BBF, from local disk or S3/GCS/Azure. No conversion, no loading step.
- **Answers SQL.** A full analytical dialect built on DataFusion, with joins, aggregates, window
  functions, views, and materialized views.
- **Owns data when you want it to.** Managed tables give you `INSERT` / `UPDATE` / `DELETE`, stored
  inside a single portable `beacon.db` file.
- **Reaches other systems.** Query Postgres and MySQL, or federate against another Beacon with
  remote tables and `ATTACH`.
- **Pushes work down.** Filters and projections are pushed into the files and remote systems it
  reads, so a query fetches only the bytes it needs.

## What it is used for

| Use case | Why BeaconDB fits |
| --- | --- |
| Exploring a collection of NetCDF or Zarr files from a notebook | Query thousands of files with one SQL statement, no preprocessing |
| Shipping an application with an embedded query engine | The engine is a library, and the database is one file you can copy |
| Turning array data into tabular results | Multi-dimensional variables become columns you can filter and aggregate |
| Joining local files against a remote catalog | `ATTACH` a remote Beacon and join it with local data |
| Preparing extracts for downstream tools | Export results to Parquet, CSV, NetCDF, or Arrow |

If instead you need to serve datasets to many people over the network, with access control and a web
UI, that is the same engine packaged as
[Beacon Data Lake](/docs/2.0.0-rc2/getting-started).

## How this chapter is organized

1. **[How It Works](/docs/2.0.0-rc2/beacondb/how-it-works)**: the engine, the query pipeline, and the
   storage model.
2. **[Data Sources](/docs/2.0.0-rc2/beacondb/data-sources/)**: reading external files and object storage,
   connecting to other databases, and BeaconDB's own internal format.
3. **[SQL Reference](/docs/2.0.0-rc2/beacondb/sql/)**: statements, functions, and syntax.
4. **[Python Binding](/docs/2.0.0-rc2/beacondb/python/)**: the client API, from `connect()` to dataframes.
5. **[Guides](/docs/2.0.0-rc2/beacondb/guides/)**: task-oriented walkthroughs.

## Language bindings

Each binding is a thin front-end over the same engine: same SQL, same formats, same `beacon.db`.

| Binding | Status | Docs |
|---|---|---|
| **Python** (`pip install beacondb`) | Available | [Python binding](/docs/2.0.0-rc2/beacondb/python/) |
| **Rust** | Planned | |
| **C ABI** (for .NET, Go, Node, Java) | Planned | |

The Python binding is the reference front-end today: a PEP 249 connection, a lazy relation API,
Arrow-native results, `ATTACH`, secrets, streaming, and file sinks.

**[Get started with the Python binding](/docs/2.0.0-rc2/beacondb/python/getting-started)**
