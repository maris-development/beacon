---
description: BeaconDB is an embeddable SQL engine for scientific data. It reads NetCDF, Zarr, Parquet, GeoTIFF and more in place, from disk or object storage.
---

# BeaconDB

BeaconDB is an **embeddable SQL engine for scientific data**. It runs inside your process. It reads
your existing files where they are. It answers SQL over them. There is no server and no import step.

```python
import beacondb

con = beacondb.connect("beacon.db")
con.sql("SELECT platform, avg(temperature) AS t "
        "FROM read_netcdf('argo/**/*.nc') GROUP BY platform").df()
```

The model is simple: one process, one file, full SQL. The formats make the difference. BeaconDB
reads the formats that scientific data uses. This includes multi-dimensional array formats. Other
engines cannot open those formats.

## What it does

- **Reads files in place.** NetCDF, Zarr, Parquet, GeoParquet, CSV, Arrow, GeoTIFF, Atlas, Delta,
  ODV and BBF, on local disk or on S3, GCS and Azure. There is no conversion and no load step.
- **Answers SQL.** A full analytical dialect on DataFusion, with joins, aggregates, window
  functions, views and materialized views.
- **Owns data when you want it to.** Managed tables give you `INSERT`, `UPDATE` and `DELETE`. One
  portable `beacon.db` file holds them.
- **Reaches other systems.** Query Postgres and MySQL. Query another Beacon with remote tables and
  `ATTACH`.
- **Pushes work down.** Beacon pushes filters and projections into the files and remote systems that
  it reads. A query then fetches only the bytes that it needs.

## What it is used for

| Use case | Why BeaconDB fits |
| --- | --- |
| Explore a collection of NetCDF or Zarr files from a notebook | Query thousands of files with one SQL statement. No preparation step. |
| Ship an application with an embedded query engine | The engine is a library. The database is one file that you can copy. |
| Turn array data into tabular results | Multi-dimensional variables become columns. You filter and aggregate them. |
| Join local files against a remote catalog | `ATTACH` a remote Beacon. Then join it with local data. |
| Prepare extracts for other tools | Export the results to Parquet, CSV, NetCDF or Arrow. |

Do you need to serve datasets to many people over the network, with access control and a web UI?
Then use the same engine as [Beacon Data Lake](/docs/2.0.0-rc2/getting-started).

## How this chapter is organized

1. **[How It Works](/docs/2.0.0-rc2/beacondb/how-it-works)**: the engine, the query pipeline and the
   storage model.
2. **[Data Sources](/docs/2.0.0-rc2/beacondb/data-sources/)**: external files, object storage, other
   databases and the internal format of BeaconDB.
3. **[SQL Reference](/docs/2.0.0-rc2/beacondb/sql/)**: statements, functions and syntax.
4. **[Python Binding](/docs/2.0.0-rc2/beacondb/python/)**: the client API, from `connect()` to
   dataframes.
5. **[Guides](/docs/2.0.0-rc2/beacondb/guides/)**: step-by-step instructions for common tasks.

## Language bindings

Each binding is a thin front-end over the same engine. The SQL, the formats and the `beacon.db` file
stay the same.

| Binding | Status | Docs |
|---|---|---|
| **Python** (`pip install beacondb`) | Available | [Python binding](/docs/2.0.0-rc2/beacondb/python/) |
| **Rust** | Planned | |
| **C ABI** (for .NET, Go, Node, Java) | Planned | |

The Python binding is the reference front-end today. It gives a PEP 249 connection, a lazy relation
API, Arrow-native results, `ATTACH`, secrets, streams and file sinks.

**[Get started with the Python binding](/docs/2.0.0-rc2/beacondb/python/getting-started)**
