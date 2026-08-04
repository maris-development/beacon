---
description: BeaconDB is an embeddable analytical database for scientific data. It is the Beacon engine as a Python package, backed by one portable beacon.db file.
---

# BeaconDB

**BeaconDB** is the Beacon engine as an embeddable Python package. It is an in-process analytical
database for scientific data. Run `pip install beacondb`, then `import beacondb`. The whole engine
runs **in your process**. There is no server and no HTTP.

```python
import beacondb

con = beacondb.connect("beacon.db")
con.sql("SELECT 1 AS a").fetchall()                     # [(1,)]
con.sql("SELECT * FROM read_parquet('obs/*.parquet')").df()
```

## Why BeaconDB

- **In-process, no setup.** You run no server. The engine links into your Python process.
- **Reads your formats in place.** NetCDF, Zarr, Parquet, GeoParquet, CSV, HDF5, ODV, GeoTIFF, Delta
  and more. There is no import step. It uses the same
  [readers](/docs/2.0.0-rc2/beacondb/python/querying#reading-files) and the same
  [SQL](/docs/2.0.0-rc2/beacondb/sql/) as Beacon Data Lake.
- **One portable file.** One `beacon.db` holds everything that Beacon *owns*: its catalog and its
  managed data. It references everything else. Copy the file and the managed lake goes with it.
- **Arrow-native results.** Results cross into Python over the Arrow PyCapsule protocol. Any Arrow
  consumer reads them without an extra dependency. Use `.df()`, `.pl()` or `.arrow()` for pandas,
  polars or pyarrow.
- **Federates outward.** [`ATTACH`](/docs/2.0.0-rc2/beacondb/python/remote-catalogs) a remote Beacon
  Data Lake and query it. You can then join remote tables against local files.

## Install

```bash
pip install beacondb                 # core
pip install "beacondb[pandas]"       # + .df()
pip install "beacondb[polars]"       # + .pl()
pip install "beacondb[sqlalchemy]"   # + the beacondb:// dialect
pip install "beacondb[all]"          # all of the above
```

The wheel needs nothing else at run time. Results cross the Arrow PyCapsule protocol. Only the
methods that return their types need pyarrow, pandas or polars.

### Platform support

`beacondb` holds the whole engine. It therefore ships as a compiled wheel. One **abi3** wheel per
platform covers CPython 3.10 and later. Beacon publishes wheels for:

| Platform | Architectures |
| --- | --- |
| Linux (glibc, `manylinux_2_28`) | `x86_64`, `aarch64` |
| macOS | `arm64` (Apple silicon), `x86_64` (Intel) |
| Windows | `x64` |

Beacon also publishes a **source distribution**. `pip install beacondb` therefore works on a platform
without a wheel. Pip then compiles the engine instead of a download. This needs a full build
toolchain and takes a long time.

::: warning No Alpine / musl wheel
There is at this moment **no musllinux wheel**. On Alpine, or on any musl image such as
`python:3.12-alpine`, pip uses the source distribution. It then builds the whole engine from source.
Beacon provides Rust and `protoc` automatically. You still need a C toolchain and the system HDF5 and
netCDF packages (`apk add build-base linux-headers hdf5-dev netcdf-dev`). The compile takes a long
time.

Use a glibc image instead. `python:3.12-slim` (Debian) is the smallest option. It gets the prebuilt
wheel. You change nothing else:

```dockerfile
FROM python:3.12-slim      # not python:3.12-alpine
RUN pip install beacondb
```

Must you stay on Alpine? Then see
[build from source](/docs/2.0.0-rc2/beacondb/python/building#building-on-alpine-musl) for the `apk`
prerequisites. Beacon plans to publish musllinux wheels again. This gap is temporary.
:::

You can force a source build on a platform that *does* have a wheel. Use
`pip install beacondb --no-binary beacondb`. This lets you compile against your own HDF5.

## Next steps

- [Getting started](/docs/2.0.0-rc2/beacondb/python/getting-started): connect, first query, auth modes.
- [Querying](/docs/2.0.0-rc2/beacondb/python/querying): lazy relations, readers, file sinks, streams.
- [Bring data in](/docs/2.0.0-rc2/beacondb/python/data-in): `register()` and `append()`.
- [Remote catalogs](/docs/2.0.0-rc2/beacondb/python/remote-catalogs): `ATTACH` a remote Beacon.
- [Secrets](/docs/2.0.0-rc2/beacondb/python/secrets): credentials for S3, GCS, Azure and a remote Beacon.
- [SQLAlchemy](/docs/2.0.0-rc2/beacondb/python/sqlalchemy): the `beacondb://` dialect.
- [API reference](/docs/2.0.0-rc2/beacondb/python/api-reference) · [Building from source](/docs/2.0.0-rc2/beacondb/python/building)
