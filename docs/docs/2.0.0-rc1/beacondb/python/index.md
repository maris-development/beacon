---
description: BeaconDB is an embeddable analytical database for scientific data, the Beacon engine as a Python package, in-process, backed by a single portable beacon.db file.
---

# BeaconDB

**BeaconDB** is the Beacon engine as an embeddable Python package, an in-process analytical database
for scientific data. `pip install beacondb`, `import beacondb`, and the whole engine runs **in-process**.
There is no server and no HTTP.

```python
import beacondb

con = beacondb.connect("beacon.db")
con.sql("SELECT 1 AS a").fetchall()                     # [(1,)]
con.sql("SELECT * FROM read_parquet('obs/*.parquet')").df()
```

## Why BeaconDB

- **In-process, zero setup.** No server to run; the engine links into your Python process.
- **Reads your formats in place.** NetCDF, Zarr, Parquet/GeoParquet, CSV, HDF5, ODV, GeoTIFF, Delta,
  and more, no ingest step. Same [readers](/docs/2.0.0-rc1/beacondb/python/querying#reading-files) and
  [SQL](/docs/2.0.0-rc1/beacondb/sql/) as Beacon Data Lake.
- **One portable file.** A single `beacon.db` holds everything Beacon *owns* (its catalog and managed
  data) and references everything else. Copy the file and the managed lake travels with it.
- **Arrow-native results.** Results cross into Python over the Arrow PyCapsule protocol, so any Arrow
  consumer reads them with no dependency of ours; `.df()` / `.pl()` / `.arrow()` are there when you
  want pandas / polars / pyarrow.
- **Federates outward.** [`ATTACH`](/docs/2.0.0-rc1/beacondb/python/remote-catalogs) a remote Beacon Data Lake and
  query it, joining remote tables against local files.

## Install

```bash
pip install beacondb                 # core
pip install "beacondb[pandas]"       # + .df()
pip install "beacondb[polars]"       # + .pl()
pip install "beacondb[sqlalchemy]"   # + the beacondb:// dialect
pip install "beacondb[all]"          # all of the above
```

Nothing is required at runtime beyond the wheel: results cross the Arrow PyCapsule protocol, so
pyarrow/pandas/polars are only needed by the methods that return their types.

### Platform support

`beacondb` embeds the whole engine, so it ships as a compiled wheel — one **abi3** wheel per
platform covers CPython 3.10+. Wheels are published for:

| Platform | Architectures |
| --- | --- |
| Linux (glibc, `manylinux_2_28`) | `x86_64`, `aarch64` |
| macOS | `arm64` (Apple silicon), `x86_64` (Intel) |
| Windows | `x64` |

A **source distribution** is published alongside them, so `pip install beacondb` still works on a
platform with no wheel — it just compiles the engine instead of downloading it, which needs a full
build toolchain and takes a long time.

::: warning No Alpine / musl wheel
There is currently **no musllinux wheel**, so on Alpine or any musl-based image
(`python:3.12-alpine`, `alpine`) pip falls through to the sdist and builds the whole engine from
source. Rust and `protoc` are provisioned automatically, but you still need a C toolchain and
system HDF5/netCDF (`apk add build-base linux-headers hdf5-dev netcdf-dev`), and the compile takes
a long time.

Use a glibc-based image instead. `python:3.12-slim` (Debian) is the smallest drop-in, gets the
prebuilt wheel, and needs no other change:

```dockerfile
FROM python:3.12-slim      # not python:3.12-alpine
RUN pip install beacondb
```

If you must stay on Alpine, [build from source](/docs/2.0.0-rc1/beacondb/python/building#building-on-alpine-musl)
has the `apk` prerequisites. musllinux wheels are expected to return; this is a temporary gap.
:::

To force a source build on a platform that *does* have a wheel (to compile against your own HDF5,
say), use `pip install beacondb --no-binary beacondb`.

## Next steps

- [Getting started](/docs/2.0.0-rc1/beacondb/python/getting-started), connect, first query, auth modes.
- [Querying](/docs/2.0.0-rc1/beacondb/python/querying), lazy relations, readers, file sinks, streaming.
- [Bringing data in](/docs/2.0.0-rc1/beacondb/python/data-in)-`register()` and `append()`.
- [Remote catalogs](/docs/2.0.0-rc1/beacondb/python/remote-catalogs)-`ATTACH` a remote Beacon.
- [Secrets](/docs/2.0.0-rc1/beacondb/python/secrets), S3/GCS/Azure and remote-Beacon credentials.
- [SQLAlchemy](/docs/2.0.0-rc1/beacondb/python/sqlalchemy), the `beacondb://` dialect.
- [API reference](/docs/2.0.0-rc1/beacondb/python/api-reference) · [Building from source](/docs/2.0.0-rc1/beacondb/python/building)
