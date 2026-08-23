# beacondb

Python bindings for **beacon-db**: an embeddable, in-process analytical database for scientific data.

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
running Beacon server, use [`beacon-datalake-cli`](../../beacon-clients/beacon-datalake-cli/) or
[`@beacon/client`](../../beacon-clients/beacon-ts/).

## Install

```bash
pip install beacondb                 # core
pip install "beacondb[pandas]"       # + .df()
pip install "beacondb[polars]"       # + .pl()
pip install "beacondb[sqlalchemy]"   # + the beacondb:// dialect
pip install "beacondb[all]"          # all of the above
```

Nothing is required at runtime: results cross into Python over the Arrow PyCapsule
protocol (`__arrow_c_stream__`), so any Arrow consumer can read them with no dependency of
ours. pyarrow/pandas/polars are only needed by the methods that return their types.

### Platform support

`beacondb` embeds the whole engine, so it ships as a compiled **abi3** wheel — one per platform,
covering CPython 3.10+:

| Platform | Architectures |
| --- | --- |
| Linux (glibc, `manylinux_2_34`) | `x86_64`, `aarch64` |
| macOS | `arm64` (Apple silicon) |
| Windows | `x64` |

A **source distribution** is published alongside them, so `pip install beacondb` still works where
there is no wheel — it compiles the engine instead of downloading it, which takes a long time.

**Rust and protoc install themselves.** maturin bootstraps a Rust toolchain when `cargo` is
missing, and `protoc` — required by `prost-build` via Lance, which does not bundle it — is a
declared build dependency (`protoc-wheel-0`) that pip installs for you. Both go into pip's
isolated build environment, so nothing lands on your system. Opt out of the Rust bootstrap with
`MATURIN_NO_INSTALL_RUST=1`.

What you *do* need is a C/C++ toolchain and system HDF5/netCDF, because a source build uses the
crate's default features and links them dynamically. Those defaults also link **PROJ 9.6.2 or
later** for `ST_Transform`; without one the build compiles PROJ itself, which needs `cmake` and
the `sqlite3` program. For the static, self-contained variant instead, pass the features through
maturin's PEP 517 hook:

```bash
# needs cmake + sqlite3, not hdf5-dev/netcdf-dev/proj
MATURIN_PEP517_ARGS="--features static-netcdf,spatial-proj-bundled" pip install beacondb
pip install beacondb --no-binary beacondb   # force source on a wheel platform
```

**No Alpine / musl wheel.** There is currently no musllinux wheel, so on Alpine or any musl-based
image (`python:3.12-alpine`, `alpine`) pip falls through to the sdist and builds the whole engine
from source. Use a glibc-based image instead; `python:3.12-slim` (Debian) is the smallest drop-in,
gets the prebuilt wheel, and needs no other change:

```dockerfile
FROM python:3.12-slim      # not python:3.12-alpine
RUN pip install beacondb
```

If you must stay on Alpine, `apk add --no-cache build-base linux-headers hdf5-dev netcdf-dev cmake
sqlite` first, then `pip install beacondb`. musllinux wheels are expected to return; this is a
temporary gap.

## Auth is off by default

Opening a database locally needs no credentials and permits everything — the usual contract
for a file-backed embedded database: **possession of the file is full control.**

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
a boundary for *served* access (the `beacon-server` HTTP / Flight SQL transports), not
against local possession of the file.

## One file, one handle

The container file is held under an exclusive lock, so one process opens one `beacon.db`.
A second `connect()` to the same path in the same process returns the same underlying
database (sharing its lock); from another process it raises `OperationalError`. Use
`cursor()` for an independent result slot, and `connect_as()` for a different identity.

`connect(read_only=True)` opens a **read-only** handle: every write — DDL/DML and beacon's
side-effecting statements (`ATTACH`, `CREATE SECRET`, `INSERT`, …) — is refused, while `SELECT`
and `SHOW …` work. (The file is still opened exclusively, so this is a per-connection writability
guarantee, not multi-process concurrency yet.)

## SQL in, results out

`sql()`, `query()`, `table()` and `view()` return a **relation**: a query that has been built
but not yet run. You shape it in SQL, and nothing touches the engine until a terminal method:

```python
rel = con.sql("""
    SELECT user_id, count(*) AS n
    FROM events
    WHERE kind = 'click'
    GROUP BY user_id
    ORDER BY n DESC
    LIMIT 10
""")

rel.sql              # inspect the SQL — runs nothing
rel.explain()        # the logical + physical plan, still without running the query
rel.explain(analyze=True)  # run it and annotate each operator with rows/time/bytes
rel.df()             # now it runs
```

Relational method chaining (`.filter()`, `.aggregate()`, `.join()`, …) is not currently
exposed — write the SQL instead. Inspecting `rel.sql`, `rel.columns` or `rel.types` stays
free, so you can check a query before paying to run it.

### Streaming large results

`.arrow()`/`.df()`/`.pl()` collect the whole result into memory. For a result too big for that,
`.record_batch()` returns a **`pyarrow.RecordBatchReader`** that pulls batches from the engine on
demand — the GIL is released during each pull — so memory stays bounded:

```python
reader = con.read_parquet("huge/*.parquet").record_batch()   # nothing pulled yet
for batch in reader:                                          # one batch at a time
    process(batch)

con.sql("SELECT * FROM obs").record_batch(50_000)            # ~50k rows per batch
```

`record_batch(batch_size)` re-chunks to roughly that many rows; omit it for the engine's native
batches (zero-copy). `fetch_record_batch` and `fetch_arrow_reader` are aliases. Each call runs the
query afresh.

## Reading files, and beacon's own formats

The readers are beacon's table functions, surfaced as methods. Every one returns a lazy
relation:

```python
con.read_parquet("obs/*.parquet").df()
con.read_hdf5("data.h5").df()   # netCDF-4 is HDF5; plain HDF5 (array datasets) reads too
con.read_csv("stations.csv"); con.read_zarr(...); con.read_delta(...); con.list_datasets()

# to filter or aggregate, call the reader as a table function in SQL
con.sql("SELECT * FROM read_parquet('obs/*.parquet') WHERE depth <= 100").df()
```

They are resolved from the catalog (`beacon.system.table_functions`), so *any* table
function beacon registers is a method — `con.table_functions()` lists them, and new ones
appear with no client update. `con.read(fn, *args)` is the general form.

**Reader options.** Format options can be passed positionally or by keyword. Keyword options are
matched by name to the reader's declared parameters (from the catalog), so you set the one you
mean without counting slots, and the universal `columns=[...]` keyword projects just those columns:

```python
con.read_csv("stations.csv", delimiter=";")                 # a named format option
con.read_parquet("obs/*.parquet", columns=["depth", "temp"]) # project columns as you read
```

HDF5 is read through the same path (netCDF-4 *is* HDF5): `con.read_hdf5("data.h5")`, and
`.h5`/`.hdf5` files also work as external tables —
`CREATE EXTERNAL TABLE t STORED AS H5 LOCATION 'data.h5'` (or `STORED AS HDF5`, or a `*.h5`
glob).

## Writing files

```python
rel.to_parquet("out.parquet")
rel.to_csv("out.csv")
rel.to_arrow_ipc("out.arrow")
rel.to_netcdf("out.nc")                      # a real NetCDF-4 file
rel.to_hdf5("out.h5")                        # NetCDF-4 is HDF5 — same writer, HDF5 name
rel.to_nd_netcdf("grid.nc", ["depth"])       # multi-dimensional
rel.to_geoparquet("pts.parquet", longitude="lon", latitude="lat")
rel.to_odv("out.zip", longitude="lon", latitude="lat",       # Ocean Data View archive
           depth="pres", time="juld", key="platform")
```

`to_odv` infers the ODV layout from the schema — columns matching ODV's standard headers become
the longitude/latitude/depth/time/key columns, the rest are classified as data columns (those with
a `<col>_qf`/`<col>_qc` flag companion) or metadata. Since most data doesn't use ODV's exact header
names, map them explicitly as above (`qf_schema=` overrides the quality-flag schema, default
`SEADATANET`). A mapped column the schema lacks fails clearly at write time.

Local paths only for now; a `scheme://` destination raises `NotSupportedError`.

## Bringing Python data in

Register a pandas / pyarrow / polars frame (or any Arrow object, or a beacondb relation) as a
table queryable by name:

```python
import pandas as pd
con.register("events", pd.DataFrame({"a": [1, 2, 3]}))
con.sql("SELECT sum(a) FROM events").fetchall()
con.unregister("events")
```

By default the table is **session-only** — held in memory for the process, never written into
`beacon.db` (copying the file does not carry it, reopening does not see it). Pass
`persist=True` to write it into `beacon.db` as a managed table instead, so it survives a reopen
and travels with the file:

```python
con.register("kept", pd.DataFrame({"x": [1, 2, 3]}), persist=True)
```

`persist=True` is real DDL: it needs write privileges (a super-user — the default with auth
off) and refuses to overwrite an existing table (drop it first to replace). `register()` needs
`pyarrow` installed either way.

To add rows to an **existing** managed table, `con.append(name, frame)` (an `INSERT INTO`):

```python
con.append("obs", pd.DataFrame({"a": [4, 5]}))   # appends; errors if `obs` doesn't exist
```

## Attaching another Beacon (remote catalogs)

Point beacondb at a running **beacon-server** server and mirror its whole catalog under a local
name — every remote schema and table becomes queryable as `name.schema.table`, in the usual
`ATTACH` style:

```python
con.attach("lake", "beacon://datalake.example.org:50051",
           username="analyst", password=…, tls=True)   # or token=… , or nothing for anonymous

con.sql("SELECT platform, avg(temperature) AS t "
        "FROM lake.public.obs WHERE depth < 100 GROUP BY platform").df()

# join LOCAL data against a REMOTE table in one statement
con.sql("SELECT l.*, r.temp FROM local_tbl l JOIN lake.public.argo r ON l.id = r.id").df()

con.attached()        # ['lake']
con.detach("lake")    # True
```

The same thing works as **SQL**, so it reaches any entry point (the beacon-server server, CLI,
SQLAlchemy), not just this binding — `con.attached()` reflects either path:

```python
con.execute("ATTACH 'beacon://datalake.example.org:50051' AS lake "
            "WITH ('username' 'analyst', 'password' '…', 'tls' 'true')")
con.execute("DETACH lake")
```

## Secrets for object stores (S3, GCS, Azure)

Give beacon credentials for a cloud object store as a named, scoped `SECRET` — instead
of environment variables. A `read_parquet('s3://…')` then resolves the best-matching secret by
longest scope prefix:

```python
con.execute("CREATE SECRET my_s3 (TYPE S3, KEY_ID '…', SECRET '…', "
            "REGION 'eu-west-1', SCOPE 's3://my-bucket')")
con.read_parquet("s3://my-bucket/obs/*.parquet").df()   # uses my_s3

con.sql("SHOW SECRETS").df()      # name, type, scope, option_keys, persistent — never values
con.execute("DROP SECRET my_s3")
```

`TYPE` is `S3`/`GCS`/`AZURE`/`HTTP`; `SCOPE` defaults to the whole backend (`s3://`) so one secret
can be the default and a longer scope overrides it per bucket. The conventional `CREATE SECRET`
parameter names (`KEY_ID`, `SECRET`, `REGION`, `SESSION_TOKEN`, `ENDPOINT`, …) map to
`object_store` config keys, and
any native `object_store` key works directly.

**Session vs. persistent.** By default a secret is session-only (in memory for the process). A
`CREATE PERSISTENT SECRET` is written **into the `beacon.db` file, encrypted**, so a copied file
carries its own cloud access — the piece that makes the single-file story reach external data:

```python
con = beacondb.connect("beacon.db", secrets_key=…)   # base64 32-byte key (or $BEACON_SECRETS_KEY)
con.execute("CREATE PERSISTENT SECRET my_s3 (TYPE S3, KEY_ID '…', SECRET '…', SCOPE 's3://bucket')")
# reopen later with the same key -> my_s3 is still there
```

Persisting requires a master key (`secrets_key=` or `BEACON_SECRETS_KEY`) — beacon **refuses to
write a plaintext credential to disk** — and a file-backed database (not `:memory:`). Credential
values are encrypted with XChaCha20-Poly1305; only the name/type/scope are stored in the clear.
Like `ATTACH`, this is also plain SQL, so the server/CLI/SQLAlchemy get it too.

**Remote-Beacon credentials, too.** A `TYPE BEACON` secret stores the credentials for a remote
Beacon (`ATTACH`), so you keep them in one place — and can `PERSISTENT` them into the file — instead
of inlining them on every attach:

```python
con.execute("CREATE SECRET lake (TYPE BEACON, USERNAME 'analyst', PASSWORD '…')")   # or TOKEN '…'
con.attach("lake", "beacon://datalake:50051", secret="lake")     # or WITH ('secret' 'lake') in SQL
con.execute("DROP SECRET lake")                                   # remove it from the store
```

`secret=` is mutually exclusive with inline `token`/`username`/`password`, and only a `TYPE BEACON`
secret can be used for an attach. Any secret is removed with `DROP SECRET <name>` (which also
deletes its persisted copy).

It runs over **Arrow Flight SQL**, and the DataFusion federation optimizer pushes the largest
federatable sub-plan — filters, projections, aggregates, and joins *between* remote tables — down
to the remote, which executes it on its full engine (its own readers, managed tables, even its own
federated sources) and streams back only the reduced result. So the heavy scan stays on the
datalake; your laptop gets the answer.

`attach` contacts the remote immediately to enumerate its schemas and tables, so an unreachable or
unauthorized endpoint fails there, not on first query. The listing is a snapshot — re-attach to
pick up tables created on the remote afterward; each table's schema resolves lazily on first use.

Credentials are either a `username`/`password` pair (sent as HTTP Basic, validated against the
remote's auth store — the durable choice) or a bearer `token` (not both); omit both only if the
remote allows anonymous access. The remote enforces its own RBAC against whoever you authenticate
as — unlike local file access, this *is* a governed boundary. Every remote call (enumeration,
schema fetch, and each pushed-down scan) carries the credential. `url` accepts
`beacon://`/`grpc://`/`http(s)://` or a bare `host:port`; `tls=True` (or an `https://` url) uses TLS.

## Beacon extras

Beyond SQL, the connection exposes beacon's own surface:

```python
# beacon's structured (non-SQL) query — the payload the HTTP API and TS client use
con.json_query({
    "select": ["depth", "temperature"],
    "from": "obs",
    "filter": {"column": "depth", "gt_eq": 50, "lt_eq": 100},
    "sort_by": [{"Desc": "depth"}],
    "limit": 5,
}).df()

con.list_tables()            # user tables in the default schema
con.functions()              # a relation over beacon.system.functions
con.table_functions()        # the read_* / list_datasets names
con.metrics()                # per-query execution metrics; con.metrics(query_id=…) to narrow
con.refresh("ext_table")     # re-list an external table / rebuild a materialized view
```

`SUMMARIZE` profiles a table or query — one row per column with min/max, distinct count, avg/std
(numeric columns), non-null count, and null percentage — the first thing to run on a new dataset:

```python
con.sql("SUMMARIZE obs").df()
con.sql("SUMMARIZE SELECT * FROM read_parquet('s3://bucket/obs/*.parquet')").df()
```

Beyond that, beacon inherits DataFusion's *friendly SQL* extensions: `SELECT * EXCLUDE (col)`
/ `REPLACE (…)`, `GROUP BY ALL`, `QUALIFY`, `UNION BY NAME`, `FROM`-first (`FROM t SELECT …`),
`DESCRIBE`, `SHOW TABLES`/`SHOW COLUMNS`, list literals, and trailing commas all work.

## Building from source

`beacondb` embeds the whole engine, so building the wheel needs the native toolchain the engine
links — not just a Rust compiler:

- **protoc** (Lance generates protobuf at build time)
- **HDF5 + netCDF** headers/libraries (the netCDF reader/writer)
- **PROJ 9.6.2+** and pkg-config (the `ST_Transform` spatial function links it). Without one, the
  build compiles PROJ from source, which needs `cmake` and the `sqlite3` program
- a Rust toolchain — **1.94 or later**, enforced by `rust-version` in the workspace `Cargo.toml`

```bash
# macOS
brew install protobuf hdf5 netcdf proj pkg-config
# Debian/Ubuntu
sudo apt-get install -y protobuf-compiler libhdf5-dev libnetcdf-dev libproj-dev pkg-config

pip install maturin
maturin develop            # build + install into the current venv (debug)
maturin build --release    # produce a wheel in ./target/wheels (or --out dist)
```

The wheel is **abi3** (`cp310-abi3`), so one wheel per platform covers CPython 3.10+. It ships
`py.typed` and `_beacondb.pyi` type stubs — including the catalog-driven `read_*` readers — so
editors get completion.

**Portable wheels (`static-netcdf`, `spatial-proj-bundled`).** Distributable wheels link netCDF,
HDF5 and PROJ *statically*, compiling them from source, so the wheel carries them and needs no
system libraries — the only way to ship a portable **Windows** wheel (there's no `apt`/`brew` for
HDF5 there):

```bash
# needs protoc, cmake and sqlite3
maturin build --release --features static-netcdf,spatial-proj-bundled
```

A static PROJ embeds its CRS database (`proj.db`) inside the library, so `ST_Transform` works out
of the wheel with no `PROJ_DATA` and no data files beside it.

CI (`.github/workflows/publish-beacondb.yml`, triggered by the release tag `v*`, or by a
`beacondb-v*` tag for a beacondb-only release) builds this way for Linux (manylinux_2_34,
x86_64 + aarch64), macOS (arm64), and Windows (x64), then publishes to PyPI via trusted
publishing. The Linux wheels add `vendored-openssl` for the MySQL/PostgreSQL drivers. Local
`maturin develop` stays **dynamic** (links system libs) since it's much faster to iterate on.

Two honest caveats: the wheel is **large** (60-90 MB, by platform — it contains a full
DataFusion/Lance/netCDF engine), and there is **no minimal build** yet. `beacon-core` compiles every format
unconditionally; cargo feature gates that would let a slim wheel drop netCDF/GDAL/TIFF are still
to be added (see the plan).

## SQLAlchemy

A `beacondb://` dialect ships with the package (`pip install "beacondb[sqlalchemy]"`), so the
SQLAlchemy ecosystem — `pandas.read_sql`, reflection, notebooks, BI tools — works out of the box:

```python
from sqlalchemy import create_engine, text
engine = create_engine("beacondb:///beacon.db")     # or "beacondb://" for in-memory
# auth and options ride on the URL query:
#   beacondb:///beacon.db?auth=true&username=u&password=p&datasets=/data

import pandas as pd
pd.read_sql("SELECT platform, avg(temperature) AS t FROM obs GROUP BY platform", engine)
```

Reflection (`inspect(engine).get_table_names()`, `get_columns(...)`, `has_table(...)`) is answered
from beacon's `information_schema`. The engine is autocommit — `commit()`/`rollback()` are no-ops,
since beacon has no multi-statement transactions.

## Status

Working today: `connect()` with both auth modes; the DB-API path (`execute`/`fetchone`/
`fetchmany`/`fetchall`, `description`/`rowcount`, `cursor`); `connect_as`/`as_anonymous`/
`whoami`; context managers; the Arrow PyCapsule protocol with `.arrow()`/`.df()`/`.pl()`;
the lazy relation (`filter`/`project`/`aggregate`/`order`/`limit`/`distinct`/`join`/`union`/
`count`/`sum`/`min`/`max`/`mean`/`query`, terminals `fetch*`/`arrow`/`df`/`pl`/
`record_batch`(streaming `pyarrow.RecordBatchReader`, `batch_size=`)/`explain`(+`analyze=True`)/
`show`/`create`/`create_view`, metadata
`sql`/`columns`/`types`/`shape`/`__len__`); the catalog-driven `read_*` readers (with keyword
format options and `columns=[...]` projection); the `to_parquet`/`to_csv`/
`to_arrow_ipc`/`to_netcdf`/`to_hdf5`/`to_nd_netcdf`/`to_geoparquet`/`to_odv` sinks; `register`/`unregister` of
pandas/pyarrow/polars frames (session-only or `persist=True`); bound parameters —
`execute(sql, params)` / `executemany(sql, rows)` with `?` or `$1` placeholders, bound (never
interpolated) so they are injection-safe; the beacon extras
(`json_query`/`functions`/`table_functions`/`metrics`/`list_tables`/`refresh`); attaching a remote
Beacon as a catalog (`attach`/`detach`/`attached`, queryable as `name.schema.table` with Flight SQL
pushdown); and a SQLAlchemy `beacondb://` dialect (engine, reflection, `pandas.read_sql`).

Not yet: replacement scans (querying a bare local variable), Python UDFs, and multi-statement
transactions. See
[plans/python-interface-requirements.md](../../plans/python-interface-requirements.md).

## Licence

AGPL-3.0. The wheel holds the whole engine, which is AGPL-3.0, so the package carries the
same licence. The wheel ships the text under `beacondb-<version>.dist-info/licenses/`. The Beacon
clients that speak to a server over HTTP or Arrow Flight SQL are Apache-2.0 instead; see
[LICENSING.md](https://github.com/maris-development/beacon/blob/main/LICENSING.md).
