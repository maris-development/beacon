---
description: Full reference of the BEACON_* environment variables. It covers the server, engine, storage, S3, Flight SQL, crawlers and formats, with their defaults, and the SQL statements that change a setting without a restart.
---

# Configuration

You configure Beacon with **environment variables**. There is no configuration
file. Beacon reads every option below from the environment at startup. An unset
variable takes the default from this page.

Many engine and format settings also change **at runtime**, through SQL, with no
restart. See [Change a setting at runtime](#change-a-setting-at-runtime). The
table for each section marks which ones.

::: info
Every setting uses a `BEACON_*` name. The S3 credential variables are the
exception. They use the standard `AWS_*` names, so they work with your AWS tools.
See [S3 object storage](#s3-object-storage).
:::

## Change a setting at runtime

`SET` changes a setting on the running server:

```sql
SET beacon.netcdf.use_rust_reader = true;
SET beacon.sql.stream_coalesce.target_rows = 131072;
SET beacon.default_table = 'observations';
```

A setting name is its `BEACON_*` variable, lowercased, under the `beacon.`
namespace: `BEACON_NETCDF_USE_RUST_READER` becomes `beacon.netcdf.use_rust_reader`.

`SHOW SETTINGS` lists every setting you can change, with its value, the value the
server started with, and what it does. Any authenticated user can read it.

```sql
SHOW SETTINGS;
SHOW beacon.default_table;
SELECT * FROM beacon.system.settings WHERE name LIKE 'beacon.netcdf.%';
```

### Scope

A `SET` applies to the **whole server**, not to one client. Beacon runs one query
engine for every connection, so the next query from any user sees the new value.
Only the super-user may change a setting. This is the scope `SET` has always had
for the engine options below.

### Persistence

`SET` lasts until the server stops. `ALTER SYSTEM SET` also writes the value into
the database file, and the server applies it again at the next start:

```sql
ALTER SYSTEM SET beacon.netcdf.use_rust_reader = 'true';
```

`RESET` puts a setting back to the value the server started with. `ALTER SYSTEM
RESET` also removes the stored value:

```sql
RESET beacon.netcdf.use_rust_reader;
ALTER SYSTEM RESET beacon.netcdf.use_rust_reader;
```

Order of precedence, strongest first: `ALTER SYSTEM SET` > environment variable >
default. `ALTER SYSTEM` needs a file-backed database. An in-memory database
refuses it.

### Engine options

The `beacon.` namespace also covers the query engine's own options. Both spellings
work:

```sql
SET beacon.execution.batch_size = 8192;
SET datafusion.execution.batch_size = 8192;   -- the same option
```

`SELECT * FROM information_schema.df_settings` lists both namespaces.

### Settings you cannot change at runtime

A setting that Beacon uses once, at startup, stays an environment variable. This
covers the port, the host, the worker threads, the data directory, the S3 store,
the credentials, the Flight SQL server, CORS, the crawler, the file statistics,
and each reader cache **size**. A `SET` on one of these fails and names the
variable to edit:

```
`beacon.port` can only be set when the server starts: set the `BEACON_PORT`
environment variable and restart
```

### What a change reaches

A `SET` on a format setting applies to the next `read_netcdf(...)`,
`read_zarr(...)` or similar call. A **registered** external table keeps the
settings it was created with, because Beacon builds its reader once. Run
`REFRESH <table>` to rebuild it with the current settings.

A per-table `CREATE EXTERNAL TABLE ... OPTIONS (...)` value always wins over a
`SET`, which in turn wins over the environment variable.

## Server

| Variable | Default | Description |
| --- | --- | --- |
| `BEACON_HOST` | `0.0.0.0` | IP address the HTTP API listens on. |
| `BEACON_PORT` | `5001` | Port the HTTP API listens on. |
| `BEACON_WORKER_THREADS` | `8` | Number of worker threads for the async runtime. |
| `BEACON_LOG_LEVEL` | `info` | Log level: `trace`, `debug`, `info`, `warn`, `error`, or `off`. Case does not matter. The level applies to all Beacon crates. At `debug` and `trace`, loud dependencies such as DataFusion, Arrow, `object_store`, and hyper stay at `info`. An unknown value stops the server at startup. |
| `RUST_LOG` | _(unset)_ | Full log filter, in [`tracing-subscriber` EnvFilter](https://docs.rs/tracing-subscriber/latest/tracing_subscriber/filter/struct.EnvFilter.html) syntax (e.g. `debug,datafusion=trace`). It replaces `BEACON_LOG_LEVEL`. Use it to see the dependency logs that `BEACON_LOG_LEVEL` holds back. An invalid value prints a warning, and Beacon uses `BEACON_LOG_LEVEL`. |
| `BEACON_BASE_PATH` | _(empty)_ | Optional URL path prefix for the HTTP API, OpenAPI document, and Swagger UI (e.g. `/beacon`). Useful behind a reverse proxy. Normalized to exactly one leading slash and no trailing slash, so `beacon`, `/beacon`, and `/beacon/` are equivalent. Only URL-safe characters are allowed (letters, digits, `-`, `_`, `.`, `~`, and `/` as a separator); any other character causes Beacon to exit at startup with a descriptive error. |
| `BEACON_WEB_UI_DIR` | `web` | Directory holding the built admin web UI. Served at `{BEACON_BASE_PATH}/admin` when the directory exists, and skipped otherwise. Resolved relative to the working directory (`/beacon/web` in the Docker image). |

## Admin

The admin credentials protect the write operations. This covers DDL and DML over
HTTP, and the admin endpoints.

| Variable | Default | Description |
| --- | --- | --- |
| `BEACON_ADMIN_USERNAME` | `beacon-admin` | Super-user username for management endpoints. |
| `BEACON_ADMIN_PASSWORD` | `beacon-password` | Super-user password, **change this in production**. |

## Authentication & access control

Beacon adds role-based access control on top of the super-user above. It gives
read-only users and roles in SQL. It gives grants and denies on a table or a path.
It also gives anonymous access and optional OIDC. The
[Access Control guide](/docs/2.0.0-rc2/security/access-control) holds the full
model and the SQL reference. These variables control it:

| Variable | Default | Description |
| --- | --- | --- |
| `BEACON_AUTH_ENFORCE` | `false` | Enforce read authorization (default-deny). When `false`, authorization is a no-op. |
| `BEACON_AUTH_ANONYMOUS_ENABLED` | `true` | Allow unauthenticated requests as the built-in `anonymous` user. |
| `BEACON_OIDC_ENABLED` | `false` | Accept OIDC bearer tokens in addition to local passwords. |
| `BEACON_OIDC_ISSUER` | _(none)_ | Expected token issuer. |
| `BEACON_OIDC_JWKS_URL` | _(none)_ | JWKS endpoint used to validate token signatures. |
| `BEACON_OIDC_AUDIENCE` | _(none)_ | Expected audience; validated only when set. |
| `BEACON_OIDC_ROLES_CLAIM` | `realm_access.roles` | Token claim (dot-path) holding role names. |
| `BEACON_OIDC_USERNAME_CLAIM` | `preferred_username` | Token claim holding the username. |
| `BEACON_OIDC_JWKS_CACHE_TTL_SECS` | `300` | How long to cache the issuer's JWKS. |

## Secrets

The master key encrypts the stored credentials at rest. Today it covers the
`password` of an external
[SQL database table](/docs/2.0.0-rc2/data-sources/sql-databases). You need
the key to create a database table with a password. Without the key, Beacon
rejects that `CREATE`. Beacon never writes plaintext.

| Variable | Default | Description |
| --- | --- | --- |
| `BEACON_SECRETS_KEY` | _(none)_ | Base64-encoded **32-byte** key (e.g. `openssl rand -base64 32`). If set, it must decode to exactly 32 bytes or Beacon exits at startup. Losing or changing it makes previously stored credentials undecryptable, recreate those tables with the new key. |

## Query engine

| Variable | Default | Description |
| --- | --- | --- |
| `BEACON_ENABLE_SQL` | `true` | Enable the raw SQL query interface. Set to `false` to disable it (the JSON query API stays available). |
| `BEACON_VM_MEMORY_SIZE` | `8192` | Working memory (MB) available to the query engine. More is better for larger datasets and memory-heavy operations such as spatial joins and `GROUP BY`. |
| `BEACON_DEFAULT_TABLE` | `default` | Table queried when a request omits the source. Only applies to the JSON query API, SQL queries must always specify a source. Settable at runtime as `beacon.default_table`. |
| `BEACON_ENABLE_PUSHDOWN_PROJECTION` | `true` | Push column projection down into file readers so only requested columns are decoded. Settable at runtime as `beacon.enable_pushdown_projection`. |
| `BEACON_ENABLE_ND_PIPELINE` | `false` | Enable the N-dimensional pipeline optimizer for zarr/netcdf reads: sink element-wise projections below the grid broadcast so `lat * 2` and similar run on the coordinate axis instead of the full cross-product. The base nd pipeline always runs; this only enables the node-rewriting optimization. Settable at runtime as `beacon.enable_nd_pipeline`. |
| `BEACON_BATCH_SIZE` | `64000` | Batch size, in rows, for NetCDF reads (local and MPIO). Settable at runtime as `beacon.batch_size`. |
| `BEACON_STATS_CACHE_CAPACITY` | `10000` | Maximum number of per-file statistics entries cached for query pruning. Read once at startup. |

### SQL result-stream coalescing

A query can produce small record batches. Beacon merges them into larger batches
before it streams them to the client. This gives more throughput on a result with
many small batches.

| Variable | Default | Description |
| --- | --- | --- |
| `BEACON_SQL_STREAM_COALESCE_ENABLED` | `true` | Enable coalescing of the SQL result stream. Settable at runtime as `beacon.sql.stream_coalesce.enabled`. |
| `BEACON_SQL_STREAM_COALESCE_TARGET_ROWS` | `65536` | Target rows per coalesced batch. Settable at runtime as `beacon.sql.stream_coalesce.target_rows`. |
| `BEACON_SQL_STREAM_COALESCE_FLUSH_TIMEOUT_MS` | `25` | Max time (ms) to wait while accumulating rows before flushing a partial batch. Settable at runtime as `beacon.sql.stream_coalesce.flush_timeout_ms`. |
| `BEACON_SQL_STREAM_COALESCE_MAX_ROWS` | `262144` | Hard upper bound on rows per coalesced batch. Settable at runtime as `beacon.sql.stream_coalesce.max_rows`. |

## Arrow Flight SQL

Beacon also gives an [Arrow Flight SQL](https://arrow.apache.org/docs/format/FlightSql.html)
endpoint on its own port. Clients such as JetBrains DataGrip and the Python ADBC
driver use it. See [Connect](/docs/2.0.0-rc2/connect/datagrip). Flight
SQL authenticates with a bearer token. The HTTP API works differently.

| Variable | Default | Description |
| --- | --- | --- |
| `BEACON_FLIGHT_SQL_ENABLE` | `true` | Enable the Arrow Flight SQL server. |
| `BEACON_FLIGHT_SQL_HOST` | `0.0.0.0` | Address the Flight SQL server binds to. |
| `BEACON_FLIGHT_SQL_PORT` | `32011` | Port the Flight SQL server listens on. |
| `BEACON_FLIGHT_SQL_ALLOW_ANONYMOUS` | `false` | Allow unauthenticated Flight SQL sessions. |
| `BEACON_FLIGHT_SQL_TOKEN_TTL_SECS` | `3600` | Lifetime (seconds) of an issued session token. |
| `BEACON_FLIGHT_SQL_STATEMENT_TTL_SECS` | `300` | Lifetime (seconds) of a server-side statement handle. |
| `BEACON_FLIGHT_SQL_PREPARED_STATEMENT_TTL_SECS` | `900` | Lifetime (seconds) of a prepared-statement handle. |

## Storage and data directories

Beacon keeps all local state under one root directory.

| Variable | Default | Description |
| --- | --- | --- |
| `BEACON_DATA_DIR` | `./data` | Root directory for all local data. |

Beacon creates and uses these paths under `BEACON_DATA_DIR`:

| Path | Purpose |
| --- | --- |
| `datasets/` | Local datasets store (the files you query in place). |
| `tables/beacon.db` | The single-file tables store: catalog, managed table data, and the auth directory. |
| `tmp/` | Temporary files (e.g. materialized query output). |

With Docker, mount the subdirectories that you want to keep. Two examples are
`-v ./datasets:/beacon/data/datasets` and `-v ./tables:/beacon/data/tables`.

## S3 object storage

Set `BEACON_S3_DATA_LAKE=true` to put the **datasets** store on an S3-compatible
bucket. Beacon then does not use the local `datasets/` directory. Beacon finds and
queries every file in the bucket. This works like a local datasets directory.
`tables/beacon.db` and `tmp/` stay on local disk. `BEACON_DATA_DIR` therefore
still applies.

| Variable | Default | Description |
| --- | --- | --- |
| `BEACON_S3_DATA_LAKE` | `false` | Use an S3-compatible bucket as the datasets store. When `false`, the local filesystem is used. |
| `BEACON_S3_BUCKET` | _(none)_ | Bucket name. **Required** when `BEACON_S3_DATA_LAKE=true`; Beacon exits at startup if it is missing. Never inferred from the endpoint. |
| `BEACON_S3_ENABLE_VIRTUAL_HOSTING` | `false` | Use virtual-hosted-style addressing (bucket in the host) instead of path-style (`{endpoint}/{bucket}/{key}`). |
| `BEACON_S3_ALLOW_HTTP` | `true` | Allow plain `http://` endpoints (useful for local MinIO; disable for production). |

### S3 credentials and endpoint (`AWS_*`)

Beacon opens the bucket with `AmazonS3Builder::from_env()` from object-store. The
credentials, the endpoint and the region therefore come from the standard AWS
environment chain.

These cover the **datasets store itself**, which is what every query reads through. Paths in SQL
stay relative to that store's root, so a client never names the bucket and never supplies a
credential. See [Object Storage](/docs/2.0.0-rc2/data-sources/object-storage).

| Variable | Default | Description |
| --- | --- | --- |
| `AWS_ENDPOINT` | _(none)_ | S3-compatible endpoint URL, e.g. `https://s3.amazonaws.com` or `http://minio:9000`. The bucket is always taken from `BEACON_S3_BUCKET`, never parsed from this URL. |
| `AWS_REGION` | _(none)_ | S3 region. (Note: `AWS_DEFAULT_REGION` is **not** used, set `AWS_REGION`.) |
| `AWS_ACCESS_KEY_ID` | _(none)_ | Access key. Only required when the object store needs authentication. |
| `AWS_SECRET_ACCESS_KEY` | _(none)_ | Secret key. Only required when the object store needs authentication. |
| `AWS_SKIP_SIGNATURE` | _(none)_ | Set to `true` to send unsigned requests, useful for public/anonymous buckets. |

## Crawler

A [crawler](/docs/2.0.0-rc2/server/crawlers) finds the files under a prefix. It
then registers them as external tables.

| Variable | Default | Description |
| --- | --- | --- |
| `BEACON_CRAWLER_ENABLE` | `true` | Master switch for crawler scheduling and event triggers. When `false`, crawlers can still be defined and run on demand, but no background tasks are spawned. |
| `BEACON_CRAWLER_DEFAULT_INTERVAL_SECS` | `900` | Fallback poll interval (seconds) for an event-driven crawler on a deployment where storage events are unavailable. |

## File statistics

Beacon records the value range of each column in each file. A query then prunes the files that
cannot match. See [File statistics](/docs/2.0.0-rc2/internals/file-statistics).

Beacon does not enable this feature by default. For netCDF, also set
`BEACON_NETCDF_USE_RUST_READER=true`; for HDF5, `BEACON_HDF5_USE_RUST_READER=true` (see
[File formats](#file-formats)). Without that second variable, Beacon reads each such file and
records no ranges.

| Variable | Default | Description |
| --- | --- | --- |
| `BEACON_FILE_STATS_ENABLE` | `false` | Master switch. When `false`, Beacon finds nothing, reads nothing and starts no background task. |
| `BEACON_FILE_STATS_INTERVAL_SECS` | `900` | The seconds between two passes. The first pass runs one interval after startup, not at startup. A restart starts the interval again, so a server that restarts more often than this never runs a pass. Set `BEACON_FILE_STATS_ON_STARTUP=true` there. |
| `BEACON_FILE_STATS_ON_STARTUP` | `false` | Collect at each boot, and do not wait for the first tick. Beacon finds the files and reads every one that has no statistics, in the background. The server answers queries while this runs. The timer continues after it. |
| `BEACON_FILE_STATS_CONCURRENCY` | one quarter of the cores, minimum 2 | The files that Beacon reads at the same time. A pass uses part of the machine, so it does not compete with queries. Increase this value above your core count for data in object storage. |
| `BEACON_FILE_STATS_BATCH_FILES` | `10000` | The files that Beacon reads in one pass. This value limits the memory of one pass. |
| `BEACON_FILE_STATS_TARGET_GROUP_FILES` | `10000` | The files that one segment covers. A small value prunes more for a rare column. It also adds segments to read for a common column. |
| `BEACON_FILE_STATS_MIN_GROUP_FILES` | `500` | Beacon does not split a group below this size, even across folders. |
| `BEACON_FILE_STATS_PREFIX_DEPTH` | *(derived)* | The folder depth for a group. Leave this variable unset. Beacon derives the depth from your paths and handles roots of different shapes. |
| `BEACON_FILE_STATS_SCAN_PREFIX` | *(all files)* | Beacon finds files under this prefix of the datasets store only. |
| `BEACON_FILE_STATS_DISCOVERY_CHUNK` | `10000` | The files that Beacon registers in one transaction. Beacon does not hold a large listing in memory. |

## CORS

| Variable | Default | Description |
| --- | --- | --- |
| `BEACON_CORS_ALLOWED_METHODS` | `GET,POST,PUT,DELETE,OPTIONS` | Allowed HTTP methods. |
| `BEACON_CORS_ALLOWED_ORIGINS` | `*` | Allowed origins. |
| `BEACON_CORS_ALLOWED_HEADERS` | `Content-Type,Authorization` | Allowed request headers. |
| `BEACON_CORS_EXPOSE_HEADERS` | `x-beacon-query-id` | Response headers exposed to browser JS on cross-origin requests. The default lets a cross-origin UI (e.g. the Vite dev server) read the `x-beacon-query-id` the SDK surfaces. |
| `BEACON_CORS_ALLOWED_CREDENTIALS` | `false` | Allow credentials. |
| `BEACON_CORS_MAX_AGE` | `3600` | Preflight cache duration (seconds). |

## File formats

These settings tune one format each. See
[Performance Tuning](/docs/2.0.0-rc2/server/performance-tuning) to know when to
change them.

### NetCDF

| Variable | Default | Description |
| --- | --- | --- |
| `BEACON_NETCDF_ENABLE_STATISTICS` | `true` | Compute and cache per-file statistics used for query pruning. Settable at runtime as `beacon.netcdf.enable_statistics`. |
| `BEACON_NETCDF_USE_READER_CACHE` | `true` | Cache opened NetCDF readers in memory. Settable at runtime as `beacon.netcdf.use_reader_cache`. |
| `BEACON_NETCDF_READER_CACHE_SIZE` | `128` | Max NetCDF reader entries to keep cached. |
| `BEACON_NETCDF_USE_RUST_READER` | `false` | Read NetCDF with the pure-Rust reader instead of the netCDF-C library. Settable at runtime as `beacon.netcdf.use_rust_reader`. |

### HDF5

A NetCDF-4 file is an HDF5 file, and the netCDF-C library opens a plain HDF5 file too. Beacon
therefore reads `.h5` and `.hdf5` through the netCDF-C library by default. HDF5 carries its own
reader flag, so you can move one format at a time.

| Variable | Default | Description |
| --- | --- | --- |
| `BEACON_HDF5_USE_RUST_READER` | `false` | Read HDF5 with the pure-Rust reader instead of the netCDF-C library. Settable at runtime as `beacon.hdf5.use_rust_reader`. |
| `BEACON_HDF5_ENABLE_STATISTICS` | `true` | Compute per-file statistics used for query pruning. Needs the pure-Rust reader. Settable at runtime as `beacon.hdf5.enable_statistics`. |
| `BEACON_HDF5_USE_READER_CACHE` | `true` | Cache opened HDF5 readers in memory. Settable at runtime as `beacon.hdf5.use_reader_cache`. |
| `BEACON_HDF5_READER_CACHE_SIZE` | `128` | Max HDF5 reader entries to keep cached. |

The pure-Rust reader also reads two layouts the netCDF data model cannot express: a nested group,
and a compound dataset. See
[Performance Tuning](/docs/2.0.0-rc2/server/performance-tuning#hdf5-pure-rust-reader).

### Zarr

| Variable | Default | Description |
| --- | --- | --- |
| `BEACON_ZARR_ENABLE_STATISTICS` | `true` | Compute per-file statistics used for query pruning. Settable at runtime as `beacon.zarr.enable_statistics`. |

A store answers from its `actual_range` metadata where it can. Where it cannot, it reads only its
rank-0 and rank-1 arrays — the coordinates a `WHERE` clause names. A data grid of rank 2 or higher
is never read, so a scan costs what it always did. Turn statistics off for a collection of many
small stores, where even a rank-1 read per store adds up.

`valid_min` and `valid_max` are never used as a range. They state which values are *valid*, not
which values a store holds, so a store may hold values outside them.

### Atlas

| Variable | Default | Description |
| --- | --- | --- |
| `BEACON_ATLAS_USE_READER_CACHE` | `true` | Cache opened Atlas store readers in memory, avoiding re-opening the same `atlas.json` across queries. Settable at runtime as `beacon.atlas.use_reader_cache`. |
| `BEACON_ATLAS_READER_CACHE_SIZE` | `32` | Max Atlas reader entries to keep cached. |
| `BEACON_ATLAS_USE_PRUNING` | `true` | Drop the datasets a predicate cannot match, from the collection's statistics, before reading them. A pure optimization: off trades throughput for skipping the pruning-index build. Settable at runtime as `beacon.atlas.use_pruning`. |

### Beacon Binary Format (BBF)

| Variable | Default | Description |
| --- | --- | --- |
| `BEACON_ENABLE_BBF_SPLIT_STREAMS_SLICE` | `false` | Split large batches into smaller slices for better memory use and parallelism on BBF queries. Settable at runtime as `beacon.bbf.split_streams_slice`. |

### Managed Lance tables

These tune the [managed table](/docs/2.0.0-rc2/server/index) engine. An empty
value leaves the choice to Lance.

The first four apply when Beacon **writes** a table. They shape the files a
`CREATE TABLE` or `INSERT` produces, and never the files already on disk. Change
one and rewrite the table to apply it. `BEACON_LANCE_MATERIALIZATION` is a read
setting and applies to the next query.

| Variable | Default | Description |
| --- | --- | --- |
| `BEACON_LANCE_COMPRESSION` | _(none)_ | Block compression for string and binary columns: `fsst`, `zstd`, `lz4`, or `none`. Numeric columns are never compressed by this setting. Settable at runtime as `beacon.lance.compression`. |
| `BEACON_LANCE_NUMERIC_COMPRESSION` | _(Lance default)_ | Block compression for numeric columns: `zstd`, `lz4`, or `none`. `none` also turns off bitpacking and RLE, which usually measures **larger**. Settable at runtime as `beacon.lance.numeric_compression`. |
| `BEACON_LANCE_VERSION` | `2.2` | Lance file format version for new data: `2.0`, `2.1` or `2.2`. An append keeps the version of the table it appends to. Settable at runtime as `beacon.lance.version`. |
| `BEACON_LANCE_MINICHUNK` | _(Lance default)_ | Minichunk size, in bytes, for fixed-width columns. A size of 32KB and up needs version `2.2`. Settable at runtime as `beacon.lance.minichunk`. |
| `BEACON_LANCE_MATERIALIZATION` | _(width rule)_ | Column materialization on a filtered scan: `late` or `early`. Unset keeps Beacon's rule, which reads late above 16 projected columns. Settable at runtime as `beacon.lance.materialization`. |

## API documentation metadata

These settings change the metadata of the OpenAPI document and of the Swagger and
Scalar UIs. Your deployment can therefore brand its own API docs. You recompile
nothing. Every setting is optional. The title and the description have defaults.

| Variable | Default | Description |
| --- | --- | --- |
| `BEACON_API_TITLE` | `Beacon Rest API` | API document title. |
| `BEACON_API_DESCRIPTION` | _(built-in summary)_ | API document description. |
| `BEACON_API_TERMS_OF_SERVICE` | _(none)_ | Terms-of-service URL. |
| `BEACON_API_CONTACT_NAME` | _(none)_ | Contact name. |
| `BEACON_API_CONTACT_URL` | _(none)_ | Contact URL. |
| `BEACON_API_CONTACT_EMAIL` | _(none)_ | Contact email. |
| `BEACON_API_LICENSE_NAME` | _(none)_ | License name. |
| `BEACON_API_LICENSE_URL` | _(none)_ | License URL. |
| `BEACON_API_LICENSE_IDENTIFIER` | _(none)_ | SPDX license identifier. |

## Miscellaneous

| Variable | Default | Description |
| --- | --- | --- |
| `BEACON_ENABLE_SYS_INFO` | `false` | Expose host system information (CPU, memory) via the API. |
