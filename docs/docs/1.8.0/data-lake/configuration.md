---
description: Complete reference of Beacon's BEACON_* environment variables — server, query engine, storage, S3, Arrow Flight SQL, crawler, file formats, and API docs metadata — with their defaults.
---

# Configuration

Beacon is configured entirely through **environment variables**. There is no
configuration file: every option below is read from the environment at startup.
Unset variables fall back to the defaults listed here.

::: info
All settings use `BEACON_*` names, except the S3 credential variables, which use
the standard `AWS_*` names so they interoperate with existing AWS tooling (see
[S3 object storage](#s3-object-storage)).
:::

## Server

| Variable | Default | Description |
| --- | --- | --- |
| `BEACON_HOST` | `0.0.0.0` | IP address the HTTP API listens on. |
| `BEACON_PORT` | `5001` | Port the HTTP API listens on. |
| `BEACON_WORKER_THREADS` | `8` | Number of worker threads for the async runtime. |
| `BEACON_LOG_LEVEL` | `info` | Log level: `trace`, `debug`, `info`, `warn`, `error` (case-insensitive). |
| `BEACON_BASE_PATH` | _(empty)_ | Optional URL path prefix for the HTTP API, OpenAPI document, and Swagger UI (e.g. `/beacon`). Useful behind a reverse proxy. Normalized to exactly one leading slash and no trailing slash, so `beacon`, `/beacon`, and `/beacon/` are equivalent. Only URL-safe characters are allowed (letters, digits, `-`, `_`, `.`, `~`, and `/` as a separator); any other character causes Beacon to exit at startup with a descriptive error. |
| `BEACON_WEB_UI_DIR` | `web` | Directory holding the built admin web UI. Served at `{BEACON_BASE_PATH}/admin` when the directory exists, and skipped otherwise. Resolved relative to the working directory (`/beacon/web` in the Docker image). |

## Admin

The admin credentials gate the authenticated write/management surface (DDL/DML
over HTTP and the admin endpoints).

| Variable | Default | Description |
| --- | --- | --- |
| `BEACON_ADMIN_USERNAME` | `beacon-admin` | Super-user username for management endpoints. |
| `BEACON_ADMIN_PASSWORD` | `beacon-password` | Super-user password — **change this in production**. |

## Authentication & access control

Beacon adds role-based access control on top of the super-user above: read-only
SQL-managed users and roles, table/path grants and denies, anonymous access, and
optional OIDC. See the [Access Control guide](/docs/1.8.0/security/access-control)
for the full model and SQL reference. The variables that tune it:

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

Master key used to encrypt persisted credentials at rest — currently the
`password` of external [SQL database tables](./sql-databases.md). It is required
to create a database table with a password; without it, such a `CREATE` is
rejected rather than writing plaintext.

| Variable | Default | Description |
| --- | --- | --- |
| `BEACON_SECRETS_KEY` | _(none)_ | Base64-encoded **32-byte** key (e.g. `openssl rand -base64 32`). If set, it must decode to exactly 32 bytes or Beacon exits at startup. Losing or changing it makes previously stored credentials undecryptable — recreate those tables with the new key. |

## Query engine

| Variable | Default | Description |
| --- | --- | --- |
| `BEACON_ENABLE_SQL` | `true` | Enable the raw SQL query interface. Set to `false` to disable it (the JSON query API stays available). |
| `BEACON_VM_MEMORY_SIZE` | `8192` | Working memory (MB) available to the query engine. More is better for larger datasets and memory-heavy operations such as spatial joins and `GROUP BY`. |
| `BEACON_DEFAULT_TABLE` | `default` | Table queried when a request omits the source. Only applies to the JSON query API — SQL queries must always specify a source. |
| `BEACON_ENABLE_PUSHDOWN_PROJECTION` | `true` | Push column projection down into file readers so only requested columns are decoded. |
| `BEACON_ENABLE_ND_PIPELINE` | `false` | Enable the N-dimensional pipeline optimizer for zarr/netcdf reads: sink element-wise projections below the grid broadcast so `lat * 2` and similar run on the coordinate axis instead of the full cross-product. The base nd pipeline always runs; this only enables the node-rewriting optimization. |
| `BEACON_SANITIZE_SCHEMA` | `false` | Sanitize dataset schemas (normalize column names/types) during discovery. |
| `BEACON_ST_WITHIN_POINT_CACHE_SIZE` | `10000` | Cache size for `st_within_point` geometry lookups. |
| `BEACON_BATCH_SIZE` | `64000` | Batch size, in rows, for NetCDF reads (local and MPIO). |
| `BEACON_STATS_CACHE_CAPACITY` | `10000` | Maximum number of per-file statistics entries cached for query pruning. Read once at startup. |

### SQL result-stream coalescing

Small record batches produced by a query are merged into larger ones before being
streamed to the client, which improves throughput for fine-grained results.

| Variable | Default | Description |
| --- | --- | --- |
| `BEACON_SQL_STREAM_COALESCE_ENABLED` | `true` | Enable coalescing of the SQL result stream. |
| `BEACON_SQL_STREAM_COALESCE_TARGET_ROWS` | `65536` | Target rows per coalesced batch. |
| `BEACON_SQL_STREAM_COALESCE_FLUSH_TIMEOUT_MS` | `25` | Max time (ms) to wait while accumulating rows before flushing a partial batch. |
| `BEACON_SQL_STREAM_COALESCE_MAX_ROWS` | `262144` | Hard upper bound on rows per coalesced batch. |

## Arrow Flight SQL

Beacon also exposes an [Arrow Flight SQL](https://arrow.apache.org/docs/format/FlightSql.html)
endpoint on its own port, used by clients such as JetBrains DataGrip and the
Python ADBC driver (see [Connect](../connect/jetbrains-datagrip.md)). Unlike the
HTTP API, Flight SQL uses bearer-token authentication.

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

Beacon keeps all local state under a single root directory.

| Variable | Default | Description |
| --- | --- | --- |
| `BEACON_DATA_DIR` | `./data` | Root directory for all local data. |
| `BEACON_ENABLE_FS_EVENTS` | `false` | Watch the local datasets directory so new files are picked up automatically (uses inotify on Linux). Set to `true` to enable live auto-refresh of external tables and event-driven crawler triggering. Not used with the S3 data lake. Mounted Docker volumes can interfere with filesystem events — test this in your deployment environment. |

The following sub-directories are created under `BEACON_DATA_DIR` and used by
Beacon:

| Sub-directory | Purpose |
| --- | --- |
| `datasets/` | Local datasets store (the files you query in place). |
| `tables/` | Persisted external tables, views, and managed-table definitions. |
| `tmp/` | Temporary files (e.g. materialized query output). |
| `indexes/` | Dataset path/index data. |
| `cache/` | Internal caches. |

When mounting volumes with Docker, mount the sub-directories you want to persist
(e.g. `-v ./datasets:/beacon/data/datasets`, `-v ./tables:/beacon/data/tables`).

## S3 object storage

Set `BEACON_S3_DATA_LAKE=true` to back the **datasets** store with S3-compatible
object storage instead of the local filesystem. The `tables/`, `tmp/`, `indexes/`,
and `cache/` directories remain on local disk.

| Variable | Default | Description |
| --- | --- | --- |
| `BEACON_S3_DATA_LAKE` | `false` | Use S3-compatible object storage as the datasets store. When `false`, the local filesystem is used. |
| `BEACON_S3_BUCKET` | _(none)_ | Bucket name. **Required** when `BEACON_S3_DATA_LAKE=true` — Beacon exits at startup if it is missing. Never inferred from the endpoint. |
| `BEACON_S3_ENABLE_VIRTUAL_HOSTING` | `false` | Use virtual-hosted-style addressing (bucket in the host) instead of path-style (`{endpoint}/{bucket}/{key}`). |
| `BEACON_S3_ALLOW_HTTP` | `true` | Allow plain `http://` endpoints (useful for local MinIO; disable for production). |
| `BEACON_ENABLE_S3_EVENTS` | `false` | Reserved: wire S3 change notifications into the event listener. |

### S3 credentials and endpoint (`AWS_*`)

Credentials and the endpoint are resolved through the standard AWS environment
chain (object-store's `from_env`), so the usual `AWS_*` variables apply. The
endpoint and region Beacon captures here always override the corresponding
environment values.

| Variable | Default | Description |
| --- | --- | --- |
| `AWS_ENDPOINT` | _(none)_ | S3-compatible endpoint URL, e.g. `https://s3.amazonaws.com` or `http://minio:9000`. The bucket is always taken from `BEACON_S3_BUCKET`, never parsed from this URL. |
| `AWS_REGION` | _(none)_ | S3 region. (Note: `AWS_DEFAULT_REGION` is **not** used — set `AWS_REGION`.) |
| `AWS_ACCESS_KEY_ID` | _(none)_ | Access key. Only required when the object store needs authentication. |
| `AWS_SECRET_ACCESS_KEY` | _(none)_ | Secret key. Only required when the object store needs authentication. |
| `AWS_SKIP_SIGNATURE` | _(none)_ | Set to `true` to send unsigned requests — useful for public/anonymous buckets. |

## Crawler

The [crawler](./crawlers.md) discovers files under a prefix and registers them as
external tables.

| Variable | Default | Description |
| --- | --- | --- |
| `BEACON_CRAWLER_ENABLE` | `true` | Master switch for crawler scheduling and event triggers. When `false`, crawlers can still be defined and run on demand, but no background tasks are spawned. |
| `BEACON_CRAWLER_DEFAULT_INTERVAL_SECS` | `900` | Fallback poll interval (seconds) for an event-driven crawler on a deployment where storage events are unavailable. |

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

Per-format tuning. See [Performance Tuning](./performance-tuning.md) for guidance
on when to change these.

### NetCDF

| Variable | Default | Description |
| --- | --- | --- |
| `BEACON_NETCDF_ENABLE_STATISTICS` | `true` | Compute and cache per-file statistics used for query pruning. |
| `BEACON_NETCDF_USE_READER_CACHE` | `true` | Cache opened NetCDF readers in memory. |
| `BEACON_NETCDF_READER_CACHE_SIZE` | `128` | Max NetCDF reader entries to keep cached. |

### Atlas

| Variable | Default | Description |
| --- | --- | --- |
| `BEACON_ATLAS_USE_READER_CACHE` | `true` | Cache opened Atlas store readers in memory, avoiding re-opening the same `atlas.json` across queries. |
| `BEACON_ATLAS_READER_CACHE_SIZE` | `32` | Max Atlas reader entries to keep cached. |

### Beacon Binary Format (BBF)

| Variable | Default | Description |
| --- | --- | --- |
| `BEACON_ENABLE_BBF_SPLIT_STREAMS_SLICE` | `false` | Split large batches into smaller slices for better memory use and parallelism on BBF queries. |

## API documentation metadata

These customize the top-level metadata of the generated OpenAPI document and the
Swagger / Scalar UIs, so a deployment can brand its own API docs without
recompiling. All are optional except the title and description, which have
defaults.

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
