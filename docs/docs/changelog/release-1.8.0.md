# What's new in Beacon 1.8.0 — Lance tables, Delta Lake, database federation, and an admin UI

Beacon is an open-source data lakehouse query engine for scientific and climate
data, with native subsetting over NetCDF, Zarr, Parquet, Arrow IPC, CSV,
GeoTIFF, GeoParquet, Atlas and BBF. 1.8.0 is a big one: managed tables get a
fast new **Lance** engine, Beacon learns to read **Delta Lake** and **federate to
PostgreSQL/MySQL**, file discovery is automated with **crawlers**, and the server
now ships a bundled **admin web UI** alongside a new **TypeScript SDK** and a
**Python CLI**.

Here's everything that landed.

## Lance-backed managed tables — a new default engine

Managed tables — the ones you create with `CREATE TABLE` and own, mutate, and
index — are now backed by [**Lance**](https://lancedb.github.io/lance/) by
default. Lance gives them fast local CRUD, native updates and deletes via
deletion vectors, and **secondary indexes**:

```sql
CREATE TABLE observations (id BIGINT, platform VARCHAR, temperature DOUBLE);

INSERT INTO observations VALUES (1, 'argo', 12.5), (2, 'glider', 9.0);
UPDATE observations SET temperature = 12.6 WHERE id = 1;
DELETE FROM observations WHERE temperature < 10;

-- Secondary indexes: btree, bitmap, and full-text
CREATE INDEX ON observations (platform);
```

The [**Apache Iceberg**](https://iceberg.apache.org/) engine introduced in 1.7 is
still here for object-store and cloud deployments, where table data and metadata
live alongside your datasets on S3. You choose per session — or set a
deployment-wide default:

```sql
SET beacon.table_engine = 'iceberg';   -- next CREATE TABLE is Iceberg-backed
SET beacon.table_engine = 'lance';     -- back to the default
```

```bash
# Or deployment-wide:
BEACON_DEFAULT_TABLE_ENGINE=iceberg
```

Lance tables live on the local filesystem (the tables directory) even when your
datasets are on S3; if you want managed tables on object storage, use Iceberg.
The full reference is in [CREATE TABLE (Managed)](/docs/1.8.0/sql/managed-tables).

## Delta Lake

Beacon can now read [**Delta Lake**](https://delta.io/) tables in place — locally
or on S3 — with the `read_delta()` table function or a `STORED AS DELTA` external
table, including **time travel** and **appends**:

```sql
-- Read the current version ad-hoc
SELECT count(*) FROM read_delta('delta/ocean_profiles');

-- Register it (optionally pinned to a historical snapshot via OPTIONS), then append
CREATE EXTERNAL TABLE events STORED AS DELTA LOCATION 'delta/events';
INSERT INTO events SELECT * FROM read_parquet('new/*.parquet');

-- Time travel: register the table as it looked at a past version or timestamp
CREATE EXTERNAL TABLE events_v12
STORED AS DELTA LOCATION 'delta/events'
OPTIONS ('version' '12');
```

Beacon reads the Delta transaction log to resolve exactly which Parquet files
make up the current (or a historical) version, and injects its own object store
so local and S3 Delta tables both work through the same path. See the
[Delta Lake chapter](/docs/1.8.0/data-lake/delta-lake).

## Query PostgreSQL and MySQL directly

External tables can now point at a table in an external **PostgreSQL** or
**MySQL** database. Once registered you `SELECT`, `JOIN`, and aggregate it like
any other table, but the data stays in the source database — Beacon pushes
filters, projected columns, `LIMIT`, and aggregates **down to the database** so
only the reduced result crosses the wire:

```sql
CREATE EXTERNAL TABLE orders
STORED AS POSTGRES
LOCATION 'public.orders'
OPTIONS (
  'host' 'db.internal',
  'port' '5432',
  'user' 'beacon_ro',
  'password' 'secret',
  'database' 'shop'
);

-- Join a relational table against your in-place scientific data
SELECT o.region, avg(m.temperature)
FROM orders o JOIN read_netcdf('argo/*.nc') m ON o.station = m.platform
GROUP BY o.region;
```

The `password` option is **encrypted at rest** with a deployment master key
(`BEACON_SECRETS_KEY`) and never returned by the API. This is built on
DataFusion's federation layer — the same pushdown mechanism behind 1.7's remote
tables. See the [SQL Databases chapter](/docs/1.8.0/data-lake/sql-databases).

## Crawlers — automatic external-table discovery

Pointing Beacon at a bucket of files no longer means writing a `CREATE EXTERNAL
TABLE` by hand. A **crawler** scans the datasets store, infers a merged schema,
detects partitions, and registers external tables for you — AWS Glue-style:

```sql
CREATE CRAWLER argo
ON 'argo/'
WITH ('format' 'nc', 'detect_partitions' 'true', 'schedule' '15m');

RUN CRAWLER argo;     -- discover files and (re)register the tables
SHOW CRAWLERS;
```

Crawlers support partition detection and triggers, and can be managed over admin
REST endpoints (and from the new web UI). See the
[Crawlers chapter](/docs/1.8.0/data-lake/crawlers).

## A bundled admin web UI

Beacon now ships an **admin web interface** built into the server and the Docker
image — no extra deployment. When present, it's served at **`/admin`**:

```
http://localhost:5001/admin
```

It's an Athena-style console: a SQL workbench with a searchable data panel, a
CodeMirror editor (run with ⌘/Ctrl + Enter), a results grid, CSV/Parquet
download, and an **Explain** plan tree — plus pages to manage tables, datasets,
crawlers, and external tables. The UI is admin-only (gated by the
`BEACON_ADMIN_USERNAME` / `BEACON_ADMIN_PASSWORD` credentials) and is built
entirely on the new TypeScript SDK. See the
[Admin Web UI guide](/docs/1.8.0/connect/web-admin-ui).

## New clients: a TypeScript SDK and a Python CLI

Two new ways to talk to Beacon from outside the browser:

- **`@beacon/client`** — an isomorphic [**TypeScript/JavaScript SDK**](/docs/1.8.0/connect/beacon-typescript-sdk)
  for Node.js and the browser. It runs SQL or the JSON DSL, decodes the
  zstd-compressed Arrow result stream into plain JS objects, and ships an EF
  Core / LINQ-style fluent query builder:

  ```ts
  const { rows } = await beacon
    .from({ netcdf: { paths: ["argo.nc"] } })
    .select("TEMP", column("PSAL", "salinity"))
    .where((x) => x.depth.gte(0).and(x.depth.lte(100)))
    .take(100)
    .execute();
  ```

- **`beacon-cli`** — a [**Python terminal client**](/docs/1.8.0/connect/beacon-cli)
  with an interactive REPL and one-shot subcommands. Run SQL, explore
  tables/datasets/schemas, render results in the terminal, and export to CSV,
  Parquet, Arrow IPC, or NetCDF without leaving the shell.

## EXPLAIN ANALYZE over the API

Performance work gets a new endpoint: `POST /api/explain-analyze-query` **runs**
a query and returns the physical plan annotated with per-operator runtime metrics
— rows, bytes, and time per node — the analog of SQL `EXPLAIN ANALYZE`. It sits
alongside the existing `POST /api/explain-query` (plan only, no execution). See
the [API querying chapter](/docs/1.8.0/api/querying/).

## Under the hood

A round of internal work makes Beacon leaner and easier to operate:

- **Runtime-owned configuration.** Config is now threaded through
  `Runtime::new(Arc<Config>)` rather than being process-global, with `S3Config`
  the single source of truth for object storage and the storage config owning all
  data directories.
- **A slimmer data lake.** The bespoke `TableManager` / `FileManager` / `DataLake`
  layers were replaced with a native DataFusion `SessionContext`.
- **Per-format configuration & per-table `OPTIONS`** for NetCDF, Atlas, and BBF.
- **Faster builds.** Dependencies were trimmed and the build tuned for compile
  time; `table-config` moved to an admin-only REST endpoint; and the REST API's
  OpenAPI/Swagger documentation was enriched.

## Fixes

1.8.0 also lands a batch of correctness fixes surfaced by a much-expanded
integration-test suite — CTAS/`INSERT` row truncation, Lance string-predicate
`UPDATE`/`DELETE`, `ALTER TABLE ADD COLUMN` for text types, MySQL TLS
connections, Delta `INSERT` reopen, n-dimensional `count(*)`, PostgreSQL/MySQL
federated execution, NetCDF explicit-dimension subsetting, `read_zarr` listing,
and an `EXPLAIN ANALYZE` panic over external NetCDF tables. The full list is in
the [changelog](/docs/changelog/).

## Upgrading

- **Datasets and external tables** (NetCDF, Zarr, Parquet, Atlas, Delta, …) are
  unaffected and need no migration.
- **Managed tables** default to the new **Lance** engine. Existing Iceberg tables
  keep working; set `BEACON_DEFAULT_TABLE_ENGINE=iceberg` if you want new tables
  to stay on Iceberg.
- **PostgreSQL/MySQL external tables** require `BEACON_SECRETS_KEY` to be set when
  a `password` is supplied — `CREATE` fails closed without it.

---

Full details are in the [changelog](/docs/changelog/) and on the
[GitHub release page](https://github.com/maris-development/beacon/releases).
