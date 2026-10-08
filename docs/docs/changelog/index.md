# Changelog

> **Release posts:** [Upgrade from 1.8.0 to 2.0](/docs/2.0.1/upgrade) · [What's new in 1.8.0](/docs/changelog/release-1.8.0) · [What's new since 1.7.0](/docs/changelog/release-1.7.0) · [What's new in 1.6.0](/docs/changelog/release-1.6.0)

All notable changes to Beacon are documented here, newest first. Entries are
grouped into **Added** (new features), **Changed** (behaviour or internal
changes), and **Fixed** (bug fixes).

## v2.0.1 — 2026-10-08

Beacon 2.0.1 is a patch release of the 2.0 line. A 2.0.0 server upgrades in place: change the image
tag to `v2.0.1`. Two changes can break a setup. Read the GeoTIFF and the table extensions items
below first. The
[full changelog](https://github.com/maris-development/beacon/blob/main/CHANGELOG.md) lists each
change.

### Added

- **Table and column comments.** `COMMENT ON TABLE` and `COMMENT ON COLUMN` add metadata, such as
  a description, a unit or a source. The table schema shows it over HTTP and Flight SQL. See
  [COMMENT ON](/docs/2.0.1/sql/comment-on).
- **Role settings and query limits.** `ALTER ROLE ... SET` stores a setting on a role.
  `query_cpu_limit_ms` limits the CPU time of a query, and `query_output_row_limit` limits its
  output rows. See [Query limits](/docs/2.0.1/security/access-control#query-limits).
- **`SELECT *` on Atlas and BBF tables with declared columns.** See
  [Atlas](/docs/2.0.1/formats/atlas) and [BBF](/docs/2.0.1/formats/bbf).

### Changed

- **Breaking: GeoTIFF coordinates are pixel centers.** `geo.lon` and `geo.lat` of a PixelIsArea
  file move by half a pixel. See [GeoTIFF](/docs/2.0.1/formats/geotiff).
- **A read of a table checks only the table grant.** Path rules apply to the read functions only.
  See [How Beacon checks a read](/docs/2.0.1/security/access-control#how-beacon-checks-a-read).
- **`CREATE EXTERNAL TABLE` reports schema errors.** A schema that Beacon cannot read makes the
  statement fail. Before, the table got no columns.

### Removed

- **Breaking: table extensions.** `SET EXTENSION`, `DROP EXTENSION` and `SHOW EXTENSIONS` are
  removed. Use [`COMMENT ON`](/docs/2.0.1/sql/comment-on) for metadata and a view for a preset.

### Fixed

- **`IS NOT NULL` on an n-dimensional column** no longer drops datasets that hold matches.
- **Federated Postgres, MySQL, ODBC and remote Beacon tables** apply every pushed `WHERE` filter.
  Before, a query could return wrong rows with no error.
- **Remote Beacon tables** federate a bare `SELECT *`, keep `COPY` and `INSERT` local, and keep
  the alias of a subquery.
- **`read_tiff`** reads sparse, stripped and PackBits GeoTIFFs.
- **`DROP TABLE`** clears the Lance caches, so a new table with the same name works.
- **The Flight SQL handshake** accepts the `username` and `password` of ADBC clients.

## v2.0.0 — 2026-09-29

Beacon 2.0.0 is a major release. A 1.8.0 server does not upgrade in place. A 2.0.0 server does not
read the table definitions of a 1.8.0 server, so create your tables again. Read
[Upgrade from 1.8.0](/docs/2.0.1/upgrade) before you change the image tag. The
[full changelog](https://github.com/maris-development/beacon/blob/main/CHANGELOG.md) lists each
change.

### Added

- **N-dimensional execution.** NetCDF, HDF5, Zarr, Atlas, BBF and GeoTIFF read through an nd
  pipeline. A `WHERE` filter and a projection run before the broadcast. See
  [How it works](/docs/2.0.1/how-it-works).
- **A single-file database.** One `beacon.db` file holds the catalog, the managed tables and the
  users. See [Storage internals](/docs/2.0.1/internals/storage).
- **File statistics.** Beacon records the column ranges of each file. A query skips each file that
  holds no match. See [File statistics](/docs/2.0.1/internals/file-statistics).
- **New readers.** Read [Iceberg](/docs/2.0.1/formats/iceberg) tables and
  [Icechunk](/docs/2.0.1/formats/icechunk) repositories. Pure-Rust readers read
  [netCDF](/docs/2.0.1/formats/netcdf) and [HDF5](/docs/2.0.1/formats/hdf5).
- **123 spatial functions with PostGIS names.** See
  [Spatial Functions](/docs/2.0.1/sql/spatial-functions).
- **Secrets and remote catalogs.** `CREATE SECRET` holds credentials. `ATTACH` queries another
  Beacon server. See [ATTACH](/docs/2.0.1/data-sources/attach).
- **An embedded engine.** `pip install beacondb` runs the engine in your Python process.

### Changed

- **One product, one name.** "Beacon Data Lake" and "BeaconDB" are now Beacon.
- **The netCDF and HDF5 readers are pure Rust by default.** Set
  `BEACON_NETCDF_USE_RUST_READER=false` or `BEACON_HDF5_USE_RUST_READER=false` to use netCDF-C.
- **File statistics are on by default.** Set `BEACON_FILE_STATS_ENABLE=false` to stop them.
- **`BEACON_S3_DATA_LAKE` is now `BEACON_S3_DATASETS`.** The old name still works.
- **The minimum Rust version is 1.94.** A build from source also needs PROJ 9.6.2 or later.

### Removed

- **`st_within_point` and `st_geojson_as_wkt`.** Use the PostGIS functions, such as `ST_Within`.

## v1.8.0 — 2026-06-27

### Added

- **Authentication & role-based access control.** Beacon gains a built-in
  auth/RBAC layer: a single config-defined super-user (`BEACON_ADMIN_*`) that owns
  all writes and management, read-only SQL-managed users and roles
  (`CREATE USER` / `CREATE ROLE` / `GRANT` / `DENY` / `REVOKE`), and `SELECT`
  grants/denies scoped to tables (`ON TABLE`) or file globs (`ON PATH`) with
  deny-wins, default-deny evaluation. Enforcement is opt-in
  (`BEACON_AUTH_ENFORCE`), anonymous access is configurable
  (`BEACON_AUTH_ANONYMOUS_ENABLED`), and **OIDC** bearer tokens are supported
  (`BEACON_OIDC_*`) with Beacon still owning the grant model. See the
  [Access Control guide](/docs/1.8.0/security/access-control).
- **Lance-backed managed tables.** Managed tables (`CREATE TABLE`) are now backed
  by [Lance](https://lancedb.github.io/lance/): fast local CRUD, native
  updates/deletes via deletion vectors, and secondary `INDEX`es (btree, bitmap,
  full-text). Lance is the sole managed-table engine — the short-lived Apache
  Iceberg managed engine has been removed (Iceberg will return as an external,
  read-in-place source). See
  [CREATE TABLE (Managed)](/docs/1.8.0/sql/managed-tables).
- **Delta Lake tables.** Read [Delta Lake](https://delta.io/) tables in place with
  `read_delta()` or `CREATE EXTERNAL TABLE … STORED AS DELTA`, including time
  travel, and append with `INSERT INTO`. See the
  [Delta Lake chapter](/docs/1.8.0/data-lake/delta-lake).
- **PostgreSQL / MySQL external tables.** `CREATE EXTERNAL TABLE … STORED AS
  POSTGRES | MYSQL` registers a federated pointer at a table in an external
  relational database; Beacon pushes filters, projected columns, `LIMIT`, and
  aggregates down to the source. Passwords are encrypted at rest with
  `BEACON_SECRETS_KEY`. See the
  [SQL Databases chapter](/docs/1.8.0/data-lake/sql-databases).
- **Crawlers.** `CREATE CRAWLER` discovers files in the datasets store and
  registers external tables automatically — AWS Glue-style — with partition
  detection and triggers, plus admin REST endpoints to list, run, and delete
  them. See the [Crawlers chapter](/docs/1.8.0/data-lake/crawlers).
- **Admin Web UI.** A React admin console is now bundled into the Beacon server
  and Docker image and served at `/admin`: an Athena-style SQL workbench plus
  pages to manage tables, datasets, crawlers, and external tables. See the
  [Admin Web UI guide](/docs/1.8.0/connect/web-admin-ui).
- **TypeScript / JavaScript SDK** (`@beacon/client`). An isomorphic SDK for
  Node.js and the browser, with an EF Core / LINQ-style query builder and Arrow
  result decoding. See the [TypeScript SDK guide](/docs/1.8.0/connect/beacon-typescript-sdk).
- **`beacon-datalake-cli`.** A Python terminal client (interactive REPL and one-shot
  subcommands) that runs SQL, explores tables / datasets / schemas, and exports
  to CSV, Parquet, Arrow IPC, or NetCDF. See the
  [Beacon Datalake CLI guide](/docs/1.8.0/connect/beacon-cli).
- **`EXPLAIN ANALYZE` endpoint.** `POST /api/explain-analyze-query` runs a query
  and returns the physical plan annotated with per-operator runtime metrics
  (rows, bytes, time). See the [API querying chapter](/docs/1.8.0/api/querying/).
- **Per-format configuration & per-table `OPTIONS`.** NetCDF, Atlas, and BBF
  accept per-format configuration and per-table `OPTIONS (...)` clauses.
- **Broadcast-compatible `SELECT *` for n-dimensional data.** `SELECT *` over
  NetCDF and Zarr datasets auto-narrows to a broadcast-compatible default set of
  dimensions, with the selection now robust across irregular variable shapes.

### Changed

- **Runtime-owned configuration.** Configuration is threaded through
  `Runtime::new(Arc<Config>)` instead of being process-global. The storage config
  owns the data directories and optional S3 settings, with `S3Config` as the
  single source of truth for object storage.
- **Slimmer data lake.** The bespoke `TableManager` / `FileManager` / `DataLake`
  layers were replaced with a native DataFusion `SessionContext`.
- **`table-config` is now an admin-only REST endpoint** (`GET /api/admin/table-config`).
- **Legacy logical tables are read as external tables**, so tables created by
  older versions keep resolving.
- **Reduced dependencies and tuned the build** for faster compile times.
- **File-format table functions accept a single path/glob string** in addition to
  a list, and the dataset-schema endpoint now accepts `.tif` as well as `.tiff`.
- **Enriched OpenAPI / Swagger documentation** for the REST API.

### Fixed

- A batch of bugs surfaced by an expanded integration-test suite: CTAS / `INSERT`
  row truncation, Lance string-predicate `UPDATE` / `DELETE`, `ALTER TABLE ADD
  COLUMN` for `VARCHAR` / `Utf8View`, inverted-index lookups, MySQL TLS
  connections, Delta `INSERT` reopen, n-dimensional `count(*)` returning `0`,
  PostgreSQL / MySQL federated execution, NetCDF explicit-dimension subsetting,
  and `read_zarr` directory listing.
- **`EXPLAIN ANALYZE` panic** over external NetCDF / n-dimensional tables.
- **Root redirect** now always points to the Swagger UI.
- **Clear error for output formats on non-`SELECT` statements.** Requesting an
  output format for a statement that produces no result set (DDL/DML) now returns
  an explanatory error instead of failing obscurely.
- **`super_type` Arrow coercion bugs** surfaced by expanded unit-test coverage.

## v1.7.3 — 2026-06-19

### Added

- **GeoParquet read support.** `.geoparquet` files are read with their geometry
  columns decoded to native [GeoArrow](https://geoarrow.org/); query them with
  the `read_geoparquet()` table function or register a `STORED AS GEOPARQUET`
  external table. See the [GeoParquet SQL chapter](/docs/1.7.3/sql/geoparquet)
  and the [data-lake GeoParquet chapter](/docs/1.7.3/data-lake/geoparquet).
- **Federated remote tables.** `CREATE EXTERNAL TABLE … STORED AS REMOTE` points
  at a table on another Beacon instance over Arrow Flight SQL, pushing filters,
  projected columns, `LIMIT`, and whole joins/aggregates down to the remote so
  only the reduced result set crosses the network. See the
  [remote-tables SQL chapter](/docs/1.7.3/sql/remote-tables) and the
  [federation setup chapter](/docs/1.7.3/data-lake/remote-tables).

## v1.7.2 — 2026-06-16

### Changed

- **Error handling & logging overhaul.** The storage and format crates — NetCDF,
  Zarr, GeoTIFF, Atlas, object storage, configuration, the DataFusion extensions,
  and the n-dimensional array layers — now surface structured, contextual errors
  and clearer log output on read and configuration failures.

### Fixed

- **Docker image build.** Corrected the `Dockerfile` so the image builds again
  after the query-engine crate consolidation (it referenced crates that no
  longer exist).

## v1.7.1 — 2026-06-15

### Changed

- **Unified query execution.** JSON DSL and SQL queries now run through a single
  physical execution pipeline in `beacon-core`, driven by one custom physical
  planner for all statements. The standalone `beacon-query` and `beacon-planner`
  crates were removed.
- **Simplified data lake.** All table types are now described by a single
  `LogicalTableDefinition` model, replacing the previous separate file-CRUD path.

### Fixed

- **Swagger UI / Scalar redirects** now resolve correctly when Beacon is served
  under a non-empty base path (`BEACON_BASE_PATH`).

## v1.7.0 — 2026-06-10

### Added

- **Row mutations on managed tables.** Alongside `INSERT`, managed tables now
  support `DELETE ... WHERE`, `UPDATE ... SET ... WHERE`, and `CREATE TABLE AS
  SELECT`. `DELETE` and `UPDATE` are copy-on-write.
- **Schema evolution with `ALTER TABLE`** on managed tables — `ADD COLUMN`,
  `DROP COLUMN`, `RENAME COLUMN`, and `ALTER COLUMN ... TYPE` (safe widening
  promotions). Existing rows keep reading correctly: added columns read `NULL`
  and renames preserve values. See [CREATE TABLE (Managed)](/docs/1.7.3/sql/managed-tables).
- **CF `calendar` support.** CF time-unit parsing now honours the optional CF
  `calendar` attribute, so non-Gregorian calendars are interpreted correctly.
- **SeaDataNet L05 mappings.** New UDFs map SeaDataNet instrument L05 codes for
  salinity and temperature.

### Changed

- **Managed tables are now backed by [Apache Iceberg](https://iceberg.apache.org/)**
  instead of the previous Parquet-manifest format, giving them an ACID,
  schema-tracked, snapshot-based storage layer. Data and metadata live in
  Beacon's internal area of the configured storage (local or S3), alongside the
  datasets.
- **CF time parsing** for NetCDF and Zarr is centralized in one module, replacing
  the previous per-backend regex parsing.
- **Global NetCDF attributes** are now surfaced with a leading dot (e.g.
  `.Conventions`) to cleanly distinguish them from variable attributes.
- **Filesystem event watching** (`BEACON_ENABLE_FS_EVENTS`) now defaults to
  enabled, so new files in watched datasets are picked up automatically.
- **Removed periodic table auto-sync** and its `BEACON_TABLE_SYNC_INTERVAL_SECS`
  setting — managed tables are now transactional and no longer need background
  refreshes.

## v1.6.1 — 2026-06-04

### Added

- **Atlas file format.** Atlas is a directory-based array store — a single
  `atlas.json` registry describing one or more datasets — that Beacon discovers
  and queries automatically, just like Parquet or Zarr. Query a store with the
  `read_atlas()` table function or register it as an external table with `STORED
  AS ATLAS`. Atlas keeps per-dataset column statistics, so Beacon prunes whole
  datasets before reading any array data and only loads the projected arrays.
  This makes re-encoding large NetCDF or Zarr collections into a single Atlas
  collection the recommended way to speed up repeated spatial/temporal range
  queries. See [github.com/maris-development/atlas](https://github.com/maris-development/atlas).
- **Materialized views.** `CREATE MATERIALIZED VIEW` runs a query once and
  persists the result as Parquet, so repeated, aggregation-heavy queries read
  straight from the cached result instead of recomputing. The new `REFRESH`
  statement does a full recompute and atomically swaps in the new result,
  leaving the previous result intact if the refresh fails.
- **Configurable base path** via the `BEACON_BASE_PATH` environment variable. The
  HTTP API, OpenAPI document, and Swagger UI can be served under a path prefix,
  making it easier to run Beacon behind a reverse proxy or on a shared subpath.
- **LZW-compressed, stripped GeoTIFFs** can now be decompressed, broadening the
  range of GeoTIFF/COG files Beacon can read.
- **Zarr v3 support.** Zarr reading moved onto Beacon's shared n-dimensional
  array engine, adding Zarr v3 support and predicate pushdown for Zarr-backed
  datasets.

### Changed

- **Upgraded the query engine to DataFusion 53 and Arrow 58.** The Arrow version
  is now configurable at build time to ease integration with downstream tooling.

### Fixed

- **EDMO code extraction** now uses the last set of parentheses in a SeaDataNet
  originator string, so institution names that themselves contain parentheses
  are mapped to the correct EDMO code.

## v1.6.0 — 2026-05-08

### Added

- **Flight SQL.** In addition to the existing HTTP query endpoint, datasets can
  be queried over the Flight SQL protocol — a more efficient, Arrow-native
  interface that opens Beacon up to clients such as JetBrains DataGrip, DBeaver,
  and other Apache Arrow Flight and BI tools.
- **SQL tables.** Create custom tables backed by Parquet files (local or in the
  cloud), populated from other files or bare `INSERT` statements — not tied to an
  existing dataset.
- **SQL views.** Define views on top of datasets and expose them via the API,
  combining and transforming underlying datasets into purpose-built shapes.
- **GeoTIFF support.** Read and query TIFF files, including GeoTIFF and
  Cloud-Optimized GeoTIFF (COG), via the new `read_tiff()` table function —
  adding raster data alongside Beacon's tabular formats.
- **ODV ASCII support.** ODV ASCII files can be read directly as datasets and
  queried via the API, and streamed over the S3 protocol for efficient access to
  large ODV datasets in S3-compatible object storage.
- **Ragged NetCDF & Zarr datasets.** Read datasets whose variables have differing
  lengths and don't conform to a rectangular structure.
- **NetCDF chunked streaming.** Large NetCDF files are read in chunks rather than
  loaded wholesale into memory, reducing memory use and improving performance.
- **NetCDF coordinate filter pushdown.** Filters on coordinate variables are
  applied during reading, so far less data is read and processed for bounded
  spatial/temporal queries.
- **NetCDF statistics & partition pruning.** Per-column min/max statistics let
  Beacon prune whole files before reading, with new metadata table functions
  (`view_dataset_statistics`, `view_external_table_statistics`,
  `view_statistics_cache`) to inspect cached statistics.
- **Merged tables.** A table type that references and combines data from other
  tables, with dependency tracking that prevents deleting a table a merged table
  still relies on.

### Fixed

- **NetCDF scalar attributes.** Attributes stored as a single-element list were
  not read correctly; all scalar attributes are now read properly.

## v1.5.4 — 2026-01-05

### Added

- **SQL querying.** Query datasets using SQL syntax in addition to the existing
  JSON query format.
- **SQL querying for native datasets** (e.g. NetCDF, Parquet) directly, without
  first creating a collection.
- **ODV output units.** Set the `unit` field per column in the ODV output schema,
  improving the clarity of the output data.

### Fixed

- **NetCDF FillValue** is now set correctly on output, so missing values are
  properly represented.

### Docs

- Added querying examples for both SQL and JSON query formats.

## v1.2.0 — 2025-09-01

### Added

- **S3 / object storage.** All file sources are abstracted via the Object Store
  crate, supporting backends such as MinIO, AWS S3, and Cloudflare R2.
- **ODV ASCII streaming over the S3 protocol.**
- **NetCDF cloud reading** using `#mode=bytes` (http/https cloud storage
  endpoints only).
- **New table types:** Preset Tables (data collections with metadata
  descriptions) and Geo-Spatial Tables (geo-spatial collections with metadata
  descriptions).

### Changed

- Rewrote file-source reading to support S3.
- Updated dependencies for the latest Arrow, DataFusion, and GeoParquet.

## v1.0.1 — 2025-05-05

### Added

- **SQL querying** ([#6](https://github.com/maris-development/beacon/issues/6))
  and **SQL querying for native datasets** such as NetCDF and Parquet
  ([#33](https://github.com/maris-development/beacon/issues/33)).
- **ODV output units.** Set the `unit` field per column in the ODV output schema
  ([#52](https://github.com/maris-development/beacon/issues/52)).

### Fixed

- **NetCDF FillValue / missing-value flag** is now set correctly on output
  ([#45](https://github.com/maris-development/beacon/issues/45)).

### Docs

- Added querying examples for both SQL and JSON query formats.
