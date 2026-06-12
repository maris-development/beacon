# Changelog

> **Release posts:** [What's new since 1.7.0](/docs/changelog/release-1.7.0) · [What's new in 1.6.0](/docs/changelog/release-1.6.0)

All notable changes to Beacon are documented here, newest first. Entries are
grouped into **Added** (new features), **Changed** (behaviour or internal
changes), and **Fixed** (bug fixes).

## v1.7.0 — 2026-06-10

### Added

- **Row mutations on managed tables.** Alongside `INSERT`, managed tables now
  support `DELETE ... WHERE`, `UPDATE ... SET ... WHERE`, and `CREATE TABLE AS
  SELECT`. `DELETE` and `UPDATE` are copy-on-write.
- **Schema evolution with `ALTER TABLE`** on managed tables — `ADD COLUMN`,
  `DROP COLUMN`, `RENAME COLUMN`, and `ALTER COLUMN ... TYPE` (safe widening
  promotions). Existing rows keep reading correctly: added columns read `NULL`
  and renames preserve values. See [CREATE TABLE (Managed)](/docs/1.7.0/sql/managed-tables).
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
