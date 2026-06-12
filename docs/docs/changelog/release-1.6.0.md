# What's new in Beacon 1.6.0 — Flight SQL, SQL tables & views, and more formats

*Released 2026-05-08 (covering everything since 1.5.0)*

Beacon is a high-performance data lake for ocean and climate data, with native
subsetting over Zarr, NetCDF, Parquet, Arrow IPC, CSV, GeoTIFF and BBF. The 1.6.0
release rounds out the work from the 1.5.x series into a major step forward: a
faster query protocol (**Flight SQL**), first-class **SQL tables and views**, two
new readable formats (**GeoTIFF** and **ODV ASCII**), and a substantial set of
NetCDF performance improvements built on a rebuilt n-dimensional array engine.

Here's everything that landed.

## Flight SQL

Beacon now speaks **Flight SQL** alongside the existing HTTP query endpoint.
Flight SQL is a high-throughput, Arrow-native wire protocol, so results stream
back as Arrow record batches with far less serialization overhead than JSON.

It also opens Beacon up to a much wider client ecosystem: BI tools and SQL IDEs
that support Flight SQL — such as **JetBrains DataGrip** and **DBeaver** — can
now connect directly, as can any Apache Arrow Flight client.

## SQL tables and views

1.6.0 introduces first-class, user-defined **SQL tables**, backed by Parquet
files on local disk or in the cloud:

```sql
CREATE TABLE my_observations (...);
INSERT INTO my_observations SELECT * FROM read_netcdf('argo/*.nc');
```

These are tables you define and populate yourself — from other files or from
bare `INSERT` statements — rather than tables tied directly to an existing
dataset.

Alongside them come **SQL views**, letting data managers define custom views on
top of datasets and expose them through the API:

```sql
CREATE VIEW surface_temps AS
SELECT lon, lat, time, temperature FROM observations WHERE depth < 1.0;
```

Views make it easy to combine and transform underlying datasets into purpose-built
shapes without duplicating data.

Behind the scenes, this release also added **merged tables** — a table type that
references and combines data from other tables, with dependency tracking that
prevents you from deleting a table another merged table still relies on.

## Two new readable formats

**GeoTIFF.** Beacon can now read and query TIFF files, including GeoTIFF and
Cloud-Optimized GeoTIFF (COG). A new `read_tiff()` table function reads files
straight from a glob path, bringing raster data alongside Beacon's existing
tabular support.

```sql
SELECT * FROM read_tiff('s3://my-bucket/bathymetry/*.tif');
```

**ODV ASCII.** ODV ASCII files can be read directly as datasets and queried
through the API. They can also be **streamed over the S3 protocol**, giving
efficient access to large ODV datasets held in S3-compatible object storage.

## NetCDF performance

A rebuilt **scalable n-dimensional array architecture** underpins a set of
NetCDF reader improvements aimed at large files:

- **Streamed, chunked reading.** Large NetCDF files are now read in chunks
  rather than loaded wholesale into memory, cutting memory use and improving
  performance on big datasets.
- **Filter pushdown for coordinate variables.** Filters on coordinate variables
  are applied during reading, so Beacon reads and processes far less data for a
  bounded spatial/temporal query.
- **Statistics & partition pruning.** Beacon can compute per-column min/max
  statistics for NetCDF files and use them to prune whole files before reading,
  with new metadata table functions (`view_dataset_statistics`,
  `view_external_table_statistics`, `view_statistics_cache`) to inspect what's
  cached.

## Ragged datasets

Beacon now supports reading **"ragged" NetCDF and Zarr datasets** — files whose
variables have differing lengths and don't conform to a neat rectangular
structure. This broadens the range of real-world datasets you can load and query
directly.

## Fixes & mappings

- **NetCDF scalar attribute fix.** Attributes stored as a single-element list
  were not being read correctly; all scalar attributes are now read properly.
- **Blue Cloud mappings.** Additional SeaDataNet platform and parameter mappings
  (L05/L06/L22/L33, Argo EDMO mappings, callsign updates) landed across the
  1.5.x series and are included here.

## Upgrading

1.6.0 is additive for existing workflows — datasets and queries continue to work
unchanged. The new SQL tables, views, Flight SQL endpoint, and `read_tiff()` /
ODV support are all opt-in. See the [getting started guide](/docs/1.6.0/) and the
[changelog](/docs/changelog/) for full details.

---

Full details are on the
[GitHub release page](https://github.com/maris-development/beacon/releases).
