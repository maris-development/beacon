# What's new in Beacon since 1.7.0 — Atlas, materialized views, and Iceberg-backed tables

Beacon is a high-performance data lake for ocean and climate data, with native
subsetting over Zarr, NetCDF, Parquet, Arrow IPC, CSV, GeoTIFF and BBF. The
latest work is some of the most substantial yet: a brand-new columnar array
format (**Atlas**), **materialized views**, and a complete rebuild of managed
tables on top of **Apache Iceberg** — plus a stack of data-handling fixes and a
major query-engine upgrade.

Here's everything that landed.

## Atlas: a faster format for re-encoded collections

The biggest performance addition is support for the
[**Atlas**](https://github.com/maris-development/atlas) file format. Atlas is a
directory-based array store — a single `atlas.json` registry describing one or
more datasets — that Beacon discovers and queries automatically, just like
Parquet or Zarr.

```sql
-- Query an Atlas store directly
SELECT * FROM read_atlas('s3://my-bucket/argo-atlas/');

-- Or register it as an external table
CREATE EXTERNAL TABLE argo STORED AS ATLAS LOCATION 's3://my-bucket/argo-atlas/';
```

What makes Atlas fast is that it keeps **per-dataset column statistics**. Beacon
uses them to prune whole datasets *before* reading any array data, then loads
only the projected arrays. For workloads that repeatedly run spatial/temporal
range queries over large collections, re-encoding your NetCDF or Zarr files into
a single Atlas collection is now the recommended way to speed things up.

## Materialized views

Beacon now supports **materialized views**. `CREATE MATERIALIZED VIEW` runs a
query once and persists the result as Parquet, so repeated, aggregation-heavy
queries read straight from the cached result instead of recomputing it.

```sql
CREATE MATERIALIZED VIEW monthly_means AS
SELECT date_trunc('month', time) AS month, avg(temperature) AS mean_temp
FROM observations
GROUP BY 1;

-- Recompute when the underlying data changes
REFRESH monthly_means;
```

The new `REFRESH` statement does a full recompute and **atomically swaps in**
the new result — if the refresh fails, the previous result is left intact.
Dropping a view cleans up its backing Parquet files automatically.

## Managed tables, rebuilt on Apache Iceberg

The headline change: the managed tables you create with `CREATE TABLE` are now
backed by [Apache Iceberg](https://iceberg.apache.org/), replacing the previous
custom Parquet-manifest format. That gives every managed table:

- **ACID transactions** — writes either land completely or not at all.
- **Snapshots** — every change produces a new immutable snapshot.
- **Tracked schema** — the table knows its own schema and evolves it safely.

Data and metadata live in Beacon's internal area of whatever storage you've
configured — local disk or S3-compatible object storage — alongside your
datasets. There's no separate catalog service to run; the Iceberg warehouse is
initialized automatically at startup.

### Row-level mutations

Because managed tables are now transactional, they support full row mutations,
not just appends:

```sql
-- Append rows
INSERT INTO observations SELECT * FROM read_netcdf('argo/*.nc');

-- Delete rows matching a predicate (copy-on-write)
DELETE FROM observations WHERE qc_flag = 4;

-- Update rows in place (copy-on-write)
UPDATE observations SET unit = 'degree_C' WHERE unit = 'Celsius';

-- Create and populate a table from a query in one step
CREATE TABLE surface_temps AS
SELECT lon, lat, time, temperature FROM observations WHERE depth < 1.0;
```

`DELETE` and `UPDATE` are copy-on-write: they rewrite the affected data files and
commit a new snapshot, so readers always see a consistent view.

### Schema evolution with `ALTER TABLE`

Managed tables can now change shape without a rebuild:

```sql
ALTER TABLE observations ADD COLUMN salinity DOUBLE;
ALTER TABLE observations DROP COLUMN legacy_flag;
ALTER TABLE observations RENAME COLUMN temp TO temperature;
ALTER TABLE observations ALTER COLUMN temperature TYPE DOUBLE;  -- safe widening
```

Existing rows keep reading correctly: newly added columns read as `NULL` for
older rows, and renames preserve the underlying values. `ALTER COLUMN ... TYPE`
is limited to safe widening promotions so existing data is never corrupted. See
the [CREATE TABLE (Managed)](/docs/1.7.1/sql/managed-tables) docs for the full
reference.

## Query engine upgrade

Beacon's underlying query engine was upgraded to **DataFusion 53** and **Arrow
58**, keeping it current with the latest upstream performance and feature
work. The Arrow version is now configurable at build time, easing integration
with downstream tooling that pins a specific Arrow release.

## Better data handling

A number of correctness and format improvements also landed:

- **CF time & calendar handling.** Time-unit parsing for NetCDF and Zarr is now
  centralized in one CF-time module and understands the optional CF `calendar`
  attribute, so non-Gregorian calendars are interpreted correctly.
- **Zarr v3 support.** Zarr reading moved onto Beacon's shared n-dimensional
  array engine, adding Zarr v3 support and predicate pushdown for Zarr-backed
  datasets.
- **LZW GeoTIFFs.** Beacon can now decompress LZW-compressed, stripped GeoTIFFs,
  broadening the range of GeoTIFF/COG files it can read.
- **Global attribute naming.** Global (file-level) NetCDF attributes are now
  surfaced with a leading dot — e.g. `.Conventions` — to cleanly distinguish
  them from variable attributes.

## SeaDataNet mappings & fixes

- **New L05 mappings.** Added UDFs to map SeaDataNet instrument **L05** codes for
  salinity and temperature, plus a mapping-table consistency fix.
- **EDMO extraction fix.** EDMO code extraction now reads the *last* set of
  parentheses in a SeaDataNet originator string, so institution names that
  themselves contain parentheses map to the correct EDMO code.

## Operability

- **Configurable base path.** Via the `BEACON_BASE_PATH` environment variable,
  the HTTP API, OpenAPI document, and Swagger UI can be served under a path
  prefix — handy when running Beacon behind a reverse proxy or on a shared
  subpath.
- **Filesystem events on by default.** Filesystem event watching
  (`BEACON_ENABLE_FS_EVENTS`) now defaults to enabled, so new files in watched
  datasets are picked up automatically.
- **Simpler configuration.** The periodic table auto-sync feature and its
  `BEACON_TABLE_SYNC_INTERVAL_SECS` setting have been removed — managed tables
  are now transactional and no longer need background refreshes.

## Upgrading

- **Datasets and external tables** (NetCDF, Zarr, Parquet, Atlas, …) are
  unaffected and need no migration.
- **Managed tables** change on-disk format (Parquet-manifest → Iceberg). Tables
  created with the old format should be re-created on the new engine.
- If you relied on `BEACON_TABLE_SYNC_INTERVAL_SECS`, drop it from your
  configuration — it no longer exists.

---

Full details are in the [changelog](/docs/changelog/) and on the
[GitHub release page](https://github.com/maris-development/beacon/releases).
