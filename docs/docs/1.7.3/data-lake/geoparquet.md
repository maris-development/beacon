# GeoParquet

[GeoParquet](https://geoparquet.org/) files (`.geoparquet`) are Parquet files that carry geospatial geometry columns and a `geo` metadata key. Beacon reads them in addition to writing them. This chapter covers setting GeoParquet up as a dataset and external table; for querying geometry with SQL see the [GeoParquet SQL chapter](../sql/geoparquet.md).

## Auto-discovery

Like every other format, GeoParquet datasets are discovered from the configured storage root automatically — no registration step is required. Drop `.geoparquet` files into the datasets folder (or an S3 prefix) and they become immediately queryable with the [`read_geoparquet()`](../sql/geoparquet.md#querying-with-read_geoparquet) table function. Reading works the same over local disk and S3-compatible object stores.

- Geometry columns described in the file's `geo` metadata are decoded to their native [GeoArrow](https://geoarrow.org/) representation on read.
- A non-geospatial Parquet file (no `geo` metadata) is read like ordinary Parquet, so the format is safe to point at mixed folders.
- Column projection is applied — only the columns a query selects are materialized.

## Registering an external table

Register a stable table name over one or more files with `STORED AS GEOPARQUET`:

```sql
CREATE EXTERNAL TABLE stations
STORED AS GEOPARQUET
LOCATION 'spatial/stations/*.geoparquet';

SELECT * FROM stations LIMIT 10;
```

The `LOCATION` follows the same glob/folder semantics as other [external tables](./external-tables.md): point at a folder to glob every `.geoparquet` under it, or give an explicit glob pattern.

## Writing GeoParquet

Beacon can also *write* GeoParquet. When a query result is written in GeoParquet output, longitude/latitude columns are mapped into a geometry column automatically. Beacon detects them by name — common longitude names (`lon`, `long`, `lng`, `longitude`, `easting`, `x`) and latitude names (`lat`, `latitude`, `northing`, `y`) — or you can set the columns explicitly via `OPTIONS`. See [querying output formats](../api/querying/index.md).

:::warning
Spatial bounding-box pruning (row-group skipping via the GeoParquet `bbox` covering) is not yet applied on read — queries perform a full scan with column projection. Geometry-aware predicate pushdown is planned.
:::
