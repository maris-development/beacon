---
description: Read GeoParquet files with read_geoparquet(). Geometry columns are decoded to native GeoArrow and can be filtered with Beacon's geospatial functions.
---

# GeoParquet

## Reading

```text
read_geoparquet(glob_paths)
```

Reads [GeoParquet](https://geoparquet.org/) files. Geometry columns described in the file's `geo` metadata are decoded to their native [GeoArrow](https://geoarrow.org/) representation; files without geometry are read like ordinary Parquet.

```sql
SELECT * FROM read_geoparquet('spatial/**/*.geoparquet') LIMIT 100
```

## Inspecting the schema

Before writing a query it is usually worth checking which columns a file actually has, and
what their types are.

`read_schema()` does not cover this format, so inspect it through the reader itself.
A `LIMIT 0` query resolves the schema without returning any rows:

```sql
SELECT * FROM read_geoparquet('spatial/**/*.geoparquet') LIMIT 0;
```

To go further than names and types, [`SUMMARIZE`](/docs/2.0.0-rc1/beacondb/sql/summarize) profiles every column in one pass, adding
min/max, distinct counts, and the share of nulls:

```sql
SUMMARIZE (SELECT * FROM read_geoparquet('spatial/**/*.geoparquet'));
```

If the files are registered as a table, `DESCRIBE` works directly:

```sql
DESCRIBE stations;
```

From Python, the Arrow schema of any relation is available without collecting rows:

```python
con.sql("SELECT * FROM read_geoparquet('spatial/**/*.geoparquet') LIMIT 0").arrow().schema
```

## Format details

[GeoParquet](https://geoparquet.org/) files (`.geoparquet`) are Parquet files that carry geospatial geometry columns and a `geo` metadata key. Beacon reads them in addition to writing them.

- Geometry columns described in the file's `geo` metadata are decoded to their native [GeoArrow](https://geoarrow.org/) representation on read (a non-geospatial Parquet file is read like ordinary Parquet, so `read_geoparquet()` is safe to point at mixed folders).
- Column projection is applied, only the columns a query selects are materialized.
- Works over local disk and S3-compatible object stores.

Query a GeoParquet file with the [`read_geoparquet()`](/docs/2.0.0-rc1/beacondb/sql/table-functions#read-geoparquet) table function:

```sql
SELECT * FROM read_geoparquet(['spatial/**/*.geoparquet']) LIMIT 100
```

Or register a stable table name with an [external table](/docs/2.0.0-rc1/beacondb/data-sources/external-tables):

```sql
CREATE EXTERNAL TABLE stations
STORED AS GEOPARQUET
LOCATION 'spatial/stations/*.geoparquet';

SELECT * FROM stations LIMIT 10;
```

### Geometry columns

Geometry columns are decoded to native GeoArrow. For point data with separated coordinates this surfaces as a `Struct` column with `x` / `y` child fields, addressed with standard struct accessors:

```sql
SELECT geometry['x'] AS lon, geometry['y'] AS lat
FROM stations
```

### Spatial filtering

Geometry pairs naturally with Beacon's [geospatial functions](/docs/2.0.0-rc1/beacondb/sql/function-reference#geospatial-functions). For example, keep only rows inside a bounding polygon with [`st_within_point`](/docs/2.0.0-rc1/beacondb/sql/function-reference#st-within-point-wkt-lon-lat):

```sql
SELECT station_id, geometry
FROM stations
WHERE st_within_point(
    'POLYGON ((-10 35, 40 35, 40 60, -10 60, -10 35))',
    geometry['x'],
    geometry['y']
)
```

:::tip
Beacon can also *write* GeoParquet: a query result with longitude/latitude columns is mapped into a geometry column on output. See [querying output formats](/docs/2.0.0-rc1/api/querying/).
:::

:::warning
Spatial bounding-box pruning (row-group skipping via the GeoParquet `bbox` covering) is not yet applied on read, queries perform a full scan with column projection. Geometry-aware predicate pushdown is planned.
:::

## As an external table

```sql
CREATE EXTERNAL TABLE stations
STORED AS GEOPARQUET
LOCATION 'spatial/stations/*.geoparquet'
```

Geometry columns are decoded to their native [GeoArrow](https://geoarrow.org/) representation on read. See [GeoParquet in Supported Formats](/docs/2.0.0-rc1/data-lake/datasets#supported-formats) for details.

See [Creating External Tables](/docs/2.0.0-rc1/beacondb/data-sources/external-tables) for the full DDL, and [Reading External Files](/docs/2.0.0-rc1/beacondb/data-sources/) for the general reading model.
