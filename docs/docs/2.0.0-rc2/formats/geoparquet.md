---
description: Read GeoParquet files with read_geoparquet(). Beacon decodes geometry columns to native GeoArrow. Filter them with the geospatial functions.
---

# GeoParquet

## Read the files

```text
read_geoparquet(glob_paths)
```

Beacon reads [GeoParquet](https://geoparquet.org/) files. The `geo` metadata of a file describes its
geometry columns. Beacon decodes those columns to native [GeoArrow](https://geoarrow.org/). Beacon
reads a file without geometry as ordinary Parquet.

```sql
SELECT * FROM read_geoparquet('spatial/**/*.geoparquet') LIMIT 100
```

## Inspect the schema

Check the columns and the types before you write a query:

```sql
SELECT * FROM read_geoparquet('spatial/**/*.geoparquet') LIMIT 0;
```

[Inspect a schema](/docs/2.0.0-rc2/formats/inspect-a-schema) compares the `_schema` functions,
`SUMMARIZE`, `DESCRIBE` and `LIMIT 0`, and says what each one costs.

## Format details

A [GeoParquet](https://geoparquet.org/) file (`.geoparquet`) is a Parquet file with geometry columns
and a `geo` metadata key. Beacon reads and writes this format.

- Beacon decodes the geometry columns from the `geo` metadata to native
  [GeoArrow](https://geoarrow.org/). Beacon reads a plain Parquet file as ordinary Parquet. You can
  therefore point `read_geoparquet()` at a mixed folder.
- Beacon applies column projection. It materializes only the columns that a query selects.
- The reader works on local disk and on S3-compatible object stores.

Query a GeoParquet file with the
[`read_geoparquet()`](/docs/2.0.0-rc2/sql/table-functions#read-geoparquet) table function:

```sql
SELECT * FROM read_geoparquet(['spatial/**/*.geoparquet']) LIMIT 100
```

You can also register a stable table name with an
[external table](/docs/2.0.0-rc2/data-sources/external-tables):

```sql
CREATE EXTERNAL TABLE stations
STORED AS GEOPARQUET
LOCATION 'spatial/stations/*.geoparquet';

SELECT * FROM stations LIMIT 10;
```

### Geometry columns

Beacon decodes a geometry column to native GeoArrow. Point data with separate coordinates becomes a
`Struct` column with `x` and `y` child fields. Use the standard struct accessors:

```sql
SELECT geometry['x'] AS lon, geometry['y'] AS lat
FROM stations
```

### Spatial filters

Use the [spatial functions](/docs/2.0.0-rc2/sql/spatial-functions). Those
functions carry PostGIS names. Build the geometry from the two coordinate columns:

```sql
SELECT station_id, temperature
FROM stations
WHERE ST_Intersects(
    ST_Point(longitude, latitude),
    ST_GeomFromText('POLYGON ((-10 35, 40 35, 40 60, -10 60, -10 35))')
)
```

A measurement or an aggregate reads the same expression:

```sql
SELECT ST_XMin(ST_Extent(ST_Point(longitude, latitude))) AS west,
       ST_XMax(ST_Extent(ST_Point(longitude, latitude))) AS east,
       count(*) AS stations
FROM stations
WHERE ST_DWithin(ST_Point(longitude, latitude), ST_GeomFromText('POINT(4 52)'), 5.0)
```

Beacon also holds
[`st_within_point`](/docs/2.0.0-rc2/sql/function-reference#st-within-point-wkt-lon-lat). It takes
a WKT string and two ordinate columns.

A spatial function also reads a GeoParquet geometry column directly:

```sql
SELECT count(*)
FROM read_geoparquet(['spatial/stations/*.geoparquet'])
WHERE ST_Intersects(geometry, ST_GeomFromText('POLYGON((3 51, 5 51, 5 53, 3 53, 3 51))'))
```

:::tip
Beacon also *writes* GeoParquet. It maps the longitude and latitude columns of a query result into a
geometry column on output. See [output formats](/docs/2.0.0-rc2/api/querying/).
:::

## What the scan skips

A GeoParquet file states a bounding box per row group. The `covering` metadata names the four
columns that hold it. A file with a native geometry encoding states no covering, and Beacon reads
the box from the coordinate columns instead.

A spatial filter drops each row group whose box lies outside the query box. Five predicates state a
query box: `ST_Intersects`, `ST_Within`, `ST_Contains`, `ST_BBoxIntersects`, and `ST_DWithin` with
a constant distance. A box test is not an exact test, so the predicate still runs over each row the
scan keeps. Any other predicate reads every row group.

`EXPLAIN ANALYZE` reports the result as `geoparquet_row_groups_considered`,
`geoparquet_row_groups_pruned` and `geoparquet_files_pruned`.

A GeoParquet file also reports its row count and a range per plain column, from the file metadata.
So `beacon.system.file_stats` holds a range, and file pruning drops a file before the row group
step runs. A geometry column reports no range: a range is one minimum and one maximum value, and a
bounding box is neither.

## As an external table

```sql
CREATE EXTERNAL TABLE stations
STORED AS GEOPARQUET
LOCATION 'spatial/stations/*.geoparquet'
```

Beacon decodes the geometry columns to native [GeoArrow](https://geoarrow.org/) on read. See
[GeoParquet in Supported Formats](/docs/2.0.0-rc2/server/datasets#supported-formats) for the
details.

See [Create External Tables](/docs/2.0.0-rc2/data-sources/external-tables) for the full DDL. See [Data Sources](/docs/2.0.0-rc2/data-sources/) for the
full read model.
