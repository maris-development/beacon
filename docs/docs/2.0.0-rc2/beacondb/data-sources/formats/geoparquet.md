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

Check the columns of a file before you write a query. Also check their types.

`read_schema()` does not cover this format. Inspect it through the reader. A `LIMIT 0` query
returns the schema and no rows:

```sql
SELECT * FROM read_geoparquet('spatial/**/*.geoparquet') LIMIT 0;
```

[`SUMMARIZE`](/docs/2.0.0-rc2/beacondb/sql/summarize) gives more than names and types. It profiles every column in
one pass. It adds the minimum, the maximum, the distinct count and the share of nulls:

```sql
SUMMARIZE (SELECT * FROM read_geoparquet('spatial/**/*.geoparquet'));
```

If the files have a table name, use `DESCRIBE`:

```sql
DESCRIBE stations;
```

From Python, read the Arrow schema of a relation. Beacon collects no rows:

```python
con.sql("SELECT * FROM read_geoparquet('spatial/**/*.geoparquet') LIMIT 0").arrow().schema
```

## Format details

A [GeoParquet](https://geoparquet.org/) file (`.geoparquet`) is a Parquet file with geometry columns
and a `geo` metadata key. Beacon reads and writes this format.

- Beacon decodes the geometry columns from the `geo` metadata to native
  [GeoArrow](https://geoarrow.org/). Beacon reads a plain Parquet file as ordinary Parquet. You can
  therefore point `read_geoparquet()` at a mixed folder.
- Beacon applies column projection. It materializes only the columns that a query selects.
- The reader works on local disk and on S3-compatible object stores.

Query a GeoParquet file with the
[`read_geoparquet()`](/docs/2.0.0-rc2/beacondb/sql/table-functions#read-geoparquet) table function:

```sql
SELECT * FROM read_geoparquet(['spatial/**/*.geoparquet']) LIMIT 100
```

You can also register a stable table name with an
[external table](/docs/2.0.0-rc2/beacondb/data-sources/external-tables):

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

Use a geometry column with the
[geospatial functions](/docs/2.0.0-rc2/beacondb/sql/function-reference#geospatial-functions). This
example keeps only the rows inside a polygon. It uses
[`st_within_point`](/docs/2.0.0-rc2/beacondb/sql/function-reference#st-within-point-wkt-lon-lat):

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
Beacon also *writes* GeoParquet. It maps the longitude and latitude columns of a query result into a
geometry column on output. See [output formats](/docs/2.0.0-rc2/api/querying/).
:::

:::warning
Beacon does not yet use the GeoParquet `bbox` covering to skip row groups on read. A query runs a
full scan with column projection. Beacon plans support for geometry predicate pushdown.
:::

## As an external table

```sql
CREATE EXTERNAL TABLE stations
STORED AS GEOPARQUET
LOCATION 'spatial/stations/*.geoparquet'
```

Beacon decodes the geometry columns to native [GeoArrow](https://geoarrow.org/) on read. See
[GeoParquet in Supported Formats](/docs/2.0.0-rc2/data-lake/datasets#supported-formats) for the
details.

See [Create External Tables](/docs/2.0.0-rc2/beacondb/data-sources/external-tables) for the full DDL. See [Data Sources](/docs/2.0.0-rc2/beacondb/data-sources/) for the
full read model.
