# GeoParquet

[GeoParquet](https://geoparquet.org/) files (`.geoparquet`) are Parquet files that carry geospatial geometry columns plus a `geo` metadata key. Beacon reads them like any other dataset: geometry columns are decoded to their native [GeoArrow](https://geoarrow.org/) representation, and everything you can do in SQL — `SELECT`, `WHERE`, `JOIN`, aggregates — works on top.

This chapter covers querying GeoParquet with SQL. For registering GeoParquet datasets as tables and the format's read behaviour, see [GeoParquet in the data-lake setup](../data-lake/geoparquet.md).

## Querying with `read_geoparquet()`

The `read_geoparquet()` table function reads one or more files matched by a glob, with no registration step:

```sql
SELECT * FROM read_geoparquet(['spatial/**/*.geoparquet']) LIMIT 100
```

A file without geometry (a plain Parquet file with no `geo` metadata) is read exactly like ordinary Parquet, so `read_geoparquet()` is safe to point at mixed folders.

Only the columns a query selects are materialized — column projection is pushed into the Parquet reader:

```sql
SELECT station_id, geometry
FROM read_geoparquet(['spatial/stations/*.geoparquet'])
WHERE station_id = 'GL_001'
```

## Geometry columns

Geometry columns named in the file's `geo` metadata are decoded to native GeoArrow on read. For point data with separated coordinates this surfaces as a `Struct` column with `x` / `y` child fields, which you can address with standard struct accessors:

```sql
SELECT geometry['x'] AS lon, geometry['y'] AS lat
FROM read_geoparquet(['spatial/stations/*.geoparquet'])
```

## Querying a GeoParquet external table

Register a stable name once with a [`STORED AS GEOPARQUET`](../data-lake/geoparquet.md) external table, then query it like any table:

```sql
CREATE EXTERNAL TABLE stations
STORED AS GEOPARQUET
LOCATION 'spatial/stations/*.geoparquet';

SELECT * FROM stations LIMIT 10;
```

## Spatial filtering

Geometry data pairs naturally with Beacon's [geospatial functions](./function-reference.md#geospatial-functions). For example, keep only rows whose longitude/latitude fall inside a bounding polygon with [`st_within_point`](./function-reference.md#st_within_point):

```sql
SELECT station_id, geometry
FROM stations
WHERE st_within_point(
    'POLYGON ((-10 35, 40 35, 40 60, -10 60, -10 35))',
    geometry['x'],
    geometry['y']
)
```

You can pass a GeoJSON polygon instead by wrapping it with [`st_geojson_as_wkt`](./function-reference.md#st_geojson_as_wkt).

:::warning
Spatial bounding-box pruning (row-group skipping via the GeoParquet `bbox` covering) is not yet applied on read — queries perform a full scan with column projection. Geometry-aware predicate pushdown is planned, so `st_*` filters above run on the full scan rather than skipping row groups.
:::

:::tip Writing GeoParquet
Beacon can also *write* GeoParquet: a query result with longitude/latitude columns is mapped into a geometry column on output. See [querying output formats](../api/querying/index.md).
:::
