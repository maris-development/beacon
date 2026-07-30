---
description: Read GeoTIFF and Cloud-Optimized GeoTIFF rasters with read_tiff(), including TIFF tags exposed as columns.
---

# GeoTIFF

## Reading

```text
read_tiff(glob_paths)
```

Reads GeoTIFF and Cloud-Optimized GeoTIFF files.

```sql
SELECT * FROM read_tiff('rasters/elevation.tif')
```

## Inspecting the schema

Before writing a query it is usually worth checking which columns a file actually has, and
what their types are.

[`read_schema()`](/docs/2.0.0-rc1/beacondb/sql/table-functions-utility#read-schema) returns the
inferred column names and types **without reading any data**, which makes it the cheapest
option on large collections:

```sql
SELECT * FROM read_schema('rasters/*.tif', 'tiff');
```

Pass a list to see the combined schema across several locations, which is how you spot files
that disagree about a column:

```sql
SELECT * FROM read_schema(['rasters/*.tif', 'other/*.tif'], 'tiff');
```

To go further than names and types, [`SUMMARIZE`](/docs/2.0.0-rc1/beacondb/sql/summarize) profiles every column in one pass, adding
min/max, distinct counts, and the share of nulls:

```sql
SUMMARIZE (SELECT * FROM read_tiff('rasters/*.tif'));
```

If the files are registered as a table, `DESCRIBE` works directly:

```sql
DESCRIBE elevation;
```

From Python, the Arrow schema of any relation is available without collecting rows:

```python
con.sql("SELECT * FROM read_tiff('rasters/*.tif') LIMIT 0").arrow().schema
```

## Format details

Raster data in GeoTIFF and Cloud-Optimized GeoTIFF (COG) formats is supported. COG files are particularly efficient over S3 because Beacon can issue range requests to read only the required tiles.

### Tag attributes

GeoTIFF files carry TIFF tags and GeoTIFF metadata (e.g. `nodata`, `crs`, `scale`). Beacon exposes these per-band as extra columns using dot notation: `<band>.<attribute>`. For example, a band column `band_1` with a `nodata` tag is accessible as `band_1.nodata`.

Attribute columns preserve the original type (string, integer, float, …) as stored in the file.

File-level tags that are not tied to a specific band are exposed with a leading dot and no band prefix: `.<attribute>`. For example, a file-level `crs` tag is accessible as the column `.crs`.

```sql
SELECT band_1, "band_1.nodata", "band_1.scale", ".crs"
FROM read_tiff(['rasters/elevation.tif'])
LIMIT 1
```

## As an external table

```sql
CREATE EXTERNAL TABLE elevation
STORED AS TIFF
LOCATION 'rasters/elevation.tif'
```

See [Creating External Tables](/docs/2.0.0-rc1/beacondb/data-sources/external-tables) for the full DDL, and [Reading External Files](/docs/2.0.0-rc1/beacondb/data-sources/) for the general reading model.
