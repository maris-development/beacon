---
description: Read GeoTIFF and Cloud-Optimized GeoTIFF rasters with read_tiff(). Beacon also shows the TIFF tags as columns.
---

# GeoTIFF

## Read the files

```text
read_tiff(glob_paths)
```

Beacon reads GeoTIFF and Cloud-Optimized GeoTIFF files.

```sql
SELECT * FROM read_tiff('rasters/elevation.tif')
```

## Inspect the schema

Check the columns of a file before you write a query. Also check their types.

[`read_schema()`](/docs/2.0.0-rc2/beacondb/sql/table-functions-utility#read-schema) returns the column names and types **without a read of any data**. It is
therefore the cheapest option on a large collection:

```sql
SELECT * FROM read_schema('rasters/*.tif', 'tiff');
```

Pass a list to get the combined schema of several locations. This shows the files that disagree
about a column:

```sql
SELECT * FROM read_schema(['rasters/*.tif', 'other/*.tif'], 'tiff');
```

[`SUMMARIZE`](/docs/2.0.0-rc2/beacondb/sql/summarize) gives more than names and types. It profiles every column in
one pass. It adds the minimum, the maximum, the distinct count and the share of nulls:

```sql
SUMMARIZE (SELECT * FROM read_tiff('rasters/*.tif'));
```

If the files have a table name, use `DESCRIBE`:

```sql
DESCRIBE elevation;
```

From Python, read the Arrow schema of a relation. Beacon collects no rows:

```python
con.sql("SELECT * FROM read_tiff('rasters/*.tif') LIMIT 0").arrow().schema
```

## Format details

Beacon supports raster data in GeoTIFF and Cloud-Optimized GeoTIFF (COG) format. A COG file works
well over S3. Beacon sends range requests and reads only the tiles that it needs.

### Tag attributes

A GeoTIFF file carries TIFF tags and GeoTIFF metadata such as `nodata`, `crs` and `scale`. Beacon
shows these per band as extra columns. It uses dot notation: `<band>.<attribute>`. The `nodata` tag
of the `band_1` column becomes `band_1.nodata`.

An attribute column keeps the type from the file: string, integer, float and so on.

A file tag belongs to no band. Beacon shows it with a leading dot and no band prefix:
`.<attribute>`. The file tag `crs` becomes the column `.crs`.

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

See [Create External Tables](/docs/2.0.0-rc2/beacondb/data-sources/external-tables) for the full DDL. See [Data Sources](/docs/2.0.0-rc2/beacondb/data-sources/) for the
full read model.
