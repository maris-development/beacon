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

Check the columns and the types before you write a query:

```sql
SELECT * FROM read_tiff('rasters/*.tif') LIMIT 0;
```

[Inspect a schema](/docs/2.0.0-rc5/formats/inspect-a-schema) compares the `_schema` functions,
`SUMMARIZE`, `DESCRIBE` and `LIMIT 0`, and says what each one costs.

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

See [Create External Tables](/docs/2.0.0-rc5/data-sources/external-tables) for the full DDL. See [Data Sources](/docs/2.0.0-rc5/data-sources/) for the
full read model.

### `OPTIONS`

`STORED AS TIFF` reads no key. Beacon ignores an `OPTIONS` clause on this format. See
[`OPTIONS`](/docs/2.0.0-rc5/sql/create-external-table#options) for the formats that do read one.
