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

[Inspect a schema](/docs/2.0.1/formats/inspect-a-schema) compares the `_schema` functions,
`SUMMARIZE`, `DESCRIBE` and `LIMIT 0`, and says what each one costs.

## Format details

Beacon supports raster data in GeoTIFF and Cloud-Optimized GeoTIFF (COG) format. A COG file works
well over S3. Beacon sends range requests and reads only the tiles that it needs.

### Pixel coordinates

The `geo.lon` and `geo.lat` columns give the center of each pixel. This agrees with CF and with
`read_netcdf`. Beacon reads the raster type of the file. A PixelIsArea file stores the corner of
the first pixel, so Beacon adds half a pixel. A PixelIsPoint file stores the center, so Beacon adds
nothing. A file with no raster type is a PixelIsArea file.

### Row count

A raster reads as one row for each pixel, but only when the query uses a band column or both
`geo.lon` and `geo.lat`. `geo.lon` alone spans the image width, and a tag column such as
`image.width` is a scalar. So `SELECT count(*) FROM read_tiff('dem.tif') WHERE "geo.lon" > 5`
counts columns of pixels, not pixels. Add a band column to count pixels:

```sql
SELECT count(*), max("band.0") FROM read_tiff('dem.tif') WHERE "geo.lon" > 5
```

See [A projection can change the row count](/docs/2.0.1/arrays-to-tables#a-projection-can-change-the-row-count).

### Supported layouts

Beacon reads tiled and stripped files. It reads classic TIFF and BigTIFF, in both byte orders.
A band can be an 8, 16, 32 or 64 bit integer, or a 32 or 64 bit float.

Beacon decodes these compression types: none, PackBits, LZW, Deflate, JPEG and ZSTD. It also
decodes the horizontal predictor and the floating point predictor. A file with another compression,
such as LERC, LZMA or WebP, gives an error that names the file.

Beacon reads the full-resolution image. It skips overviews and masks. A pixel that only a GDAL mask
band hides keeps its value. Set a nodata value to hide such pixels.

A sparse file has blocks with no data. GDAL writes such a file with `SPARSE_OK=TRUE`. Beacon reads
no bytes for a sparse block. Each pixel of that block gets the nodata value, so the query shows
`NULL`. If the file has no nodata value, each pixel gets 0.

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

See [Create External Tables](/docs/2.0.1/data-sources/external-tables) for the full DDL. See [Data Sources](/docs/2.0.1/data-sources/) for the
full read model.

### `OPTIONS`

`STORED AS TIFF` reads no key. Beacon ignores an `OPTIONS` clause on this format. See
[`OPTIONS`](/docs/2.0.1/sql/create-external-table#options) for the formats that do read one.
