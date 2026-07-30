---
description: Read external files with SQL. Every supported format has a read_* table function, from Parquet and CSV to NetCDF, Zarr, Atlas and GeoTIFF, readable from local disk or object storage.
---

# External Files

Beacon reads files directly in a `FROM` clause. Every supported format has a `read_*` table function
that takes a path or a glob, so you can query files without registering anything first:

```sql
-- one file
SELECT * FROM read_parquet('profiles/2024.parquet') LIMIT 10;

-- a glob across many files
SELECT * FROM read_netcdf('argo/**/*.nc') WHERE temperature > 20;

-- a list of paths or globs
SELECT * FROM read_csv(['a.csv', 'b.csv']);
```

Paths resolve against Beacon's storage root and may point at local disk or
[object storage](/docs/2.0.0-rc1/beacondb/sql/secrets) (`s3://`, `gs://`, `az://`).

## Formats

| Format | Function | `STORED AS` | Recognized files |
| --- | --- | --- | --- |
| [Parquet](/docs/2.0.0-rc1/beacondb/data-sources/formats/parquet) | `read_parquet` | `PARQUET` | `.parquet` |
| [GeoParquet](/docs/2.0.0-rc1/beacondb/data-sources/formats/geoparquet) | `read_geoparquet` | `GEOPARQUET` | `.geoparquet` |
| [CSV / TSV](/docs/2.0.0-rc1/beacondb/data-sources/formats/csv) | `read_csv` | `CSV` | `.csv`, `.tsv` |
| [Arrow IPC](/docs/2.0.0-rc1/beacondb/data-sources/formats/arrow) | `read_arrow` | `ARROW` | `.arrow`, `.feather` |
| [NetCDF](/docs/2.0.0-rc1/beacondb/data-sources/formats/netcdf) | `read_netcdf` | `NC` | `.nc` |
| [Zarr](/docs/2.0.0-rc1/beacondb/data-sources/formats/zarr) | `read_zarr` | `ZARR` | `zarr.json` marker |
| [Atlas](/docs/2.0.0-rc1/beacondb/data-sources/formats/atlas) | `read_atlas` | `ATLAS` | `atlas.json` marker |
| [GeoTIFF / COG](/docs/2.0.0-rc1/beacondb/data-sources/formats/geotiff) | `read_tiff` | `TIFF` | `.tif`, `.tiff` |
| [BBF](/docs/2.0.0-rc1/beacondb/data-sources/formats/bbf) | `read_bbf` | `BBF` | `.bbf` |
| [Delta Lake](/docs/2.0.0-rc1/beacondb/data-sources/formats/delta-lake) | `read_delta` | `DELTA` | `_delta_log/` directory |
| [ODV ASCII](/docs/2.0.0-rc1/beacondb/data-sources/formats/odv) | `read_odv_ascii` | not supported | `.txt` |

Every format is auto-discovered from the datasets store on Beacon Data Lake except **Delta Lake** and
**ODV ASCII**, which are read by pointing a function (or, for Delta, an external table) at them
directly.

## Seeing what is inside a file

Before querying an unfamiliar dataset, check which columns it has and what their types are.
[`read_schema()`](/docs/2.0.0-rc1/beacondb/sql/table-functions-utility#read-schema) does this **without
reading any data**:

```sql
SELECT * FROM read_schema('argo/**/*.nc', 'netcdf');
```

The second argument is the format: `parquet`, `netcdf` (or `nc`), `zarr`, `arrow`, `csv`, `bbf`, or
`tiff` (or `tif`). For the formats it does not cover (GeoParquet, Atlas, Delta Lake, ODV), resolve the
schema through the reader with a `LIMIT 0` query instead.

To also see value ranges, distinct counts, and null shares,
[`SUMMARIZE`](/docs/2.0.0-rc1/beacondb/sql/summarize) profiles every column in a single pass:

```sql
SUMMARIZE (SELECT * FROM read_netcdf('argo/**/*.nc'));
```

Each format chapter below has an **Inspecting the schema** section with the exact calls for that
format.

## Reading many files at once

Globs (`*`, `**`) expand across directories, so a single call can span thousands of files. Beacon
merges their schemas and prunes files that cannot match your filters.

```sql
SELECT platform, avg(temperature) AS t
FROM read_netcdf('argo/**/*.nc')
WHERE depth < 100
GROUP BY platform;
```

Array formats such as [Zarr](/docs/2.0.0-rc1/beacondb/data-sources/formats/zarr) and [Atlas](/docs/2.0.0-rc1/beacondb/data-sources/formats/atlas) point at their marker file (`zarr.json`,
`atlas.json`) rather than at individual chunk files.

If files share a schema but differ in column sets, combine them with
[`UNION BY NAME`](/docs/2.0.0-rc1/beacondb/sql/union-by-name).

## Giving files a table name

A `read_*` call is ideal for ad-hoc queries. When many queries share the same source, register it
once as an [external table](/docs/2.0.0-rc1/beacondb/data-sources/external-tables) and query it by name:

```sql
CREATE EXTERNAL TABLE ocean_profiles
STORED AS PARQUET
LOCATION 'profiles/';

SELECT * FROM ocean_profiles LIMIT 10;
```

## See also

- [Table Functions](/docs/2.0.0-rc1/beacondb/sql/table-functions): every reader signature in one place.
- [Reading External Files](/docs/2.0.0-rc1/beacondb/data-sources/): the reading model end to end.
- [Secrets](/docs/2.0.0-rc1/beacondb/sql/secrets): credentials for S3, GCS, and Azure.
