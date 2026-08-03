---
description: Read external files with SQL. Every supported format has a read_* table function, from Parquet and CSV to NetCDF, Zarr, Atlas and GeoTIFF.
---

# External Files

Beacon reads files directly in a `FROM` clause. Every supported format has a `read_*` table
function. The function takes a path or a glob. You register nothing first:

```sql
-- one file
SELECT * FROM read_parquet('profiles/2024.parquet') LIMIT 10;

-- a glob across many files
SELECT * FROM read_netcdf('argo/**/*.nc') WHERE temperature > 20;

-- a list of paths or globs
SELECT * FROM read_csv(['a.csv', 'b.csv']);
```

Beacon resolves a path against its storage root. A path points at local disk or at
[object storage](/docs/2.0.0-rc2/beacondb/sql/secrets) with an `s3://`, `gs://` or `az://` prefix.

## Formats

| Format | Function | `STORED AS` | Recognized files |
| --- | --- | --- | --- |
| [Parquet](/docs/2.0.0-rc2/beacondb/data-sources/formats/parquet) | `read_parquet` | `PARQUET` | `.parquet` |
| [GeoParquet](/docs/2.0.0-rc2/beacondb/data-sources/formats/geoparquet) | `read_geoparquet` | `GEOPARQUET` | `.geoparquet` |
| [CSV / TSV](/docs/2.0.0-rc2/beacondb/data-sources/formats/csv) | `read_csv` | `CSV` | `.csv`, `.tsv` |
| [Arrow IPC](/docs/2.0.0-rc2/beacondb/data-sources/formats/arrow) | `read_arrow` | `ARROW` | `.arrow`, `.feather` |
| [NetCDF](/docs/2.0.0-rc2/beacondb/data-sources/formats/netcdf) | `read_netcdf` | `NC` | `.nc` |
| [Zarr](/docs/2.0.0-rc2/beacondb/data-sources/formats/zarr) | `read_zarr` | `ZARR` | `zarr.json` marker |
| [Atlas](/docs/2.0.0-rc2/beacondb/data-sources/formats/atlas) | `read_atlas` | `ATLAS` | `atlas.json` marker |
| [GeoTIFF / COG](/docs/2.0.0-rc2/beacondb/data-sources/formats/geotiff) | `read_tiff` | `TIFF` | `.tif`, `.tiff` |
| [BBF](/docs/2.0.0-rc2/beacondb/data-sources/formats/bbf) | `read_bbf` | `BBF` | `.bbf` |
| [Delta Lake](/docs/2.0.0-rc2/beacondb/data-sources/formats/delta-lake) | `read_delta` | `DELTA` | `_delta_log/` directory |
| [ODV ASCII](/docs/2.0.0-rc2/beacondb/data-sources/formats/odv) | `read_odv_ascii` | not supported | `.txt` |

Beacon Data Lake finds every format in the dataset store automatically. **Delta Lake** and **ODV
ASCII** are the exception. Point a function at them. For Delta, you can also use an external table.

## See inside a file

Check the columns and types of an unfamiliar dataset first.
[`read_schema()`](/docs/2.0.0-rc2/beacondb/sql/table-functions-utility#read-schema) does this
**without a read of any data**:

```sql
SELECT * FROM read_schema('argo/**/*.nc', 'netcdf');
```

The second argument is the format: `parquet`, `netcdf` (or `nc`), `zarr`, `arrow`, `csv`, `bbf` or
`tiff` (or `tif`). GeoParquet, Atlas, Delta Lake and ODV are not in that list. Get their schema from
the reader with a `LIMIT 0` query.

[`SUMMARIZE`](/docs/2.0.0-rc2/beacondb/sql/summarize) also gives value ranges, distinct counts and
null shares. It profiles every column in one pass:

```sql
SUMMARIZE (SELECT * FROM read_netcdf('argo/**/*.nc'));
```

Each format chapter below has an **Inspect the schema** section. It gives the exact calls for that
format.

## Read many files at once

A glob (`*`, `**`) expands across directories. One call can therefore cover thousands of files.
Beacon merges their schemas. It also prunes the files that cannot match your filters.

```sql
SELECT platform, avg(temperature) AS t
FROM read_netcdf('argo/**/*.nc')
WHERE depth < 100
GROUP BY platform;
```

Array formats such as [Zarr](/docs/2.0.0-rc2/beacondb/data-sources/formats/zarr) and
[Atlas](/docs/2.0.0-rc2/beacondb/data-sources/formats/atlas) use a marker file. Point at `zarr.json`
or `atlas.json`, not at the chunk files.

Some files share a schema but have different columns. Combine those files with
[`UNION BY NAME`](/docs/2.0.0-rc2/beacondb/sql/union-by-name).

## Give files a table name

A `read_*` call fits an ad-hoc query. When many queries share one source, register it once as an
[external table](/docs/2.0.0-rc2/beacondb/data-sources/external-tables). Then query it by name:

```sql
CREATE EXTERNAL TABLE ocean_profiles
STORED AS PARQUET
LOCATION 'profiles/';

SELECT * FROM ocean_profiles LIMIT 10;
```

## See also

- [Table Functions](/docs/2.0.0-rc2/beacondb/sql/table-functions): every reader signature in one place.
- [Data Sources](/docs/2.0.0-rc2/beacondb/data-sources/): the full read model.
- [Secrets](/docs/2.0.0-rc2/beacondb/sql/secrets): credentials for S3, GCS and Azure.
