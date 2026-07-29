---
description: Which file formats Beacon Data Lake auto-discovers from its datasets store, and where those files live on disk or in object storage.
---

# Supported Formats

Beacon Data Lake discovers datasets from its configured storage root automatically, with no
registration step. Place files in the datasets folder (or S3 prefix) and they become immediately
queryable through [reader functions](/docs/2.0.0-rc1/beacondb/data-sources/formats/) or
[external tables](/docs/2.0.0-rc1/beacondb/data-sources/external-tables).

Default local path inside the Docker container: `/beacon/data/datasets/`

## Format support matrix

| Format | Recognized files | `STORED AS` | `read_*` function | Output format |
| --- | --- | --- | --- | --- |
| [Parquet](/docs/2.0.0-rc1/beacondb/data-sources/formats/parquet) | `.parquet` | `PARQUET` | `read_parquet` | yes |
| [GeoParquet](/docs/2.0.0-rc1/beacondb/data-sources/formats/geoparquet) | `.geoparquet` | `GEOPARQUET` | `read_geoparquet` | yes |
| [CSV / TSV](/docs/2.0.0-rc1/beacondb/data-sources/formats/csv) | `.csv`, `.tsv` | `CSV` | `read_csv` | yes |
| [Arrow IPC](/docs/2.0.0-rc1/beacondb/data-sources/formats/arrow) | `.arrow`, `.feather` | `ARROW` | `read_arrow` | yes (`ipc`) |
| [NetCDF](/docs/2.0.0-rc1/beacondb/data-sources/formats/netcdf) | `.nc` | `NC` | `read_netcdf` | yes (+ ND-NetCDF) |
| [Zarr](/docs/2.0.0-rc1/beacondb/data-sources/formats/zarr) | `zarr.json` marker | `ZARR` | `read_zarr` | no |
| [Atlas](/docs/2.0.0-rc1/beacondb/data-sources/formats/atlas) | `atlas.json` marker | `ATLAS` | `read_atlas` | no |
| [GeoTIFF / COG](/docs/2.0.0-rc1/beacondb/data-sources/formats/geotiff) | `.tif`, `.tiff` | `TIFF` | `read_tiff` | no |
| [BBF](/docs/2.0.0-rc1/beacondb/data-sources/formats/bbf) | `.bbf` | `BBF` | `read_bbf` | no |
| [Delta Lake](/docs/2.0.0-rc1/beacondb/data-sources/formats/delta-lake) | `_delta_log/` directory | `DELTA` | `read_delta` | no |
| [ODV ASCII](/docs/2.0.0-rc1/beacondb/data-sources/formats/odv) | `.txt` | not supported | `read_odv_ascii` | yes |

Every format above is auto-discovered from the datasets store except **Delta Lake** and **ODV
ASCII**: point a [`read_*` function](/docs/2.0.0-rc1/beacondb/data-sources/formats/) (or, for Delta,
`CREATE EXTERNAL TABLE … STORED AS DELTA LOCATION …`) at those directly. "Output format" marks
formats a query result can be exported to via
[`output.format`](/docs/2.0.0-rc1/api/querying/#output-formats).

:::tip Per-format reference
Each format has its own chapter covering its read behaviour, attribute columns, limitations, and
tuning. See [External Files](/docs/2.0.0-rc1/beacondb/data-sources/formats/).
:::

## Where files live

The datasets store is either a local directory or an S3-compatible bucket:

- **Local disk**: files under the datasets folder, mounted into the container.
- **Object storage**: an S3, GCS, or Azure prefix. See
  [Object Storage](/docs/2.0.0-rc1/beacondb/data-sources/object-storage) for credentials and setup.

Files are read in place. Beacon never copies or converts them, and adding a file makes it queryable
immediately. To register many datasets under a prefix as named tables in one step, including
partitioned layouts, use a [crawler](/docs/2.0.0-rc1/data-lake/crawlers).

## Next

- **[External Files](/docs/2.0.0-rc1/beacondb/data-sources/formats/)**: the per-format reading reference.
- **[Creating External Tables](/docs/2.0.0-rc1/beacondb/data-sources/external-tables)**: give a set of files a stable table name.
- **[Performance Tuning](/docs/2.0.0-rc1/data-lake/performance-tuning)**: layout and format choices that speed up scans.
