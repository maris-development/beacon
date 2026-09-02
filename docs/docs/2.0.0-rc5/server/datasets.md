---
description: The file formats that Beacon finds in its datasets store. This page also shows where those files live.
---

# Supported Formats

Beacon finds the datasets in its storage root automatically. You register nothing. Copy
your files into the datasets folder or the S3 prefix. You can then query them at once. Use a
[reader function](/docs/2.0.0-rc5/formats/) or an
[external table](/docs/2.0.0-rc5/data-sources/external-tables).

The default local path in the Docker container is `/beacon/data/datasets/`.

## Format support matrix

| Format | Recognized files | `STORED AS` | `read_*` function | Output format |
| --- | --- | --- | --- | --- |
| [Parquet](/docs/2.0.0-rc5/formats/parquet) | `.parquet` | `PARQUET` | `read_parquet` | yes |
| [GeoParquet](/docs/2.0.0-rc5/formats/geoparquet) | `.geoparquet` | `GEOPARQUET` | `read_geoparquet` | yes |
| [CSV / TSV](/docs/2.0.0-rc5/formats/csv) | `.csv`, `.tsv` | `CSV` | `read_csv` | yes |
| [Arrow IPC](/docs/2.0.0-rc5/formats/arrow) | `.arrow`, `.feather` | `ARROW` | `read_arrow` | yes (`ipc`) |
| [NetCDF](/docs/2.0.0-rc5/formats/netcdf) | `.nc` | `NC` | `read_netcdf` | yes (+ ND-NetCDF) |
| [Zarr](/docs/2.0.0-rc5/formats/zarr) | `zarr.json` marker | `ZARR` | `read_zarr` | no |
| [Atlas](/docs/2.0.0-rc5/formats/atlas) | `data.atlas` file | `ATLAS` | `read_atlas` | yes |
| [GeoTIFF / COG](/docs/2.0.0-rc5/formats/geotiff) | `.tif`, `.tiff` | `TIFF` | `read_tiff` | no |
| [BBF](/docs/2.0.0-rc5/formats/bbf) | `.bbf` | `BBF` | `read_bbf` | no |
| [Delta Lake](/docs/2.0.0-rc5/formats/delta-lake) | `_delta_log/` directory | `DELTA` | `read_delta` | no |
| [Apache Iceberg](/docs/2.0.0-rc5/formats/iceberg) | `metadata/` directory | `ICEBERG` | `read_iceberg` | no |
| [ODV ASCII](/docs/2.0.0-rc5/formats/odv) | `.txt` | not supported | `read_odv_ascii` | yes |

Beacon finds every format above in the datasets store. **Delta Lake**, **Apache Iceberg** and **ODV
ASCII** are the exception. Point a [`read_*` function](/docs/2.0.0-rc5/formats/) at them. For
Delta and Iceberg, `CREATE EXTERNAL TABLE … STORED AS DELTA|ICEBERG LOCATION …` also works. The "Output format" column
marks the formats that
[`output.format`](/docs/2.0.0-rc5/api/querying/#output-formats) can export a query result to.

:::tip Per-format reference
Each format has its own chapter. The chapter covers the read behaviour, the attribute columns, the
limitations and the tuning. See [External Files](/docs/2.0.0-rc5/formats/).
:::

## Where files live

The datasets store is a local directory or an S3-compatible bucket:

- **Local disk**: the files under the datasets folder. Mount that folder into the container.
- **Object storage**: an S3, GCS or Azure prefix. See
  [Object Storage](/docs/2.0.0-rc5/data-sources/object-storage) for the credentials and the
  setup.

Beacon reads the files in place. Beacon never copies or converts them. A new file is queryable at
once. Use a [crawler](/docs/2.0.0-rc5/server/crawlers) to register many datasets under a prefix
as named tables in one step. A crawler also handles a partitioned layout.

## Next

- **[External Files](/docs/2.0.0-rc5/formats/)**: the read reference for each format.
- **[Create External Tables](/docs/2.0.0-rc5/data-sources/external-tables)**: give a set of files a stable table name.
- **[Performance Tuning](/docs/2.0.0-rc5/server/performance-tuning)**: the layout and format choices that make a scan faster.
