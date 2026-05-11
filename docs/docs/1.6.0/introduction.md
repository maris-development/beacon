# Introduction

:::info Open Source (AGPL V3)
Beacon is open source under the AGPL V3 license. Source code and contributions: [github.com/maris-development/beacon](https://github.com/maris-development/beacon)
:::

Beacon is a data lakehouse query engine built for scientific datasets. Point it at your existing files — on disk or in S3 — and it exposes a SQL query API instantly, with no data migration or preprocessing required.

Clients query Beacon using **SQL** or **JSON** and receive results as a file (Parquet, NetCDF, Arrow IPC, …) or a streaming Arrow IPC response. Beacon handles filtering, aggregation, and joins across files entirely server-side.

## Supported formats

| Format | Notes |
| ------ | ----- |
| NetCDF | `.nc`, `.nc4`, `.cdf` |
| Zarr | v2 and v3 |
| Parquet | Native columnar, Hive partitioning supported |
| GeoTIFF / COG | Cloud-Optimized GeoTIFF supported |
| ODV ASCII | Ocean Data View spreadsheet format |
| CSV | Header row required, delimiter configurable |
| Arrow IPC | `.arrow`, `.ipc` stream files |
| Beacon Binary Format | Beacon's native ingest format |

## How it fits together

```text
                    Clients
           (notebooks · apps · scripts)
                       |
                  SQL / JSON
                       |
                       v
               +---------------+
               |    Beacon     |
               | query engine  |
               +-------+-------+
                       |
           +-----------+-----------+
           |                       |
           v                       v
    Local files               S3 / Object Store
    (NetCDF, Zarr, …)         (existing bucket)
           |                       |
           +-----------+-----------+
                       |
                       v
              Result returned to client
              (Parquet · NetCDF · Arrow)
```
