# Introduction

:::info Open Source (AGPL V3)
Beacon is open source under the AGPL V3 license. Source code and contributions: [github.com/maris-development/beacon](https://github.com/maris-development/beacon)
:::

Beacon is a data lakehouse query engine built for scientific datasets. Point it at your existing files — on disk or in S3 — and it exposes a SQL query API instantly, with no data migration or preprocessing required.

Clients query Beacon using **SQL** or **JSON** and receive results as a file (Parquet, NetCDF, Arrow IPC, …) or a streaming Arrow IPC response. Beacon handles filtering, aggregation, and joins across files entirely server-side.

## Your first query

You can query files directly, with no setup, or register them once and query by name:

```sql
-- Query files directly, no setup required
SELECT * FROM read_netcdf(['argo/**/*.nc']) LIMIT 100;

-- Or register a table once, then query it by name
SELECT temperature, latitude, longitude
FROM ocean_profiles
WHERE latitude BETWEEN 0 AND 70
LIMIT 100;
```

Queries are submitted over the HTTP API (`POST /api/query`) or Arrow Flight SQL.

## Supported formats

| Format | Notes |
| ------ | ----- |
| NetCDF | `.nc`, `.nc4`, `.cdf` |
| Zarr | v2 and v3 |
| Atlas | Array store optimized for NetCDF/Zarr query performance |
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

## Key concepts

A few terms used throughout the docs:

- **[Dataset](/docs/1.7.0/data-lake/datasets)** — an individual file Beacon reads in place (NetCDF, Zarr, Parquet, …).
- **[External table](/docs/1.7.0/data-lake/external-tables)** — a registered name over one or more files (a folder or glob pattern).
- **[Collection](/docs/1.7.0/data-lake/collections)** — a logical table spanning many datasets with a merged schema.
- **[View](/docs/1.7.0/data-lake/view)** — a saved query exposed as a table.
- **[Managed table](/docs/1.7.0/sql/managed-tables)** — an Iceberg-backed table Beacon owns and can mutate (`INSERT` / `UPDATE` / `DELETE`).

## Next steps

- **[Get started](/docs/1.7.0/getting-started)** — run Beacon with Docker, locally or against S3.
- **[Connect a client](/docs/1.7.0/connect/jetbrains-datagrip)** — JetBrains DataGrip, Python ADBC/SDK, or the CLI.
- **[Write queries](/docs/1.7.0/sql/)** — the SQL guide.
- **[Register your data](/docs/1.7.0/data-lake/)** — external tables, collections, and views.
