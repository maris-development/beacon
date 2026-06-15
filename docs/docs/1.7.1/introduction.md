---
description: How Beacon fits together — send SQL or JSON to the query engine, read NetCDF, Zarr, Parquet, GeoTIFF and more in place from local files or S3, and stream results back as Parquet, NetCDF or Arrow.
---

# Introduction

:::info Open Source (AGPL V3)
Beacon is open source under the AGPL V3 license. Source code and contributions: [github.com/maris-development/beacon](https://github.com/maris-development/beacon)
:::

Beacon is a data lakehouse query engine built for scientific datasets. Point it at your existing files — on disk or in S3 — and it exposes a SQL query API instantly, with no data migration or preprocessing required.

<QueryFlow />

Clients query Beacon using **SQL** or **JSON** and receive results as a file (Parquet, NetCDF, Arrow IPC, …) or a streaming Arrow IPC response. Beacon handles filtering, aggregation, and joins across files entirely server-side.

## Your first query

The same query three ways — as raw SQL, over the HTTP API, or from the Python SDK. No setup required: `read_netcdf()` reads the files in place.

::: code-group

```sql [SQL]
SELECT time, latitude, longitude, temperature
FROM read_netcdf(['argo/**/*.nc'])
WHERE temperature > 20
LIMIT 100
```

```http [HTTP]
POST /api/query
Content-Type: application/json

{
  "sql": "SELECT time, latitude, longitude, temperature FROM read_netcdf(['argo/**/*.nc']) WHERE temperature > 20 LIMIT 100",
  "output": { "format": "csv" }
}
```

```python [Python]
from beacon_api import Client

client = Client("https://your-beacon-node")

df = client.sql_query(
    "SELECT time, latitude, longitude, temperature "
    "FROM read_netcdf(['argo/**/*.nc']) "
    "WHERE temperature > 20 LIMIT 100"
).to_pandas_dataframe()
```

:::

SQL is sent over the HTTP API (`POST /api/query`, with `BEACON_ENABLE_SQL=true`) or Arrow Flight SQL. Prefer querying by name? Register the files as an [external table](/docs/1.7.1/data-lake/external-tables) first.

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

## Key concepts

A few terms used throughout the docs:

- **[Dataset](/docs/1.7.1/data-lake/datasets)** — an individual file Beacon reads in place (NetCDF, Zarr, Parquet, …).
- **[External table](/docs/1.7.1/data-lake/external-tables)** — a registered name over one or more files (a folder or glob pattern), with a merged schema across them.
- **[View](/docs/1.7.1/data-lake/view)** — a saved query exposed as a table.
- **[Managed table](/docs/1.7.1/sql/managed-tables)** — an Iceberg-backed table Beacon owns and can mutate (`INSERT` / `UPDATE` / `DELETE`).

## Next steps

- **[Get started](/docs/1.7.1/getting-started)** — run Beacon with Docker, locally or against S3.
- **[Connect a client](/docs/1.7.1/connect/jetbrains-datagrip)** — JetBrains DataGrip, Python ADBC/SDK, or the CLI.
- **[Write queries](/docs/1.7.1/sql/)** — the SQL guide.
- **[Register your data](/docs/1.7.1/data-lake/)** — external tables and views.
