# Introduction

:::info Open Source (AGPL V3)
Beacon is an open source project available under the AGPL V3 license. You can find the source code and contribute to its development on GitHub: [beacon](https://github.com/maris-development/beacon)
:::

Beacon is a high-performance data lake query engine designed for data providers who want to expose large scientific datasets to users with minimal setup effort and excellent query performance.

It allows you to rapidly set up a query endpoint on top of your existing storage (local filesystem or S3-compatible object storage). Users can query datasets using **SQL** or **JSON** queries, and Beacon returns the result as a **single file** (e.g. a tabular export) in various data formats or as a continous data stream (using Arrow IPC).

Beacon works well with common earth-science and oceanographic formats out of the box. You can expose datasets such as:

- NetCDF files
- Zarr stores
- Parquet files
- Virtual collections (e.g. a logical dataset created using Beacon composed from many NetCDF files)
- Other supported formats in the Beacon ecosystem

## How it fits together

```text
(SQL / JSON)
+-------------------------------+
|           Clients             |
|  notebooks • apps • scripts   |
+---------------+---------------+
                |
                v
+-------------------+
|       Beacon      |
|  query + execution|
+----+----------+---+
     |          |
     |          |
     v          v
+----------------+    +---------------------+
| Local datasets |    |   S3 / Object Store |
| (files, zarr)  |    | (existing bucket)   |
+--------+-------+    +----------+----------+
         \                     /
          \                   /
           v                 v
      +---------------------------+
      |      Scan + Compute       |
      |   (filter/agg/join)       |
      +------------+--------------+
                   |
                   v
      +--------------------------+
      | Single result file       |
      | returned to the client   |
      +--------------------------+
```
