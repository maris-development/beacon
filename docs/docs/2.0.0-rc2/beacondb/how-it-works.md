---
description: How BeaconDB runs a query, from SQL to DataFusion plan to Arrow execution. How predicate and projection pushdown reach into files and remote systems.
---

# How It Works

BeaconDB uses **Rust** and two Apache projects. [Arrow](https://arrow.apache.org/) holds columnar
data in memory. [DataFusion](https://datafusion.apache.org/) plans and runs the SQL. All of this
happens inside your process.

## The query pipeline

A query moves through four stages:

1. **Parse and plan.** Beacon parses the SQL and builds a logical plan. It resolves the plan against
   the catalog: tables, views, attached catalogs and reader functions.
2. **Optimize.** The planner rewrites the plan. It also decides how much work it can push *into* the
   data sources.
3. **Scan.** The readers open only the files, chunks and columns that the plan still needs. They
   decode them into Arrow record batches.
4. **Execute and stream.** Operators run over Arrow batches. The results stream out. A query over
   terabytes of files therefore never holds everything in memory.

## Read in place

There is no load step. A reader table function such as `read_netcdf()` or `read_parquet()` opens
your files at query time. It shows them as a table:

```sql
SELECT time, latitude, temperature
FROM read_netcdf('argo/**/*.nc')
WHERE temperature > 20;
```

A glob expands across directories. One statement can therefore cover thousands of files. Beacon
merges their schemas and reads them in parallel. You can also register the same files once as an
[external table](/docs/2.0.0-rc2/beacondb/data-sources/external-tables) and query them by name.

## Pushdown

Pushdown gives most of the performance. BeaconDB does not read everything and filter afterwards. It
moves filters and column selections as close to the data as possible:

- **Projection pushdown**: Beacon decodes only the columns that a query names. A query over 3 of 200
  variables reads about 3 columns of bytes.
- **Predicate pushdown**: Beacon turns filters into file, row group and chunk pruning. A time range
  filter on a [Zarr](/docs/2.0.0-rc2/beacondb/data-sources/formats/zarr) store fetches only the
  chunks in that range. On [Atlas](/docs/2.0.0-rc2/beacondb/data-sources/formats/atlas), Beacon uses
  the stored statistics. It can drop whole datasets before it reads any array data.
- **Federated pushdown**: Beacon sends filters, projections, limits and whole aggregates to
  [SQL databases](/docs/2.0.0-rc2/beacondb/data-sources/sql-databases) and to
  [remote Beacons](/docs/2.0.0-rc2/beacondb/data-sources/remote-tables). Only the reduced result
  travels back.

Use `EXPLAIN` to see what the planner pushes down:

```sql
EXPLAIN SELECT count(*) FROM read_parquet('obs/*.parquet') WHERE depth < 50;
```

## Arrays become tables

Scientific formats often hold multi-dimensional data. BeaconDB maps an array variable onto columns.
Ordinary SQL then works on it. Beacon also exposes the metadata as columns. The attributes of a
variable appear as `variable.attribute`, for example `temperature.units`. File attributes appear as
`.attribute`. [File Formats](/docs/2.0.0-rc2/beacondb/data-sources/formats/) documents the behaviour
of each format, including the treatment of dimensions.

## Storage: what BeaconDB owns

Most data stays where it is. BeaconDB *owns* the content of one
[`beacon.db` file](/docs/2.0.0-rc2/beacondb/data-sources/internal-format). That file holds the
catalog with the table, view and secret definitions. It also holds the managed table data. Copy that
one file and the database goes with it. The copy still references the external files and the remote
systems.

```text
your query
    │
    ▼
BeaconDB engine  ──►  beacon.db     (catalog + managed tables, owned)
    │
    ├──────────────►  files          (NetCDF, Zarr, Parquet, ... read in place)
    ├──────────────►  object storage (S3, GCS, Azure)
    └──────────────►  other systems  (Postgres, MySQL, other Beacons)
```

A local file gives you full control. Access control is therefore off by default. Switch it on for a
served boundary. See [access control](/docs/2.0.0-rc2/security/access-control).

## The same engine, served

[Beacon Data Lake](/docs/2.0.0-rc2/getting-started) is this engine with a service layer around it.
The service layer adds an HTTP API, an Arrow Flight SQL API, a managed dataset store, crawlers,
role-based access control and a web admin UI. The planner, the readers, the formats and the SQL
dialect stay the same. A query that you develop against BeaconDB behaves the same on the server.
