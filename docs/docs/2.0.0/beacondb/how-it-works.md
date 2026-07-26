---
description: How BeaconDB executes a query, from SQL parsing through DataFusion planning to Arrow execution, and how predicate and projection pushdown reach into files, object storage, and remote systems.
---

# How It Works

BeaconDB is built in **Rust** on two Apache projects: [Arrow](https://arrow.apache.org/) for
columnar in-memory data, and [DataFusion](https://datafusion.apache.org/) for SQL planning and
execution. Everything below happens inside your process.

## The query pipeline

A query moves through four stages:

1. **Parse and plan.** SQL is parsed and turned into a logical plan, resolved against the catalog
   (tables, views, attached catalogs, and reader functions).
2. **Optimize.** The planner rewrites the plan and, critically, decides how much work can be pushed
   *into* the data sources rather than done afterwards.
3. **Scan.** Readers open only the files, chunks, and columns the plan still needs, decoding them
   into Arrow record batches.
4. **Execute and stream.** Operators run over Arrow batches and results stream out, so a query that
   returns a few rows from terabytes of files never materializes everything in memory.

## Reading in place

There is no ingest step. A reader table function such as `read_netcdf()` or `read_parquet()` opens
your files at query time and presents them as a table:

```sql
SELECT time, latitude, temperature
FROM read_netcdf('argo/**/*.nc')
WHERE temperature > 20;
```

Globs expand across directories, so one statement can span thousands of files. Beacon merges their
schemas and reads them concurrently. The same files can equally be registered once as an
[external table](/docs/2.0.0/beacondb/data-sources/external-tables) and queried by name.

## Pushdown

Pushdown is where most of the performance comes from. Instead of reading everything and filtering
afterwards, BeaconDB moves filters and column selections as close to the data as possible:

- **Projection pushdown**: only the columns a query references are decoded. Selecting 3 of 200
  variables reads roughly 3 columns' worth of bytes.
- **Predicate pushdown**: filters become file, row-group, and chunk pruning. A time-range filter on a
  [Zarr](/docs/2.0.0/beacondb/data-sources/formats/zarr) store fetches only the intersecting chunks;
  on [Atlas](/docs/2.0.0/beacondb/data-sources/formats/atlas) it can drop whole datasets using stored
  statistics before reading any array data.
- **Federated pushdown**: for [SQL databases](/docs/2.0.0/beacondb/data-sources/sql-databases) and
  [remote Beacons](/docs/2.0.0/beacondb/data-sources/remote-tables), filters, projections, limits and
  whole aggregates are sent to the other system, so only the reduced result travels back.

Use `EXPLAIN` to see what the planner pushed down:

```sql
EXPLAIN SELECT count(*) FROM read_parquet('obs/*.parquet') WHERE depth < 50;
```

## Arrays become tables

Scientific formats are often multi-dimensional. BeaconDB maps array variables onto columns so that
ordinary SQL works on them, and exposes their metadata as columns too: a variable's attributes are
available as `variable.attribute` (for example `temperature.units`), and file-level attributes as
`.attribute`. Per-format behaviour, including how dimensions are handled, is documented in
[File Formats](/docs/2.0.0/beacondb/data-sources/formats/).

## Storage: what BeaconDB owns

Most data stays where it is. What BeaconDB *owns* lives in a single
[`beacon.db` file](/docs/2.0.0/beacondb/data-sources/internal-format): its catalog (table, view, and
secret definitions) and any managed table data. Copy that one file and the database travels with it,
still referencing the external files and remote systems it knows about.

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

Opening a local file gives you full control, so access control is off by default. It can be switched
on for served boundaries; see [access control](/docs/2.0.0/security/access-control).

## The same engine, served

[Beacon Data Lake](/docs/2.0.0/getting-started) is this engine with a service layer around it: an
HTTP and Arrow Flight SQL API, a managed dataset store, crawlers, role-based access control, and a
web admin UI. The planner, readers, formats, and SQL dialect are identical, so a query developed
against BeaconDB behaves the same when served.
