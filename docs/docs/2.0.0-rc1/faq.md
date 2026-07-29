---
description: Answers to common questions about Beacon, from choosing between BeaconDB and Beacon Data Lake to credentials, schema mismatches, slow queries, and the errors people hit most often.
---

# FAQ

Common questions and the errors people actually hit, grouped by what you were trying to do.

## Choosing and getting started

### Should I use BeaconDB or Beacon Data Lake?

Use **BeaconDB** when the data is yours to query: a notebook, a script, or an application that runs
on one machine. Use **Beacon Data Lake** when other people or services need to query it over the
network, or when you need access control, a web UI, or crawlers.

They are the same engine, so this is not a lock-in decision. You can develop against BeaconDB and
deploy the identical queries to a server. See
[Which should I use?](/docs/2.0.0-rc1/introduction#which-should-i-use).

### Do I need to import or convert my files first?

No. Beacon reads files in place. Point a
[`read_*()` function](/docs/2.0.0-rc1/beacondb/data-sources/formats/) or an
[external table](/docs/2.0.0-rc1/beacondb/data-sources/external-tables) at a path and query it. Nothing
is copied or rewritten.

### Which formats are supported?

Parquet, GeoParquet, CSV/TSV, Arrow IPC, NetCDF, Zarr, Atlas, GeoTIFF/COG, BBF, Delta Lake, and ODV
ASCII. Each has its own chapter in [File Formats](/docs/2.0.0-rc1/beacondb/data-sources/formats/).

## Reading files

### How do I see what columns and types a file has?

[`read_schema()`](/docs/2.0.0-rc1/beacondb/sql/table-functions-utility#read_schema) returns the schema
without reading any data:

```sql
SELECT * FROM read_schema('argo/**/*.nc', 'netcdf');
```

It covers `parquet`, `netcdf`, `zarr`, `arrow`, `csv`, `bbf`, and `tiff`. For GeoParquet, Atlas,
Delta Lake, and ODV, use a `LIMIT 0` query or
[`SUMMARIZE`](/docs/2.0.0-rc1/beacondb/sql/summarize) instead.

### My files have different columns and the query fails or returns nulls

Combine them by column name rather than position with
[`UNION BY NAME`](/docs/2.0.0-rc1/beacondb/sql/union-by-name). Missing columns become nulls instead of
misaligning.

For NetCDF collections that mix variables with incompatible dimensionality, pass an explicit
dimension list so only compatible variables are returned:

```sql
SELECT * FROM read_netcdf('argo/**/*.nc', ['time', 'pressure']);
```

### My Zarr store is not being discovered

Beacon supports **Zarr v3**, identified by a `zarr.json` entry file. Version 2 stores, which use
`.zarray` / `.zgroup` / `.zattrs`, are not discovered. Point at the `zarr.json` marker rather than at
the store directory or its chunks.

### Why isn't my ODV or Delta Lake data showing up automatically?

Those two are the exception: every other format is auto-discovered from the datasets store, but ODV
ASCII and Delta Lake are not. Point
[`read_odv_ascii()`](/docs/2.0.0-rc1/beacondb/data-sources/formats/odv) or
[`read_delta()`](/docs/2.0.0-rc1/beacondb/data-sources/formats/delta-lake) at them directly, or register
a Delta table with `CREATE EXTERNAL TABLE … STORED AS DELTA`.

### Is there a `STORED AS ODV`?

No. ODV ASCII has no external-table form. Read it with `read_odv_ascii()`, and wrap it in a
[view](/docs/2.0.0-rc1/data-lake/view) if you want a stable name.

## Object storage and credentials

### How do I query files on S3?

Use an `s3://` path. For a public bucket, set `AWS_SKIP_SIGNATURE=true` and nothing else is needed.
For a private one, store credentials as a named
[secret](/docs/2.0.0-rc1/beacondb/sql/secrets):

```sql
CREATE SECRET my_s3 (TYPE S3, KEY_ID '…', SECRET '…', REGION 'eu-west-1', SCOPE 's3://my-bucket');
```

See [Query Data on S3](/docs/2.0.0-rc1/beacondb/guides/query-s3).

### My region setting is being ignored

Beacon reads `AWS_REGION`. It does **not** use `AWS_DEFAULT_REGION`.

### My NetCDF files on S3 fail to read, but Parquet works

NetCDF over object storage currently supports **anonymous access only**; authenticated S3 reads are
not yet supported for NetCDF. Either make the objects publicly readable, copy them locally, or
convert the collection to a format with full object-store support such as
[Zarr](/docs/2.0.0-rc1/beacondb/data-sources/formats/zarr) or
[Atlas](/docs/2.0.0-rc1/beacondb/data-sources/formats/atlas).

### `CREATE PERSISTENT SECRET` fails

Persisting a secret requires a configured master key (`BEACON_SECRETS_KEY`, or `secrets_key=` on
`beacondb.connect`) and a file-backed database. Beacon deliberately refuses to write a plaintext
credential to disk. A plain `CREATE SECRET` works without a key but lives only for the session.

### I rotated `BEACON_SECRETS_KEY` and my SQL database tables stopped working

A persisted credential can only be decrypted with the same key. If the key is lost or rotated, drop
and recreate the affected tables with the new key.

## Queries and performance

### My query is slow

Start with `EXPLAIN` to see whether your filter and column list reached the scan. If the predicate
sits above the scan, nothing is being pruned.

The usual fixes, in order of impact: select fewer columns, filter on coordinate columns, avoid
wrapping predicates in functions the reader cannot interpret, and consolidate large NetCDF/Zarr
collections into [Atlas](/docs/2.0.0-rc1/beacondb/data-sources/formats/atlas). Full detail in
[Speed Up Slow Queries](/docs/2.0.0-rc1/beacondb/guides/speed-up-queries).

### My spatial filter on GeoParquet doesn't prune anything

That is expected today. Spatial bounding-box pruning (row-group skipping via the GeoParquet `bbox`
covering) is not yet applied, so `st_*` filters run over a full scan with column projection.
Geometry-aware predicate pushdown is planned.

### My query runs out of memory

Don't collect the whole result. Pull batches instead:

```python
for batch in con.sql("SELECT * FROM obs").record_batch(50_000):
    process(batch)
```

On the server, queries can spill to disk; spilling uses the OS temp area, so make sure it is on fast
storage with free space. See
[Performance Tuning](/docs/2.0.0-rc1/data-lake/performance-tuning).

## The `beacon.db` file

### Can two processes open the same `beacon.db`?

No. A file-backed database is held under an **exclusive lock**, so one process opens one
`beacon.db`. A second `connect()` to the same path *within the same process* shares the existing
connection. If you need concurrent access from several processes or machines, run
[Beacon Data Lake](/docs/2.0.0-rc1/getting-started) and connect to it instead.

### Does copying `beacon.db` copy my data?

It copies everything Beacon **owns**: the catalog and any managed table data. It does not copy the
external files it references. Those must still be reachable from wherever you open the file. See
[Internal Format](/docs/2.0.0-rc1/beacondb/data-sources/internal-format).

### When should I use a managed table instead of an external table?

Use an external table to read data you already have. Use a managed table when Beacon should own the
rows and you need `INSERT` / `UPDATE` / `DELETE`. For a cached result that needs periodic refreshing,
a [materialized view](/docs/2.0.0-rc1/beacondb/sql/create-materialized-view) is usually the better fit.

### Can I write query results to S3?

Not yet. File sinks write to local paths only; a `scheme://` destination raises
`NotSupportedError`. Write locally and upload, or use a managed table.

## Server and security

### Do I need to enable SQL on the server?

SQL is **enabled by default** (`BEACON_ENABLE_SQL=true`). Set it to `false` to disable the raw SQL
interface, which leaves the JSON query API available. Arrow Flight SQL is unaffected by this flag.

### Anyone can read my data even though I set an admin password

The `BEACON_ADMIN_*` credentials gate the admin UI and write/management operations, not reads. To
restrict who can read data, enable access control with `BEACON_AUTH_ENFORCE=true`. See
[Access Control](/docs/2.0.0-rc1/security/access-control).

### Why is authentication off by default in BeaconDB?

Because opening a local file is already full control, following the usual embedded-database
contract. It can be switched on with `auth=True` when you need the served-boundary behaviour.

### My remote table fails with an authentication error

Remote tables connect **anonymously** and store no credentials, so the remote instance must allow
anonymous Flight SQL access (`BEACON_FLIGHT_SQL_ALLOW_ANONYMOUS=true` on the remote). If you need
authenticated federation, use [`ATTACH`](/docs/2.0.0-rc1/beacondb/data-sources/attach), which accepts a
username/password, a token, or a secret.

### I changed a table on the remote and my remote table still shows the old columns

A remote table's schema is fetched once, when the table is created, and pinned into its definition.
Drop and recreate the remote table to pick up the new schema.

### My `st_*` filter isn't pushed down to the remote

Custom functions must exist on **both** sides. A predicate using a Beacon UDF only pushes down if the
remote has the same function; otherwise Beacon falls back to executing it locally. Keep
remote-pushed predicates to standard SQL comparisons where it matters.

### New files aren't picked up automatically

Beacon does not watch storage for changes — neither local filesystem nor S3 change events are wired
up. A [crawler](/docs/2.0.0-rc1/data-lake/crawlers) with a schedule is the way to pick up new files.

## Python

### `.df()` raises an ImportError

The dataframe helpers are optional extras:

```bash
pip install "beacondb[pandas]"       # .df()
pip install "beacondb[polars]"       # .pl()
pip install "beacondb[sqlalchemy]"   # the beacondb:// dialect
pip install "beacondb[all]"          # all of the above
```

`.arrow()` works without any extra.

### Can I use BeaconDB with SQLAlchemy?

Yes, a `beacondb://` dialect ships with the package. See
[SQLAlchemy](/docs/2.0.0-rc1/beacondb/python/sqlalchemy).

---

Still stuck? Open an issue on
[GitHub](https://github.com/maris-development/beacon), or check
[Concepts](/docs/2.0.0-rc1/concepts) for how the pieces fit together.
