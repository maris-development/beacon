---
description: Answers to common Beacon questions. Fix credentials, schema mismatches, slow queries and frequent errors.
---

# FAQ

This page answers common questions. It also lists the errors that people hit most. The sections
follow your task.

## Choose and start

### Do I have to run a server to try Beacon?

No. Query a public node first. Install the Python client, point it at the node and run SQL. See
[Query the public node](/docs/2.0.0-rc3/quickstart#query-the-public-node).

Run your own server when the data is yours, or when you need access control, a web UI or crawlers.

### Do I need to import or convert my files first?

No. Beacon reads files in place. Point a
[`read_*()` function](/docs/2.0.0-rc3/formats/) or an
[external table](/docs/2.0.0-rc3/data-sources/external-tables) at a path. Then query it.
Beacon copies nothing.

### Which formats are supported?

Parquet, GeoParquet, CSV, TSV, Arrow IPC, NetCDF, Zarr, Atlas, GeoTIFF, COG, BBF, Delta Lake and ODV
ASCII. Each format has its own chapter in
[File Formats](/docs/2.0.0-rc3/formats/).

## Read files

### How do I see what columns and types a file has?

[`read_<format>_schema()`](/docs/2.0.0-rc3/sql/table-functions-utility#read-format-schema) returns the
schema. It reads no data:

```sql
SELECT * FROM read_netcdf_schema('argo/**/*.nc');
```

It covers `parquet`, `netcdf`, `zarr`, `arrow`, `csv`, `bbf` and `tiff`. For GeoParquet, Atlas, Delta
Lake and ODV, use a `LIMIT 0` query. [`SUMMARIZE`](/docs/2.0.0-rc3/sql/summarize) also
works.

### My files have different columns

The query fails or returns nulls. Use
[`UNION BY NAME`](/docs/2.0.0-rc3/sql/union-by-name) to combine the files by column name,
not by position. Beacon sets a missing column to null. The columns stay aligned.

Some NetCDF collections mix variables with different dimensions. Give an explicit dimension list.
Beacon then returns only the compatible variables:

```sql
SELECT * FROM read_netcdf('argo/**/*.nc', ['time', 'pressure']);
```

### Beacon does not find my Zarr store

Beacon supports **Zarr v3**. A `zarr.json` entry file marks a v3 store. Beacon does not find version
2 stores. A version 2 store uses `.zarray`, `.zgroup` and `.zattrs` files. Point at the `zarr.json`
marker, not at the store directory or the chunks.

### Why does Beacon not find my ODV, Delta Lake or Iceberg data?

These three formats are the exception. Beacon finds every other format in the dataset store
automatically. Beacon does not find ODV ASCII, Delta Lake and Apache Iceberg. Point
[`read_odv_ascii()`](/docs/2.0.0-rc3/formats/odv) or
[`read_delta()`](/docs/2.0.0-rc3/formats/delta-lake) or
[`read_iceberg()`](/docs/2.0.0-rc3/formats/iceberg) at the data. For Delta and Iceberg,
you can also create an external table with `CREATE EXTERNAL TABLE … STORED AS DELTA|ICEBERG`.

### Is there a `STORED AS ODV`?

No. ODV ASCII has no external table form. Read it with `read_odv_ascii()`. Wrap the call in a
[view](/docs/2.0.0-rc3/server/view) to get a stable name.

## Object storage and credentials

### How do I query files on S3?

Use an `s3://` path. For a public bucket, set `AWS_SKIP_SIGNATURE=true`. You need nothing else. For a
private bucket, store the credentials as a named
[secret](/docs/2.0.0-rc3/sql/secrets):

```sql
CREATE SECRET my_s3 (TYPE S3, KEY_ID '…', SECRET '…', REGION 'eu-west-1', SCOPE 's3://my-bucket');
```

See [Query Data on S3](/docs/2.0.0-rc3/guides/query-s3).

### Beacon ignores my region setting

Beacon reads `AWS_REGION`. It does **not** use `AWS_DEFAULT_REGION`.

### My NetCDF files on S3 fail to read, but Parquet works

NetCDF on object storage supports **anonymous access only**. Beacon cannot yet do an authenticated S3
read for NetCDF. Make the objects public, or copy them to local disk. You can also convert the
collection to [Zarr](/docs/2.0.0-rc3/formats/zarr) or
[Atlas](/docs/2.0.0-rc3/formats/atlas). Both formats have full object storage
support.

### `CREATE PERSISTENT SECRET` fails

A persistent secret needs a master key and a file-backed database. Set the key with
`BEACON_SECRETS_KEY`. Beacon never writes a plaintext credential to disk. A plain `CREATE SECRET` needs no key. That secret lives only for the session.

### I rotated `BEACON_SECRETS_KEY` and my SQL database tables stopped working

Beacon decrypts a persistent credential with the same key only. If you lose or change the key, drop
the affected tables. Then create them again with the new key.

## Queries and performance

### My query is slow

Run `EXPLAIN` first. It shows if your filter and column list reach the scan. A predicate above the
scan prunes nothing.

The usual fixes come in this order of impact. Select fewer columns. Filter on coordinate columns. Do
not put a predicate inside a function that the reader cannot interpret. Merge large NetCDF or Zarr
collections into [Atlas](/docs/2.0.0-rc3/formats/atlas). See
[Speed Up Slow Queries](/docs/2.0.0-rc3/guides/speed-up-queries) for the full detail.

### My spatial filter on GeoParquet prunes nothing

Five predicates state a bounding box, and only those skip row groups: `ST_Intersects`, `ST_Within`,
`ST_Contains`, `ST_BBoxIntersects`, and `ST_DWithin` with a constant distance. A filter such as
`ST_Distance(geometry, …) < 100` states no box, so the scan reads every row group.

Rewrite the filter as one of the five. `EXPLAIN ANALYZE` reports `geoparquet_row_groups_pruned`,
which tells you if the rewrite worked. See
[what the scan skips](/docs/2.0.0-rc3/formats/geoparquet).

### My query runs out of memory

Do not collect the whole result. Read batches instead:

```python
import adbc_driver_flightsql.dbapi as flight_sql

conn = flight_sql.connect("grpc+tls://beacon.example.com:32011")
cursor = conn.cursor()
cursor.execute("SELECT * FROM obs")

while (batch := cursor.fetch_record_batch().read_next_batch()) is not None:
    process(batch)
```

On the server, a query can spill to disk. The spill goes to the OS temp area. Put that area on fast
storage with free space. See
[Performance Tuning](/docs/2.0.0-rc3/server/performance-tuning).

## The `beacon.db` file

### Can two processes open the same `beacon.db`?

No. Beacon holds the file under an **exclusive lock**. One server opens one `beacon.db`, so two
servers cannot share a data directory. For access from several processes or machines, run one
[server](/docs/2.0.0-rc3/getting-started) and connect every client to it.

### Does a copy of `beacon.db` include my data?

A copy includes everything that Beacon **owns**: the catalog and the managed table data. A copy does
not include the external files. Beacon must still reach those files from the new location. See
[Storage internals](/docs/2.0.0-rc3/internals/storage).

### When should I use a managed table instead of an external table?

Use an external table to read data that you already have. Use a managed table when Beacon owns the
rows. A managed table also gives you `INSERT`, `UPDATE` and `DELETE`. For a cached result with a
periodic refresh, use a
[materialized view](/docs/2.0.0-rc3/sql/create-materialized-view).

### Can I write query results to S3?

Not yet. A file sink writes to local paths only. A `scheme://` destination raises
`NotSupportedError`. Write to local disk and upload the file. You can also use a managed table.

## Server and security

### Do I need to enable SQL on the server?

SQL is **on by default**. The setting is `BEACON_ENABLE_SQL=true`. Set it to `false` to switch off
the raw SQL interface. The JSON query API stays available. This flag does not change Arrow Flight
SQL.

### Anyone can read my data even though I set an admin password

The `BEACON_ADMIN_*` credentials protect the admin UI and the write operations. They do not protect
reads. To control who reads data, set `BEACON_AUTH_ENFORCE=true`. See
[Access Control](/docs/2.0.0-rc3/security/access-control).

### My remote table fails with an authentication error

A remote table connects **anonymously** and stores no credentials. The remote server must allow
anonymous Flight SQL access. Set `BEACON_FLIGHT_SQL_ALLOW_ANONYMOUS=true` on the remote server. For
an authenticated connection, use [`ATTACH`](/docs/2.0.0-rc3/data-sources/attach). It accepts
a user name and password, a token or a secret.

### My remote table still shows the old columns

Beacon reads the schema of a remote table once, at creation time. Beacon then pins the schema into
the table definition. Drop the remote table and create it again to get the new schema.

### Beacon does not push my `st_*` filter down to the remote

A custom function must exist on **both** sides. Beacon pushes a predicate with a UDF down only if the
remote server has the same function. If not, Beacon runs the predicate locally. Use standard SQL
comparisons in the predicates that must push down.

### Beacon does not find new files automatically

Beacon does not watch storage for changes. It reads no local file system events and no S3 events.
Use a [crawler](/docs/2.0.0-rc3/server/crawlers) with a schedule to find new files.

## Python

### Which Python package do I install?

`beacon-api` is the client. It talks to a running server over HTTP:

```bash
pip install beacon-api
```

See [Python client](/docs/2.0.0-rc3/connect/python). For a terminal instead of a notebook, install
[`beacon-datalake-cli`](/docs/2.0.0-rc3/connect/cli).

### Can I connect a SQL tool instead?

Yes. The server speaks Arrow Flight SQL and JDBC. See
[DataGrip / JDBC](/docs/2.0.0-rc3/connect/datagrip) and
[Python ADBC](/docs/2.0.0-rc3/connect/python-adbc).

---

Do you still have a problem? Open an issue on
[GitHub](https://github.com/maris-development/beacon). Or read
[Concepts](/docs/2.0.0-rc3/concepts) for the full picture.
