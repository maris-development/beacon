---
description: Answers to common Beacon questions. Choose between BeaconDB and Beacon Data Lake. Fix credentials, schema mismatches, slow queries and frequent errors.
---

# FAQ

This page answers common questions. It also lists the errors that people hit most. The sections
follow your task.

## Choose and start

### Should I use BeaconDB or Beacon Data Lake?

Use **BeaconDB** for a notebook, a script or an application on one machine. Use **Beacon Data Lake**
when other people or services query the data over the network. Also use it when you need access
control, a web UI or crawlers.

Both options use the same engine. This choice does not lock you in. Develop against BeaconDB. Then
run the same queries on a server. See
[Which should I use?](/docs/2.0.0-rc2/introduction#which-should-i-use).

### Do I need to import or convert my files first?

No. Beacon reads files in place. Point a
[`read_*()` function](/docs/2.0.0-rc2/beacondb/data-sources/formats/) or an
[external table](/docs/2.0.0-rc2/beacondb/data-sources/external-tables) at a path. Then query it.
Beacon copies nothing.

### Which formats are supported?

Parquet, GeoParquet, CSV, TSV, Arrow IPC, NetCDF, Zarr, Atlas, GeoTIFF, COG, BBF, Delta Lake and ODV
ASCII. Each format has its own chapter in
[File Formats](/docs/2.0.0-rc2/beacondb/data-sources/formats/).

## Read files

### How do I see what columns and types a file has?

[`read_schema()`](/docs/2.0.0-rc2/beacondb/sql/table-functions-utility#read-schema) returns the
schema. It reads no data:

```sql
SELECT * FROM read_schema('argo/**/*.nc', 'netcdf');
```

It covers `parquet`, `netcdf`, `zarr`, `arrow`, `csv`, `bbf` and `tiff`. For GeoParquet, Atlas, Delta
Lake and ODV, use a `LIMIT 0` query. [`SUMMARIZE`](/docs/2.0.0-rc2/beacondb/sql/summarize) also
works.

### My files have different columns

The query fails or returns nulls. Use
[`UNION BY NAME`](/docs/2.0.0-rc2/beacondb/sql/union-by-name) to combine the files by column name,
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

### Why does Beacon not find my ODV or Delta Lake data?

These two formats are the exception. Beacon finds every other format in the dataset store
automatically. Beacon does not find ODV ASCII and Delta Lake. Point
[`read_odv_ascii()`](/docs/2.0.0-rc2/beacondb/data-sources/formats/odv) or
[`read_delta()`](/docs/2.0.0-rc2/beacondb/data-sources/formats/delta-lake) at the data. For Delta,
you can also create an external table with `CREATE EXTERNAL TABLE … STORED AS DELTA`.

### Is there a `STORED AS ODV`?

No. ODV ASCII has no external table form. Read it with `read_odv_ascii()`. Wrap the call in a
[view](/docs/2.0.0-rc2/data-lake/view) to get a stable name.

## Object storage and credentials

### How do I query files on S3?

Use an `s3://` path. For a public bucket, set `AWS_SKIP_SIGNATURE=true`. You need nothing else. For a
private bucket, store the credentials as a named
[secret](/docs/2.0.0-rc2/beacondb/sql/secrets):

```sql
CREATE SECRET my_s3 (TYPE S3, KEY_ID '…', SECRET '…', REGION 'eu-west-1', SCOPE 's3://my-bucket');
```

See [Query Data on S3](/docs/2.0.0-rc2/beacondb/guides/query-s3).

### Beacon ignores my region setting

Beacon reads `AWS_REGION`. It does **not** use `AWS_DEFAULT_REGION`.

### My NetCDF files on S3 fail to read, but Parquet works

NetCDF on object storage supports **anonymous access only**. Beacon cannot yet do an authenticated S3
read for NetCDF. Make the objects public, or copy them to local disk. You can also convert the
collection to [Zarr](/docs/2.0.0-rc2/beacondb/data-sources/formats/zarr) or
[Atlas](/docs/2.0.0-rc2/beacondb/data-sources/formats/atlas). Both formats have full object storage
support.

### `CREATE PERSISTENT SECRET` fails

A persistent secret needs a master key and a file-backed database. Set the key with
`BEACON_SECRETS_KEY`, or with `secrets_key=` on `beacondb.connect`. Beacon never writes a plaintext
credential to disk. A plain `CREATE SECRET` needs no key. That secret lives only for the session.

### I rotated `BEACON_SECRETS_KEY` and my SQL database tables stopped working

Beacon decrypts a persistent credential with the same key only. If you lose or change the key, drop
the affected tables. Then create them again with the new key.

## Queries and performance

### My query is slow

Run `EXPLAIN` first. It shows if your filter and column list reach the scan. A predicate above the
scan prunes nothing.

The usual fixes come in this order of impact. Select fewer columns. Filter on coordinate columns. Do
not put a predicate inside a function that the reader cannot interpret. Merge large NetCDF or Zarr
collections into [Atlas](/docs/2.0.0-rc2/beacondb/data-sources/formats/atlas). See
[Speed Up Slow Queries](/docs/2.0.0-rc2/beacondb/guides/speed-up-queries) for the full detail.

### My spatial filter on GeoParquet prunes nothing

This behaviour is correct today. Beacon does not yet use the GeoParquet `bbox` covering to skip row
groups. An `st_*` filter therefore runs over a full scan with column projection. Beacon plans support
for geometry predicate pushdown.

### My query runs out of memory

Do not collect the whole result. Read batches instead:

```python
for batch in con.sql("SELECT * FROM obs").record_batch(50_000):
    process(batch)
```

On the server, a query can spill to disk. The spill goes to the OS temp area. Put that area on fast
storage with free space. See
[Performance Tuning](/docs/2.0.0-rc2/data-lake/performance-tuning).

## The `beacon.db` file

### Can two processes open the same `beacon.db`?

No. Beacon holds a file-backed database under an **exclusive lock**. One process opens one
`beacon.db`. A second `connect()` to the same path *in the same process* shares the open connection.
For access from several processes or machines, run
[Beacon Data Lake](/docs/2.0.0-rc2/getting-started) and connect to it.

### Does a copy of `beacon.db` include my data?

A copy includes everything that Beacon **owns**: the catalog and the managed table data. A copy does
not include the external files. Beacon must still reach those files from the new location. See
[Internal Format](/docs/2.0.0-rc2/beacondb/data-sources/internal-format).

### When should I use a managed table instead of an external table?

Use an external table to read data that you already have. Use a managed table when Beacon owns the
rows. A managed table also gives you `INSERT`, `UPDATE` and `DELETE`. For a cached result with a
periodic refresh, use a
[materialized view](/docs/2.0.0-rc2/beacondb/sql/create-materialized-view).

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
[Access Control](/docs/2.0.0-rc2/security/access-control).

### Why is authentication off by default in BeaconDB?

A local file already gives the user full control. This is the usual contract for an embedded
database. Set `auth=True` to switch access control on.

### My remote table fails with an authentication error

A remote table connects **anonymously** and stores no credentials. The remote server must allow
anonymous Flight SQL access. Set `BEACON_FLIGHT_SQL_ALLOW_ANONYMOUS=true` on the remote server. For
an authenticated connection, use [`ATTACH`](/docs/2.0.0-rc2/beacondb/data-sources/attach). It accepts
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
Use a [crawler](/docs/2.0.0-rc2/data-lake/crawlers) with a schedule to find new files.

## Python

### `.df()` raises an ImportError

The dataframe helpers are optional extras:

```bash
pip install "beacondb[pandas]"       # .df()
pip install "beacondb[polars]"       # .pl()
pip install "beacondb[sqlalchemy]"   # the beacondb:// dialect
pip install "beacondb[all]"          # all of the above
```

`.arrow()` needs no extra.

### Can I use BeaconDB with SQLAlchemy?

Yes. The package includes a `beacondb://` dialect. See
[SQLAlchemy](/docs/2.0.0-rc2/beacondb/python/sqlalchemy).

---

Do you still have a problem? Open an issue on
[GitHub](https://github.com/maris-development/beacon). Or read
[Concepts](/docs/2.0.0-rc2/concepts) for the full picture.
