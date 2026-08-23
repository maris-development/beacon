---
description: A Beacon server reads its datasets from one store, a local directory or one S3-compatible bucket. Queries use the same relative paths either way.
---

# Object Storage (S3)

A Beacon server reads its datasets from **exactly one store**. That store is either:

- a **local directory**, or
- a **single S3-compatible bucket**.

You choose which at startup. A server does not mix the two, does not switch per query, and does not
reach a second bucket.

## Queries do not name the store

This is the part that matters when you write SQL. **Every path in a query is relative to the
datasets root.** The same query text works on a local server and on a bucket-backed one:

```sql
SELECT * FROM read_parquet('obs/*.parquet') LIMIT 10;
```

On a local server, `obs/` is a directory. On a bucket, `obs/` is a key prefix. The query text does
not change. The client does not know which one it reads.

::: warning Do not put a scheme in the path
`read_parquet('s3://my-bucket/obs/*.parquet')` does **not** reach `my-bucket`. On a server the
scheme is ignored and the whole string is joined onto the datasets root, which gives a path that
matches nothing.

Write relative paths. Always.
:::

The same holds for [external tables](/docs/2.0.0-rc4/data-sources/external-tables):

```sql
CREATE EXTERNAL TABLE ocean_profiles
STORED AS PARQUET
LOCATION 'obs/';
```

This design is deliberate. An operator decides where the bytes live. That decision happens once, in
the configuration. Every client then gets the same names and the same paths. A query on a test
server runs without change on the production server. You can move an archive from disk to a bucket.
No saved query breaks.

## Point a server at a bucket

Set the bucket and give credentials through the standard `AWS_*` environment chain:

```bash
docker run -d --name beacon -p 5001:5001 \
  -e BEACON_S3_DATASETS=true \
  -e BEACON_S3_BUCKET=your-bucket-name \
  -e AWS_ENDPOINT=https://s3.amazonaws.com \
  -e AWS_REGION=eu-west-1 \
  -e AWS_ACCESS_KEY_ID=your-access-key \
  -e AWS_SECRET_ACCESS_KEY=your-secret-key \
  ghcr.io/maris-development/beacon:latest
```

Beacon then finds every file in that bucket automatically, exactly as it does in a local directory.

| Variable | Purpose |
| --- | --- |
| `BEACON_S3_DATASETS` | Set to `true` to put the datasets store on object storage. Replaces `BEACON_S3_DATA_LAKE`, which still works. |
| `BEACON_S3_BUCKET` | The bucket name. Required. Beacon never derives it from the endpoint. |
| `AWS_ENDPOINT` | The endpoint URL, for example `https://s3.amazonaws.com` or `http://minio:9000`. |
| `AWS_REGION` | The region. Beacon does not use `AWS_DEFAULT_REGION`. |
| `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` | The credentials, if the store needs them. |
| `AWS_SKIP_SIGNATURE` | Set to `true` for a public or anonymous bucket. |

Beacon exits at startup when `BEACON_S3_DATASETS` has no bucket. It does not wait for the first
query to fail.

See [Configuration](/docs/2.0.0-rc4/server/configuration#s3-object-storage) for every option. It
also covers virtual-hosted addressing and plain HTTP endpoints for a local MinIO.

::: info There is no SQL statement for storage credentials
Credentials belong to the store. You choose the store at startup. The credentials therefore come
from the environment. [`CREATE SECRET`](/docs/2.0.0-rc4/sql/secrets) covers one case only. It holds
the credentials for **another Beacon server**, which you reach with
[`ATTACH`](/docs/2.0.0-rc4/data-sources/attach).
:::

## Data that a server does not own

A server has one store. To read data elsewhere, query the **server** that owns it:

```sql
ATTACH 'beacon://wod.example.org:50051' AS wod;

SELECT * FROM wod."easy-wod" LIMIT 10;
```

The remote server runs its own scan. It then streams the result back. See
[ATTACH](/docs/2.0.0-rc4/data-sources/attach) and
[Remote Tables](/docs/2.0.0-rc4/sql/remote-tables).

## Performance notes

- **Cloud-optimized formats are faster.** Parquet,
  [Zarr](/docs/2.0.0-rc4/formats/zarr),
  [Atlas](/docs/2.0.0-rc4/formats/atlas) and Cloud-Optimized GeoTIFF support
  range requests. Beacon fetches only the chunks that a query needs.
- **NetCDF and HDF5 need an anonymous bucket on the netCDF-c reader.** netCDF-c opens a file by URL
  and does not use the credential chain. Keep the default `BEACON_NETCDF_USE_RUST_READER=true` and
  `BEACON_HDF5_USE_RUST_READER=true`, or set `AWS_SKIP_SIGNATURE=true`. The pure-Rust reader fetches
  byte ranges through the object store, so a private bucket works and no local copy is made. See
  [NetCDF](/docs/2.0.0-rc4/formats/netcdf) and [HDF5](/docs/2.0.0-rc4/formats/hdf5).
- **A large bucket is slow to list.** Register a [crawler](/docs/2.0.0-rc4/server/crawlers). It
  builds the file list before a query needs it.
- **Use narrow filters.** Predicate and projection pushdown reduce the bytes Beacon fetches. See
  [Performance Tuning](/docs/2.0.0-rc4/server/performance-tuning).
