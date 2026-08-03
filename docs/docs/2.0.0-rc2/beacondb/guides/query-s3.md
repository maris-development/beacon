---
description: Query Parquet, Zarr, and other files stored on S3, GCS, or Azure with BeaconDB, using named secrets for credentials and anonymous access for public buckets.
---

# Query Data on S3

BeaconDB reads object storage the same way it reads local files. Only the path changes.

```sql
SELECT * FROM read_parquet('s3://my-bucket/obs/*.parquet') LIMIT 10;
```

Supported schemes are `s3://` (including S3-compatible stores such as MinIO), `gs://` for Google
Cloud Storage, and `az://` for Azure Blob Storage.

## Public buckets

Anonymous access needs no configuration beyond telling the client not to sign requests:

```bash
export AWS_SKIP_SIGNATURE=true
```

Then query the bucket directly. This is the quickest way to try BeaconDB against an open dataset.

## Private buckets

Store credentials once as a named [secret](/docs/2.0.0-rc2/beacondb/sql/secrets) rather than putting keys
in environment variables or query strings:

```sql
CREATE SECRET my_s3 (
  TYPE S3,
  KEY_ID '…', SECRET '…', REGION 'eu-west-1',
  SCOPE 's3://my-bucket'
);

SELECT * FROM read_parquet('s3://my-bucket/obs/*.parquet');   -- resolves my_s3
```

`SCOPE` is a URL prefix and the longest match wins, so you can define a broad default and override it
per bucket:

```sql
CREATE SECRET default_s3 (TYPE S3, KEY_ID '…', SECRET '…', SCOPE 's3://');
CREATE SECRET special   (TYPE S3, KEY_ID '…', SECRET '…', SCOPE 's3://restricted-bucket');
```

To keep a secret across restarts, persist it. It is encrypted into the `beacon.db` file:

```sql
CREATE PERSISTENT SECRET my_s3 (TYPE S3, KEY_ID '…', SECRET '…', SCOPE 's3://my-bucket');
```

This requires a configured master key (`BEACON_SECRETS_KEY`, or `secrets_key=` on
`beacondb.connect`). BeaconDB refuses to write a plaintext credential to disk.

Inspect what is configured with `SHOW SECRETS`, which never returns values.

## Making it fast

Object storage is high-latency, so the goal is to fetch as few bytes as possible:

- **Prefer cloud-optimized formats.** Parquet,
  [Zarr](/docs/2.0.0-rc2/beacondb/data-sources/formats/zarr),
  [Atlas](/docs/2.0.0-rc2/beacondb/data-sources/formats/atlas), and Cloud-Optimized GeoTIFF are built for
  range requests, so only the needed chunks are fetched.
- **Select only the columns you need.** Projection pushdown turns a narrow `SELECT` directly into
  fewer bytes read.
- **Filter early.** Predicates prune row groups and chunks before any data is transferred.
- **Mind NetCDF.** Over object storage, NetCDF currently supports anonymous access only. See
  [NetCDF](/docs/2.0.0-rc2/beacondb/data-sources/formats/netcdf).

## Registering a bucket as a table

As with local files, a stable name beats repeating the URL:

```sql
CREATE EXTERNAL TABLE remote_obs
STORED AS PARQUET
LOCATION 's3://my-bucket/obs/';
```

For serving an entire bucket as the dataset store of a Beacon Data Lake instance, see
[Object Storage](/docs/2.0.0-rc2/beacondb/data-sources/object-storage).
