---
description: Query Parquet, Zarr and other files on S3, GCS or Azure. Use named secrets for credentials, or anonymous access for a public bucket.
---

# Query Data on S3

BeaconDB reads object storage in the same way as local files. Only the path changes.

```sql
SELECT * FROM read_parquet('s3://my-bucket/obs/*.parquet') LIMIT 10;
```

Beacon supports three schemes. Use `s3://` for S3 and any S3-compatible store such as MinIO. Use
`gs://` for Google Cloud Storage. Use `az://` for Azure Blob Storage.

## Public buckets

Anonymous access needs one setting. It tells the client to send unsigned requests:

```bash
export AWS_SKIP_SIGNATURE=true
```

Then query the bucket. This is the fastest way to try BeaconDB on an open dataset.

## Private buckets

Store the credentials once as a named [secret](/docs/2.0.0-rc2/beacondb/sql/secrets). Do not put the
keys in an environment variable or in a query string:

```sql
CREATE SECRET my_s3 (
  TYPE S3,
  KEY_ID '…', SECRET '…', REGION 'eu-west-1',
  SCOPE 's3://my-bucket'
);

SELECT * FROM read_parquet('s3://my-bucket/obs/*.parquet');   -- resolves my_s3
```

`SCOPE` is a URL prefix. The longest match wins. You can therefore set a broad default and override
it for one bucket:

```sql
CREATE SECRET default_s3 (TYPE S3, KEY_ID '…', SECRET '…', SCOPE 's3://');
CREATE SECRET special   (TYPE S3, KEY_ID '…', SECRET '…', SCOPE 's3://restricted-bucket');
```

Make a secret persistent to keep it after a restart. Beacon encrypts it into the `beacon.db` file:

```sql
CREATE PERSISTENT SECRET my_s3 (TYPE S3, KEY_ID '…', SECRET '…', SCOPE 's3://my-bucket');
```

A persistent secret needs a master key. Set it with `BEACON_SECRETS_KEY`, or with `secrets_key=` on
`beacondb.connect`. BeaconDB never writes a plaintext credential to disk.

Use `SHOW SECRETS` to see your secrets. It never returns the values.

## Make it fast

Object storage has a high latency. Fetch as few bytes as possible:

- **Use cloud-optimized formats.** Parquet,
  [Zarr](/docs/2.0.0-rc2/beacondb/data-sources/formats/zarr),
  [Atlas](/docs/2.0.0-rc2/beacondb/data-sources/formats/atlas) and Cloud-Optimized GeoTIFF support
  range requests. Beacon then fetches only the chunks that it needs.
- **Select only the columns that you need.** Projection pushdown turns a narrow `SELECT` into fewer
  bytes.
- **Filter early.** A predicate prunes row groups and chunks before any transfer.
- **Watch NetCDF.** On object storage, NetCDF supports anonymous access only. See
  [NetCDF](/docs/2.0.0-rc2/beacondb/data-sources/formats/netcdf).

## Register a bucket as a table

A stable name is better than a repeated URL. This is the same as with local files:

```sql
CREATE EXTERNAL TABLE remote_obs
STORED AS PARQUET
LOCATION 's3://my-bucket/obs/';
```

You can also serve a whole bucket as the dataset store of a Beacon Data Lake server. See
[Object Storage](/docs/2.0.0-rc2/beacondb/data-sources/object-storage).
