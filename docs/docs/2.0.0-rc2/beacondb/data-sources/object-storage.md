---
description: Query files on S3, GCS, and Azure directly with Beacon. Store credentials as named secrets, or back the whole datasets store with an S3-compatible bucket.
---

# Object Storage (S3, GCS, Azure)

Beacon reads from object storage the same way it reads local files: point a
[`read_*` function](/docs/2.0.0-rc2/beacondb/data-sources/formats/) or an
[external table](/docs/2.0.0-rc2/beacondb/data-sources/external-tables) at a URL and query it. Nothing is downloaded up front, only
the byte ranges a query needs are fetched.

```sql
SELECT * FROM read_parquet('s3://my-bucket/obs/*.parquet') LIMIT 10;
```

Supported schemes are `s3://` (and any S3-compatible store such as MinIO), `gs://` for Google Cloud
Storage, and `az://` for Azure Blob Storage.

## Credentials as secrets

The recommended way to authenticate is a named [secret](/docs/2.0.0-rc2/beacondb/sql/secrets), which
keeps credentials out of environment variables and out of your queries:

```sql
CREATE SECRET my_s3 (
  TYPE S3,
  KEY_ID '…', SECRET '…', REGION 'eu-west-1',
  SCOPE 's3://my-bucket'
);

SELECT * FROM read_parquet('s3://my-bucket/obs/*.parquet');   -- uses my_s3
```

`SCOPE` is a URL prefix and the longest match wins, so a broad `s3://` secret acts as the default
while `s3://my-bucket` overrides it for that bucket. Use `CREATE PERSISTENT SECRET` to store it
encrypted inside the `beacon.db` file so it survives restarts. See
[Secrets](/docs/2.0.0-rc2/beacondb/sql/secrets) for the full syntax, including `TYPE GCS` and
`TYPE AZURE`.

:::tip Public buckets
Anonymous access needs no secret at all. For a public S3 bucket, set `AWS_SKIP_SIGNATURE=true` so
requests are sent unsigned.
:::

## Backing the datasets store with S3

On Beacon Data Lake you can put the entire **datasets store** on an S3-compatible bucket, so every
file in the bucket is auto-discovered and queryable without any DDL:

```bash
docker run -d --name beacon -p 5001:5001 \
  -e BEACON_S3_DATA_LAKE=true \
  -e BEACON_S3_BUCKET=your-bucket-name \
  -e AWS_ENDPOINT=https://s3.amazonaws.com \
  -e AWS_REGION=eu-west-1 \
  -e AWS_ACCESS_KEY_ID=your-access-key \
  -e AWS_SECRET_ACCESS_KEY=your-secret-key \
  ghcr.io/maris-development/beacon:latest
```

The bucket is opened through the standard AWS environment chain, so the same `AWS_*` variables also
apply to `s3://` URLs in external tables. A secret attached to a specific table takes precedence.

| Variable | Purpose |
| --- | --- |
| `BEACON_S3_DATA_LAKE` | Set to `true` to use object storage as the datasets store. |
| `BEACON_S3_BUCKET` | Bucket name. Required, and never inferred from the endpoint. |
| `AWS_ENDPOINT` | Endpoint URL, for example `https://s3.amazonaws.com` or `http://minio:9000`. |
| `AWS_REGION` | Region. Note that `AWS_DEFAULT_REGION` is not used. |
| `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` | Credentials, when the store requires them. |
| `AWS_SKIP_SIGNATURE` | Set to `true` for public or anonymous buckets. |

See [Configuration](/docs/2.0.0-rc2/data-lake/configuration#s3-object-storage) for every option, including
virtual-hosted addressing and plain-HTTP endpoints for local MinIO.

## Performance notes

- **Cloud-optimized formats win.** Parquet, [Zarr](/docs/2.0.0-rc2/beacondb/data-sources/formats/zarr),
  [Atlas](/docs/2.0.0-rc2/beacondb/data-sources/formats/atlas), and Cloud-Optimized GeoTIFF are designed
  for range requests, so Beacon fetches only the chunks a query needs.
- **NetCDF over object storage currently supports anonymous access only.** See
  [NetCDF](/docs/2.0.0-rc2/beacondb/data-sources/formats/netcdf) for the details.
- **Narrow your filters.** Predicate and projection pushdown translate directly into fewer bytes
  fetched. See [Performance Tuning](/docs/2.0.0-rc2/data-lake/performance-tuning).
