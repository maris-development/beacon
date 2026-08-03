---
description: Query files on S3, GCS and Azure with Beacon. Store credentials as named secrets, or put the whole datasets store on an S3 bucket.
---

# Object Storage (S3, GCS, Azure)

Beacon reads object storage in the same way as local files. Point a
[`read_*` function](/docs/2.0.0-rc2/beacondb/data-sources/formats/) or an
[external table](/docs/2.0.0-rc2/beacondb/data-sources/external-tables) at a URL. Then query it.
Beacon downloads nothing first. It fetches only the byte ranges that a query needs.

```sql
SELECT * FROM read_parquet('s3://my-bucket/obs/*.parquet') LIMIT 10;
```

Beacon supports three schemes. Use `s3://` for S3 and any S3-compatible store such as MinIO. Use
`gs://` for Google Cloud Storage. Use `az://` for Azure Blob Storage.

## Credentials as secrets

Authenticate with a named [secret](/docs/2.0.0-rc2/beacondb/sql/secrets). A secret keeps your
credentials out of the environment variables and out of your queries:

```sql
CREATE SECRET my_s3 (
  TYPE S3,
  KEY_ID '…', SECRET '…', REGION 'eu-west-1',
  SCOPE 's3://my-bucket'
);

SELECT * FROM read_parquet('s3://my-bucket/obs/*.parquet');   -- uses my_s3
```

`SCOPE` is a URL prefix. The longest match wins. A broad `s3://` secret is therefore the default. A
`s3://my-bucket` secret then overrides it for that bucket. Use `CREATE PERSISTENT SECRET` to store
the secret encrypted inside the `beacon.db` file. It then survives a restart. See
[Secrets](/docs/2.0.0-rc2/beacondb/sql/secrets) for the full syntax, with `TYPE GCS` and
`TYPE AZURE`.

:::tip Public buckets
Anonymous access needs no secret. For a public S3 bucket, set `AWS_SKIP_SIGNATURE=true`. Beacon then
sends unsigned requests.
:::

## Put the datasets store on S3

On Beacon Data Lake you can put the whole **datasets store** on an S3-compatible bucket. Beacon then
finds every file in the bucket. You can query them without DDL:

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

Beacon opens the bucket through the standard AWS environment chain. The same `AWS_*` variables
therefore also apply to an `s3://` URL in an external table. A secret on a specific table wins over
them.

| Variable | Purpose |
| --- | --- |
| `BEACON_S3_DATA_LAKE` | Set to `true` to put the datasets store on object storage. |
| `BEACON_S3_BUCKET` | The bucket name. Required. Beacon never derives it from the endpoint. |
| `AWS_ENDPOINT` | The endpoint URL, for example `https://s3.amazonaws.com` or `http://minio:9000`. |
| `AWS_REGION` | The region. Beacon does not use `AWS_DEFAULT_REGION`. |
| `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` | The credentials, if the store needs them. |
| `AWS_SKIP_SIGNATURE` | Set to `true` for a public or anonymous bucket. |

See [Configuration](/docs/2.0.0-rc2/data-lake/configuration#s3-object-storage) for every option. It
also covers virtual-hosted addressing and plain HTTP endpoints for a local MinIO.

## Performance notes

- **Cloud-optimized formats are faster.** Parquet,
  [Zarr](/docs/2.0.0-rc2/beacondb/data-sources/formats/zarr),
  [Atlas](/docs/2.0.0-rc2/beacondb/data-sources/formats/atlas) and Cloud-Optimized GeoTIFF support
  range requests. Beacon fetches only the chunks that a query needs.
- **NetCDF on object storage supports anonymous access only.** See
  [NetCDF](/docs/2.0.0-rc2/beacondb/data-sources/formats/netcdf) for the details.
- **Use narrow filters.** Predicate and projection pushdown reduce the bytes that Beacon fetches.
  See [Performance Tuning](/docs/2.0.0-rc2/data-lake/performance-tuning).
