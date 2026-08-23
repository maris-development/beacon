---
description: Query Parquet, Zarr and other files on the bucket a Beacon server is configured for, or on a public bucket over anonymous access.
---

# Query Data on S3

A Beacon server reads its datasets from one store: a local directory, or one S3-compatible bucket.
The bucket is chosen at startup, not per query.

**Your SQL does not change either way.** Paths are relative to the datasets root:

```sql
SELECT * FROM read_parquet('obs/*.parquet') LIMIT 10;
```

On a local server `obs/` is a directory. On a bucket-backed server it is a key prefix. You write the
same thing.

::: warning A scheme in the path does not work
`read_parquet('s3://my-bucket/obs/*.parquet')` does not reach `my-bucket`. The scheme is ignored and
the string is joined onto the datasets root. Write relative paths.
:::

## Point the server at your bucket

Credentials come from the standard `AWS_*` environment chain:

```bash
docker run -d --name beacon -p 5001:5001 \
  -e BEACON_S3_DATASETS=true \
  -e BEACON_S3_BUCKET=my-bucket \
  -e AWS_ACCESS_KEY_ID=… \
  -e AWS_SECRET_ACCESS_KEY=… \
  -e AWS_REGION=eu-west-1 \
  ghcr.io/maris-development/beacon:latest
```

For a public bucket, drop the keys and set `AWS_SKIP_SIGNATURE=true`.

See [Object Storage](/docs/2.0.0-rc4/data-sources/object-storage) for every setting, and
[Configuration](/docs/2.0.0-rc4/server/configuration) for the full list.

::: info There is no SQL statement for storage credentials
`CREATE SECRET` covers one case. It holds the credentials for **another Beacon server**, which you
reach with [`ATTACH`](/docs/2.0.0-rc4/data-sources/attach). Storage credentials come from the
configuration. A server has one store. It selects that store at startup.
:::

## Make it fast

Object storage has a high latency. Fetch as few bytes as possible:

- **Use cloud-optimized formats.** Parquet,
  [Zarr](/docs/2.0.0-rc4/formats/zarr),
  [Atlas](/docs/2.0.0-rc4/formats/atlas) and Cloud-Optimized GeoTIFF support
  range requests. Beacon then fetches only the chunks that it needs.
- **Select only the columns that you need.** Projection pushdown turns a narrow `SELECT` into fewer
  bytes.
- **Filter early.** A predicate prunes row groups and chunks before any transfer.
- **Watch NetCDF.** On object storage, NetCDF supports anonymous access only. See
  [NetCDF](/docs/2.0.0-rc4/formats/netcdf).

## Register a prefix as a table

A stable name is better than a repeated glob. This is the same as with local files, because the
path is relative either way:

```sql
CREATE EXTERNAL TABLE remote_obs
STORED AS PARQUET
LOCATION 'obs/';
```

See [External Tables](/docs/2.0.0-rc4/data-sources/external-tables).
