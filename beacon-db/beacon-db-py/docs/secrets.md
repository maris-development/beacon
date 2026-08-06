---
description: Give BeaconDB credentials for object stores and remote Beacons as named secrets. A secret lives in the session or in the beacon.db file.
---

# Secrets

Give Beacon the credentials of a cloud object store or a remote Beacon as a named, scoped `SECRET`.
A secret replaces an environment variable. This page uses the shared
[`CREATE SECRET`](/docs/2.0.0-rc2/beacondb/sql/secrets) statement. The notes below cover beacondb
only.

## Object store secrets

```python
con.execute("CREATE SECRET my_s3 (TYPE S3, KEY_ID '…', SECRET '…', "
            "REGION 'eu-west-1', SCOPE 's3://my-bucket')")
con.read_parquet("s3://my-bucket/obs/*.parquet").df()   # resolves my_s3, no env vars

con.sql("SHOW SECRETS").df()      # name, type, scope, option_keys, persistent — never values
con.execute("DROP SECRET my_s3")
```

`TYPE` takes `S3`, `GCS`, `AZURE` or `HTTP`. Without a `SCOPE`, the secret covers the whole backend.
A longer scope overrides it for one bucket. Beacon takes the secret with the longest scope prefix
when it opens a store.

## Session vs. persistent

By default a secret lives in the session only. Beacon holds it in memory for the process. Beacon
writes a `CREATE PERSISTENT SECRET` **into the `beacon.db` file, encrypted** with
XChaCha20-Poly1305. A copy of the file therefore holds its own cloud access:

```python
con = beacondb.connect("beacon.db", secrets_key=…)   # base64 32-byte key (or $BEACON_SECRETS_KEY)
con.execute("CREATE PERSISTENT SECRET my_s3 (TYPE S3, KEY_ID '…', SECRET '…', SCOPE 's3://bucket')")
# reopen later with the same key -> my_s3 is still there
```

A persistent secret needs a **master key**. Set it with `secrets_key=` or with the
`BEACON_SECRETS_KEY` environment variable. BeaconDB **never writes a plaintext credential to disk**.
A persistent secret also needs a file-backed database, not `:memory:`.

## Remote Beacon secrets

A `TYPE BEACON` secret holds the credentials for
[`ATTACH`](/docs/2.0.0-rc2/beacondb/python/remote-catalogs):

```python
con.execute("CREATE SECRET lake (TYPE BEACON, USERNAME 'analyst', PASSWORD '…')")  # or TOKEN '…'
con.attach("lake", "beacon://datalake:50051", secret="lake")
```
