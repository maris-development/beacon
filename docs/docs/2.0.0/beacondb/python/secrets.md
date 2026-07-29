---
description: Give BeaconDB credentials for object stores (S3/GCS/Azure) and remote Beacons as named secrets, session-only or persisted encrypted into the beacon.db file.
---

# Secrets

Give Beacon credentials for a cloud object store or a remote Beacon as a named, scoped
`SECRET`, instead of environment variables. This is the shared [`CREATE SECRET`](/docs/2.0.0/beacondb/sql/secrets)
statement; the notes here are what's specific to beacondb.

## Object-store secrets

```python
con.execute("CREATE SECRET my_s3 (TYPE S3, KEY_ID '…', SECRET '…', "
            "REGION 'eu-west-1', SCOPE 's3://my-bucket')")
con.read_parquet("s3://my-bucket/obs/*.parquet").df()   # resolves my_s3, no env vars

con.sql("SHOW SECRETS").df()      # name, type, scope, option_keys, persistent — never values
con.execute("DROP SECRET my_s3")
```

`TYPE` is `S3`/`GCS`/`AZURE`/`HTTP`; `SCOPE` defaults to the whole backend and a longer scope overrides
it per bucket. The best-matching secret (longest scope prefix) supplies credentials when a store is
built.

## Session vs. persistent

By default a secret is session-only (in memory for the process). A `CREATE PERSISTENT SECRET` is
written **into the `beacon.db` file, encrypted** (XChaCha20-Poly1305), so a copied file carries its
own cloud access:

```python
con = beacondb.connect("beacon.db", secrets_key=…)   # base64 32-byte key (or $BEACON_SECRETS_KEY)
con.execute("CREATE PERSISTENT SECRET my_s3 (TYPE S3, KEY_ID '…', SECRET '…', SCOPE 's3://bucket')")
# reopen later with the same key -> my_s3 is still there
```

Persisting requires a **master key** (`secrets_key=` or the `BEACON_SECRETS_KEY` env var), BeaconDB
**refuses to write a plaintext credential to disk**: and a file-backed database (not `:memory:`).

## Remote-Beacon secrets

A `TYPE BEACON` secret stores the credentials for [`ATTACH`](/docs/2.0.0/beacondb/python/remote-catalogs):

```python
con.execute("CREATE SECRET lake (TYPE BEACON, USERNAME 'analyst', PASSWORD '…')")  # or TOKEN '…'
con.attach("lake", "beacon://datalake:50051", secret="lake")
```
