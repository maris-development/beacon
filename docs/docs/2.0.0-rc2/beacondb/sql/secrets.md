---
description: CREATE SECRET stores credentials for object stores (S3/GCS/Azure) and remote Beacons as named, scoped secrets, session or persisted encrypted into the beacon.db file.
---

# Secrets

`CREATE SECRET` stores credentials, for a cloud object store or a remote Beacon, as a **named,
scoped** secret, so `read_parquet('s3://…')` and [`ATTACH`](/docs/2.0.0-rc2/beacondb/sql/remote-tables) resolve
them without environment variables.

## Object-store secrets

```sql
CREATE SECRET my_s3 (
  TYPE S3,
  KEY_ID '…', SECRET '…', REGION 'eu-west-1',
  SCOPE 's3://my-bucket'
);

SELECT * FROM read_parquet('s3://my-bucket/obs/*.parquet');   -- uses my_s3
```

- **`TYPE`**: `S3`, `GCS`, `AZURE`, `HTTP`, or `BEACON` (remote Beacon; see below).
- **`SCOPE`**: a URL prefix; the best match (longest scope prefix) wins, so a broad `s3://` secret is
  the default and `s3://bucket` overrides it. Omitted, it defaults to the whole backend.
- The standard parameter names (`KEY_ID`, `SECRET`, `REGION`, `SESSION_TOKEN`, `ENDPOINT`, …) map to
  the underlying `object_store` config keys; any native key also works.

## Inspect and remove

```sql
SHOW SECRETS;                 -- name, type, scope, option keys (never values), persistent
DROP SECRET my_s3;
DROP SECRET IF EXISTS my_s3;
```

## Session vs. persistent

A plain `CREATE SECRET` is **session-only** (held in memory). `CREATE PERSISTENT SECRET` is written
**into the database file, encrypted** (XChaCha20-Poly1305), and reloaded on open:

```sql
CREATE PERSISTENT SECRET my_s3 (TYPE S3, KEY_ID '…', SECRET '…', SCOPE 's3://bucket');
```

Persisting requires a configured **master key** (`BEACON_SECRETS_KEY`, or `secrets_key=` in
[`beacondb.connect`](/docs/2.0.0-rc2/beacondb/python/secrets)) and a file-backed database, Beacon **refuses to
write a plaintext credential to disk**. Only the name/type/scope are stored in the clear; the values
are encrypted.

## Remote-Beacon secrets

A `TYPE BEACON` secret stores the credentials for [`ATTACH`](/docs/2.0.0-rc2/beacondb/sql/remote-tables):

```sql
CREATE SECRET lake (TYPE BEACON, USERNAME 'analyst', PASSWORD '…');   -- or TOKEN '…'
ATTACH 'beacon://datalake:50051' AS lake WITH ('secret' 'lake');
```
