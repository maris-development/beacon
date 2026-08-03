---
description: CREATE SECRET stores credentials for object stores and remote Beacons as named, scoped secrets. A secret lives in the session or in the beacon.db file.
---

# Secrets

`CREATE SECRET` stores a credential as a **named, scoped** secret. The credential belongs to a cloud
object store or to a remote Beacon. `read_parquet('s3://…')` and
[`ATTACH`](/docs/2.0.0-rc2/beacondb/sql/remote-tables) then find it. You need no environment
variable.

## Object store secrets

```sql
CREATE SECRET my_s3 (
  TYPE S3,
  KEY_ID '…', SECRET '…', REGION 'eu-west-1',
  SCOPE 's3://my-bucket'
);

SELECT * FROM read_parquet('s3://my-bucket/obs/*.parquet');   -- uses my_s3
```

- **`TYPE`**: `S3`, `GCS`, `AZURE`, `HTTP` or `BEACON`. `BEACON` covers a remote Beacon. See below.
- **`SCOPE`**: a URL prefix. The longest scope prefix wins. A broad `s3://` secret is therefore the
  default. A `s3://bucket` secret then overrides it. Without a `SCOPE`, the secret covers the whole
  backend.
- The standard parameter names map to the `object_store` configuration keys. The names include
  `KEY_ID`, `SECRET`, `REGION`, `SESSION_TOKEN` and `ENDPOINT`. A native key also works.

## Inspect and remove

```sql
SHOW SECRETS;                 -- name, type, scope, option keys (never values), persistent
DROP SECRET my_s3;
DROP SECRET IF EXISTS my_s3;
```

## Session vs. persistent

A plain `CREATE SECRET` lives in the **session only**. Beacon holds it in memory. Beacon writes a
`CREATE PERSISTENT SECRET` **into the database file, encrypted** with XChaCha20-Poly1305. Beacon
reloads it when it opens the file:

```sql
CREATE PERSISTENT SECRET my_s3 (TYPE S3, KEY_ID '…', SECRET '…', SCOPE 's3://bucket');
```

A persistent secret needs a **master key** and a file-backed database. Set the key with
`BEACON_SECRETS_KEY`, or with `secrets_key=` in
[`beacondb.connect`](/docs/2.0.0-rc2/beacondb/python/secrets). Beacon **never writes a plaintext
credential to disk**. Beacon stores the name, the type and the scope in the clear. It encrypts the
values.

## Remote Beacon secrets

A `TYPE BEACON` secret holds the credentials for
[`ATTACH`](/docs/2.0.0-rc2/beacondb/sql/remote-tables):

```sql
CREATE SECRET lake (TYPE BEACON, USERNAME 'analyst', PASSWORD '…');   -- or TOKEN '…'
ATTACH 'beacon://datalake:50051' AS lake WITH ('secret' 'lake');
```
