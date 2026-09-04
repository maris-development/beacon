---
description: CREATE SECRET stores the credentials for ATTACH to a remote Beacon server as a named secret. A secret lives in the session or encrypted in the beacon.db file.
---

# Secrets

`CREATE SECRET` stores a credential under a name. [`ATTACH`](/docs/2.0.0-rc5/data-sources/attach)
then uses that name to reach a remote Beacon server. The credential never appears in the statement
that uses it, and never appears in a log.

::: info Storage credentials are not secrets
A server reads its datasets from **one** store: either a local directory or **one** S3-compatible
bucket. That store is chosen at startup, by
[configuration](/docs/2.0.0-rc5/server/configuration), and it takes its credentials from the
standard `AWS_*` environment chain. There is nothing to declare in SQL. See
[Object Storage](/docs/2.0.0-rc5/data-sources/object-storage).

A secret covers the one case that the configuration cannot. It holds the credentials for **another
Beacon server**. You choose that server for each query, not at startup.
:::

## Create a secret

```sql
CREATE SECRET wod (TYPE BEACON, USERNAME 'analyst', PASSWORD '…');
```

Use a bearer token instead of a username and password:

```sql
CREATE SECRET wod (TYPE BEACON, TOKEN '…');
```

## Use it

Name the secret in the `ATTACH` statement:

```sql
CREATE SECRET wod (TYPE BEACON, USERNAME 'analyst', PASSWORD '…');
ATTACH 'beacon://wod.example.org:50051' AS wod WITH ('secret' 'wod');

SELECT count(*) FROM wod.profiles;
```

The secret name and the catalog alias are independent. They are the same above only for
readability.

## Inspect and remove

```sql
SHOW SECRETS;                 -- name, type, option keys (never values), persistent
DROP SECRET wod;
DROP SECRET IF EXISTS wod;
```

`SHOW SECRETS` never returns a credential value.

## Session or persistent

A plain `CREATE SECRET` lives in the **session only**. Beacon holds it in memory and forgets it when
the session ends.

`CREATE PERSISTENT SECRET` writes it **into `beacon.db`, encrypted** with XChaCha20-Poly1305. Beacon
reloads it when it opens the file:

```sql
CREATE PERSISTENT SECRET wod (TYPE BEACON, USERNAME 'analyst', PASSWORD '…');
```

A persistent secret needs a **master key**. Set the `BEACON_SECRETS_KEY` environment variable to
base64 of 32 bytes. `CREATE PERSISTENT SECRET` fails without the key. It never writes a plaintext
credential.

Beacon stores the name and the type in the clear. It encrypts the values.

::: warning Key changes
Beacon decrypts a persistent secret with the same key only. Do you change `BEACON_SECRETS_KEY`? Then
drop each persistent secret. Create each one again under the new key.
:::

## Next

- [ATTACH](/docs/2.0.0-rc5/data-sources/attach): mirror a remote catalog
- [Remote Tables](/docs/2.0.0-rc5/sql/remote-tables): one remote table instead of a whole catalog
- [Access Control](/docs/2.0.0-rc5/security/access-control): who may run `CREATE SECRET`
