---
description: The beacon.db file is the storage of a Beacon server. It holds the catalog and the managed table data in one portable container.
---

# Storage internals (`beacon.db`)

Everything a Beacon server **owns** lives in one file. It only **references** everything else.

You rarely touch this file directly. `BEACON_DATA_DIR` decides where it goes, and the server opens
it at startup. This page explains what is inside it, because that decides which statements change
what, and what a backup has to copy.

The `beacon.db` file is a single-file container. It is an object store on redb. It holds:

- **The catalog**: the definitions of your external tables, views, materialized views, attached
  catalogs and registered schemas.
- **Managed table data**: the rows of the tables that Beacon owns and can change.
- **Persistent secrets**: credentials for another Beacon server, encrypted at rest.

Beacon does *not* copy external files into the container. A `beacon.db` that references `obs/` in
the datasets store holds the definition, not the data. Copy the file and the catalog goes with it.
The copy still points at the same relative paths, so it needs a server whose datasets store has
them.

:::tip One process, one file
Beacon holds the file under an exclusive lock. One process therefore opens one `beacon.db`. Two
servers cannot share a data directory.
:::

## External vs managed tables

This is the main decision when you bring data into Beacon:

| | External table | Managed table |
| --- | --- | --- |
| Where the data lives | In your files, unchanged | Inside `beacon.db` |
| Who writes it | You, with other tools | Beacon |
| Changes | Read-only | `INSERT`, `UPDATE`, `DELETE` |
| Best for | Existing archives that you query | Results, curated subsets, working sets |

Use an [external table](/docs/2.0.0-rc5/data-sources/external-tables) to query data that you already
have. Use a managed table when Beacon owns the rows and changes them.

## Managed tables

Create a managed table like an ordinary SQL table. The
[Lance](https://lancedb.github.io/lance/) storage engine holds the data:

```sql
CREATE TABLE observations AS
SELECT time, latitude, longitude, temperature
FROM read_netcdf('argo/**/*.nc')
WHERE temperature IS NOT NULL;

INSERT INTO observations
SELECT * FROM read_parquet('new_batch.parquet');

UPDATE observations SET platform = 'argo' WHERE platform IS NULL;
DELETE FROM observations WHERE temperature < -5;
```

A managed table supports `ALTER TABLE`, indexes and the usual DDL. See
[Managed Tables](/docs/2.0.0-rc5/sql/managed-tables) in the SQL reference. It gives the full
statement grammar, the index types and the limitations.

A common pattern runs an expensive scan once, then queries the result many times:

```sql
CREATE TABLE hot_profiles AS
SELECT * FROM read_netcdf('argo/**/*.nc') WHERE temperature > 25;
```

Does the result need a periodic refresh instead of one copy? Then use a
[materialized view](/docs/2.0.0-rc5/sql/create-materialized-view).

## Secrets

Beacon holds the credentials for another Beacon server as named
[secrets](/docs/2.0.0-rc5/sql/secrets). A session secret lives in memory. A
`CREATE PERSISTENT SECRET` goes into `beacon.db` **encrypted** with XChaCha20-Poly1305. Beacon
reloads it when it opens the file. A persistent secret needs a master key. Beacon never writes a
plaintext credential to disk.

## Portability

The catalog and the managed data share one file. To move or back up a server, copy that file. The
copy holds the catalog and the owned rows. It does not hold the external files, so the new location
must still reach them.
