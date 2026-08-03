---
description: The beacon.db file is the storage of BeaconDB. It holds the catalog and the managed table data in one portable container.
---

# Internal Format (`beacon.db`)

Everything that BeaconDB **owns** lives in one file. BeaconDB only **references** everything else.

```python
con = beacondb.connect("beacon.db")   # one portable file
con = beacondb.connect(":memory:")    # ephemeral, nothing persisted
```

The `beacon.db` file is a single-file container. It is an object store on redb. It holds:

- **The catalog**: the definitions of your external tables, views, materialized views, attached
  catalogs and registered schemas.
- **Managed table data**: the rows of the tables that BeaconDB owns and can change.
- **Persistent secrets**: credentials, encrypted at rest.

Beacon does *not* copy external files into the container. A `beacon.db` that references
`s3://bucket/obs/*.parquet` holds the definition, not the data. Copy the file and the catalog goes
with it. The copy still points at the same sources.

:::tip In-memory databases
`:memory:` gives each connection its own ephemeral database. Beacon holds a file-backed database
under an exclusive lock. One process therefore opens one `beacon.db`.
:::

## External vs managed tables

This is the main decision when you bring data into BeaconDB:

| | External table | Managed table |
| --- | --- | --- |
| Where the data lives | In your files, unchanged | Inside `beacon.db` |
| Who writes it | You, with other tools | BeaconDB |
| Changes | Read-only | `INSERT`, `UPDATE`, `DELETE` |
| Best for | Existing archives that you query | Results, curated subsets, working sets |

Use an [external table](./external-tables.md) to query data that you already have. Use a managed
table when BeaconDB owns the rows and changes them.

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
[Managed Tables](/docs/2.0.0-rc2/beacondb/sql/managed-tables) in the SQL reference. It gives the full
statement grammar, the index types and the limitations.

A common pattern runs an expensive scan once, then queries the result many times:

```sql
CREATE TABLE hot_profiles AS
SELECT * FROM read_netcdf('argo/**/*.nc') WHERE temperature > 25;
```

Does the result need a periodic refresh instead of one copy? Then use a
[materialized view](/docs/2.0.0-rc2/beacondb/sql/create-materialized-view).

## Secrets

Beacon holds the credentials for object stores and remote Beacons as named, scoped
[secrets](/docs/2.0.0-rc2/beacondb/sql/secrets). A session secret lives in memory. A
`CREATE PERSISTENT SECRET` goes into `beacon.db` **encrypted** with XChaCha20-Poly1305. Beacon
reloads it when it opens the file. A persistent secret needs a master key. BeaconDB never writes a
plaintext credential to disk.

## Portability

The catalog and the managed data share one file. To move a database, copy that file. This makes
`beacon.db` useful in three cases. You can ship a prepared dataset with an application. You can give
a working set to a colleague. You can checkpoint an analysis. The copy holds the catalog and the
owned rows. Beacon must still reach the external files from the new location.
