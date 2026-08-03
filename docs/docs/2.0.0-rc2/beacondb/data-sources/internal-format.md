---
description: The beacon.db file is BeaconDB's own storage. It holds the catalog and any managed table data in one portable container, while still referencing external files and remote systems.
---

# Internal Format (`beacon.db`)

Everything BeaconDB **owns** lives in a single file. Everything else it merely **references**.

```python
con = beacondb.connect("beacon.db")   # one portable file
con = beacondb.connect(":memory:")    # ephemeral, nothing persisted
```

The `beacon.db` file is a single-file container (a redb-backed object store) holding:

- **The catalog**: definitions of your external tables, views, materialized views, attached
  catalogs, and registered schemas.
- **Managed table data**: rows for tables BeaconDB owns and can mutate.
- **Persistent secrets**: credentials, encrypted at rest.

External files are *not* copied into it. A `beacon.db` that references `s3://bucket/obs/*.parquet`
stores the definition, not the data. Copy the file and the catalog travels with it, still pointing at
the same sources.

:::tip In-memory databases
`:memory:` gives each connection its own ephemeral database. A file-backed database is held under an
exclusive lock, so one process opens one `beacon.db`.
:::

## External vs managed tables

This is the main decision when bringing data into BeaconDB:

| | External table | Managed table |
| --- | --- | --- |
| Where the data lives | Your files, untouched | Inside `beacon.db` |
| Who writes it | You, with other tools | BeaconDB |
| Mutations | Read-only | `INSERT`, `UPDATE`, `DELETE` |
| Best for | Existing archives you query | Results, curated subsets, working sets |

Use an [external table](./external-tables.md) to query data you already have. Use a managed table
when you want BeaconDB to own and mutate the rows.

## Managed tables

Managed tables are created like ordinary SQL tables and are backed by the
[Lance](https://lancedb.github.io/lance/) storage engine:

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

They support `ALTER TABLE`, indexes, and the usual DDL. See
[Managed Tables](/docs/2.0.0-rc2/beacondb/sql/managed-tables) in the SQL reference for the full
statement grammar, index types, and limitations.

A common pattern is materializing an expensive scan once, then querying it repeatedly:

```sql
CREATE TABLE hot_profiles AS
SELECT * FROM read_netcdf('argo/**/*.nc') WHERE temperature > 25;
```

For results that need periodic refreshing rather than a one-off copy, a
[materialized view](/docs/2.0.0-rc2/beacondb/sql/create-materialized-view) is usually the better fit.

## Secrets

Credentials for object stores and remote Beacons are stored as named, scoped
[secrets](/docs/2.0.0-rc2/beacondb/sql/secrets). A session secret lives in memory; a
`CREATE PERSISTENT SECRET` is written into `beacon.db` **encrypted** (XChaCha20-Poly1305) and
reloaded when the file is opened. Persisting requires a configured master key, and BeaconDB refuses
to write a plaintext credential to disk.

## Portability

Because the catalog and managed data share one file, moving a database is a file copy. That makes
`beacon.db` useful for shipping a prepared dataset alongside an application, handing a working set to
a colleague, or checkpointing an analysis. What travels is the catalog and owned rows; the external
files it references must still be reachable from wherever the file is opened.
