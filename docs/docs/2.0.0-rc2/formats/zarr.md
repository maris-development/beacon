---
description: Read Zarr v3 stores with read_zarr(). Chunk-level predicate pushdown fetches only the chunks that a query needs.
---

# Zarr

## Read the files

```text
read_zarr(glob_paths)
read_zarr(glob_paths, dimensions)
```

Beacon reads the Zarr stores that match one or more glob patterns. Each path must point at a
`zarr.json` entry file.

The optional `dimensions` argument selects the arrays. Beacon returns an array only if the list
holds all of its dimensions. Use the argument to drop arrays with many dimensions.

Predicate pushdown is automatic. Beacon prunes chunks and slices the coordinate dimensions such as
`time`, `latitude` and `longitude`. It uses the `WHERE` clause of your query. You declare no
statistics columns.

```sql
SELECT * FROM read_zarr('sst/*/zarr.json')

-- Range queries are pruned automatically
SELECT time, sst
FROM read_zarr('sst/*/zarr.json')
WHERE time >= '2024-01-01'
```

## Inspect the schema

Check the columns and the types before you write a query:

```sql
SELECT * FROM read_zarr('sst/*/zarr.json') LIMIT 0;
```

[Inspect a schema](/docs/2.0.0-rc2/formats/inspect-a-schema) compares the `_schema` functions,
`SUMMARIZE`, `DESCRIBE` and `LIMIT 0`, and says what each one costs.

## Format details

Beacon queries a Zarr dataset with chunk-level predicate pushdown. It reads only the chunks that can
match your predicates. Spatial and time range queries are therefore fast, also on a large
multi-dimensional store.

- Beacon supports Zarr v3 stores. The `zarr.json` entry file marks such a store. Beacon does not find v2 stores, which use `.zarray`, `.zgroup` and `.zattrs` files.
- Beacon decompresses a compressed chunk automatically. It supports zstd, gzip, blosc and more.
- One external table can hold several Zarr stores. Use a glob pattern such as `sst/*/zarr.json`.
- Beacon fully supports S3 and other object stores.

### Array attributes

A Zarr array holds its attributes in the `attributes` section of its `zarr.json`. Beacon shows these
as extra columns. It uses dot notation: `<array>.<attribute>`. The `units` attribute of the `sst`
array becomes the column `sst.units`.

An attribute column keeps the type from the file: string, integer, float and so on.

A root attribute of the store belongs to no array. Beacon shows it with a leading dot and no array
prefix: `.<attribute>`. The root attribute `Conventions` becomes the column `.Conventions`.

```sql
SELECT sst, "sst.units", "sst.long_name", ".Conventions"
FROM read_zarr(['sst/*/zarr.json'])
LIMIT 1
```

Limitations:

- Beacon does not support user-defined data types.

:::tip
Predicate pushdown is automatic. Beacon prunes chunks and slices the coordinate dimensions such as
`time`, `latitude` and `longitude`. It uses the filters of your query. You configure nothing.

Do you query a collection often? Then convert the Zarr stores into one
[Atlas](/docs/2.0.0-rc2/formats/atlas) collection. Atlas is an array store
with statistics. Beacon can drop whole datasets before it reads a chunk.
:::

## As an external table

A Zarr table must point at a `zarr.json` entry file, not at a folder:

```sql
CREATE EXTERNAL TABLE sst_zarr
STORED AS ZARR
LOCATION 'sst/zarr.json'
```

Use a glob to cover several Zarr stores:

```sql
CREATE EXTERNAL TABLE sst_zarr
STORED AS ZARR
LOCATION 'sst/*/zarr.json'
```

See [Create External Tables](/docs/2.0.0-rc2/data-sources/external-tables) for the full DDL. See [Data Sources](/docs/2.0.0-rc2/data-sources/) for the
full read model.
