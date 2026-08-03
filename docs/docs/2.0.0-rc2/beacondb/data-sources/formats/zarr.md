---
description: Read Zarr v3 stores with read_zarr(), using chunk-level predicate pushdown so only the chunks that satisfy a query are fetched.
---

# Zarr

## Reading

```text
read_zarr(glob_paths)
read_zarr(glob_paths, dimensions)
```

Reads Zarr stores matching one or more glob patterns. Each path should point at a `zarr.json` entry file.

The optional `dimensions` argument restricts the arrays returned to those whose dimensions are a subset of the provided list, use it to drop high-dimensional arrays you don't need.

Predicate pushdown is automatic: Beacon prunes chunks and slices coordinate dimensions (e.g. `time`, `latitude`, `longitude`) based on the query's `WHERE` clause, no statistics columns need to be declared.

```sql
SELECT * FROM read_zarr('sst/*/zarr.json')

-- Range queries are pruned automatically
SELECT time, sst
FROM read_zarr('sst/*/zarr.json')
WHERE time >= '2024-01-01'
```

## Inspecting the schema

Before writing a query it is usually worth checking which columns a file actually has, and
what their types are.

[`read_schema()`](/docs/2.0.0-rc2/beacondb/sql/table-functions-utility#read-schema) returns the
inferred column names and types **without reading any data**, which makes it the cheapest
option on large collections:

```sql
SELECT * FROM read_schema('sst/*/zarr.json', 'zarr');
```

Point at the store's `zarr.json` marker, exactly as you would when querying it.

Pass a list to see the combined schema across several locations, which is how you spot files
that disagree about a column:

```sql
SELECT * FROM read_schema(['sst/*/zarr.json', 'other/zarr.json'], 'zarr');
```

To go further than names and types, [`SUMMARIZE`](/docs/2.0.0-rc2/beacondb/sql/summarize) profiles every column in one pass, adding
min/max, distinct counts, and the share of nulls:

```sql
SUMMARIZE (SELECT * FROM read_zarr('sst/*/zarr.json'));
```

If the files are registered as a table, `DESCRIBE` works directly:

```sql
DESCRIBE sst_zarr;
```

From Python, the Arrow schema of any relation is available without collecting rows:

```python
con.sql("SELECT * FROM read_zarr('sst/*/zarr.json') LIMIT 0").arrow().schema
```

## Format details

Zarr datasets are queried using chunk-level predicate pushdown, Beacon reads only the chunks that can satisfy the query predicates, which makes spatial and temporal range queries fast even over large multi-dimensional stores.

- Zarr v3 stores are supported, identified by their `zarr.json` entry file. (v2 stores, which use `.zarray`/`.zgroup`/`.zattrs` instead, are not discovered.)
- Compressed chunks (zstd, gzip, blosc, …) are decompressed transparently.
- Multiple Zarr stores can be combined in a single external table using glob patterns (e.g. `sst/*/zarr.json`).
- S3 and other object-store backends are fully supported.

### Array attributes

Zarr arrays store per-array attributes in the `attributes` section of their `zarr.json`. Beacon exposes these as extra columns using dot notation: `<array>.<attribute>`. For example, an array `sst` with a `units` attribute is accessible as the column `sst.units`.

Attribute columns preserve the original type (string, integer, float, …) as stored in the file.

Root-level store attributes (not tied to a specific array) are exposed with a leading dot and no array prefix: `.<attribute>`. For example, a root attribute `Conventions` is accessible as the column `.Conventions`.

```sql
SELECT sst, "sst.units", "sst.long_name", ".Conventions"
FROM read_zarr(['sst/*/zarr.json'])
LIMIT 1
```

Limitations:

- User-defined data types are not supported.

:::tip
Predicate pushdown is automatic, Beacon prunes chunks and slices coordinate dimensions like `time`, `latitude`, and `longitude` based on your query's filters, with nothing to configure.

For collections that are queried repeatedly, convert the Zarr stores into a single [Atlas](/docs/2.0.0-rc2/beacondb/data-sources/formats/atlas) collection, a statistics-aware array store that lets Beacon prune whole datasets before any chunk is read.
:::

## As an external table

Zarr tables should point at `zarr.json` entry files rather than a folder:

```sql
CREATE EXTERNAL TABLE sst_zarr
STORED AS ZARR
LOCATION 'sst/zarr.json'
```

To span multiple Zarr stores with a glob:

```sql
CREATE EXTERNAL TABLE sst_zarr
STORED AS ZARR
LOCATION 'sst/*/zarr.json'
```

See [Creating External Tables](/docs/2.0.0-rc2/beacondb/data-sources/external-tables) for the full DDL, and [Reading External Files](/docs/2.0.0-rc2/beacondb/data-sources/) for the general reading model.
