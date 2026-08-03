---
description: Read Atlas array stores with read_atlas(). Statistics-based dataset pruning makes range queries over large collections fast.
---

# Atlas

## Reading

```text
read_atlas(glob_paths)
read_atlas(glob_paths, dimensions)
```

Reads [Atlas](/docs/2.0.0-rc2/beacondb/data-sources/formats/atlas) array stores matching one or more glob patterns. Each path must point at an `atlas.json` marker file, an exact path or a glob such as `**/atlas.json`.

The optional `dimensions` argument filters the arrays to those matching the listed dimension names. Atlas prunes whole datasets using per-column statistics, so range queries over large collections only read the datasets that can match the predicate.

```sql
SELECT * FROM read_atlas('collections/sensor/atlas.json')

-- Combine every Atlas store under a prefix, keeping a subset of dimensions
SELECT time, temperature
FROM read_atlas(['collections/**/atlas.json'], ['time', 'latitude', 'longitude'])
WHERE time >= '2024-01-01'
```

## Inspecting the schema

Before writing a query it is usually worth checking which columns a file actually has, and
what their types are.

`read_schema()` does not cover this format, so inspect it through the reader itself.
A `LIMIT 0` query resolves the schema without returning any rows:

```sql
SELECT * FROM read_atlas('collections/sensor/atlas.json') LIMIT 0;
```

Point at the store's `atlas.json` marker, exactly as you would when querying it.

To go further than names and types, [`SUMMARIZE`](/docs/2.0.0-rc2/beacondb/sql/summarize) profiles every column in one pass, adding
min/max, distinct counts, and the share of nulls:

```sql
SUMMARIZE (SELECT * FROM read_atlas('collections/sensor/atlas.json'));
```

If the files are registered as a table, `DESCRIBE` works directly:

```sql
DESCRIBE sensor_atlas;
```

From Python, the Arrow schema of any relation is available without collecting rows:

```python
con.sql("SELECT * FROM read_atlas('collections/sensor/atlas.json') LIMIT 0").arrow().schema
```

## Format details

[Atlas](https://github.com/maris-development/atlas) is a directory-based array store designed for fast analytical access to multi-dimensional scientific data. Like Parquet or Zarr, it is just another file format: place Atlas stores in the datasets folder and Beacon discovers and queries them automatically, no registration step is required. An Atlas store is a directory containing a single `atlas.json` registry that describes one or more named datasets, each holding its own set of arrays.

What it does:

- **Statistics-based dataset pruning.** Atlas keeps per-dataset, per-column statistics. When a query carries a predicate (e.g. a time or latitude range), Beacon drops whole datasets that cannot match *before reading any array data*, so range queries over large collections only touch the relevant data.
- **Column projection.** Only the arrays referenced by a query are read, keeping I/O proportional to the columns actually selected.
- **Compact, self-describing layout.** Arrays are stored compressed (zstd) and the `atlas.json` registry is opened once and cached for the lifetime of the process, avoiding repeated metadata parsing across queries.
- **Object-store friendly.** Atlas stores can live on local disk or S3-compatible object storage.

Query an Atlas store with the [`read_atlas()`](/docs/2.0.0-rc2/beacondb/sql/table-functions#read-atlas) table function, pointing at its `atlas.json` marker file, an exact path or a glob such as `**/atlas.json`. An optional second argument filters the arrays to those matching the listed dimensions.

```sql
SELECT * FROM read_atlas(['collections/sensor/atlas.json'])
```

### External tables over Atlas

For a stable, reusable table name, register the store as an [External Table](/docs/2.0.0-rc2/beacondb/data-sources/external-tables#atlas). Like Zarr, point the `LOCATION` at the `atlas.json` marker (or a glob over several markers):

```sql
CREATE EXTERNAL TABLE sensor_atlas
STORED AS ATLAS
LOCATION 'collections/sensor/atlas.json';

SELECT time, temperature
FROM sensor_atlas
WHERE time >= '2024-01-01';
```

### Optimizing NetCDF and Zarr with Atlas

Atlas is the recommended way to speed up repeated queries over large NetCDF or Zarr collections: convert the source files into a single Atlas collection. Consolidating many NetCDF or Zarr files into one statistics-aware store lets Beacon prune whole datasets using column statistics and read only the projected arrays, so spatial and temporal range queries are typically much faster than scanning the original files directly.

See the [Atlas repository](https://github.com/maris-development/atlas) for the store format and tooling to build Atlas collections.

:::tip
Heavy, repeated aggregations over an Atlas collection, or any other table, can be cached with a [materialized view](/docs/2.0.0-rc2/beacondb/sql/create-materialized-view) and recomputed with `REFRESH` when the underlying data changes.
:::

## As an external table

Like Zarr, Atlas tables point at the store's `atlas.json` marker file rather than a folder:

```sql
CREATE EXTERNAL TABLE sensor_atlas
STORED AS ATLAS
LOCATION 'collections/sensor/atlas.json'
```

To combine several Atlas stores under one table, use a glob over their markers:

```sql
CREATE EXTERNAL TABLE sensor_atlas
STORED AS ATLAS
LOCATION 'collections/*/atlas.json'
```

See [Atlas](/docs/2.0.0-rc2/beacondb/data-sources/formats/atlas) for what the format does and how it speeds up NetCDF/Zarr workloads.

See [Creating External Tables](/docs/2.0.0-rc2/beacondb/data-sources/external-tables) for the full DDL, and [Reading External Files](/docs/2.0.0-rc2/beacondb/data-sources/) for the general reading model.
