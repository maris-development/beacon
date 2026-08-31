---
description: Read Atlas array stores with read_atlas(). Dataset pruning with statistics makes range queries over large collections fast.
---

# Atlas

## Read the files

```text
read_atlas(glob_paths)
read_atlas(glob_paths, dimensions)
```

Beacon reads the [Atlas](/docs/2.0.0-rc5/formats/atlas) array stores that match
one or more glob patterns. Each path must point at an `atlas.json` marker file. Give an exact path
or a glob such as `**/atlas.json`.

The optional `dimensions` argument selects the arrays with the listed dimension names. Atlas holds
statistics for each column. Beacon drops whole datasets with those statistics. A range query over a
large collection therefore reads only the datasets that can match the predicate.

```sql
SELECT * FROM read_atlas('collections/sensor/atlas.json')

-- Combine every Atlas store under a prefix, keeping a subset of dimensions
SELECT time, temperature
FROM read_atlas(['collections/**/atlas.json'], ['time', 'latitude', 'longitude'])
WHERE time >= '2024-01-01'
```

## Inspect the schema

Check the columns and the types before you write a query:

```sql
SELECT * FROM read_atlas('collections/sensor/atlas.json') LIMIT 0;
```

[Inspect a schema](/docs/2.0.0-rc5/formats/inspect-a-schema) compares the `_schema` functions,
`SUMMARIZE`, `DESCRIBE` and `LIMIT 0`, and says what each one costs.

## Format details

[Atlas](https://github.com/maris-development/atlas) is an array store in a directory. It gives fast
analytical access to multi-dimensional scientific data. Atlas is a file format, like Parquet or
Zarr. Put an Atlas store in the datasets folder. Beacon then finds it and queries it automatically.
You register nothing. An Atlas store is a directory with one `atlas.json` registry. The registry
describes one or more named datasets. Each dataset holds its own arrays.

What it does:

- **Dataset pruning with statistics.** Atlas keeps statistics for each dataset and each column. A
  query with a predicate, for example a time or latitude range, drops the datasets that cannot
  match. Beacon drops them *before it reads any array data*. A range query over a large collection
  therefore touches only the relevant data.
- **Column projection.** Beacon reads only the arrays that a query names. The I/O stays proportional
  to the selected columns.
- **Compact, self-describing layout.** Atlas compresses the arrays with zstd. Beacon opens the
  `atlas.json` registry once and caches it for the life of the process. It therefore parses the
  metadata only once.
- **Object storage support.** An Atlas store lives on local disk or on S3-compatible object storage.

Query an Atlas store with the
[`read_atlas()`](/docs/2.0.0-rc5/sql/table-functions#read-atlas) table function. Point at
the `atlas.json` marker file. Give an exact path or a glob such as `**/atlas.json`. The optional
second argument selects the arrays with the listed dimensions.

```sql
SELECT * FROM read_atlas(['collections/sensor/atlas.json'])
```

### External tables over Atlas

For a stable table name, register the store as an
[external table](/docs/2.0.0-rc5/data-sources/external-tables#atlas). Point the `LOCATION`
at the `atlas.json` marker, as with Zarr. A glob over several markers also works:

```sql
CREATE EXTERNAL TABLE sensor_atlas
STORED AS ATLAS
LOCATION 'collections/sensor/atlas.json';

SELECT time, temperature
FROM sensor_atlas
WHERE time >= '2024-01-01';
```

### Optimize NetCDF and Zarr with Atlas

Do you query a large NetCDF or Zarr collection often? Then convert the source files into one Atlas
collection. Atlas merges many files into one store with statistics. Beacon can then drop whole
datasets with the column statistics. It reads only the arrays that you select. A spatial or time
range query is therefore much faster than a scan of the original files.

The [Atlas repository](https://github.com/maris-development/atlas) documents the store format. It
also holds the tools that build an Atlas collection.

:::tip
Cache a large, repeated aggregation with a
[materialized view](/docs/2.0.0-rc5/sql/create-materialized-view). This works over an Atlas
collection and over any other table. Run `REFRESH` when the source data changes.
:::

## As an external table

An Atlas table points at the `atlas.json` marker file, not at a folder. This is the same as Zarr:

```sql
CREATE EXTERNAL TABLE sensor_atlas
STORED AS ATLAS
LOCATION 'collections/sensor/atlas.json'
```

Use a glob over the markers to put several Atlas stores in one table:

```sql
CREATE EXTERNAL TABLE sensor_atlas
STORED AS ATLAS
LOCATION 'collections/*/atlas.json'
```

See [Atlas](/docs/2.0.0-rc5/formats/atlas) for the format details. That page
also explains how Atlas speeds up NetCDF and Zarr work.

See [Create External Tables](/docs/2.0.0-rc5/data-sources/external-tables) for the full DDL. See [Data Sources](/docs/2.0.0-rc5/data-sources/) for the
full read model.
