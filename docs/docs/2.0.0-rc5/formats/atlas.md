---
description: Read Atlas collections with read_atlas(). One file holds thousands of datasets, and their statistics let a range query skip whole datasets before it reads them.
---

# Atlas

## Read the files

```text
read_atlas(glob_paths)
read_atlas(glob_paths, dimensions)
```

Beacon reads the [Atlas](https://github.com/maris-development/atlas) collections that match one or
more glob patterns. A collection is one file, `data.atlas`, so a path names that file. Give an
exact path or a glob such as `**/data.atlas`.

The optional `dimensions` argument keeps the arrays whose dimensions are all in the list. Use it to
drop the wide grids of a collection and keep its coordinates.

```sql
SELECT * FROM read_atlas('collections/sensor/data.atlas')

-- Combine every collection under a prefix, keeping a subset of dimensions
SELECT time, temperature
FROM read_atlas(['collections/**/data.atlas'], ['time', 'latitude', 'longitude'])
WHERE time >= '2024-01-01'
```

## Inspect the schema

Check the columns and the types before you write a query:

```sql
SELECT * FROM read_atlas('collections/sensor/data.atlas') LIMIT 0;
```

[Inspect a schema](/docs/2.0.0-rc5/formats/inspect-a-schema) compares the `_schema` functions,
`SUMMARIZE`, `DESCRIBE` and `LIMIT 0`, and says what each one costs.

## Format details

Atlas keeps thousands of N-dimensional datasets in one immutable file. A dataset holds what a
NetCDF file holds: named arrays that share dimensions, plus attributes. A collection holds many:

```text
my_collection/
├── data.atlas      one segment per variable, then a footer describing them all
└── deleted.mask    optional: the datasets a delete has hidden
```

The file stores one segment per **variable**, not one per dataset. A segment holds one array name
across the whole collection, and each dataset's copy sits inside it. Three properties follow, and
they are the point of the format:

- **The catalogue is one read.** Opening a collection reads its footer and nothing else. Listing
  the datasets and asking what each declares are then free. Ten datasets and a million cost the
  same.
- **One variable is one read.** Everything else about a column — its shape, its statistics, its
  attribute values — sits in that variable's segment, and one open answers for every dataset in the
  collection.
- **Data arrives block by block.** Reading a region of an array fetches only the blocks that region
  overlaps, and a block holds one type for a run of neighbouring datasets, so it compresses well.

What Beacon does with that:

- **Dataset pruning.** Every array records its minimum, its maximum and its null count. A query
  with a predicate — a time or latitude range, say — judges every dataset of a collection in one
  vectorised pass and never opens the ones that cannot match. Judging a column costs one request,
  whatever the dataset count. A dataset-level attribute is exact, so `WHERE ".platform" = 'p3'`
  prunes on it too.
- **One dataset is one unit of work.** A collection's datasets are spread across every core, and a
  worker takes the next one when it is free, so a collection of a million small datasets and one of
  four large ones both divide evenly. A dataset stored in several chunks divides further, so a
  single large dataset still uses every core.
- **Column projection.** Only the arrays a query names get read, and only their attributes are
  fetched.
- **Object storage.** A collection reads from local disk, S3, GCS, Azure and HTTP alike.

### Columns

| Atlas | Column |
| --- | --- |
| array `temperature` | `temperature` |
| attribute `units` of `temperature` | `temperature.units` |
| dataset attribute `platform` | `.platform` |

An attribute holds a number, a string or a boolean. Atlas stores no timestamp attribute, so a date
kept as an attribute arrives as the number or the string it was written as.

The leading dot on a dataset attribute is what NetCDF and Zarr use too, and it keeps an attribute
from colliding with an array of the same name. Quote such a column: `SELECT ".platform"`.

```sql
SELECT temperature, "temperature.units", ".platform"
FROM read_atlas('collections/sensor/data.atlas')
LIMIT 1
```

### Types and decoding

Atlas stores its own types, including a native nanosecond timestamp, so **Beacon applies no CF
decoding to a collection**. The ingest path does it instead: `atlas create` reads each NetCDF file
with xarray, which applies `scale_factor`, `add_offset` and the CF time units before the write. An
array therefore reads back exactly as it is stored. This is the one place Atlas differs from
[NetCDF](/docs/2.0.0-rc5/formats/netcdf) and [Zarr](/docs/2.0.0-rc5/formats/zarr) — see
[CF decoding](/docs/2.0.0-rc5/cf-decoding).

A cell nobody wrote reads as the array's fill value, and the fill reads as null. Two consequences
are worth knowing:

- A float array ingested from NetCDF carries a `NaN` fill, and `NaN` never equals itself, so a
  `NaN` cell reads as `NaN` rather than as null. That is the same rule every Beacon format follows.
- A string array carries an empty-string fill, so an empty string reads as null. The ingest cannot
  store a null string, so this mirrors what was written.

Not readable as columns: a `Bool` array, a `List` or `FixedSizeList` array, and a list-valued
attribute. Each is dropped from the schema rather than failing the query.

### Two datasets that disagree

Atlas reconciles nothing: two datasets may declare one array name with two types, and it stores
each as declared. Beacon merges them the way it merges the files of any other format. Two numeric types widen to one that holds
both. Two different families — a number and a string — refuse the table by name:

```text
Incompatible types for field 'value': Utf8 in 'obs/data.atlas#a' vs Int64 in 'obs/data.atlas#b'
```

Set `BEACON_TYPE_WIDENING_ON_CONFLICT=keep_first` to take the first dataset's type instead. See
[Configuration](/docs/2.0.0-rc5/server/configuration#query-engine).

### Building a collection

`pip install atlas-python` gives the `atlas` command. Point it at a directory of NetCDF files:

```bash
atlas create /data/argo /collections/argo
```

That writes `/collections/argo/data.atlas`, one dataset per file, named after the file. It works
against a local path and against a bucket. See the
[Atlas documentation](https://github.com/maris-development/atlas).

:::warning Collections written before Atlas 0.17
Beacon reads container format version 8, which Atlas 0.17 writes. An older collection — a v1
container from Atlas 0.16, or the directory of per-array files behind an `atlas.json` registry that
came before it — is not read. Rewrite it with `atlas create`, then point at the `data.atlas` it
produces.
:::

### Optimize NetCDF and Zarr with Atlas

Do you query a large NetCDF or Zarr collection often? Then convert the source files into one Atlas
collection. Atlas merges many files into one container with statistics, so Beacon drops whole
datasets before it reads an array. A spatial or time range query is therefore much faster than a
scan of the original files.

:::tip
Cache a large, repeated aggregation with a
[materialized view](/docs/2.0.0-rc5/sql/create-materialized-view). This works over an Atlas
collection and over any other table. Run `REFRESH` when the source data changes.
:::

## As an external table

An Atlas table points at the `data.atlas` file itself, not at the folder around it:

```sql
CREATE EXTERNAL TABLE sensor_atlas
STORED AS ATLAS
LOCATION 'collections/sensor/data.atlas'
```

Use a glob to put several collections in one table:

```sql
CREATE EXTERNAL TABLE sensor_atlas
STORED AS ATLAS
LOCATION 'collections/*/data.atlas'
```

See [Create External Tables](/docs/2.0.0-rc5/data-sources/external-tables) for the full DDL. See
[Data Sources](/docs/2.0.0-rc5/data-sources/) for the full read model.

### `OPTIONS`

`STORED AS ATLAS` reads four keys:

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `read_dimensions` | List of dimension names | The default grid of each dataset | The dimensions the table reads. An array survives only when the list holds every one of its own. |
| `use_pruning` | Boolean | `true` (`BEACON_ATLAS_USE_PRUNING`) | Drop the datasets a predicate rules out before reading them. Turning it off only costs speed: pruning never changes an answer. |
| `use_reader_cache` | Boolean | `true` (`BEACON_ATLAS_USE_READER_CACHE`) | Reuse an opened collection across queries. |
| `enable_statistics` | Boolean | `true` (`BEACON_ATLAS_ENABLE_STATISTICS`) | Whether `ANALYZE FILES` records this collection's column ranges. A query never measures a collection, so this affects the analyzer alone. |

```sql
CREATE EXTERNAL TABLE sensor_atlas
STORED AS ATLAS
LOCATION 'collections/*/data.atlas'
OPTIONS ('read_dimensions' 'time,lat,lon')
```

See [`OPTIONS`](/docs/2.0.0-rc5/sql/create-external-table#options) for the rules that hold for every
key. See [Arrays to tables](/docs/2.0.0-rc5/arrays-to-tables#the-dimensions-argument) for the grid
rule.
