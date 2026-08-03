# Performance Tuning

## Beacon Query Engine Settings

This chapter gives the performance settings that are safe to change in production.

Beacon uses DataFusion. Performance tuning covers three points:

- The parallelism that Beacon can use, in threads.
- The memory of the query engine, before a spill to disk.
- The I/O that Beacon can avoid, with projection pushdown and caches.

::: tip
Every setting below is an environment variable. [configuration.md](configuration.md) holds the full list.
:::

### CPU and concurrency

#### `BEACON_WORKER_THREADS`

This value sizes the Tokio runtime of Beacon. That runtime runs the API requests and the query work.

- On a dedicated machine, set `BEACON_WORKER_THREADS` to the number of physical cores.
- On a shared host, set a lower value. Other services then keep enough CPU.

More threads help an I/O-heavy workload, such as a read from object storage or a NetCDF read over
HTTP. A CPU-heavy workload, such as an aggregate or a join, does not scale past the CPU count.

### Memory and disk spilling

#### `BEACON_VM_MEMORY_SIZE`

This value sets the size of the DataFusion memory pool, in MB. A query above this limit spills to
disk.

- A larger value gives better performance, because Beacon spills less often.
- Do you see high disk activity and slow queries under load? Then increase this value first.

::: warning
A spill goes to the OS temp area, through the DataFusion disk manager. Put that directory on fast
storage. It also needs enough free space.
:::

### Avoid unnecessary reads

#### `BEACON_ENABLE_PUSHDOWN_PROJECTION`

With this setting on, Beacon projects only the columns from the `select` list of your JSON query. It
builds the scan with those columns.

- The default is `true`. A query on a wide dataset therefore decodes only the selected columns.
- Set it to `false` only for a suspected projection bug, or to force the simplest scan.

### Query language and parsing

#### `BEACON_ENABLE_SQL`

This flag controls the SQL parser and the SQL execution.

- Set it to `true` to allow SQL queries.
- Keep it `false` if you use the JSON query API only. This reduces the exposed interface.

### Object store listings

#### `BEACON_S3_DATA_LAKE`, `BEACON_S3_BUCKET`, `BEACON_S3_ENABLE_VIRTUAL_HOSTING`

These settings decide if the datasets store lives on an S3-compatible bucket. They also set the
address form. Every listing and every read on object storage costs network latency.

- Put Beacon near the object store in the network. This gives better performance.
- Are the listings slow? Then use a [crawler](/docs/2.0.0-rc2/data-lake/crawlers). It keeps the
  catalog current in the background. Beacon then does not scan at query time.

## NetCDF Tuning

Two points control the NetCDF performance:

- The number of times that Beacon opens the file and infers the schema.
- The cache of the open readers.

::: tip
For a very large `.nc` file, split it into smaller files. You can also convert it to a chunked
format such as Zarr. Your access pattern decides the gain. A scan returns batches of
`BEACON_BATCH_SIZE` rows. The default is `64000`.
:::

### Reader cache (no repeated file open)

#### `BEACON_NETCDF_USE_READER_CACHE` and `BEACON_NETCDF_READER_CACHE_SIZE`

With the reader cache on, Beacon uses an open NetCDF reader again. The cache key is the path and the
last modification time. This helps when several queries read the same files.

Recommendations:

- Keep `BEACON_NETCDF_USE_READER_CACHE=true`, the default, for a workload with repeated access.
- Increase `BEACON_NETCDF_READER_CACHE_SIZE` if your active set of NetCDF files is larger than the
  default of 128.
- Switch the reader cache off if your files change very often. This also keeps the number of open
  file handles low.

### Suggested start values

For a deployment with many NetCDF files:

- `BEACON_NETCDF_USE_READER_CACHE=true`
- `BEACON_NETCDF_READER_CACHE_SIZE=16384`. Match the cache size to your active set of files.

`BEACON_NETCDF_ENABLE_STATISTICS` controls the statistics of each file. Beacon uses them to prune a
query. The default is `true`. Keep it on. Switch it off only to debug the pruning.

## Zarr predicate pushdown

The Zarr reader of Beacon applies predicate pushdown **automatically**. It uses the shared
N-dimensional engine. Beacon reads the filters of your query. It then does two things:

- It prunes the Zarr chunks that cannot match the predicate. It reads only the other chunks.
- It slices the one-dimensional coordinate arrays to your ranges. Examples are `time`, `latitude`
  and `longitude`.

You configure nothing. You declare no `statistics_columns`. You compute no statistics first. Read the
store and filter it. The engine does the rest.

### SQL

```http
POST /api/query
Content-Type: application/json

{
	"sql": "SELECT * FROM read_zarr(['datasets/**/*.zarr/zarr.json']) WHERE valid_time >= '2025-01-01' AND longitude < 30 LIMIT 100",
	"output": { "format": "csv" }
}
```

### JSON

```http
POST /api/query
Content-Type: application/json

{
	"from": {
		"zarr": {
			"paths": ["datasets/**/*.zarr/zarr.json"]
		}
	},
	"select": ["valid_time", "latitude", "longitude"],
	"filters": [
        { "column": "valid_time", "min": "2025-01-01" },
        { "column": "longitude", "min": 15, "max": 30 }
    ],
	"limit": 100,
	"output": { "format": "csv" }
}
```

::: tip
Do you query a collection often? Then convert the Zarr stores into one
[Atlas](/docs/2.0.0-rc2/beacondb/data-sources/formats/atlas) collection. Atlas adds dataset pruning
with statistics, next to the chunk pruning. It drops whole datasets before it reads a chunk.
:::

## Atlas Tuning

Beacon opens an [Atlas](/docs/2.0.0-rc2/beacondb/data-sources/formats/atlas) store through its
`atlas.json` registry. Beacon caches the open Atlas readers. It therefore does not open the same
store for every query.

### Reader cache (no repeated store open)

#### `BEACON_ATLAS_USE_READER_CACHE` and `BEACON_ATLAS_READER_CACHE_SIZE`

With the reader cache on, Beacon uses an open Atlas reader again. It therefore does not parse the
`atlas.json` registry for every query.

Recommendations:

- Keep `BEACON_ATLAS_USE_READER_CACHE=true`, the default, when several queries read the same Atlas
  collections.
- Increase `BEACON_ATLAS_READER_CACHE_SIZE`, default `32`, if you query more Atlas stores than the
  cache holds.
