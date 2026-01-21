# Performance Tuning

## Beacon Query Engine Settings

This chapter focuses on practical performance knobs that are safe to tune in production.

Beacon is built on DataFusion. Most performance tuning comes down to:

- How much parallelism Beacon is allowed to use (threads)
- How much memory is available to the query engine before spilling to disk
- Whether Beacon can avoid unnecessary IO (projection pushdown, caches)

::: tip
All settings below are environment variables. See the full list in [configuration.md](configuration.md).
:::

### CPU and concurrency

#### `BEACON_WORKER_THREADS`

Beacon uses this value to size its Tokio runtime (the executor that runs API requests and query work).

- If you run Beacon on a dedicated machine, start with `BEACON_WORKER_THREADS` ~= number of physical cores.
- If the same host also runs other heavy services, cap this to leave headroom.

For IO-heavy workloads (remote object storage, NetCDF reads over HTTP), more threads can help. For CPU-heavy workloads (aggregations, joins), scaling is limited by CPU.

### Memory and disk spilling

#### `BEACON_VM_MEMORY_SIZE`

This controls the DataFusion memory pool size (in MB). When queries exceed this pool, DataFusion can spill to disk.

- Larger values generally improve performance by reducing spill frequency.
- If you see high disk activity and slow queries under load, increase this first.

::: warning
Spilling uses the OS temp area (DataFusion disk manager). For best performance, ensure your temp directory is on fast storage and has enough free space.
:::

### Avoiding unnecessary reads

#### `BEACON_ENABLE_PUSHDOWN_PROJECTION`

When enabled, Beacon will attempt to project only the columns referenced by your JSON query `select` list when building the scan.

- Set to `true` when you frequently query “wide” datasets but only select a few columns.
- Leave as `false` if you suspect projection bugs or you want the simplest behavior.

### Query language and parsing

#### `BEACON_ENABLE_SQL`

SQL parsing/execution is guarded by this flag.

- Set to `true` to allow SQL queries.
- Keep `false` if you only use the JSON query API and want to reduce exposed surface area.

### Geospatial function cache

#### `BEACON_ST_WITHIN_POINT_CACHE_SIZE`

Controls the cache capacity used by the `ST_WithinPoint` implementation.

- Increase this if you run many repeated point-in-polygon style queries over similar geometries.
- Reduce it if memory pressure is an issue and you don’t benefit from reuse.

### Filesystem and object-store listing

#### `BEACON_ENABLE_FS_EVENTS`

When using local filesystem datasets, enabling filesystem events allows Beacon’s object-store layer to maintain an in-memory view of changes and avoid expensive directory rescans.

- Enable (`true`) when datasets change frequently and you care about fast “list/search datasets” operations.
- Keep disabled if you are on a platform where file watching is noisy/unsupported.

#### `BEACON_S3_DATA_LAKE`, `BEACON_ENABLE_S3_EVENTS`, `BEACON_S3_BUCKET`, `BEACON_S3_ENABLE_VIRTUAL_HOSTING`

These control whether Beacon uses S3-compatible object storage for datasets and how it addresses buckets.

- For performance, prefer placing Beacon close (network-wise) to the object store.
- If you see high latency when listing, consider enabling event-driven updates (`BEACON_ENABLE_S3_EVENTS`) if your S3 backend supports it.

## NetCDF Tuning

NetCDF performance in Beacon is mainly affected by:

- How often Beacon needs to open the file and infer schema
- Whether opened readers/schemas are cached
- Whether NetCDF reads are offloaded to a multi-process worker pool (MPIO mode)

::: tip
NetCDF scans currently read a single Arrow `RecordBatch` per file. If you have extremely large `.nc` files, performance may improve by splitting them into smaller files or converting to chunk-friendly formats (e.g. Zarr), depending on your access pattern.
:::

### Schema cache (fast repeated schema inference)

#### `BEACON_NETCDF_USE_SCHEMA_CACHE` and `BEACON_NETCDF_SCHEMA_CACHE_SIZE`

Beacon discovers an Arrow schema for NetCDF datasets by opening files and inspecting variables/attributes. With schema caching enabled, these discovered schemas are cached in-memory and keyed by:

- the object path
- the object last-modified timestamp

Recommendations:

- Keep `BEACON_NETCDF_USE_SCHEMA_CACHE=true` (default) for most deployments.
- Increase `BEACON_NETCDF_SCHEMA_CACHE_SIZE` when you query many distinct NetCDF files/tables and see repeated schema inference.
- Reduce cache size if memory is constrained and your workload touches only a small working set.

### Reader cache (avoid reopening files)

#### `BEACON_NETCDF_USE_READER_CACHE` and `BEACON_NETCDF_READER_CACHE_SIZE`

With reader caching enabled, Beacon reuses opened NetCDF readers (also keyed by path + last-modified time). This helps when the same files are accessed repeatedly across queries.

Recommendations:

- Keep `BEACON_NETCDF_USE_READER_CACHE=true` (default) for repeated-access workloads.
- Increase `BEACON_NETCDF_READER_CACHE_SIZE` if your “hot set” of NetCDF files is larger than the default (128).
- Disable reader caching if you have extremely high file churn and want to minimize open file handles.

### NetCDF multiplexer (MPIO): multi-process NetCDF reads

#### `BEACON_ENABLE_MULTIPLEXER_NETCDF`

When enabled, Beacon routes NetCDF schema reads and batch reads through a pool of external worker processes (`beacon-arrow-netcdf-mpio`). This can improve throughput by:

- parallelizing NetCDF reads across processes
- isolating blocking NetCDF operations from the main async runtime

Enable this when:

- you have many concurrent queries reading NetCDF
- NetCDF open/read calls are a bottleneck
- you want better isolation from native library behavior

Keep it disabled when:

- you run a minimal deployment and prefer fewer moving parts

#### `BEACON_NETCDF_MULTIPLEXER_PROCESSES`

Controls the number of worker processes in the MPIO pool.

- Default is half of CPU cores.
- Increase for high concurrency / IO-bound workloads.
- Decrease if the workers compete too aggressively with query execution CPU.

#### `BEACON_NETCDF_MPIO_WORKER`

Optional explicit path to the `beacon-arrow-netcdf-mpio` executable.

Use this if:

- the worker is not on `PATH`

#### `BEACON_NETCDF_MPIO_REQUEST_TIMEOUT_MS`

Per-request timeout for calls to the MPIO worker pool.

- Default is `0` (disabled).
- Set this in production to bound tail-latency if you’ve observed stuck or pathological reads.

### Suggested starting points

For a “many users, many NetCDF files” deployment:

- `BEACON_NETCDF_USE_SCHEMA_CACHE=true`
- `BEACON_NETCDF_SCHEMA_CACHE_SIZE=4096`
- `BEACON_NETCDF_USE_READER_CACHE=true`
- `BEACON_NETCDF_READER_CACHE_SIZE=512`
- `BEACON_ENABLE_MULTIPLEXER_NETCDF=true`
- `BEACON_NETCDF_MULTIPLEXER_PROCESSES` ~= `CPU/2` (then tune up/down)

For a small single-user deployment:

- Keep defaults, and only enable the MPIO multiplexer if NetCDF reads dominate runtime.
