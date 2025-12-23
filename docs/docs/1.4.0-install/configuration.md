# ⚙️ Configuration

::: info
The configuration options can be specified using Environment Variables.
:::

## Configuration Options in Beacon

Beacon provides several configuration options that allow users to customize the behavior of the tool to their needs.

## ENVIRONMENT VARIABLES

Some of the configuration options can be set using environment variables. The following environment variables can be used to set the configuration options:

- `BEACON_ADMIN_USERNAME` - The admin username for the beacon admin panel.
- `BEACON_ADMIN_PASSWORD` - The admin password for the beacon admin panel.
- `BEACON_VM_MEMORY_SIZE` - The amount of memory to allocate to the Beacon Virtual Machine in MB (default is 4096MB).
- `BEACON_DEFAULT_TABLE` - The default table to use when no table is specified in the `from` clause of a query.
- `BEACON_LOG_LEVEL` - Log level [`DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL`].
- `BEACON_HOST` - IP address to listen on. (Default is `0.0.0.0`)
- `BEACON_PORT` - Port number to listen on. (Default is `5001`)
- `BEACON_WORKER_THREADS` - Number of worker threads to use (default is 8).
- `BEACON_ST_WITHIN_POINT_CACHE_SIZE` - Size of the cache for ST_WithinPoint queries (default is 10000).

- `BEACON_S3_DATA_LAKE` - Whether to enable S3 data lake support. Set to `true` to enable. Default is `false` and uses local system file storage.
- `BEACON_S3_ENDPOINT` - The endpoint for the S3-compatible object storage. If `BEACON_S3_DATA_LAKE` is enabled, this should point to the S3-compatible storage.
- `BEACON_S3_REGION` - The region for the S3-compatible object storage. If `BEACON_S3_DATA_LAKE` is enabled, this should match the region of the S3-compatible storage. It can be left empty if using other storage solutions such as MinIO.
- `BEACON_S3_BUCKET` - The bucket name for the S3-compatible object storage. If `BEACON_S3_DATA_LAKE` is enabled, this should point to the S3-compatible bucket.
- `BEACON_S3_ACCESS_KEY_ID` - The access key ID for the S3-compatible object storage. Can be left empty if public access is enabled.
- `BEACON_S3_SECRET_ACCESS_KEY` - The secret access key for the S3-compatible object storage. Can be left empty if public access is enabled.

- `BEACON_ENABLE_SYS_INFO` - Whether to expose system information. Set to `true` to enable.

- `BEACON_CORS_ALLOWED_METHODS` - Comma-separated list of allowed HTTP methods for CORS (default is `GET,POST,PUT,DELETE,OPTIONS`).
- `BEACON_CORS_ALLOWED_ORIGINS` - Comma-separated list of allowed origins for CORS (default is `*`).
- `BEACON_CORS_ALLOWED_HEADERS` - Comma-separated list of allowed headers for CORS (default is `Content-Type,Authorization`).
- `BEACON_CORS_ALLOWED_CREDENTIALS` - Whether to allow credentials for CORS (default is `false`).
- `BEACON_CORS_MAX_AGE` - The maximum age for CORS preflight requests (default is 3600).

### NetCDF

- `BEACON_NETCDF_USE_SCHEMA_CACHE` - Whether to cache discovered NetCDF Arrow schemas in-memory (default is `true`).
- `BEACON_NETCDF_SCHEMA_CACHE_SIZE` - Max number of schema entries to keep in the in-memory schema cache (default is `1024`).
- `BEACON_NETCDF_USE_READER_CACHE` - Whether to cache opened NetCDF readers in-memory (default is `true`).
- `BEACON_NETCDF_READER_CACHE_SIZE` - Max number of reader entries to keep in the in-memory reader cache (default is `128`).

### NetCDF multiplexer (MPIO)

- `BEACON_ENABLE_MULTIPLEXER_NETCDF` - Enable NetCDF multiplexer mode (multi-process worker pool) (default is `false`).
- `BEACON_NETCDF_MULTIPLEXER_PROCESSES` - Number of worker processes to spawn when NetCDF multiplexer is enabled (default is half of CPU cores).
- `BEACON_NETCDF_MPIO_WORKER` - Optional path to the `beacon-arrow-netcdf-mpio` executable (used when NetCDF MPIO is enabled).
- `BEACON_NETCDF_MPIO_REQUEST_TIMEOUT_MS` - Per-request timeout (in milliseconds) for NetCDF MPIO worker requests. Set to `0` to disable (default is `0`).

### Optional Environment Variables

- `BEACON_TOKEN=YOUR_TOKEN` - The token to activate the beacon binary format (this can be freely retrieved for non-commercial use [here](https://beacon.maris.nl/plans)).
