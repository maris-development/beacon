# Configuration

Beacon's data lake can be configured using environment variables. Below is a list of the available configuration options along with their descriptions and default values.

::: info
The configuration options can be specified using Environment Variables.
:::

## Configuration Options in Beacon

Beacon provides several configuration options that allow users to customize the behavior of the tool to their needs.

## ENVIRONMENT VARIABLES

Some of the configuration options can be set using environment variables. The following environment variables can be used to set the configuration options:

- `BEACON_HOST` - IP address to listen on. (Default is `0.0.0.0`)
- `BEACON_PORT` - Port number to listen on. (Default is `5001`)
- `BEACON_BASE_PATH` - Optional URL path prefix to serve the HTTP API, OpenAPI document, and Swagger UI under (default is empty, i.e. served at the root). Useful when running Beacon behind a reverse proxy or on a shared subpath, e.g. `/beacon`. The value is normalized to exactly one leading slash and no trailing slash, so `beacon`, `/beacon`, and `/beacon/` are equivalent. Only URL-safe characters are allowed (letters, digits, `-`, `_`, `.`, `~`, and `/` as a separator); any other character (e.g. spaces, `?`, `#`, `%`) causes Beacon to exit at startup with a descriptive error.
- `BEACON_ADMIN_USERNAME` - The admin username for the beacon admin panel.
- `BEACON_ADMIN_PASSWORD` - The admin password for the beacon admin panel.
- `BEACON_VM_MEMORY_SIZE` - The amount of memory to allocate to the Beacon Virtual Machine in MB (default is 8192MB). More is better for performance, especially when working with larger datasets and performing actions such as spatial joins and group by.
- `BEACON_ENABLE_SQL` - Whether to enable the SQL query engine. Default is `true`.
- `BEACON_WORKER_THREADS` - Number of worker threads to use (default is 8).
- `BEACON_ST_WITHIN_POINT_CACHE_SIZE` - Size of the cache for ST_WithinPoint queries (default is 10000).
- `BEACON_DEFAULT_TABLE` - The default table to use when no table is specified in the `from` clause of a query. Only applicable to the JSON query API, as SQL queries must specify a source. Default is `default`.
- `BEACON_LOG_LEVEL` - Log level [`DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL`]. Default is `INFO`.

- `BEACON_S3_DATA_LAKE` - Whether to enable S3 data lake support. Set to `true` to enable. Default is `false` and uses local system file storage. To connect to S3-compatible object storage, the following environment variables should also be set:
  - `AWS_ACCESS_KEY_ID` -  S3 access_key_id. Only required when the S3-compatible object storage requires authentication.
  - `AWS_SECRET_ACCESS_KEY` - S3 secret_access_key. Only required when the S3-compatible object storage requires authentication.
  - `AWS_DEFAULT_REGION` -  S3 optional region
  - `AWS_ENDPOINT` -  S3 endpoint. This should include the full URL to the S3-compatible object storage service. If the endpoint URL does not contain the bucket name, the bucket name should be specified using the `BEACON_S3_BUCKET` environment variable.
  - `AWS_SKIP_SIGNATURE` - Set to `true` to skip request signing. Useful for local S3-compatible object storage that does not require signed requests.
- `BEACON_S3_BUCKET` - The bucket name for the S3-compatible object storage. If the env variable `BEACON_S3_DATA_LAKE` is enabled, and the `AWS_ENDPOINT` doesn't contain the bucket name, the bucket name should be specified here.

- `BEACON_ENABLE_FS_EVENTS` - Whether to enable file system events monitoring, so new files in watched datasets are picked up automatically. Uses inotify on Linux systems. Default is `true`; set to `false` to disable. This is not supported when using S3 data lake. (Be aware that using mounted volumes with Docker may interfere with filesystem events, so test this setting in your deployment environment.)

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

### Atlas

- `BEACON_ATLAS_USE_READER_CACHE` - Whether to cache opened Atlas store readers in-memory (default is `true`). This avoids re-opening the same `atlas.json` store across queries.
- `BEACON_ATLAS_READER_CACHE_SIZE` - Max number of Atlas reader entries to keep in the in-memory reader cache (default is `32`).

### Beacon Binary Format

- `BEACON_ENABLE_BBF_SPLIT_STREAMS_SLICE` - Whether to enable splitting large batches into smaller batches to better manage memory and parallelism for queries in the Beacon Binary Format (BBF). Set to `true` to enable. Default is `false`. When enabled, this allows for more efficient handling of large queries by splitting the data into multiple streams.
