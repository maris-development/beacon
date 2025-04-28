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
- `BEACON_HOST` - IP address to listen on.
- `BEACON_PORT` - Port number to listen on.
- `BEACON_SANITIZE_SCHEMAS` - Whether to sanitize schemas or not. Set to `true` to enable sanitization.
- `BEACON_ENABLE_SQL` - Whether to enable SQL support or not. Set to `true` to enable SQL support.

### Optional Environment Variables

- `BEACON_TOKEN=YOUR_TOKEN` - The token to activate extra features for Beacon.
