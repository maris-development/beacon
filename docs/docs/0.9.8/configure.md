::: info
> The configuration options can be specified in the config.toml file or using Environment Variables. If you are using the Docker image, you can find the config.toml file in the /beacon directory of the container.
:::

# Configuration

The configuration options can be specified in the config.toml file or using Environment Variables. If you are using the Docker image, you can find the config.toml file in the /beacon directory of the container.

# Configuration Options in Beacon

Beacon provides several configuration options that allow users to customize the behavior of the tool to their needs.

## ENVIRONMENT VARIABLES

::: warning
Environment variables take precedence over the options specified in the config.toml file. If an environment variable is set, it will override the value specified in the config.toml file.
:::

Some of the configuration options can be set using environment variables. The following environment variables can be used to set the configuration options:

- `ADMIN_USERNAME` - The admin username for the beacon admin panel.
- `ADMIN_PASSWORD` - The admin password for the beacon admin panel.

## Configuration Options that can be specified in the config.toml file

> Environment variables take precedence over the options specified in the config.toml file. If an environment variable is set, it will override the value specified in the config.toml file.

These options are specified in a TOML file called config.toml. To update these options, you can edit the config.toml file directly or use the command line interface to update them.

The default config.toml file:

```toml

### The number of connection threads/workers you want to assign to beacon.
workers = 8

### Port number to listen on
port = 5001

### IP address to listen on
ip_address = "0.0.0.0"

### Log level [debug, info, warning, error, critical]
log_level = "debug"

### The admin username and password for the beacon admin panel.
admin_username = "beacon-admin"
admin_password = "beacon-password"

[runtime_settings]

[runtime_settings.io_settings]
data_directory = "./data/"
### Max arena size in bytes. This is the maximum size of a internal data file that beacon uses.
max_arena_size = "128MiB"
### Max files to read at once during restart iterations to rebuild the index.
restart_chunk_size = 1000

[runtime_settings.processor_settings]
### The maximum number of datasets that can be processed by beacon in a single query. This is to prevent a beacon query from going through possibly petabytes of data.
max_datasets = 150_000

```
