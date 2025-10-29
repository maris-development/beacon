# Datasets

All datasets intended for querying through Beacon must be placed in the following directory inside the Docker container: `/beacon/data/datasets/`
No other configuration is required to make the datasets available for querying. Beacon will be able to infer the schema (columns,variables,attributes) of the dataset and make it available for querying.

## Supported File Formats

Beacon supports the following file formats for querying datasets:

- ### NetCDF

Beacon supports NetCDF but limited to native data types. User defined types are not supported.

- NetCDF4 (recommended)
- NetCDF3
  - char* arrays with a string like dimension (eg. STRLEN) will be inferred as a fixed size string

- ### Parquet

Beacon supports parquet natively through datafusion.

Limitations:

- Hive partioning is not supported.

- ### CSV

Beacon supports CSV files with the following limitations:
  
- The first row of the CSV file must contain the column names. (Header Row)
- The CSV file must be well-formed and properly formatted.
- The CSV file must be encoded in UTF-8.
- Beacon will infer the schema based on the entire CSV file, this can be optionally changed in the configuration.

- ### ODV ASCII

Fully supported. It is recommended to store the ODV ASCII files using zstd compression. This can be done using the `zstd` command line tool:

```bash
zstd -9 < input.txt > output.txt.zst
```

Beacon will automatically detect the compression and decompress the file on the fly. This also works when ODV ASCII is stored in cloud compatible storage.

- ### Arrow IPC

Fully supported.

- ### Beacon Binary Format (Standard Edition only)

Fully supported. Learn more more about this format and its usage. (Works only with the Standard Edition of Beacon.)
It fully supports S3 data lake integration and allows for efficient pruning similar to parquet.

## Mounting Datasets into the Docker Container

If datasets are stored on the host machine, they need to be mounted into the container. This can be done when starting the Docker container:

```yaml [docker-compose.community.yml] {18-19}
version: "3.8"

services:
    beacon:
        image: ghcr.io/maris-development/beacon:community-nightly-latest
        container_name: beacon
        restart: unless-stopped
        ports:
            - "8080:8080" # Adjust the port mapping as needed
        environment:
            - BEACON_ADMIN_USERNAME=admin # Replace with your admin username
            - BEACON_ADMIN_PASSWORD=securepassword # Replace with your admin password
            - BEACON_VM_MEMORY_SIZE=4096 # Adjust memory allocation as needed (in MB)
            - BEACON_DEFAULT_TABLE=default # Set default table name
            - BEACON_LOG_LEVEL=INFO # Adjust log level
            - BEACON_HOST=0.0.0.0 # Set IP address to listen on
            - BEACON_PORT=8080 # Set port number
        volumes:
            - ./data/datasets:/beacon/data/datasets # Adjust the volume mapping as required
```

## Managing Datasets

### Local File System

Adding datasets to the container can be done by copying the datasets to the `./data/datasets` directory on the host machine. The datasets will then automatically be available for querying through Beacon. There is no need to restart the container after adding datasets. The same applies to removing datasets.

### Cloud Storage

If running with Beacon attached to a cloud storage solution, then simply uploading to the appropriate bucket will make the datasets available for querying. Beacon will track changes to the datasets in the bucket and update the available datasets accordingly.

## Exploring Datasets

Once the datasets are mounted into the container, Beacon will automatically discover them and make them available for querying.
To list the available datasets, you can use the following API endpoint:

```http
GET /api/datasets
```

### Listing available columns/variables/attributes of a dataset

```http
GET /api/dataset-schema?file=example.nc
```

However, it it also possible to list the merged schema of all datasets found using a glob path:

```http
GET /api/dataset-schema?file=*.nc
```
