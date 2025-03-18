# Datasets

All datasets intended for querying through Beacon must be placed in the following directory inside the Docker container: `/beacon/data/datasets/`
No other configuration is required to make the datasets available for querying. Beacon will be able to infer the schema of the dataset and make it available for querying.

## 1. Supported File Formats

Beacon supports the following file formats for datasets:

- NetCDF
- CSV
- ODV ASCII
- Parquet
- Arrow IPC
- Beacon Binary Format (Required the Standard Edition)

## 2. Mounting Datasets into the Docker Container

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

### Managing Datasets

Adding datasets to the container can be done by copying the datasets to the `./data/datasets` directory on the host machine. The datasets will then automatically be available for querying through Beacon. There is no need to restart the container after adding datasets. The same applies to removing datasets.

## 3. Discovering Datasets

Once the datasets are mounted into the container, Beacon will automatically discover them and make them available for querying.
To list the available datasets, you can use the following API endpoint:

```http
GET /api/datasets
```

## 4. Listing available columns/variables/attributes of a dataset

```http
GET /api/dataset-schema?file=example.nc
```

However, it it also possible to list the merged schema of all datasets found using a glob path:

```http
GET /api/dataset-schema?file=*.nc
```
