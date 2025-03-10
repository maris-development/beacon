# Datasets

All datasets intended for querying through Beacon must be placed in the following directory inside the Docker container: `/data/datasets/`
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

```sh
docker run -v /path/to/host/datasets:beacon/data/datasets -d beacon:latest
```
