# Beacon ARCO Data lake Query Engine

[![Release](https://img.shields.io/github/v/release/maris-development/beacon?style=for-the-badge&label=Release&color=success)](https://github.com/maris-development/beacon/releases)
[![Docker Image](https://img.shields.io/badge/Docker-2CA5E0?style=for-the-badge&logo=docker&logoColor=white)](https://github.com/maris-development/beacon/pkgs/container/beacon)
[![Docs](https://img.shields.io/github/actions/workflow/status/maris-development/beacon/pages.yml?style=for-the-badge&label=Docs)](https://maris-development.github.io/beacon/)
[![Chat on Slack](https://img.shields.io/badge/Slack-4A154B?style=for-the-badge&logo=slack&logoColor=white)](https://beacontechnic-wwa5548.slack.com/join/shared_invite/zt-2dp1vv56r-tj_KFac0sAKNuAgUKPPDRg)

Beacon is a lightweight, high-performance ARCO data lake query engine for discovering, reading, transforming, and serving scientific array and tabular datasets. It focuses on Arrow + DataFusion interoperability and supports formats such as Parquet, NetCDF, Zarr, ODV, and CSV.

- Docs: https://maris-development.github.io/beacon/
- Installation: https://maris-development.github.io/beacon/docs/1.5.2/getting-started.html#local
- Query reference: https://maris-development.github.io/beacon/docs/1.5.2/api/querying/
- Slack: https://beacontechnic-wwa5548.slack.com/join/shared_invite/zt-2dp1vv56r-tj_KFac0sAKNuAgUKPPDRg

## Quick start (Docker)

```yaml
version: "3.8"

services:
  beacon:
    image: ghcr.io/maris-development/beacon:latest
    container_name: beacon
    restart: unless-stopped
    ports:
      - "8080:8080"
    environment:
      - BEACON_ADMIN_USERNAME=admin
      - BEACON_ADMIN_PASSWORD=securepassword
      - BEACON_VM_MEMORY_SIZE=4096
      - BEACON_DEFAULT_TABLE=default
      - BEACON_LOG_LEVEL=INFO
      - BEACON_HOST=0.0.0.0
      - BEACON_PORT=8080
    volumes:
      - ./datasets:/beacon/data/datasets
      - ./tables:/beacon/data/tables
```

Start and open the API docs at http://localhost:8080/swagger/

Add datasets by placing files (e.g., .nc, .zarr, .parquet, .csv) into ./datasets so the container can discover them via the mounted volume.

## Query examples

### SQL

```http
POST http://localhost:8080/api/query
Content-Type: application/json

{
  "sql": "SELECT TEMP, PSAL, LONGITUDE, LATITUDE FROM read_netcdf(['data/2020.nc', 'data/2021.nc']) WHERE time > '2020-01-01T00:00:00'",
  "output": { "format": "parquet" }
}

```

### JSON

```http
POST http://localhost:8080/api/query
Content-Type: application/json

{
  "query_parameters": [
    { "column_name": "TEMP", "alias": "temperature" },
    { "column_name": "PSAL", "alias": "salinity" },
    { "column_name": "TIME" },
    { "column_name": "LONGITUDE" },
    { "column_name": "LATITUDE" }
  ],
  "filters": [
    { "for_query_parameter": "temperature", "min": -2, "max": 35 },
    { "for_query_parameter": "salinity", "min": 30, "max": 42 },
    {
      "and": [
        { "for_query_parameter": "LONGITUDE", "min": -20, "max": 20 },
        { "for_query_parameter": "LATITUDE", "min": 40, "max": 65 }
      ]
    }
  ],
  "from": {
    "netcdf": { "paths": ["data/2020.nc", "data/2021.nc"] }
  },
  "output": { "format": "csv" }
}

```
