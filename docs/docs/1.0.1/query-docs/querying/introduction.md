# Querying Introduction

Querying the Beacon Data Lake is works through a REST API. The API endpoint for querying is available at the `/api/query` endpoint.

The API supports SQL queries and JSON queries. Both methods make use of the same endpoint, but the request body format is different.
In your querying (wheteher SQL or JSON), you can specify the output format (e.g., Parquet, CSV, etc.).

The following output formats are supported:

- Parquet
- CSV
- JSON
- Arrow IPC
- ODV ASCII (zip containing Timeseries, Profiles and Trajectories)
- NetCDF

## Example Query

```http

POST {beacon-host}/api/query
Content-Type: application/json
{
    "sql": "SELECT TEMP,PSAL,LONGITUDE,LATITUDE FROM read_netcdf('dataset.nc')",
    "output": {
        "format": "parquet"
    }
}
```
