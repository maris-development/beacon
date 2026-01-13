# Querying with SQL

Querying Beacon with SQL is done through a api call to the `/api/query` endpoint. The request body should be a JSON object containing the SQL query and the desired output format.

SQL queries can be used to query both directly datasets and data tables. This work both for single datasets as well as multiple datasets at once using glob patterns.

SQL is supported for both S3 and local file system datasets.

To learn about how to specify the output format, please refer to [Output Formats](./json.md#output-format) document.

## Query Data Tables using SQL

```http
POST {beacon-host}/api/query

Content-Type: application/json
{
    "sql": "SELECT TEMP,PSAL,LONGITUDE,LATITUDE FROM some_collection",
    "output": {
        "format": "parquet"
    }
}
```

## Query Zarr Datasets using SQL

Reading Zarr datasets is supported using the `read_zarr` function. You need to provide the path to the `zarr.json` file of the Zarr dataset.

Global attributes and variable attributes can be accessed using a column name like `.some_global_attribute` and `variable.some_variable_attribute`.

```http
POST {beacon-host}/api/query

Content-Type: application/json
{
    "sql": "SELECT TEMP,PSAL,LONGITUDE,LATITUDE FROM read_zarr(['datasets.zarr/zarr.json'])",
    "output": {
        "format": "parquet"
    }
}
```

> [!TIP] Improve Query Performance using Pushdown Predicates
> When querying large zarr datasets, it is recommended to use pushdown predicates by defining to the `read_zarr` function which variables to pre-fetch. This can significantly improve query performance by reducing the amount of data that needs to be read from disk.
>
> ``` http
> POST {beacon-host}/api/query
> Content-Type: application/json
> {
>     "sql": "SELECT TEMP,PSAL,LONGITUDE,LATITUDE FROM 
>             read_zarr(['datasets.zarr/zarr.json'], ['LONGITUDE', 'LATITUDE']) 
>             WHERE LONGITUDE > 10 AND LATITUDE < 50",
>     "output": {
>         "format": "parquet"
>     }
> }
>```
>

## Query NetCDF Datasets using SQL

Queries NetCDF datasets is supported using the `read_netcdf` function. You need to provide the path to the NetCDF dataset.

Global attributes and variable attributes can be accessed using a column name like `.some_global_attribute` and `variable.some_variable_attribute`.

```http
POST {beacon-host}/api/query
Content-Type: application/json
{
    "sql": "SELECT TEMP,PSAL,LONGITUDE,LATITUDE FROM read_netcdf(['dataset.nc'])",
    "output": {
        "format": "parquet"
    }
}
```

## Query Parquet Datasets using SQL

Queries Parquet datasets is supported using the `read_parquet` function. You need to provide the path to the Parquet dataset.
Filter Predicates used in the `WHERE` clause will be pushed down to the Parquet reader to improve query performance.

```http
POST {beacon-host}/api/query

Content-Type: application/json
{
    "sql": "SELECT TEMP,PSAL,LONGITUDE,LATITUDE FROM read_parquet(['dataset.parquet'])",
    "output": {
        "format": "parquet"
    }
}
```

## Query Beacon Binary Format Datasets using SQL

Queries Beacon Binary Format datasets is supported using the `read_bbf` function. You need to provide the path to the Beacon Binary Format dataset.
Filter Predicates used in the `WHERE` clause will be pushed down to the Beacon Binary Format reader to improve query performance.

```http
POST {beacon-host}/api/query

Content-Type: application/json
{
    "sql": "SELECT TEMP,PSAL,LONGITUDE,LATITUDE FROM read_bbf(['dataset.bbf'])",
    "output": {
        "format": "parquet"
    }
}
```
