# SQL

::: warning Using SQL
To enable SQL support, you need to set the `BEACON_ENABLE_SQL` environment variable to `true`.
:::

You can query the Beacon Data Lake using SQL. The SQL interface is available at the `/api/query` endpoint. You can use the `sql` parameter to specify the SQL query that you want to execute.

You can query data from the predefined data tables, but also from datasets stored in the `/beacon/data/datasets` directory inside the beacon docker container.
You can use the `from` clause to specify the data table or dataset that you want to query.


## Querying from a Data Table

You can query data from a data table using the `from` clause in your SQL query. The following example shows how to query data from a data table named `your_data_table`. Your data table can be a logical or physical data table.

You also specify the output format of the query using the `output` parameter. The following example shows how to query data from a data table and output the results in parquet format.

Other formats are also supported such as `csv`, `netcdf`, `ipc`, `json`.

```http
POST /api/query

Content-Type: application/json
{
    "sql": "SELECT TEMP,PSAL,LONGITUDE,LATITUDE FROM your_data_table",
    "output": {
        "format": "parquet"
    }
}
```

## Querying from a Dataset

You can query data from a dataset using the `from` clause in your SQL query. To do this, you need to make use of table functions. For each data format, there is a table function that you can use to query the original datasets. 

Available table functions are:

- `read_netcdf` - for querying netCDF datasets. As arguments it takes a lists of paths to the datasets that you want to query. The paths can be specified using glob patterns, so you can use wildcards to match multiple datasets.
- `read_parquet` - for querying parquet datasets. As arguments it takes a lists of paths to the datasets that you want to query. The paths can be specified using glob patterns, so you can use wildcards to match multiple datasets.
- `read_csv` - for querying CSV datasets. As arguments it takes a lists of paths to the datasets that you want to query. The paths can be specified using glob patterns, so you can use wildcards to match multiple datasets.
- `read_odv_ascii` - for querying ODV ASCII datasets. As arguments it takes a lists of paths to the datasets that you want to query. The paths can be specified using glob patterns, so you can use wildcards to match multiple datasets.

```http
POST /api/query

Content-Type: application/json
{
    "sql": "SELECT TEMP,PSAL,LONGITUDE,LATITUDE FROM read_netcdf('*.nc')",
    "output": {
        "format": "parquet"
    }
}
```

```http
POST /api/query

Content-Type: application/json
{
    "sql": "SELECT TEMP,PSAL,LONGITUDE,LATITUDE FROM read_csv('*.csv')",
    "output": {
        "format": "parquet"
    }
}
```

```http
POST /api/query

Content-Type: application/json
{
    "sql": "SELECT TEMP,PSAL,LONGITUDE,LATITUDE FROM read_odv_ascii('*.txt')",
    "output": {
        "format": "parquet"
    }
}
```
