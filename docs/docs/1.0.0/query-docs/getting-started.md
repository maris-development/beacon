# Getting Started

To quickly get started with querying data, we first need to know which content (datasets or data tables) the data lake contains. This will help us understand how to effectively query and utilize the available data resources.

## Querying Datasets

To learn which datasets are available use the following http GET request:

```http
GET /api/datasets
```

This will return a list of datasets available in the data lake.

Once we know which datasets are available, we can list the schema of the datasets using the following http GET request:
The schema will provide information about the structure of the datasets, including the names and types of the columns.

```http
GET /api/dataset-schema?file=example1.nc
```

## Querying Datasets with SQL

Once we know which datasets are available, we can query the data using SQL and using the output format parquet.
Beacon will then return the results in the specified format. The SQL interface is available at the `/api/query` endpoint. You can use the `sql` parameter to specify the SQL query that you want to execute.

The following example shows how to query a dataset using SQL:

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

You can extend the SQL query to include more complex operations such as filtering, aggregating, and joining data from multiple datasets or data tables. The SQL syntax is similar to standard SQL, so you can use familiar SQL constructs to query the data.

```sql
SELECT TEMP, "TEMP.units", PSAL, DEPTH, LATITUDE, LONGITUDE
FROM read_netcdf('example1.nc')
WHERE DEPTH < 1000 AND LATITUDE > 30 AND LONGITUDE < -80
ORDER BY DEPTH DESC
LIMIT 10
```

## Querying Data Tables

To learn which data tables are available use the following http GET request:

```http
GET /api/tables
```

This will return a list of data tables available in the data lake.
Once we know which data tables are available, we can list the schema of the data tables using the following http GET request:

The schema will provide information about the structure of the data tables, including the names and types of the columns.

```http
GET /api/table-schema?table=example1
```

## Querying Data Tables with SQL

Once we know which data tables are available, we can query the data using SQL and using the output format parquet.
Beacon will then return the results in the specified format. The SQL interface is available at the `/api/query` endpoint. You can use the `sql` parameter to specify the SQL query that you want to execute.

The following example shows how to query a data table using SQL:

```http
POST /api/query

Content-Type: application/json
{
    "sql": "SELECT TEMP,PSAL,LONGITUDE,LATITUDE FROM example1",
    "output": {
        "format": "parquet"
    }
}

```

You can extend the SQL query to include more complex operations such as filtering, aggregating, and joining data from multiple datasets or data tables. The SQL syntax is similar to standard SQL, so you can use familiar SQL constructs to query the data.

```sql
SELECT TEMP, "TEMP.units" PSAL, DEPTH, LATITUDE, LONGITUDE
FROM example1
WHERE DEPTH < 1000 AND LATITUDE > 30 AND LONGITUDE < -80
ORDER BY DEPTH DESC
LIMIT 10
```
