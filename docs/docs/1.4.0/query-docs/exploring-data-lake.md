# Exploring Data Lake

Before querying, it is important to know what content (datasets or data tables) the data lake contains. This will help us understand how to effectively query and utilize the available data resources. Beacon exposes a REST API to query the content of the data lake. The API is available at the `/api` endpoint. You can use the `GET` method to retrieve information about the datasets and data tables available in the data lake.

Data is organized in either `datasets` or `data tables`:

- **Datasets**: These are individual files that contain data in a specific format (e.g., NetCDF, Parquet, CSV). Datasets can be queried directly using SQL. For example, a dataset named `2020.nc` could be queried to retrieve specific variables or subsets of the data.
- **Data Tables**: These are structured collections of datasets that are organized in a tabular format. Data tables are used to group related datasets together and provide a higher-level interface for querying and analyzing data. For example, a data table named `2020_2022` could be created from the datasets `2020.nc`, `2021.nc`, and `2022.nc`. This table would allow users to query all three datasets as if they were a single entity.

## Exploring Datasets

To learn which datasets are available, use the following HTTP GET request:

```http
GET /api/list-datasets
```

This will return a list of datasets available in the data lake.

Once we know which datasets are available, we can list the schema of the datasets using the following HTTP GET request:

```http
GET /api/dataset-schema?file=example1.nc
```

Beacon also supports querying using multiple datasets at once using a glob pattern. This sometimes `super types` the schema of different datasets together.
To list the schema of multiple datasets together, use the following HTTP GET request:

```http
GET /api/dataset-schema?file=*.nc
```

This also works for other data formats like Parquet, CSV & ODV ASCII.

## Exploring Data Tables

To learn which data tables are available, use the following HTTP GET request:

```http
GET /api/tables
```

This will return a list of data tables available in the data lake.

Once we know which data tables are available, we can list the schema of the data tables using the following HTTP GET request:

```http
GET /api/table-schema?table=example1
```

This will return the schema of the specified data table.
