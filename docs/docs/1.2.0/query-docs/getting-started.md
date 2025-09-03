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
