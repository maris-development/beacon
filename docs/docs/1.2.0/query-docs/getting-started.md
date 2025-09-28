# Getting Started

To quickly get started with querying data, we first need to know which content (datasets or data tables) the data lake contains. This will help us understand how to effectively query and utilize the available data resources.

::: tip Python Client

We recommend using the Beacon Python Client for interacting with the Beacon Data Lake API. It provides a simple and structured way to query and manage data.
:::

## Python Client

You can use the Beacon Python API Client to interact with the Beacon Data Lake API using Python.

The documentation for the Beacon Python API Client can be found at: [https://maris-development.github.io/beacon-py/](https://maris-development.github.io/beacon-py/)

The source code for the Beacon Python API Client is available on GitHub: [https://github.com/maris-development/beacon-py](https://github.com/maris-development/beacon-py)

The Beacon Python API Client can be installed using pip:

```bash
pip install beacon-api
```

## Rest-API

Beacon provides a RESTful API for working with the data lake. Libraries are available for various programming languages to simplify the process of making API calls. Eg. the `beacon-api` Python package.

### Querying Datasets

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

### Querying Data Tables

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
