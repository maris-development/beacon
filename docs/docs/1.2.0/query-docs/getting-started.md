# Getting Started

To quickly get started with querying data, we first need to know which content (datasets or data tables) the data lake contains. This will help us understand how to effectively query and utilize the available data resources.

## Python

Firstly, install the `beacon-api` package:
This will also install its dependencies.

```bash
pip install beacon-api
```

### Example Usage

```python

from beacon_api import *

# Create a Beacon client and connect to a Beacon Instance
client = Client("http://localhost:8080")

# List the available datasets
print(client.list_datasets())

# List the available data tables/data collections
print(client.list_tables())

# Easiest way to use Beacon is via the data tables/data collections.
tables = client.list_tables()
# List available columns and the stored data type from a specific data table/collection
print(tables['argo'].get_table_schema())

table_query = tables['argo'].query()
table_query.add_select_column("column_name_1")
table_query.add_select_column("column_name_2")
table_query.add_select_column("column_name_3")
# Add a numeric range (can also be strings or datetime)
table_query.add_range_filter("column_name", 10, 30)
# Add a time range
table_query.add_range_filter("column_name2", "2010-01-01T00:00:00", "2010-12-31T23:59:59")
# Add equals filter 
table_query.add_equals_filter("column_name3", "some_station")
# Add filter to filter out all the null values from the query
table_query.add_is_null_filter("column_name")
# Materialize and execute the query by calling to_pandas_dataframe()
df = table_query.to_pandas_dataframe()
print(df)
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
