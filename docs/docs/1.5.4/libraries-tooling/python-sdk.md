# Beacon Python SDK

Beacon has an official Python client library that makes it easy to explore a data lake and run queries without manually constructing HTTP requests.

The SDK documentation lives here:

- [Beacon Python SDK docs](https://maris-development.github.io/beacon-py/latest/)

## Install

```bash
pip install beacon-api
```

Import the client:

```python
from beacon_api import Client
```

## Connect to a Beacon node

Create a client using your Beacon base URL:

```python
from beacon_api import Client

client = Client(
    "https://beacon.example.com",
    # jwt_token="<optional bearer token>",
    # proxy_headers={"X-Forwarded-For": "<optional ip>"},
    # basic_auth=("user", "pass"),
)

client.check_status()  # probes /api/health and prints the Beacon version
info = client.get_server_info()  # metadata from /api/info
print(info.get("beacon_version"))
```

## Explore tables (collections)

`list_tables()` returns a mapping of table names to `DataTable` helpers.

```python
tables = client.list_tables()

for name, table in tables.items():
    print(name, table.get_table_type(), table.get_table_description())
```

Inspect the Arrow schema for a table:

```python
default_table = tables["default"]

schema = default_table.get_table_schema_arrow()
for field in schema:
    print(f"{field.name}: {field.type}")
```

## Explore datasets (Beacon ≥ 1.4.0)

If your node supports dataset listing, you can query files directly (without creating a table first).

```python
datasets = client.list_datasets(pattern="**/*.parquet", limit=10)
first = next(iter(datasets.values()))

print(first.get_file_path(), first.get_file_format())
schema = first.get_schema()
```

## Query with the JSON builder (recommended)

Start from a table (or dataset) and chain selects and filters.

```python
stations = client.list_tables()["default"]

df = (
    stations
    .query()
    .add_select_columns([
        ("LONGITUDE", None),
        ("LATITUDE", None),
        ("JULD", None),
        ("TEMP", "temperature_c"),
    ])
    .add_range_filter("JULD", "2024-01-01T00:00:00", "2024-06-30T23:59:59")
    .to_pandas_dataframe()
)

print(df.head())
```

### Expressions and functions

The SDK exposes helpers for building common server-side expressions:

```python
from beacon_api.query import Functions

query = (
    stations
    .query()
    .add_select_column("CRUISE")
    .add_select_column("STATION")
    .add_select(Functions.concat(["CRUISE", "STATION"], alias="cast_id"))
    .add_select(Functions.try_cast_to_type("TEMP", to_type="float64", alias="temp_float"))
)
```

### Multiple filters

```python
filtered = (
    stations
    .query()
    .add_select_column("LONGITUDE")
    .add_select_column("LATITUDE")
    .add_select_column("JULD")
    .add_select_column("TEMP")
    .add_equals_filter("DATA_TYPE", "CTD")
    .add_range_filter("PRES", 0, 10)
    .add_bbox_filter("LONGITUDE", "LATITUDE", bbox=(-20, 40, -10, 55))
)
```

For custom boolean logic, compose filter nodes and pass them to `add_filter()`:

```python
from beacon_api.query import AndFilter, RangeFilter

filtered = filtered.add_filter(
    AndFilter([
        RangeFilter("TEMP", gt_eq=-2, lt_eq=35),
        RangeFilter("PSAL", gt_eq=30, lt_eq=40),
    ])
)
```

### Distinct and sorting

```python
query = (
    stations
    .query()
    .add_select_column("CRUISE")
    .add_select_column("STATION")
    .add_select_column("JULD")
    .set_distinct(["CRUISE", "STATION"])
    .add_sort("JULD", ascending=True)
)
```

### Explain the plan

`explain()` calls the Beacon `/api/explain-query` endpoint so you can inspect how the server plans to execute your request.

```python
plan = query.explain()
print(plan)
```

## Export results (Pandas / GeoPandas / Parquet / NetCDF / ...)

Every query can materialize into common Python data structures or write directly to disk.

```python
query = (
    stations
    .query()
    .add_select_column("LONGITUDE")
    .add_select_column("LATITUDE")
    .add_select_column("JULD")
    .add_select_column("TEMP")
    .add_range_filter("JULD", "2024-01-01T00:00:00", "2024-01-31T23:59:59")
)

df = query.to_pandas_dataframe()
gdf = query.to_geo_pandas_dataframe("LONGITUDE", "LATITUDE")

query.to_parquet("subset.parquet")
query.to_geoparquet("subset.geoparquet", "LONGITUDE", "LATITUDE")
query.to_csv("subset.csv")
```

::: tip
If your Beacon node supports server-side NdNetCDF (Beacon ≥ 1.5.0), you can export using `to_nd_netcdf(path, dimension_columns=[...])`.
:::

## Query with SQL

If you already have SQL, build an `SQLQuery` and use the same output helpers:

```python
sql = client.sql_query(
    """
    SELECT LONGITUDE, LATITUDE, JULD, TEMP AS temperature_c
    FROM default
    WHERE JULD BETWEEN '2024-01-01 00:00:00' AND '2024-06-30 23:59:59'
    ORDER BY JULD ASC
    """
)

df = sql.to_pandas_dataframe()
sql.to_parquet("slice.parquet")
```

## More SDK docs

- [Getting started](https://maris-development.github.io/beacon-py/latest/getting_started/)
- [Exploring](https://maris-development.github.io/beacon-py/latest/using/exploring/)
- [Querying](https://maris-development.github.io/beacon-py/latest/using/querying/)
- [API reference](https://maris-development.github.io/beacon-py/latest/reference/client/)
