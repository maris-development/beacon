
# The Beacon Python SDK: the fastest way to work with Beacon

If you want to get value from Beacon quickly, the Python SDK is the best place to start. It lets you query any Beacon instance from notebooks, scripts, or applications without wrestling with raw HTTP. You build queries, run them, and get results in a few lines—clean, repeatable, and easy to share.

This post explains what the SDK is, how it works, and why it makes Beacon more approachable for both programmers and non‑programmers.

## What it is

The SDK is a lightweight Python client for Beacon. It is designed to:

- Connect to any Beacon deployment (local, on‑prem, or cloud).
- Build JSON or SQL queries in a readable way.
- Return results in common formats that fit data workflows.

If you are not a programmer, think of it as a “remote control” for Beacon that hides the technical wiring and lets you focus on the data and the question you want to answer.

## How it works (in simple terms)

1. You create a client with the Beacon URL.
2. You build a query (SQL or JSON).
3. The SDK sends the query to Beacon and returns the results.

Under the hood, the SDK calls Beacon’s HTTP API, but you don’t need to manage headers, payloads, or response parsing yourself. That means fewer moving parts and faster results.

## Why it improves working with Beacon

### For programmers

- **Faster development**: less boilerplate code for API calls.
- **Cleaner queries**: structured helpers for building payloads.
- **Better integration**: easy to plug into pipelines and notebooks.

### For non‑programmers

- **Fewer moving parts**: no need to craft raw HTTP requests.
- **Readable workflows**: simple, repeatable steps you can share.
- **Consistent results**: the SDK handles formatting and errors for you.

## Building queries made easy

Beacon supports both JSON queries and SQL. The SDK supports both styles, so teams can choose what feels most natural:

- **SQL** for familiar analytics workflows.
- **JSON** for structured, programmatic query building.

Either way, the SDK keeps the interaction simple: connect, query, get results. It’s the shortest path from question to answer.

## Examples: why it feels easier

Below are a few short examples that show how the SDK reduces boilerplate and makes common tasks clearer.

### The query builder (JSONQuery and SQLQuery)

The SDK includes two builders:

- `JSONQuery`: created from a table or dataset via `.query()`.
- `SQLQuery`: created with `client.sql_query("SELECT ...")`.

Both builders share the same output helpers, so you can switch between JSON and SQL without changing how you export results.

### 1) A simple SQL query

```python
from beacon_api import Client

client = Client("https://beacon.example.com")

sql = client.sql_query(
    "SELECT valid_time, latitude, longitude, temp FROM my_zarr_table LIMIT 100"
)

df = sql.to_pandas_dataframe()
```

Why it’s easier: no HTTP setup, no manual request formatting, just a direct query.

### 1b) A query-builder example (JSONQuery)

```python
from beacon_api import Client

client = Client("https://beacon.example.com")
tables = client.list_tables()
collection = tables["default"]

query = (
	collection
	.query()
	.add_select_column("LONGITUDE")
	.add_select_column("LATITUDE")
	.add_select_column("JULD")
	.add_select_column("TEMP", alias="temperature_c")
	.add_range_filter("JULD", "2024-01-01T00:00:00", "2024-06-30T23:59:59")
	.add_range_filter("PRES", 0, 50)
)

df = query.to_pandas_dataframe()
```

Why it’s easier: the builder reads like a checklist, and results come back as a Pandas DataFrame in one call.

### 2) A JSON query with filters

```python
from beacon_api import Client

client = Client("https://beacon.example.com")
tables = client.list_tables()
collection = tables["default"]

df = (
	collection
	.query()
	.add_select_column("valid_time")
	.add_select_column("latitude")
	.add_select_column("longitude")
	.add_select_column("temp")
	.add_range_filter("valid_time", "2025-01-01", "2025-12-31")
	.add_range_filter("latitude", -10, 10)
	.to_pandas_dataframe()
)
```

Why it’s easier: the SDK handles the request and response details so you can focus on the query.

### 3) Query a dataset directly

```python
from beacon_api import Client

client = Client("https://beacon.example.com")

sql = client.sql_query(
    "SELECT * FROM read_zarr(['datasets/ocean/2025-01/zarr.json']) LIMIT 50"
)

df = sql.to_pandas_dataframe()
```

Why it’s easier: no extra setup is required to read a Zarr dataset directly.

### 4) Query a collection (logical table)

```python
from beacon_api import Client

client = Client("https://beacon.example.com")

sql = client.sql_query(
    "SELECT * FROM my_ocean_collection WHERE valid_time >= '2025-01-01' LIMIT 100"
)

df = sql.to_pandas_dataframe()
```

Why it’s easier: your collection acts like a single table, even if it spans many datasets.

### 5) Use statistics columns for faster Zarr queries

```python
from beacon_api import Client

client = Client("https://beacon.example.com")

sql = client.sql_query(
    "SELECT * FROM read_zarr(['datasets/**/*.zarr/zarr.json'], ['valid_time', 'latitude', 'longitude']) "
    "WHERE valid_time >= '2025-01-01' AND longitude < 30 LIMIT 100"
)

df = sql.to_pandas_dataframe()
```

Why it’s easier: you can opt into pruning in a single line, without changing your datasets.

## Output helpers (Pandas and beyond)

The SDK is designed to get data into the tools people already use. Every query (JSON or SQL) can be materialized with the same helpers:

- `to_pandas_dataframe()` for quick analysis.
- `to_geo_pandas_dataframe(lon_col, lat_col)` for spatial workflows.
- `to_dask_dataframe()` for scalable, lazy pipelines.
- `to_xarray_dataset(dimension_columns)` for gridded or multi‑dimensional data.
- `to_parquet(...)`, `to_csv(...)`, `to_arrow(...)`, `to_zarr(...)`, `to_netcdf(...)` for export.

This means you can go from a query to a notebook‑ready DataFrame in one line—and keep moving.

## Explore and query individual datasets

The SDK also helps you discover and query datasets directly (not just collections):

```python
from beacon_api import Client

client = Client("https://beacon.example.com")

datasets = client.list_datasets(pattern="**/*.parquet", limit=10)
first_file = next(iter(datasets.values()))

print(first_file.get_file_path(), first_file.get_file_format())

df = (
	first_file
	.query()
	.add_select_column("lon", alias="longitude")
	.add_select_column("lat", alias="latitude")
	.add_range_filter("time", "2023-01-01T00:00:00", "2023-12-31T23:59:59")
	.to_pandas_dataframe()
)
```

Why it’s easier: you can point at a single dataset file and query it immediately, using the same builder and output helpers.

## Learn more

- GitHub repository: https://github.com/maris-development/beacon-py
- Documentation: https://maris-development.github.io/beacon-py/latest/

## Takeaway

The Beacon Python SDK is the fastest, cleanest way to work with Beacon from Python. It lowers the barrier for non‑programmers, speeds up development for engineers, and makes data discovery feel effortless. If you want to move quickly, start here.
