# Python — ADBC (Arrow Database Connectivity)

[ADBC](https://arrow.apache.org/adbc/) is a database-connectivity standard built on Apache Arrow. The `adbc-driver-flightsql` package connects to any Arrow Flight SQL server — including Beacon — and returns results as zero-copy Arrow record batches, making it ideal for data science workloads.

## Installation

```bash
pip install adbc-driver-flightsql adbc-driver-manager pyarrow
```

## Connecting

Beacon's Arrow Flight SQL server listens on port `32011` by default (separate from the HTTP API on port `5001`). Make sure that port is reachable — see the [Docker Compose note](#expose-the-flight-sql-port) below.

### DBAPI 2.0 interface (recommended)

The `adbc_driver_flightsql.dbapi` module exposes a standard [PEP 249](https://peps.python.org/pep-0249/) interface, compatible with `pandas.read_sql` and similar helpers.

```python
import adbc_driver_flightsql.dbapi as flight_sql

conn = flight_sql.connect(
    "grpc://localhost:32011",
    db_kwargs={
        "username": "admin",        # BEACON_ADMIN_USERNAME
        "password": "securepassword", # BEACON_ADMIN_PASSWORD
    },
)
```

### Low-level `AdbcDatabase` interface

Use `adbc_driver_manager.AdbcDatabase` when you need direct control over the connection lifecycle or want to integrate with libraries that accept the ADBC database handle.

```python
import adbc_driver_manager as mgr
import adbc_driver_flightsql as flightsql

db = mgr.AdbcDatabase(
    driver=flightsql.DRIVER_PATH,
    uri="grpc://localhost:32011",
    **{flightsql.DatabaseOptions.USERNAME.value: "admin"},
    **{flightsql.DatabaseOptions.PASSWORD.value: "securepassword"},
)
conn = db.connect()
```

## Running queries

### Fetch rows with a cursor

```python
with flight_sql.connect("grpc://localhost:32011", db_kwargs={...}) as conn:
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM default LIMIT 10")
        rows = cur.fetchall()
        print(rows)
```

### Fetch as an Arrow table

```python
with flight_sql.connect("grpc://localhost:32011", db_kwargs={...}) as conn:
    with conn.cursor() as cur:
        cur.execute("SELECT time, latitude, longitude, temp FROM default LIMIT 10000")
        arrow_table = cur.fetch_arrow_table()  # pyarrow.Table, zero-copy
        print(arrow_table.schema)
```

### Read into a pandas DataFrame

```python
import pandas as pd

with flight_sql.connect("grpc://localhost:32011", db_kwargs={...}) as conn:
    df = pd.read_sql("SELECT * FROM default LIMIT 1000", conn)
    print(df.head())
```

## Expose the Flight SQL port

When running Beacon via Docker Compose, publish port `32011` alongside the HTTP API:

```yaml
services:
    beacon:
        image: ghcr.io/maris-development/beacon:latest
        ports:
            - "5001:5001"   # HTTP API
            - "32011:32011" # Arrow Flight SQL  # [!code ++]
```

## TLS connections

If your Beacon instance is behind TLS, use `grpc+tls://` as the URI scheme:

```python
conn = flight_sql.connect(
    "grpc+tls://your-beacon-host:32011",
    db_kwargs={
        "username": "admin",
        "password": "securepassword",
    },
)
```

For self-signed certificates, disable certificate verification:

```python
import adbc_driver_flightsql as flightsql

conn = flight_sql.connect(
    "grpc+tls://your-beacon-host:32011",
    db_kwargs={
        "username": "admin",
        "password": "securepassword",
        flightsql.DatabaseOptions.TLS_SKIP_VERIFY.value: "true",
    },
)
```

## Configuration reference

| Variable                            | Default   | Description                             |
| ----------------------------------- | --------- | --------------------------------------- |
| `BEACON_FLIGHT_SQL_ENABLE`          | `true`    | Enable or disable the Flight SQL server |
| `BEACON_FLIGHT_SQL_HOST`            | `0.0.0.0` | IP address to listen on                 |
| `BEACON_FLIGHT_SQL_PORT`            | `32011`   | Port for the Flight SQL gRPC server     |
| `BEACON_FLIGHT_SQL_ALLOW_ANONYMOUS` | `false`   | Allow unauthenticated connections       |
| `BEACON_FLIGHT_SQL_TOKEN_TTL_SECS`  | `3600`    | Auth token lifetime in seconds          |
