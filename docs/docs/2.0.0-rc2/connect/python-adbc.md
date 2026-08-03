# Python, ADBC (Arrow Database Connectivity)

[ADBC](https://arrow.apache.org/adbc/) is a database connectivity standard on Apache Arrow. The
`adbc-driver-flightsql` package connects to any Arrow Flight SQL server, also to Beacon. It returns
the results as Arrow record batches, with zero copy. This fits data science work.

## Install

```bash
pip install adbc-driver-flightsql adbc-driver-manager pyarrow
```

## Connect

The Arrow Flight SQL server of Beacon listens on port `32011` by default. The HTTP API uses port
`5001`. Make that port reachable. See
[Expose the Flight SQL port](#expose-the-flight-sql-port) below.

### DBAPI 2.0 interface (recommended)

The `adbc_driver_flightsql.dbapi` module gives a standard
[PEP 249](https://peps.python.org/pep-0249/) interface. It works with `pandas.read_sql` and similar
helpers.

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

Use `adbc_driver_manager.AdbcDatabase` for direct control over the connection. It also helps with a
library that takes an ADBC database handle.

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

## Run queries

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

With Docker Compose, publish port `32011` next to the HTTP API:

```yaml
services:
    beacon:
        image: ghcr.io/maris-development/beacon:latest
        ports:
            - "5001:5001"   # HTTP API
            - "32011:32011" # Arrow Flight SQL  # [!code ++]
```

## TLS connections

Does your Beacon server use TLS? Then use the `grpc+tls://` URI scheme:

```python
conn = flight_sql.connect(
    "grpc+tls://your-beacon-host:32011",
    db_kwargs={
        "username": "admin",
        "password": "securepassword",
    },
)
```

For a self-signed certificate, switch the certificate check off:

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

| Variable | Default | Description |
| ----------------------------------- | --------- | --------------------------------------- |
| `BEACON_FLIGHT_SQL_ENABLE` | `true` | Switch the Flight SQL server on or off |
| `BEACON_FLIGHT_SQL_HOST` | `0.0.0.0` | The IP address of the listener |
| `BEACON_FLIGHT_SQL_PORT` | `32011` | The port of the Flight SQL gRPC server |
| `BEACON_FLIGHT_SQL_ALLOW_ANONYMOUS` | `false` | Allow a connection without credentials |
| `BEACON_FLIGHT_SQL_TOKEN_TTL_SECS` | `3600` | The lifetime of an auth token, in seconds |
