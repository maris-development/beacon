---
description: Full reference for the BeaconDB Python API. It covers connect(), Connection, Relation, Result and the exception hierarchy.
---

# API reference

This page lists every public method. The groups follow your task. The package ships `py.typed` and
`_beacondb.pyi` stubs. Your editor therefore shows the same signatures.

Three objects carry almost the whole surface:

| Object | You get it from | What it is |
| --- | --- | --- |
| [`Connection`](#connection) | `beacondb.connect()` | An open database. Runs SQL, reads files, holds identity. |
| [`Relation`](#relation) | `con.sql()`, `con.table()`, `con.read_*()` | A **lazy** query. It runs only at a terminal method. |
| [`Result`](#result) | `con.json_query()` | An already-executed result set. |

::: tip Lazy vs eager
A `Relation` runs nothing when you build it. `rel.sql`, `rel.columns` and `rel.types` cost nothing.
The query runs at the first terminal call: `fetchall`, `df`, `arrow`, `record_batch`, `show`,
`explain` or a `to_*` sink.
:::

## Module functions

| Signature | Description |
| --- | --- |
| `connect(database=":memory:", **options)` | Open or create a database, returning a [`Connection`](#connection). See [options](#connect-options). |
| `engine_version()` | Version string of the embedded Beacon engine (distinct from the `beacondb` package version). |

### `connect()` options {#connect-options}

```python
beacondb.connect(
    database=":memory:", *, read_only=False, auth=False,
    username=None, password=None, token=None,
    admin_username=None, admin_password=None, anonymous=True,
    datasets=None, batch_size=None, memory_limit=None, cpu_limit=None,
    crawlers=False, secrets_key=None,
) -> Connection
```

| Option | Default | Description |
| --- | --- | --- |
| `database` | `":memory:"` | The path to a `beacon.db` file. Use `":memory:"` for a throwaway database in the process. Beacon creates a missing file. |
| `read_only` | `False` | Refuse every write. This covers DDL, DML and statements with a side effect, such as `ATTACH`, `CREATE SECRET` and `INSERT`. `SELECT` and `SHOW` still work. |
| `auth` | `False` | Switch the RBAC of Beacon on. With auth off, the file gives full control. See [Getting started](/docs/2.0.0-rc4/beacondb/python/getting-started#authentication). |
| `username`, `password`, `token` | `None` | The credentials of your identity. They need `auth=True`. With auth off they give an error. Beacon does not ignore them. |
| `admin_username`, `admin_password` | `None` | Create the super-user on a database without an auth store. |
| `anonymous` | `True` | `True` lets the anonymous, read-only principal connect. |
| `datasets` | `None` | The root directory for a relative dataset path. |
| `batch_size` | `None` | The number of rows in each Arrow batch. A higher value gives more throughput. A lower value gives less latency. |
| `memory_limit` | `None` | The soft memory budget of a query, in bytes. |
| `cpu_limit` | `None` | The maximum number of worker threads. The default is the core count of the machine. |
| `crawlers` | `False` | Switch the background dataset crawlers on. |
| `secrets_key` | `None` | A base64 key of 32 bytes. It encrypts the persistent secrets. `$BEACON_SECRETS_KEY` also works. A persistent secret needs it, because Beacon never stores a plaintext credential. See [Secrets](/docs/2.0.0-rc4/beacondb/python/secrets). |

## `Connection`

### Run SQL

| Method | Description |
| --- | --- |
| `execute(sql, parameters=None)` | Run one statement. Beacon **binds** the `parameters`. It never puts them into the string. They are therefore safe against injection. Use a `?` or `$1` placeholder. Returns the connection, so you can chain a `fetch*` call. |
| `executemany(sql, seq_of_parameters)` | Run the same statement once per parameter row. |
| `fetchone()` | The next row of the last `execute`, as a tuple. Returns `None` after the last row. |
| `fetchmany(size=1)` | The next rows, at most `size` of them. |
| `fetchall()` | Every remaining row. |
| `description` | The PEP 249 column metadata: one tuple of 7 items for each column. It is `None` before the first query. |
| `rowcount` | The number of rows that the last statement changes or returns. |
| `cursor()` | A separate result slot on the *same* database. Two queries can therefore run together. Neither one overwrites the rows of the other. |
| `commit()`, `rollback()` | These methods do nothing. Beacon has no transaction over several statements. They exist for DB-API tools. |
| `close()` | Close the connection and release the file lock. `__exit__` also does this. |

### Build a query

Each returns a lazy [`Relation`](#relation).

| Method | Description |
| --- | --- |
| `sql(query)` | Wrap a SQL string as a relation. Beacon runs nothing yet. |
| `query(query)` | Alias of `sql`. |
| `table(name)` | The named table as a relation. It equals `SELECT * FROM name`. |
| `view(name)` | The named view as a relation. |

### Read files

The readers come from the catalog of the engine. Every table function of Beacon is therefore a
method. A new function appears without a client update. Each reader returns a lazy `Relation`.

| Method | Description |
| --- | --- |
| `read(function, *args, **kwargs)` | The general form. Call any table function by name. |
| `read_parquet`, `read_geoparquet`, `read_csv`, `read_arrow`, `read_netcdf`, `read_hdf5`, `read_zarr`, `read_delta`, `read_iceberg`, `read_odv_ascii`, `read_tiff`, `read_bbf` | One reader for each format. Each takes a path or a glob. It also takes the options of that format as keywords. Every reader takes `columns=[...]` to project during the read. |
| `list_datasets(*args)` | A relation over the datasets that Beacon knows. |
| `table_functions()` | The names of every available reader. |

### Bring Python data in

| Method | Description |
| --- | --- |
| `register(name, obj, *, persist=False)` | Give a pandas, polars or pyarrow object a table name. Any Arrow object works. By default the table lives in the session only. Beacon holds it in memory and writes nothing to the file. `persist=True` writes it into `beacon.db` as a managed table. That is real DDL. It needs write privileges and does not overwrite. It needs `pyarrow`. |
| `append(name, obj)` | Run `INSERT INTO` on an existing managed table. It gives an error if the table does not exist. |
| `unregister(name)` | Drop a table of the session. |

### Attach a remote Beacon

| Method | Description |
| --- | --- |
| `attach(name, url, *, token=None, username=None, password=None, secret=None, tls=False)` | Mirror the catalog of a remote Beacon Data Lake under `name`. You can then query every remote table as `name.schema.table`. You can also join it against local data. Beacon contacts the remote server at once. A bad endpoint therefore fails here, not at the first query. `secret=` names a `TYPE BEACON` secret instead of inline credentials. |
| `detach(name)` | Remove the attached catalog. Returns `True` if it existed. |
| `attached()` | The names of the attached catalogs. |

### Identity

| Method | Description |
| --- | --- |
| `connect_as(username=None, password=None, token=None)` | A new connection to the same database, with a different identity. |
| `as_anonymous()` | A new connection as the anonymous, read-only principal. |
| `whoami()` | A dict that describes the current principal: `username`, `roles`, `is_super_user` and `auth`. |
| `auth_enabled` | `True` if RBAC is on for this connection. |

### Engine extras

| Method | Description |
| --- | --- |
| `json_query(spec)` | Run the structured query form of Beacon, without SQL. It takes the same payload as the HTTP API and the TypeScript client. Returns a [`Result`](#result). |
| `functions()` | A relation over every SQL function of the engine. |
| `metrics(query_id=None)` | The execution metrics of each query. Give a `query_id` to get one query. |
| `list_tables()` | The user tables in the default schema. |
| `refresh(name)` | List the files of an external table again, or build a materialized view again. |

## `Relation`

A relation is a query that Beacon builds but does not run. Shape it in **SQL**. Then call the
relation to get the answer, to convert it, or to write it to a file.

```python
rel = con.sql("SELECT kind, count(*) AS n FROM events GROUP BY kind ORDER BY n DESC LIMIT 5")
rel.sql        # inspect — runs nothing
rel.df()       # now it runs
```

::: info No method chaining
BeaconDB does not yet give relational composition, such as `.filter()`, `.aggregate()` and
`.join()`. Write the SQL instead. It uses the same engine. One statement also reads better than the
equal chain.
:::

### Get results out

These methods **run** the query.

| Method | Description |
| --- | --- |
| `fetchone()`, `fetchmany(size=1)`, `fetchall()` | The rows, as tuples. |
| `arrow()` | A `pyarrow.Table`. Aliases: `fetch_arrow_table`, `to_arrow_table`. |
| `df()` | A pandas `DataFrame`. Aliases: `to_df`, `fetchdf`. Needs `beacondb[pandas]`. |
| `pl()` | A polars `DataFrame`. Needs `beacondb[polars]`. |
| `record_batch(batch_size=None)` | A `pyarrow.RecordBatchReader`. It pulls batches **on demand**. The memory therefore stays bounded on a large result. Omit `batch_size` to get the native batches of the engine, with zero copy. Aliases: `fetch_record_batch`, `fetch_arrow_reader`. |
| `__arrow_c_stream__()` | The Arrow PyCapsule protocol. Any Arrow consumer reads the relation directly. It needs no extra dependency. |
| `show(limit=10)` | Print the first rows as a table. It returns nothing. |
| `explain(analyze=False)` | The logical and physical plan, as text. `analyze=True` runs the query. It then adds the rows, the time and the bytes to each operator. |
| `create(name)` | Store the relation as a table. |
| `create_view(name)` | Save it as a view. |

### Write to a file

A sink writes to a local path only. A `scheme://` destination raises `NotSupportedError`.

| Method | Description |
| --- | --- |
| `to_parquet(path)` | Parquet. |
| `to_csv(path)` | CSV. |
| `to_arrow_ipc(path)` | Arrow IPC. Alias: `to_ipc`. |
| `to_netcdf(path)` | A real NetCDF-4 file. |
| `to_hdf5(path)` | The same writer, under the HDF5 name. NetCDF-4 *is* HDF5. |
| `to_nd_netcdf(path, dimensions)` | Multi-dimensional NetCDF, pivoting on the named dimension columns. |
| `to_geoparquet(path, longitude=None, latitude=None)` | GeoParquet, building geometry from the named coordinate columns. |
| `to_odv(path, *, longitude=None, latitude=None, depth=None, time=None, key=None, qf_schema=None)` | Ocean Data View archive. The layout is inferred from the schema, but most data does not use ODV's exact header names, so map them explicitly. `qf_schema` overrides the quality-flag schema (default `SEADATANET`). |

### Inspect without running

| Property | Description |
| --- | --- |
| `sql` | The SQL of this relation. It runs nothing. This is the fastest way to debug a chain. |
| `columns` | Output column names. |
| `types` | Output column types. |
| `shape` | `(rows, columns)`. |
| `len(relation)` | Row count. |

## `Result`

[`json_query()`](#engine-extras) returns a `Result`. Beacon has already run the query. You compose
nothing.

| Member | Description |
| --- | --- |
| `fetchone()`, `fetchmany(size=1)`, `fetchall()` | The rows, as tuples. |
| `arrow()`, `df()` / `fetchdf()`, `pl()` | The result as pyarrow / pandas / polars. |
| `__arrow_c_stream__()` | Arrow PyCapsule protocol. |
| `description` | PEP 249 column metadata. |
| `rowcount`, `columns`, `types`, `len(result)` | Row count, column names, column types. |

## Exceptions

`beacondb.Error` is the root of the PEP 249 hierarchy. Catch it to catch every error.

```text
Error
├── InterfaceError          the binding itself was misused
└── DatabaseError
    ├── DataError           bad value, out of range, wrong type
    ├── OperationalError    the database could not do it (locked file, I/O)
    ├── IntegrityError      a constraint was violated
    ├── InternalError       the engine reached an inconsistent state
    ├── ProgrammingError    bad SQL, wrong parameter count, missing table
    │   └── NotPermittedError    authorization refusal
    └── NotSupportedError   a real feature that Beacon does not implement
```

::: warning `NotPermittedError` is not `PermissionError`
`NotPermittedError` is a subclass of `ProgrammingError`. It is not a subclass of the built-in
`PermissionError` of CPython. The built-in class therefore does not catch an authorization refusal.
:::

The package also exports `Warning`, as PEP 249 requires.

## See also

- [Getting started](/docs/2.0.0-rc4/beacondb/python/getting-started): connect, auth modes, read-only.
- [Querying](/docs/2.0.0-rc4/beacondb/python/querying): relations, readers, sinks and streams, with examples.
- [Bring data in](/docs/2.0.0-rc4/beacondb/python/data-in): `register()` and `append()`.
- [Remote catalogs](/docs/2.0.0-rc4/beacondb/python/remote-catalogs): `ATTACH` and pushdown.
- [Secrets](/docs/2.0.0-rc4/beacondb/python/secrets): object store credentials.
- [SQLAlchemy](/docs/2.0.0-rc4/beacondb/python/sqlalchemy): the `beacondb://` dialect.
