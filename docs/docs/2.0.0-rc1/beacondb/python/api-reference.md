---
description: Complete reference for the BeaconDB Python API — connect(), Connection, Relation, Result, and the exception hierarchy, with a description of every method.
---

# API reference

Every public method, grouped by what you are trying to do. The package ships `py.typed` and
`_beacondb.pyi` stubs, so your editor gives the same signatures inline.

Three objects carry almost the whole surface:

| Object | You get it from | What it is |
| --- | --- | --- |
| [`Connection`](#connection) | `beacondb.connect()` | An open database. Runs SQL, reads files, holds identity. |
| [`Relation`](#relation) | `con.sql()`, `con.table()`, `con.read_*()` | A **lazy** query — runs only at a terminal method. |
| [`Result`](#result) | `con.json_query()` | An already-executed result set. |

::: tip Lazy vs eager
A `Relation` runs nothing when you build it. Inspecting `rel.sql`, `rel.columns` or `rel.types`
costs nothing; the query executes on the first terminal call — `fetchall`, `df`, `arrow`,
`record_batch`, `show`, `explain`, or any `to_*` sink.
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
| `database` | `":memory:"` | Path to a `beacon.db` file, or `":memory:"` for a throwaway in-process database. A missing file is created. |
| `read_only` | `False` | Refuse every write — DDL, DML, and side-effecting statements (`ATTACH`, `CREATE SECRET`, `INSERT`). `SELECT` and `SHOW` still work. |
| `auth` | `False` | Turn on Beacon's RBAC. Off means possession of the file is full control; see [Getting started](/docs/2.0.0-rc1/beacondb/python/getting-started#authentication). |
| `username`, `password`, `token` | `None` | Credentials to authenticate as. Only valid with `auth=True` — passing them with auth off is an error, not a no-op. |
| `admin_username`, `admin_password` | `None` | Bootstrap the super-user on a database that has no auth store yet. |
| `anonymous` | `True` | Whether the anonymous, read-only principal may connect at all. |
| `datasets` | `None` | Root directory for relative dataset paths. |
| `batch_size` | `None` | Rows per Arrow batch the engine produces. Raise for throughput, lower for latency. |
| `memory_limit` | `None` | Soft memory budget in bytes for query execution. |
| `cpu_limit` | `None` | Maximum worker threads. Defaults to the machine's core count. |
| `crawlers` | `False` | Enable background dataset crawlers. |
| `secrets_key` | `None` | Base64 32-byte key that encrypts persistent secrets (or `$BEACON_SECRETS_KEY`). Required to write one — Beacon refuses to store a plaintext credential. See [Secrets](/docs/2.0.0-rc1/beacondb/python/secrets). |

## `Connection`

### Run SQL

| Method | Description |
| --- | --- |
| `execute(sql, parameters=None)` | Run one statement. `parameters` are **bound**, never interpolated, so they are injection-safe; use `?` or `$1` placeholders. Returns the connection for chaining into `fetch*`. |
| `executemany(sql, seq_of_parameters)` | Run the same statement once per parameter row. |
| `fetchone()` | Next row of the last `execute` as a tuple, or `None` when exhausted. |
| `fetchmany(size=1)` | Up to `size` further rows. |
| `fetchall()` | All remaining rows. |
| `description` | PEP 249 column metadata: one 7-tuple per column. `None` before any query. |
| `rowcount` | Rows affected (DML) or produced (query) by the last statement. |
| `cursor()` | An independent result slot on the *same* database, so two queries can be in flight without clobbering each other's rows. |
| `commit()`, `rollback()` | No-ops. Beacon has no multi-statement transactions; present so DB-API tooling works. |
| `close()` | Close the connection and release the file lock. Also happens on `__exit__`. |

### Build a query

Each returns a lazy [`Relation`](#relation).

| Method | Description |
| --- | --- |
| `sql(query)` | Wrap a SQL string as a relation. Nothing executes yet. |
| `query(query)` | Alias of `sql`. |
| `table(name)` | The named table as a relation, equivalent to `SELECT * FROM name`. |
| `view(name)` | The named view as a relation. |

### Read files

Readers resolve from the engine's catalog, so any table function Beacon registers is available as a
method — new ones appear without a client update. All return a lazy `Relation`.

| Method | Description |
| --- | --- |
| `read(function, *args, **kwargs)` | General form. Call any table function by name. |
| `read_parquet`, `read_geoparquet`, `read_csv`, `read_json`, `read_arrow`, `read_netcdf`, `read_hdf5`, `read_zarr`, `read_delta`, `read_odv_ascii`, `read_tiff`, `read_bbf`, `read_atlas` | Format-specific readers. Accept a path or glob, plus that format's options as keywords, and the universal `columns=[...]` to project as you read. |
| `list_datasets(*args)` | Relation over the datasets Beacon knows about. |
| `table_functions()` | Names of every reader currently available. |

### Bring Python data in

| Method | Description |
| --- | --- |
| `register(name, obj, *, persist=False)` | Make a pandas/polars/pyarrow object (or any Arrow-compatible one) queryable by name. Session-only by default — in memory for this process, never written to the file. `persist=True` writes it into `beacon.db` as a managed table, which is real DDL: needs write privileges and refuses to overwrite. Requires `pyarrow`. |
| `append(name, obj)` | `INSERT INTO` an existing managed table. Errors if it does not exist. |
| `unregister(name)` | Drop a session-registered table. |

### Attach a remote Beacon

| Method | Description |
| --- | --- |
| `attach(name, url, *, token=None, username=None, password=None, secret=None, tls=False)` | Mirror a remote Beacon Data Lake's catalog under `name`, so every remote table is queryable as `name.schema.table` and joinable against local data. Contacts the remote immediately, so a bad endpoint fails here rather than on first query. `secret=` names a `TYPE BEACON` secret instead of inline credentials. |
| `detach(name)` | Remove the attachment. Returns whether it existed. |
| `attached()` | Names of currently attached catalogs. |

### Identity

| Method | Description |
| --- | --- |
| `connect_as(username=None, password=None, token=None)` | A new connection to the same database under a different identity. |
| `as_anonymous()` | A new connection as the anonymous, read-only principal. |
| `whoami()` | Dict describing the current principal: `username`, `roles`, `is_super_user`, `auth`. |
| `auth_enabled` | Whether RBAC is on for this connection. |

### Engine extras

| Method | Description |
| --- | --- |
| `json_query(spec)` | Run Beacon's structured (non-SQL) query — the same payload the HTTP API and TypeScript client use. Returns a [`Result`](#result). |
| `functions()` | Relation over every SQL function the engine exposes. |
| `metrics(query_id=None)` | Per-query execution metrics; narrow to one query with `query_id`. |
| `list_tables()` | User tables in the default schema. |
| `refresh(name)` | Re-list an external table, or rebuild a materialized view. |

## `Relation`

A query that has been built but not yet run. Shape it in **SQL**; the relation is what you call to
get the answer out, convert it, or write it to a file.

```python
rel = con.sql("SELECT kind, count(*) AS n FROM events GROUP BY kind ORDER BY n DESC LIMIT 5")
rel.sql        # inspect — runs nothing
rel.df()       # now it runs
```

::: info No method chaining
BeaconDB does not currently expose relational composition (`.filter()`, `.aggregate()`,
`.join()`, …). Write the SQL instead — it is the same engine, and one statement is easier to read
back than an equivalent chain.
:::

### Get results out

These **execute** the query.

| Method | Description |
| --- | --- |
| `fetchone()`, `fetchmany(size=1)`, `fetchall()` | Rows as tuples. |
| `arrow()` | A `pyarrow.Table`. Aliases: `fetch_arrow_table`, `to_arrow_table`. |
| `df()` | A pandas `DataFrame`. Aliases: `to_df`, `fetchdf`. Needs `beacondb[pandas]`. |
| `pl()` | A polars `DataFrame`. Needs `beacondb[polars]`. |
| `record_batch(batch_size=None)` | A `pyarrow.RecordBatchReader` that pulls batches **on demand**, so memory stays bounded on results too large to collect. Omit `batch_size` for the engine's native batches (zero-copy). Aliases: `fetch_record_batch`, `fetch_arrow_reader`. |
| `__arrow_c_stream__()` | Arrow PyCapsule protocol — any Arrow consumer ingests the relation directly, with no dependency of ours. |
| `show(limit=10)` | Print the first rows as a table. Returns nothing. |
| `explain(analyze=False)` | The logical and physical plan as text. `analyze=True` runs the query and annotates each operator with rows, time and bytes. |
| `create(name)` | Materialize the relation as a table. |
| `create_view(name)` | Save it as a view. |

### Write to a file

Local paths only for now; a `scheme://` destination raises `NotSupportedError`.

| Method | Description |
| --- | --- |
| `to_parquet(path)` | Parquet. |
| `to_csv(path)` | CSV. |
| `to_arrow_ipc(path)` | Arrow IPC. Alias: `to_ipc`. |
| `to_netcdf(path)` | A real NetCDF-4 file. |
| `to_hdf5(path)` | Same writer under the HDF5 name — NetCDF-4 *is* HDF5. |
| `to_nd_netcdf(path, dimensions)` | Multi-dimensional NetCDF, pivoting on the named dimension columns. |
| `to_geoparquet(path, longitude=None, latitude=None)` | GeoParquet, building geometry from the named coordinate columns. |
| `to_odv(path, *, longitude=None, latitude=None, depth=None, time=None, key=None, qf_schema=None)` | Ocean Data View archive. The layout is inferred from the schema, but most data does not use ODV's exact header names, so map them explicitly. `qf_schema` overrides the quality-flag schema (default `SEADATANET`). |

### Inspect without running

| Property | Description |
| --- | --- |
| `sql` | The SQL this relation would run. Executes nothing — the fastest way to debug a chain. |
| `columns` | Output column names. |
| `types` | Output column types. |
| `shape` | `(rows, columns)`. |
| `len(relation)` | Row count. |

## `Result`

Returned by [`json_query()`](#engine-extras). Already executed, so there is nothing to compose.

| Member | Description |
| --- | --- |
| `fetchone()`, `fetchmany(size=1)`, `fetchall()` | Rows as tuples. |
| `arrow()`, `df()` / `fetchdf()`, `pl()` | The result as pyarrow / pandas / polars. |
| `__arrow_c_stream__()` | Arrow PyCapsule protocol. |
| `description` | PEP 249 column metadata. |
| `rowcount`, `columns`, `types`, `len(result)` | Row count, column names, column types. |

## Exceptions

`beacondb.Error` is the root of the PEP 249 hierarchy — catch it to catch everything.

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
It subclasses `ProgrammingError`, not CPython's built-in `PermissionError`. Catching the built-in
will not catch an authorization refusal.
:::

`Warning` is also exported, per PEP 249.

## See also

- [Getting started](/docs/2.0.0-rc1/beacondb/python/getting-started) — connecting, auth modes, read-only.
- [Querying](/docs/2.0.0-rc1/beacondb/python/querying) — relations, readers, sinks, streaming, with examples.
- [Bringing data in](/docs/2.0.0-rc1/beacondb/python/data-in) — `register()` and `append()`.
- [Remote catalogs](/docs/2.0.0-rc1/beacondb/python/remote-catalogs) — `ATTACH` and pushdown.
- [Secrets](/docs/2.0.0-rc1/beacondb/python/secrets) — object-store credentials.
- [SQLAlchemy](/docs/2.0.0-rc1/beacondb/python/sqlalchemy) — the `beacondb://` dialect.
