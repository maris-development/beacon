---
description: A condensed reference of the beacondb Connection and Relation surface.
---

# API reference

A condensed map of the surface. The package ships `py.typed` and `_beacondb.pyi` stubs, so editors
give full completion and signatures.

## `beacondb.connect(...)`

```python
beacondb.connect(
    database=":memory:", *, read_only=False, auth=False,
    username=None, password=None, token=None,
    admin_username=None, admin_password=None, anonymous=True,
    datasets=None, batch_size=None, memory_limit=None, cpu_limit=None,
    crawlers=False, secrets_key=None,
) -> Connection
```

Opens (or creates) a database. See [Getting started](/docs/2.0.0/beacondb/python/getting-started) for
`auth`, `read_only`, and `secrets_key`.

## `Connection`

| Group | Methods |
|---|---|
| DB-API | `execute(sql, params?)`, `executemany`, `fetchone`/`fetchmany`/`fetchall`, `description`, `rowcount`, `cursor`, `commit`/`rollback` (no-ops), `close` |
| Relations | `sql`, `query`, `table`, `view` → [`Relation`](#relation) |
| Readers | `read(fn, *args, **kwargs)` and catalog-resolved `read_parquet`/`read_netcdf`/… ; `table_functions()` |
| Data in | `register(name, obj, persist=False)`, `append(name, obj)`, `unregister(name)` |
| Remote catalogs | `attach(name, url, *, token=, username=, password=, secret=, tls=)`, `detach(name)`, `attached()` |
| Identity | `connect_as(...)`, `as_anonymous()`, `whoami()` |
| Extras | `json_query(dict)`, `functions()`, `metrics(query_id?)`, `list_tables()`, `refresh(name)` |

## `Relation`

Lazy; nothing runs until a terminal.

| Group | Methods |
|---|---|
| Compose | `filter`, `project`/`select`, `aggregate`, `order`/`sort`, `limit`, `distinct`, `join`, `union`/`union_all`, `count`/`sum`/`min`/`max`/`mean`, `query` |
| Terminals | `fetchall`/`fetchmany`/`fetchone`, `arrow`/`df`/`pl`, `record_batch(batch_size?)`, `explain(analyze?)`, `show(limit?)`, `create(name)`/`create_view(name)`, `__arrow_c_stream__` |
| Sinks | `to_parquet`, `to_csv`, `to_arrow_ipc`, `to_netcdf`, `to_hdf5`, `to_nd_netcdf(dims)`, `to_geoparquet(longitude=,latitude=)`, `to_odv(...)` |
| Metadata | `sql`, `columns`, `types`, `shape`, `len()` |

## Errors (PEP 249)

`beacondb.Error` is the root, with `InterfaceError`, `DatabaseError`, `OperationalError`,
`ProgrammingError`, `NotSupportedError`, `DataError`, `InternalError`, and `NotPermittedError` (a
`ProgrammingError` subclass — the authorization refusal; not the CPython `PermissionError`).
