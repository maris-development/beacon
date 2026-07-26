---
description: Install beacondb, open a database, run your first query, and understand the two auth modes.
---

# Getting started with beacondb

## Install

```bash
pip install beacondb
pip install "beacondb[pandas]"   # optional: .df()
```

The wheel is **abi3** (`cp310-abi3`), so one wheel per platform covers CPython 3.10+.

## Open a database and query

```python
import beacondb

# A file-backed database (created if missing), or ":memory:" for an ephemeral one.
con = beacondb.connect("beacon.db")

con.sql("SELECT 1 AS a").fetchall()                       # [(1,)]
con.sql("SELECT * FROM read_parquet('obs/*.parquet')").df()
```

`connect()` returns a PEP 249-style connection. Read results as rows (`fetchone`/`fetchmany`/
`fetchall`), as Arrow (`.arrow()`), or as a DataFrame (`.df()` / `.pl()`).

### In-memory vs file

```python
beacondb.connect(":memory:")     # ephemeral; nothing persisted
beacondb.connect("beacon.db")    # one portable file; managed tables + catalog live here
```

Each `:memory:` connection is its own database. A file is held under an exclusive lock, so one process
opens one `beacon.db`; a second `connect()` to the same path in the same process shares it.

## DB-API and cursors

```python
cur = con.cursor()
cur.execute("SELECT platform, temperature FROM obs WHERE platform = ?", ["6901234"])
cur.fetchone(); cur.fetchall()
cur.description; cur.rowcount
```

Parameters are **bound** (never string-interpolated), with `?` or `$1` placeholders — injection-safe.
`executemany(sql, rows)` runs the statement per row.

## Authentication

Auth is **off by default** — the SQLite/DuckDB contract: *possession of the file is full control*.

```python
con = beacondb.connect("beacon.db")
con.whoami()
# {'username': 'local', 'roles': [], 'is_super_user': True, 'auth': False}
```

Pass `auth=True` to switch on Beacon's [RBAC](/docs/2.0.0/security/access-control). A session is then
the anonymous, read-only principal until credentials are supplied:

```python
anon = beacondb.connect("beacon.db", auth=True,
                        admin_username="admin", admin_password=...)
anon.execute("CREATE TABLE t (a INT)")     # NotPermittedError
analyst = anon.connect_as(username="analyst", password=...)
```

Supplying credentials with `auth=False` is an error, not a no-op. RBAC written into a database is
**not** enforced when it is opened with `auth=False`: it is a boundary for *served* access
(beacon-datalake), not against local possession of the file.

## Read-only

```python
con = beacondb.connect("beacon.db", read_only=True)
con.sql("SELECT * FROM t").df()            # ok
con.execute("INSERT INTO t VALUES (1)")    # refused: opened read-only
```

Every write — DDL/DML and side-effecting statements (`ATTACH`, `CREATE SECRET`, …) — is refused,
while `SELECT` and `SHOW …` work.

## Next

- [Querying](/docs/2.0.0/beacondb/python/querying) — the lazy relation, readers, and file sinks.
- [Bringing data in](/docs/2.0.0/beacondb/python/data-in) — `register()` / `append()`.
- The full [SQL reference](/docs/2.0.0/beacondb/sql/) applies unchanged.
