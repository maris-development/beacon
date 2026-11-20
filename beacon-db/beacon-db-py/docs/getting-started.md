---
description: Install BeaconDB, open a database and run your first query. This page also explains the read-only mode.
---

# Getting started with BeaconDB

## Install

```bash
pip install beacondb
pip install "beacondb[pandas]"   # optional: .df()
pip install "beacondb[all]"      # optional: every integration at once
```

The wheel is **abi3** (`cp310-abi3`). One wheel per platform therefore covers CPython 3.10 and later.
Beacon publishes wheels for Linux glibc (`x86_64`, `aarch64`), macOS (`arm64`, `x86_64`) and Windows
(`x64`). There is **no wheel for Alpine or musl**. There, pip uses the source distribution and
compiles the engine. See
[platform support](/docs/2.0.2/beacondb/python/#platform-support).

## Open a database and query

```python
import beacondb

# A file-backed database (created if missing), or ":memory:" for an ephemeral one.
con = beacondb.connect("beacon.db")

con.sql("SELECT 1 AS a").fetchall()                       # [(1,)]
con.sql("SELECT * FROM read_parquet('obs/*.parquet')").df()
```

`connect()` returns a PEP 249 connection. Read the results as rows with `fetchone`, `fetchmany` or
`fetchall`. Read them as Arrow with `.arrow()`. Read them as a dataframe with `.df()` or `.pl()`.

### In-memory vs file

```python
beacondb.connect(":memory:")     # ephemeral; nothing persisted
beacondb.connect("beacon.db")    # one portable file; managed tables + catalog live here
```

Each `:memory:` connection gets its own database. Beacon holds a file under an exclusive lock. One
process therefore opens one `beacon.db`. A second `connect()` to the same path in the same process
shares that connection.

## DB-API and cursors

```python
cur = con.cursor()
cur.execute("SELECT platform, temperature FROM obs WHERE platform = ?", ["6901234"])
cur.fetchone(); cur.fetchall()
cur.description; cur.rowcount
```

Beacon **binds** the parameters. It never puts them into the string. Use a `?` or `$1` placeholder.
This is safe against injection. `executemany(sql, rows)` runs the statement for each row.

## No users

The embedded database has no users. The process that opens the file has full access. The Beacon
server owns users and roles. See [RBAC](/docs/2.0.2/security/access-control).

## Read-only

```python
con = beacondb.connect("beacon.db", read_only=True)
con.sql("SELECT * FROM t").df()            # ok
con.execute("INSERT INTO t VALUES (1)")    # refused: opened read-only
```

Beacon refuses every write. This covers DDL, DML and statements with a side effect, such as `ATTACH`
and `CREATE SECRET`. `SELECT` and `SHOW …` still work.

## Next

- [Querying](/docs/2.0.2/beacondb/python/querying): the lazy relation, the readers and the file sinks.
- [Bring data in](/docs/2.0.2/beacondb/python/data-in): `register()` and `append()`.
- The full [SQL reference](/docs/2.0.2/beacondb/sql/) applies without a change.
