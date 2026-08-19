---
description: Bring Python data into BeaconDB. Use register() for a session table or a stored table. Use append() to add rows to a managed table.
---

# Bring data in

## Register a frame as a table

Register a pandas, pyarrow or polars frame as a table. You can then query it by name. Any Arrow
object and any BeaconDB relation also work:

```python
import pandas as pd
con.register("events", pd.DataFrame({"a": [1, 2, 3]}))
con.sql("SELECT sum(a) FROM events").fetchall()
con.unregister("events")
```

By default the table lives in the **session only**. Beacon holds it in memory for the process. It
writes nothing into `beacon.db`. Pass `persist=True` to write the table into the file as a managed
table. The table then survives a reopen. It also goes with the file:

```python
con.register("kept", pd.DataFrame({"x": [1, 2, 3]}), persist=True)
```

`persist=True` runs real DDL. It needs write privileges. A super-user has them, and that is the
default with auth off. It does not overwrite an existing table. Drop the table first. `register()`
always needs `pyarrow`.

## Append to an existing table

Use `con.append(name, frame)` to add rows to an **existing** managed table. It runs an
`INSERT INTO`:

```python
con.append("obs", pd.DataFrame({"a": [4, 5]}))   # errors if `obs` doesn't exist
```

`append` uses the normal query path. The usual write rules therefore apply, such as auth and
read-only. It gives a clear error if the table does not exist. It also gives a clear error if the
columns do not match.

## Managed tables in SQL

You can create and change the same managed tables with standard SQL. See
[Managed tables](/docs/2.0.0-rc3/beacondb/sql/managed-tables):

```python
con.execute("CREATE TABLE obs AS SELECT * FROM read_parquet('obs/*.parquet')")
con.execute("UPDATE obs SET flag = 1 WHERE depth < 0")
con.execute("DELETE FROM obs WHERE temperature IS NULL")
```
