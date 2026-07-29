---
description: Bring Python data into BeaconDB with register() (session or persisted) and append() into managed tables.
---

# Bringing data in

## Register a frame as a table

Register a pandas / pyarrow / polars frame (or any Arrow object, or a BeaconDB relation) as a table
queryable by name:

```python
import pandas as pd
con.register("events", pd.DataFrame({"a": [1, 2, 3]}))
con.sql("SELECT sum(a) FROM events").fetchall()
con.unregister("events")
```

By default the table is **session-only**: held in memory for the process, never written into
`beacon.db`. Pass `persist=True` to write it into the file as a managed table, so it survives a reopen
and travels with the file:

```python
con.register("kept", pd.DataFrame({"x": [1, 2, 3]}), persist=True)
```

`persist=True` is real DDL: it needs write privileges (a super-user, the default with auth off) and
refuses to overwrite an existing table (drop it first). `register()` needs `pyarrow` installed either
way.

## Append to an existing table

To add rows to an **existing** managed table, `con.append(name, frame)` (an `INSERT INTO`):

```python
con.append("obs", pd.DataFrame({"a": [4, 5]}))   # errors if `obs` doesn't exist
```

`append` runs through the normal query path, so it is subject to the usual write gates (auth,
read-only) and errors clearly if the table is missing or the columns are incompatible.

## Managed tables in SQL

The same managed tables are created and mutated with standard SQL (see
[Managed tables](/docs/2.0.0/beacondb/sql/managed-tables)):

```python
con.execute("CREATE TABLE obs AS SELECT * FROM read_parquet('obs/*.parquet')")
con.execute("UPDATE obs SET flag = 1 WHERE depth < 0")
con.execute("DELETE FROM obs WHERE temperature IS NULL")
```
