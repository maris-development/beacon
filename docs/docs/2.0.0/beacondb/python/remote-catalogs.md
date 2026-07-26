---
description: Attach a running beacon-datalake as a catalog in beacondb and query it as name.schema.table, with Flight SQL pushdown.
---

# Remote catalogs

Point beacondb at a running **beacon-datalake** and mirror its whole catalog under a local name —
every remote schema and table becomes queryable as `name.schema.table`, DuckDB-`ATTACH` style:

```python
con.attach("lake", "beacon://datalake.example.org:50051",
           username="analyst", password=…, tls=True)   # or token=… , or nothing for anonymous

con.sql("SELECT platform, avg(temperature) AS t "
        "FROM lake.public.obs WHERE depth < 100 GROUP BY platform").df()

# join LOCAL data against a REMOTE table in one statement
con.sql("SELECT l.*, r.temp FROM local_tbl l JOIN lake.public.argo r ON l.id = r.id").df()

con.attached()        # ['lake']
con.detach("lake")    # True
```

## How it works

It runs over **Arrow Flight SQL**, and the DataFusion federation optimizer pushes the largest
federatable sub-plan — filters, projections, aggregates, and joins *between* remote tables — down to
the remote, which executes it on its full engine and streams back only the reduced result. So the
heavy scan stays on the datalake; your laptop gets the answer.

`attach` contacts the remote immediately to enumerate its schemas and tables, so an unreachable or
unauthorized endpoint fails there, not on first query. The listing is a snapshot — re-attach to pick
up tables created on the remote afterward; each table's schema resolves lazily on first use.

## Credentials

Authenticate with a `username`/`password` pair (HTTP Basic) or a bearer `token` (not both); omit both
only if the remote allows anonymous access. The remote enforces its own [RBAC](/docs/2.0.0/security/access-control)
against whoever you authenticate as — unlike local file access, this *is* a governed boundary.

You can also store the credentials as a [secret](/docs/2.0.0/beacondb/python/secrets) and reference it:

```python
con.execute("CREATE SECRET lake (TYPE BEACON, USERNAME 'analyst', PASSWORD '…')")
con.attach("lake", "beacon://datalake:50051", secret="lake")
```

## Also SQL

The same works as SQL — so it reaches any entry point — and `con.attached()` reflects either path:

```python
con.execute("ATTACH 'beacon://datalake:50051' AS lake "
            "WITH ('username' 'analyst', 'password' '…', 'tls' 'true')")
con.execute("DETACH lake")
```

See the shared [`ATTACH` reference](/docs/2.0.0/beacondb/sql/remote-tables).
