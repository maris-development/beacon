---
description: Attach a Beacon Data Lake as a catalog in BeaconDB. Query it as name.schema.table, with Flight SQL pushdown.
---

# Remote catalogs

Point BeaconDB at a **Beacon Data Lake**. BeaconDB mirrors the whole catalog under a local name. You
can then query every remote schema and table as `name.schema.table`:

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

This feature runs over **Arrow Flight SQL**. The DataFusion federation optimizer pushes the largest
sub-plan down to the remote server. The sub-plan holds filters, projections, aggregates and joins
*between* remote tables. The remote server runs the sub-plan on its full engine. It then streams back
only the reduced result. The large scan therefore stays on the data lake. Your laptop gets the
answer.

`attach` contacts the remote server at once. It lists the schemas and tables. An unreachable or
unauthorized endpoint therefore fails at that moment, not at the first query. The list is a
snapshot. Attach again to get the tables that the remote server creates later. Beacon resolves the
schema of each table at the first use.

## Credentials

Authenticate with a `username` and `password` pair for HTTP Basic, or with a bearer `token`. Do not
give both. Omit both only if the remote server allows anonymous access. The remote server applies its
own [RBAC](/docs/2.0.0-rc2/security/access-control) to your identity. Local file access has no such
boundary. This connection *does* have one.

You can also store the credentials as a [secret](/docs/2.0.0-rc2/beacondb/python/secrets). Then
reference the secret:

```python
con.execute("CREATE SECRET lake (TYPE BEACON, USERNAME 'analyst', PASSWORD '…')")
con.attach("lake", "beacon://datalake:50051", secret="lake")
```

## Also SQL

The same works in SQL. Every entry point therefore supports it. `con.attached()` shows the result of
both paths:

```python
con.execute("ATTACH 'beacon://datalake:50051' AS lake "
            "WITH ('username' 'analyst', 'password' '…', 'tls' 'true')")
con.execute("DETACH lake")
```

See the shared [`ATTACH` reference](/docs/2.0.0-rc2/beacondb/sql/remote-tables).
