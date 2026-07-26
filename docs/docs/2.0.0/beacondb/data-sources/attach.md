---
description: ATTACH mirrors an entire remote Beacon catalog under a local name, so every remote schema and table is queryable as name.schema.table, with filters, aggregates, and joins pushed down.
---

# ATTACH

`ATTACH` mirrors an **entire** remote Beacon's catalog under a local name. Every remote schema and
table becomes queryable as `name.schema.table`, without registering tables one at a time. It is the catalog-level counterpart to a single [remote table](/docs/2.0.0/beacondb/data-sources/remote-tables): same
Arrow Flight SQL federation, same pushdown of filters, aggregates, and joins between tables on that
remote.

Reach for `ATTACH` when you want to query many tables from another Beacon (or explore its catalog);
reach for a [remote table](/docs/2.0.0/beacondb/data-sources/remote-tables) when you only need one table under a fixed local name.

## Attaching a catalog

```sql
ATTACH 'beacon://datalake:50051' AS lake
  WITH ('username' 'analyst', 'password' '…', 'tls' 'true');

SELECT platform, avg(temperature) AS t
FROM lake.public.obs
WHERE depth < 100
GROUP BY platform;

DETACH lake;
```

- **URL**: `beacon://host:port`, `grpc://…`, `http(s)://…`, or a bare `host:port`. `'tls' 'true'` (or
  an `https://` URL) uses TLS.
- **Credentials**: exactly one of `'username'`/`'password'` (HTTP Basic), a bearer `'token'`, or the
  name of a [`TYPE BEACON` secret](/docs/2.0.0/beacondb/sql/secrets) via `'secret'`. Omit them for
  anonymous access. The remote enforces its own [RBAC](/docs/2.0.0/security/access-control) against whoever
  you authenticate as.
- The remote is contacted at `ATTACH` time to enumerate its schemas and tables (a snapshot); each
  table's schema resolves lazily on first use.

## How queries federate

A query against attached tables pushes down the same way remote tables do: filters, projected
columns, `LIMIT`, and whole aggregates run on the remote, and joins between tables on the **same**
attached catalog are executed remotely. Only the reduced result set travels the network. Use
`EXPLAIN` to confirm what is pushed down. See [Remote Tables](/docs/2.0.0/beacondb/data-sources/remote-tables#how-pushdown-works)
for the details and limitations, which apply identically here.

## Detaching

```sql
DETACH lake;
```

`DETACH` removes the mirrored catalog from the local session. Nothing on the remote instance is
affected.

## In BeaconDB (Python)

The same capability is available from the embedded engine as `con.attach(name, url, …)`. See
[Remote catalogs](/docs/2.0.0/beacondb/python/remote-catalogs) for the Python API.
