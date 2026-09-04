---
description: ATTACH mirrors a whole remote Beacon catalog under a local name. Query every remote table as name.schema.table, with filters and joins pushed down.
---

# ATTACH

`ATTACH` mirrors the **whole** catalog of a remote Beacon under a local name. You can then query
every remote schema and table as `name.schema.table`. You register no table one at a time. `ATTACH`
is the catalog version of a single
[remote table](/docs/2.0.0-rc5/data-sources/remote-tables). Both use Arrow Flight SQL
federation. Both push down filters, aggregates and joins between tables on that remote.

Use `ATTACH` to query many tables from another Beacon, or to explore its catalog. Use a
[remote table](/docs/2.0.0-rc5/data-sources/remote-tables) when you need one table under a
fixed local name.

## Attach a catalog

```sql
ATTACH 'beacon://datalake:50051' AS lake
  WITH ('username' 'analyst', 'password' '…', 'tls' 'true');

SELECT platform, avg(temperature) AS t
FROM lake.public.obs
WHERE depth < 100
GROUP BY platform;

DETACH lake;
```

- **URL**: `beacon://host:port`, `grpc://…`, `http(s)://…` or a bare `host:port`. Beacon uses TLS
  with `'tls' 'true'` or with an `https://` URL.
- **Credentials**: give exactly one of three options. Use `'username'` and `'password'` for HTTP
  Basic. Use `'token'` for a bearer token. Use `'secret'` with the name of a
  [`TYPE BEACON` secret](/docs/2.0.0-rc5/sql/secrets). Omit all three for anonymous access.
  The remote applies its own [RBAC](/docs/2.0.0-rc5/security/access-control) to your identity.
- Beacon contacts the remote at `ATTACH` time. It lists the schemas and tables as a snapshot. It
  resolves the schema of each table at the first use.

## How queries federate

A query over attached tables pushes down like a remote table. Beacon runs the filters, the projected
columns, the `LIMIT` and whole aggregates on the remote. It also runs a join between two tables of
the **same** attached catalog on the remote. Only the reduced result travels over the network. Use
`EXPLAIN` to check what Beacon pushes down. See
[Remote Tables](/docs/2.0.0-rc5/data-sources/remote-tables#how-pushdown-works) for the
details and the limitations. They apply here in the same way.

## Detach a catalog

```sql
DETACH lake;
```

`DETACH` removes the mirrored catalog from the local session. It changes nothing on the remote
server.

## From a client

`ATTACH` runs on the server, so any client can issue it. Send it as an ordinary statement from the
[Python client](/docs/2.0.0-rc5/connect/python), the [CLI](/docs/2.0.0-rc5/connect/cli) or the HTTP
API. See [Remote Tables](/docs/2.0.0-rc5/sql/remote-tables) for the table-level equivalent.
