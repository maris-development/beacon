# Remote Tables

A **remote table** points at a table on **another Beacon server**. Beacon queries it over Arrow
Flight SQL. After you create it, it behaves like any other table in your SQL. You can run `SELECT`,
`JOIN` and aggregates on it. The data stays on the remote server. Beacon pushes as much work as
possible down to it. Only the reduced result travels over the network.

This chapter covers queries over a remote table. The
[Remote Tables (Federation) setup chapter](/docs/2.0.0-rc2/data-sources/remote-tables)
covers the DDL with `LOCATION`, `OPTIONS` and TLS. It also covers the schema handling and the full
list of limitations.

## Query a remote table

This example uses a registered remote table, `remote_profiles`. Query it like a local table:

```sql
SELECT count(*), avg(temperature)
FROM remote_profiles
WHERE depth < 50 AND platform = 'argo';
```

The `WHERE` filter and the aggregate run **on the remote server**. Only the small aggregate result
comes back over the network.

## How pushdown works

The planner federates the largest sub-plan under a remote table. It sends that sub-plan to the remote
server as SQL. Filters, projected columns, `LIMIT` and whole aggregates push down. Use `EXPLAIN` to
check what Beacon pushes down:

```sql
EXPLAIN SELECT count(*) FROM remote_profiles WHERE depth < 50;
```

The plan shows a federated scan node in place of a local table scan. Everything under that node runs
on the remote server.

## Joins across the same remote

Tables on the **same** remote server federate together. Beacon pushes a join between two such tables
down. The join runs on the remote server:

```sql
SELECT p.id, m.station_name
FROM remote_profiles p
JOIN remote_stations m ON p.station_id = m.id;
```

A join between a remote table and a **local** table runs locally. The same holds for a join across
two *different* remote servers. Beacon fetches the remote rows and joins them on this server.

:::tip
Use standard SQL comparisons in the predicates that must push down. Beacon pushes a filter or
projection with a UDF down only if the remote server has the same function. A geospatial `st_*`
function is an example. If not, Beacon runs it locally. See the
[limitations in the setup chapter](/docs/2.0.0-rc2/data-sources/remote-tables#limitations).
:::

## Attach a whole remote catalog (`ATTACH`)

**`ATTACH`** mirrors the *whole* catalog of a remote Beacon under a local name. You register no
remote table one at a time. You can then query every remote schema and table as
`name.schema.table`. The same federation applies. Filters, aggregates and joins between tables on
that remote server push down.

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
  [`TYPE BEACON` secret](/docs/2.0.0-rc2/sql/secrets). Omit all three for anonymous access.
  The remote applies its own [RBAC](/docs/2.0.0-rc2/security/access-control) to your identity.
- Beacon contacts the remote at `ATTACH` time. It lists the schemas and tables as a snapshot. It
  resolves the schema of each table at the first use.

`ATTACH` mirrors a whole remote catalog instead of one table. See
[ATTACH](/docs/2.0.0-rc2/data-sources/attach).
