# Remote Tables (Federation)

```sql
CREATE EXTERNAL TABLE remote_profiles
STORED AS REMOTE
LOCATION 'beacon://other-beacon:50051/ocean_profiles'
```

A **remote table** points at a table living on **another Beacon instance**. Once created, you can `SELECT`, `JOIN`, aggregate, and `DROP` it like any other table — but the data stays on the remote. When you query it, Beacon pushes as much of the work as possible (filters, projected columns, `LIMIT`, and whole joins/aggregates between remote tables) **down to the remote instance**, so only the reduced result set travels over the network.

This is built on Arrow Flight SQL: the remote Beacon already exposes a Flight SQL server, and your local instance acts as a client to it.

:::warning Anonymous access required
Remote tables connect to the remote **anonymously** — no credentials are stored anywhere. The remote Beacon instance must therefore **allow anonymous Flight SQL access** (`BEACON_FLIGHT_SQL_ALLOW_ANONYMOUS=true` on the remote). If anonymous access is disabled there, queries against the remote table fail with an authentication error. Anonymous Flight SQL access is read-only, which is exactly what federation needs.
:::

:::tip External vs managed vs remote
- An [**external table**](./external-tables.md) reads files in Beacon's own storage in place.
- A [**managed table**](../sql/managed-tables.md) is owned by Beacon and mutable with `INSERT` / `UPDATE` / `DELETE`.
- A **remote table** owns no data locally at all — it is a federated pointer at a table on another Beacon, queried on demand.
:::

DDL can be submitted through any of Beacon's SQL surfaces:

- **HTTP** — `POST /api/query` with `{ "sql": "CREATE EXTERNAL TABLE ... STORED AS REMOTE ..." }`
- **Arrow Flight SQL** — any Flight SQL client (DataGrip, ADBC, DBeaver, …)

:::info
Creating a remote table is admin-only DDL. SQL must be enabled (`BEACON_ENABLE_SQL=true`) to run DDL over the HTTP API; Arrow Flight SQL does not require this flag.
:::

## Defining a remote table

```sql
CREATE EXTERNAL TABLE <local_name>
STORED AS REMOTE
LOCATION 'beacon://<host>:<port>/<remote_table>'
OPTIONS ('tls' 'false')
```

### `LOCATION`

The location encodes the remote Flight SQL endpoint and the table name on that instance:

```
beacon://<host>:<port>/<remote_table>
```

- `<host>:<port>` — the remote Beacon's Flight SQL address (its Flight SQL port, **not** the HTTP port).
- `<remote_table>` — the name of the table as it is registered on the remote instance.

### `OPTIONS`

| Option | Required | Description                                                |
| ------ | -------- | ---------------------------------------------------------- |
| `tls`  | No (default `false`) | Set to `'true'` to connect over `https` instead of `http`. |

No credentials are configured: remote tables connect anonymously, so the table definition (`table.json`) never contains secrets. The remote must allow anonymous Flight SQL access — see the note above.

## How pushdown works

When you query a remote table, Beacon's planner federates the largest sub-plan rooted at it and sends it to the remote as SQL. For example:

```sql
SELECT count(*), avg(temperature)
FROM remote_profiles
WHERE depth < 50 AND platform = 'argo';
```

The `WHERE` filter and the aggregate are executed **on the remote**, and only the small aggregated result is returned. You can confirm what gets pushed down with `EXPLAIN`:

```sql
EXPLAIN SELECT count(*) FROM remote_profiles WHERE depth < 50;
```

The plan shows a federated (virtual) scan node in place of a local table scan.

### Joins across the same remote

Tables that live on the **same** remote instance federate together, so a join between two remote tables on that instance is pushed down and executed remotely:

```sql
SELECT p.id, m.station_name
FROM remote_profiles p
JOIN remote_stations m ON p.station_id = m.id;
```

Joins that mix a remote table with a **local** table (or with a table on a *different* remote) are executed locally: Beacon fetches the needed remote rows and joins them on this instance.

## Schema handling

The remote table's schema is fetched from the remote instance **once, when the table is created**, and pinned into the table definition. This means:

- After creation, planning is fast and offline — `SELECT`, schema inspection, and joins do not need a round-trip just to learn the columns.
- On restart, the table reloads from its pinned schema, so a temporarily unreachable remote does **not** block Beacon from starting (only querying the table requires the remote to be up).
- If the remote table's schema changes, drop and recreate the remote table to pick up the new schema.

## Querying and inspecting

A remote table behaves like any other table:

```http
GET /api/tables
GET /api/table-schema?table_name=remote_profiles
```

For the SQL equivalents (`SHOW TABLES`, `DESCRIBE`), see the [`CREATE EXTERNAL TABLE`](../sql/create-table.md#querying-and-inspecting) reference.

## Removing a remote table

Dropping a remote table removes it from the local catalog only — nothing on the remote instance is affected.

```sql
DROP TABLE remote_profiles;
```

## Limitations

- **Custom functions must exist on both sides.** A filter or projection that uses a Beacon UDF (e.g. a geospatial `st_*` function) is only pushed down if the remote instance has the same function. When in doubt, the safe pattern is to keep remote-pushed predicates to standard SQL comparisons; Beacon falls back to local execution where it must.
- **One endpoint per table.** A remote table maps to a single remote Flight SQL endpoint; it does not shard across multiple remotes.
- **Remote connections are anonymous.** A remote table connects to its target Flight SQL endpoint without credentials, so the remote instance must allow anonymous access. The only connection `OPTIONS` key is `tls`.
