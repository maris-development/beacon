# Remote Tables (Federation)

```sql
CREATE EXTERNAL TABLE remote_profiles
STORED AS REMOTE
LOCATION 'beacon://other-beacon:50051/ocean_profiles'
```

A **remote table** points at a table on **another Beacon server**. After you create it, you can run
`SELECT`, `JOIN`, aggregates and `DROP` on it, like any other table. The data stays on the remote
server. Beacon pushes as much work as possible **down to the remote server**. This includes filters,
projected columns, `LIMIT` and whole joins and aggregates between remote tables. Only the reduced
result travels over the network.

Remote tables use Arrow Flight SQL. The remote Beacon runs a Flight SQL server. Your local server
acts as a client.

:::warning Anonymous access required
A remote table connects **anonymously**. Beacon stores no credentials. The remote Beacon server must
therefore **allow anonymous Flight SQL access**. Set `BEACON_FLIGHT_SQL_ALLOW_ANONYMOUS=true` on the
remote server. If anonymous access is off there, a query against the remote table fails with an
authentication error. Anonymous Flight SQL access is read-only. Federation needs no more.
:::

:::tip External vs managed vs remote

- An [**external table**](/docs/2.0.0-rc2/data-sources/external-tables) reads files in the
  storage of Beacon, in place.
- A [**managed table**](/docs/2.0.0-rc2/sql/managed-tables) belongs to Beacon. You can
  change the rows with `INSERT`, `UPDATE` and `DELETE`.
- A **remote table** holds no local data. It is a federated pointer at a table on another Beacon.
  Beacon queries it on demand.
:::

You can send the DDL through any SQL interface of Beacon:

- **HTTP**: `POST /api/query` with `{ "sql": "CREATE EXTERNAL TABLE ... STORED AS REMOTE ..." }`
- **Arrow Flight SQL**: any Flight SQL client, such as DataGrip, ADBC or DBeaver

:::info
Only an admin can create a remote table. DDL over the HTTP API needs the SQL interface. That
interface is on by default (`BEACON_ENABLE_SQL`). Arrow Flight SQL does not need this flag.
:::

## Define a remote table

```sql
CREATE EXTERNAL TABLE <local_name>
STORED AS REMOTE
LOCATION 'beacon://<host>:<port>/<remote_table>'
OPTIONS ('tls' 'false')
```

### `LOCATION`

The location holds the remote Flight SQL endpoint and the table name on that server:

```
beacon://<host>:<port>/<remote_table>
```

- `<host>:<port>`: the Flight SQL address of the remote Beacon. Use its Flight SQL port, **not** its
  HTTP port.
- `<remote_table>`: the name of the table on the remote server.

### `OPTIONS`

| Option | Required | Description |
| ------ | -------- | ---------------------------------------------------------- |
| `tls` | No (default `false`) | Set to `'true'` to connect over `https` instead of `http`. |

You configure no credentials. A remote table connects anonymously. The table definition
(`table.json`) therefore never holds a secret. The remote server must allow anonymous Flight SQL
access. See the warning above.

## How pushdown works

The planner federates the largest sub-plan under a remote table. It sends that sub-plan to the
remote server as SQL. For example:

```sql
SELECT count(*), avg(temperature)
FROM remote_profiles
WHERE depth < 50 AND platform = 'argo';
```

The `WHERE` filter and the aggregate run **on the remote server**. Only the small aggregate result
comes back. Use `EXPLAIN` to check what Beacon pushes down:

```sql
EXPLAIN SELECT count(*) FROM remote_profiles WHERE depth < 50;
```

The plan shows a federated scan node in place of a local table scan.

### Joins across the same remote

Tables on the **same** remote server federate together. Beacon pushes a join between two remote
tables down. The join runs on the remote server:

```sql
SELECT p.id, m.station_name
FROM remote_profiles p
JOIN remote_stations m ON p.station_id = m.id;
```

A join between a remote table and a **local** table runs locally. The same holds for a join across
two *different* remote servers. Beacon fetches the remote rows and joins them on this server.

## Schema handling

Beacon reads the schema of a remote table from the remote server **once, at creation time**. It then
pins the schema into the table definition. This has three effects:

- Beacon plans fast and offline after creation. `SELECT`, schema inspection and joins need no
  request to learn the columns.
- After a restart, Beacon loads the table from the pinned schema. An unreachable remote server does
  **not** block the start of Beacon. Only a query on the table needs the remote server.
- If the schema on the remote server changes, drop the remote table and create it again. Beacon then
  reads the new schema.

## Query and inspect

A remote table behaves like any other table:

```http
GET /api/tables
GET /api/table-schema?table_name=remote_profiles
```

The [`CREATE EXTERNAL TABLE`](/docs/2.0.0-rc2/sql/create-table#querying-and-inspecting)
reference gives the SQL equivalents, `SHOW TABLES` and `DESCRIBE`.

## Remove a remote table

`DROP TABLE` removes the table from the local catalog only. It changes nothing on the remote server.

```sql
DROP TABLE remote_profiles;
```

## Limitations

- **A custom function must exist on both sides.** Beacon pushes a filter or projection with a UDF
  down only if the remote server has the same function. A geospatial `st_*` function is an example.
  Use standard SQL comparisons in the predicates that must push down. Beacon runs the rest locally.
- **One endpoint per table.** A remote table maps to one remote Flight SQL endpoint. It does not
  shard across several remote servers.
- **A remote connection is anonymous.** A remote table connects to its Flight SQL endpoint without
  credentials. The remote server must allow anonymous access. The only connection option is `tls`.
