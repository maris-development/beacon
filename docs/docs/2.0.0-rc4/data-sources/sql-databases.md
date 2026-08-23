# SQL Database Tables (PostgreSQL / MySQL)

```sql
CREATE EXTERNAL TABLE orders
STORED AS POSTGRES
LOCATION 'public.orders'
OPTIONS (
  'host' 'db.internal',
  'port' '5432',
  'user' 'beacon_ro',
  'password' 'secret',
  'database' 'shop',
  'sslmode' 'require'
)
```

A **SQL database table** points at a table in an external **PostgreSQL** or **MySQL** database.
After you create it, you can run `SELECT`, `JOIN`, aggregates and `DROP` on it, like any other
table. The data stays in the source database. Beacon pushes as much work as possible **down to the
database**. This includes filters, projected columns, `LIMIT` and aggregates. Only the reduced
result travels over the network.

This feature uses
[`datafusion-table-providers`](https://github.com/datafusion-contrib/datafusion-table-providers) and
the federation layer of DataFusion.
[Remote tables](/docs/2.0.0-rc4/data-sources/remote-tables) use the same pushdown
mechanism.

:::tip External vs managed vs remote vs database

- An [**external table**](/docs/2.0.0-rc4/data-sources/external-tables) reads files in the
  storage of Beacon, in place.
- A [**managed table**](/docs/2.0.0-rc4/sql/managed-tables) belongs to Beacon. You can
  change the rows with `INSERT`, `UPDATE` and `DELETE`.
- A [**remote table**](/docs/2.0.0-rc4/data-sources/remote-tables) is a federated pointer at
  a table on another Beacon server.
- A **SQL database table** is a federated pointer at a table in an external PostgreSQL or MySQL
  database. Beacon queries it on demand. It is read-only.
:::

You can send the DDL through any SQL interface of Beacon:

- **HTTP**: `POST /api/query` with `{ "sql": "CREATE EXTERNAL TABLE ... STORED AS POSTGRES ..." }`
- **Arrow Flight SQL**: any Flight SQL client, such as DataGrip, ADBC or DBeaver

:::info
Only an admin can create a SQL database table. DDL over the HTTP API needs the SQL interface. That
interface is on by default (`BEACON_ENABLE_SQL`). Arrow Flight SQL does not need this flag.
:::

## Credentials and `BEACON_SECRETS_KEY` {#credentials}

Beacon **encrypts the `password` option at rest**. It encrypts the password with a master key before
it writes the table definition (`table.json`). It decrypts the password in memory only, at query
time.

You must therefore configure a master key:

```bash
# Generate once: 32 random bytes, base64-encoded.
export BEACON_SECRETS_KEY="$(openssl rand -base64 32)"
```

| Behavior | Without `BEACON_SECRETS_KEY` | With `BEACON_SECRETS_KEY` |
| -------- | ---------------------------- | ------------------------- |
| `CREATE` with a `password` | Beacon **rejects** the statement | Beacon encrypts and stores the password |
| `CREATE` without a `password` | Allowed | Allowed |

:::warning Key management

- Beacon decrypts a stored credential with the **same** key only. If you lose or change
  `BEACON_SECRETS_KEY`, you can no longer query the existing SQL database tables. Drop them and
  create them again with the new key.
- Beacon **never** returns the password. It does not serve a stored table definition over HTTP.
  `GET /api/admin/table-config` is deprecated and returns only a notice. The encrypted material
  never appears in the logs.
:::

## Define a SQL database table

```sql
CREATE EXTERNAL TABLE <local_name>
STORED AS POSTGRES        -- or MYSQL
LOCATION '<remote_table>'
OPTIONS ( ... )
```

### `STORED AS`

| Keyword | Engine |
| ------- | ------ |
| `POSTGRES` (or `POSTGRESQL`) | PostgreSQL |
| `MYSQL` | MySQL |

### `LOCATION`

The name of the table in the source database. It can hold the schema name:

```
public.orders      -- schema.table
orders             -- bare table name
```

### `OPTIONS`

| Option | Required | Description |
| ------ | -------- | ----------- |
| `host` | Yes\* | The database host. |
| `port` | No | The TCP port. Postgres uses `5432`, MySQL uses `3306`. |
| `user` | Yes\* | The user name. |
| `password` | No | The password. Beacon encrypts it at rest. It needs `BEACON_SECRETS_KEY`. |
| `database` | Yes\* | The name of the database or schema. |
| `sslmode` | No | The TLS mode. Postgres takes `require` or `disable`. MySQL takes `required` or `disabled`. |
| `connection_string` | No | A full driver connection string. Use it instead of the options above. |

\* Required, unless you give a `connection_string`.

## How pushdown works

The planner federates the largest sub-plan under a SQL database table. It sends that sub-plan to the
database as SQL. For example:

```sql
SELECT count(*), avg(total)
FROM orders
WHERE status = 'shipped';
```

The `WHERE` filter and the aggregate run **on the database**. Only the small aggregate result comes
back. Use `EXPLAIN` to check what Beacon pushes down:

```sql
EXPLAIN SELECT count(*) FROM orders WHERE status = 'shipped';
```

The plan shows a federated scan node in place of a local table scan. Beacon pushes a join between
two tables on the **same** database connection down. A join between a database table and a local
table runs locally. The same holds for a join across two different connections.

## Schema handling

Beacon reads the schema of the table from the database at creation time. It then pins the schema into
the definition. After a restart, Beacon loads the table from that definition. If the schema of the
source table changes, drop the Beacon table and create it again.

## Query and inspect

A SQL database table behaves like any other table:

```http
GET /api/tables
GET /api/table-schema?table_name=orders
# The stored definition, secret included, is never served over HTTP.
```

## Remove a SQL database table

`DROP TABLE` removes the table from the local catalog only. It changes nothing in the source
database.

```sql
DROP TABLE orders;
```

## Limitations

- **Read-only.** Beacon does not support `INSERT`, `UPDATE` or `DELETE` on a SQL database table.
- **One connection per table.** Each table maps to one database connection. It does not shard across
  hosts.
- **Beacon does not push a custom function down.** A predicate with a Beacon UDF, such as a
  geospatial `st_*` function, runs locally. A standard SQL comparison pushes down.
- **A credential needs a master key.** A stored password needs `BEACON_SECRETS_KEY`. See
  [above](#credentials).
