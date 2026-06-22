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

A **SQL database table** points at a table living in an external **PostgreSQL** or **MySQL** database. Once created, you can `SELECT`, `JOIN`, aggregate, and `DROP` it like any other table — but the data stays in the source database. When you query it, Beacon pushes as much work as possible (filters, projected columns, `LIMIT`, and aggregates) **down to the database**, so only the reduced result set travels over the wire.

This is built on [`datafusion-table-providers`](https://github.com/datafusion-contrib/datafusion-table-providers) and DataFusion's federation layer — the same pushdown mechanism used by [remote tables](./remote-tables.md).

:::tip External vs managed vs remote vs database
- An [**external table**](./external-tables.md) reads files in Beacon's own storage in place.
- A [**managed table**](../sql/managed-tables.md) is owned by Beacon and mutable with `INSERT` / `UPDATE` / `DELETE`.
- A [**remote table**](./remote-tables.md) is a federated pointer at a table on another Beacon instance.
- A **SQL database table** is a federated pointer at a table in an external PostgreSQL/MySQL database, queried on demand. It is read-only.
:::

DDL can be submitted through any of Beacon's SQL surfaces:

- **HTTP** — `POST /api/query` with `{ "sql": "CREATE EXTERNAL TABLE ... STORED AS POSTGRES ..." }`
- **Arrow Flight SQL** — any Flight SQL client (DataGrip, ADBC, DBeaver, …)

:::info
Creating a SQL database table is admin-only DDL. SQL must be enabled (`BEACON_ENABLE_SQL=true`) to run DDL over the HTTP API; Arrow Flight SQL does not require this flag.
:::

## Credentials and `BEACON_SECRETS_KEY` {#credentials}

The `password` option is **encrypted at rest**. Beacon encrypts it with a deployment master key before it is ever written to the table definition (`table.json`), and only decrypts it in memory when the table is queried.

You must therefore configure a master key:

```bash
# Generate once: 32 random bytes, base64-encoded.
export BEACON_SECRETS_KEY="$(openssl rand -base64 32)"
```

| Behavior | Without `BEACON_SECRETS_KEY` | With `BEACON_SECRETS_KEY` |
| -------- | ---------------------------- | ------------------------- |
| `CREATE` with a `password` | **Rejected** (fails closed) | Password encrypted and persisted |
| `CREATE` without a `password` | Allowed | Allowed |

:::warning Key management
- The persisted credential can only be decrypted with the **same** key. If you lose or rotate `BEACON_SECRETS_KEY`, existing SQL database tables can no longer be queried — drop and recreate them with the new key.
- The password is **never** returned by the API. `GET /api/admin/table-config` (admin basic-auth required) shows the `secret` field as `***`, and the encrypted material never appears in logs.
:::

## Defining a SQL database table

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

The name of the table in the source database. It may be schema-qualified:

```
public.orders      -- schema.table
orders             -- bare table name
```

### `OPTIONS`

| Option | Required | Description |
| ------ | -------- | ----------- |
| `host` | Yes\* | Database host. |
| `port` | No | TCP port (Postgres default `5432`, MySQL default `3306`). |
| `user` | Yes\* | Username. |
| `password` | No | Password. Encrypted at rest; requires `BEACON_SECRETS_KEY`. |
| `database` | Yes\* | Database/schema name to connect to. |
| `sslmode` | No | TLS mode, e.g. `require` / `disable` (Postgres) or `required` / `disabled` (MySQL). |
| `connection_string` | No | Full driver connection string, as an alternative to the discrete options above. |

\* Required unless supplied via `connection_string`.

## How pushdown works

When you query a SQL database table, Beacon federates the largest sub-plan rooted at it and sends it to the database as SQL. For example:

```sql
SELECT count(*), avg(total)
FROM orders
WHERE status = 'shipped';
```

The `WHERE` filter and the aggregate run **on the database**, and only the small aggregated result returns. Confirm what is pushed down with `EXPLAIN`:

```sql
EXPLAIN SELECT count(*) FROM orders WHERE status = 'shipped';
```

The plan shows a federated (virtual) scan node in place of a local table scan. Joins between tables on the **same** database connection are pushed down together; joins that mix a database table with a local table (or a different connection) are executed locally.

## Schema handling

The table's schema is resolved from the database when the table is created and pinned into the definition. On restart the table reloads from its definition. If the source table's schema changes, drop and recreate the Beacon table to pick it up.

## Querying and inspecting

A SQL database table behaves like any other table:

```http
GET /api/tables
GET /api/table-schema?table_name=orders
GET /api/admin/table-config?table_name=orders   # admin basic-auth; the `secret` field is shown as ***
```

## Removing a SQL database table

Dropping the table removes it from the local catalog only — nothing in the source database is affected.

```sql
DROP TABLE orders;
```

## Limitations

- **Read-only.** `INSERT` / `UPDATE` / `DELETE` against a SQL database table are not supported.
- **One connection per table.** Each table maps to a single database connection; it does not shard across hosts.
- **Custom functions are not pushed down.** Predicates using Beacon UDFs (e.g. geospatial `st_*`) are evaluated locally; standard SQL comparisons push down.
- **Credentials require a master key.** Persisting a password requires `BEACON_SECRETS_KEY` (see [above](#credentials)).
