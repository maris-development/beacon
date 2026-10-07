---
description: COMMENT ON TABLE and COMMENT ON COLUMN store a free-text comment on a table or a column. Beacon shows the comment in the Arrow schema of the table.
---

# COMMENT ON

```sql
COMMENT ON TABLE obs IS 'Argo float profiles: temperature and salinity by location, depth and time.';
COMMENT ON COLUMN obs.depth IS 'Measurement depth in meters';
```

A comment is free text on a table or on a column. It tells a person or an AI agent what the data
holds. A comment changes neither the data nor the schema. Beacon stores the comment with the table.
It survives a restart.

## Syntax

```sql
COMMENT [IF EXISTS] ON TABLE <table_name> IS '<text>' | NULL
COMMENT [IF EXISTS] ON COLUMN <table_name>.<column_name> IS '<text>' | NULL
```

- A new comment replaces the old comment.
- `IS NULL` deletes the comment. An empty text `''` also deletes it.
- `IF EXISTS` gives no error when the table does not exist.
- A view takes `COMMENT ON TABLE` too.

Only the super-user can set a comment. Each table and each column has one comment.

## Names keep their case

Beacon keeps the case of every identifier. `COMMENT ON COLUMN obs.Depth` names the column `Depth`,
not `depth`. See [Identifiers & Case](/docs/2.0.1/sql/identifiers).

`COMMENT ON COLUMN` checks that the column exists. `IS NULL` skips this check, so you can delete the
comment of a column that no longer exists.

## Read the comments

Beacon puts each comment into the Arrow schema of the table, under the metadata key `comment`:

- The table comment goes into the schema metadata.
- A column comment goes into the field metadata of that column.

`GET /api/table-schema` and the Flight SQL schema of a table therefore show the comments. A Beacon
comment replaces a `comment` key that the file format already gives.

The super-user can list every comment with SQL:

```sql
SELECT table_name, column_name, comment FROM beacon.system.comments;
```

`column_name` is `NULL` for a table comment. This table also shows the comment of a column that no
longer exists.

## Comments follow the schema

| Statement | Effect on the comments |
|---|---|
| `ALTER TABLE ... RENAME COLUMN a TO b` | The comment of `a` moves to `b`. |
| `ALTER TABLE ... DROP COLUMN a` | Beacon deletes the comment of `a`. |
| `DROP TABLE` | Beacon deletes all comments of the table. |
| `REFRESH` of a materialized view | The comments stay. |

A file of an external table can add or delete a column. Beacon then ignores the comment of the
deleted column. The comment shows again when the column comes back.

Only tables in the default schema `beacon.public` take comments. Tables of `information_schema`,
`beacon.system` and an attached catalog do not.

## Admin API

The admin API reads and writes all comments of one table as one JSON document:

| Request | Effect |
|---|---|
| `GET /api/admin/table-comments/{table}` | Returns the comments. |
| `PUT /api/admin/table-comments/{table}` | Replaces all comments. Send `{}` to delete them. |
| `DELETE /api/admin/table-comments/{table}` | Deletes all comments. |

```json
{
  "table": "Argo float profiles",
  "columns": { "depth": "Measurement depth in meters" }
}
```

`PUT` checks each column before it writes. An unknown column gives `400 Bad Request` and changes
nothing.

<!-- MCP is unreleased. Restore on release:
## Comments and AI agents

The [MCP Server](/docs/2.0.1/mcp) reads the comments. `list_tables` shows the table comment, and
`describe_table` shows each column comment. Good comments help an agent to write correct SQL.
-->

## Replace presets with views

Beacon no longer has table extensions or presets. Use a view with a comment for a named filter set:

```sql
CREATE VIEW obs_shallow AS SELECT * FROM obs WHERE depth <= 10;
COMMENT ON TABLE obs_shallow IS 'Surface layer only: depth 10 m or less';
```
