---
description: COMMENT ON TABLE and COMMENT ON COLUMN add metadata to a table or a column, such as a description, a unit or a source. Beacon shows the comments in the schema of the table.
---

# COMMENT ON

Use a comment to add metadata to a table or to a column. A comment is free text. It can give a
description, a unit, a source, a license or a contact. A comment changes neither the data nor the
schema. Beacon stores the comment with the table, so it survives a restart.

```sql
COMMENT ON TABLE ctd IS 'CTD casts in the North Sea, 2024. Source: RV Pelagia. License: CC-BY 4.0';
COMMENT ON COLUMN ctd.depth IS 'Depth below the sea surface. Unit: m';
COMMENT ON COLUMN ctd.temp IS 'Sea water temperature (ITS-90). Unit: degC';
```

The users of the table then read the comments in the [table schema](#read-the-comments). They see
what each column holds without access to the source files.

## Syntax

```sql
COMMENT [IF EXISTS] ON TABLE <table_name> IS '<text>' | NULL
COMMENT [IF EXISTS] ON COLUMN <table_name>.<column_name> IS '<text>' | NULL
```

- Each table and each column has one comment. A new comment replaces the old comment.
- `IS NULL` deletes the comment. An empty text `''` also deletes it.
- `IF EXISTS` gives no error when the table does not exist.
- `COMMENT ON COLUMN` checks that the column exists. An unknown column gives an error.

Only the super-user can set a comment. Send the statement over any SQL interface, for example
`POST /api/query` or Arrow Flight SQL.

## Which tables take a comment

| Table | Comment |
|---|---|
| External table | Yes |
| Managed table | Yes |
| View and materialized view | Yes. Use `COMMENT ON TABLE` for the view. |
| `information_schema`, `beacon.system` and an attached catalog | No |

Only tables in the default schema `beacon.public` take comments.

## Write useful comments

A comment has no fixed format. Use the same form in all your tables, so that a person or a script
can find each fact:

- Start with a short description of the content.
- Give the unit of each numeric column, for example `Unit: degC`.
- Give the source of the data and the license on the table comment.
- Name the code list or the vocabulary of a coded column, for example
  `Quality flag (SeaDataNet L20): 1 good, 4 bad`.
- Write each fact one time. Do not repeat the column name or the data type: the schema gives them.

A view can carry its own comments. Use it to describe a subset of a table:

```sql
CREATE VIEW ctd_surface AS SELECT * FROM ctd WHERE depth <= 10;
COMMENT ON TABLE ctd_surface IS 'Surface layer of the CTD casts: depth 10 m or less';
```

## Read the comments

Beacon puts each comment into the Arrow schema of the table, under the metadata key `comment`:

- The table comment goes into the schema metadata.
- A column comment goes into the field metadata of that column.

A Beacon comment replaces a `comment` key that the file format already gives.

### Over HTTP

[`GET /api/table-schema`](/docs/2.0.1/api/exploring-data#table-schema) returns the schema with the
comments. Each user who can read the table can read its comments:

```http
GET /api/table-schema?table_name=ctd
```

The response, shortened:

```json
{
  "fields": [
    {
      "name": "depth",
      "data_type": "Float64",
      "nullable": true,
      "metadata": { "comment": "Depth below the sea surface. Unit: m" }
    },
    {
      "name": "temp",
      "data_type": "Float64",
      "nullable": true,
      "metadata": { "comment": "Sea water temperature (ITS-90). Unit: degC" }
    }
  ],
  "metadata": {
    "comment": "CTD casts in the North Sea, 2024. Source: RV Pelagia. License: CC-BY 4.0"
  }
}
```

[`GET /api/tables-with-schema`](/docs/2.0.1/api/exploring-data#all-tables-with-schemas) gives the
same metadata for each table.

### Over Arrow Flight SQL

The Flight SQL schema of a table holds the comments. With
[ADBC](/docs/2.0.1/connect/python-adbc) in Python:

```python
import adbc_driver_flightsql.dbapi as flight_sql

with flight_sql.connect(
    "grpc://localhost:32011",
    db_kwargs={"username": "admin", "password": "securepassword"},
) as conn:
    schema = conn.adbc_get_table_schema("ctd")
    print(schema.metadata[b"comment"])
    for field in schema:
        print(field.name, (field.metadata or {}).get(b"comment"))
```

### With SQL

The super-user can list every comment:

```sql
SELECT table_name, column_name, comment FROM beacon.system.comments;
```

`column_name` is `NULL` for a table comment. This table also shows the comment of a column that no
longer exists.

### Where the comments do not show

- **A query result** has no comments. A `SELECT` gives the data only, also in the Arrow and Parquet
  output formats. Read the comments from the table schema.
- **`DESCRIBE <table>`** shows the column names and types only.
- **`SHOW CREATE TABLE`** shows the statement that created the table, without the comments.
  Save your `COMMENT ON` statements with your other DDL.

## Names keep their case

Beacon keeps the case of every identifier. `COMMENT ON COLUMN ctd.Depth` names the column `Depth`,
not `depth`. See [Identifiers & Case](/docs/2.0.1/sql/identifiers).

`IS NULL` does not check that the column exists. You can thus delete the comment of a column that
no longer exists.

## Comments follow the schema

| Statement | Effect on the comments |
|---|---|
| `ALTER TABLE ... RENAME COLUMN a TO b` | The comment of `a` moves to `b`. |
| `ALTER TABLE ... DROP COLUMN a` | Beacon deletes the comment of `a`. |
| `DROP TABLE` | Beacon deletes all comments of the table. |
| `REFRESH` of a materialized view | The comments stay. |

A file of an external table can add or delete a column. Beacon then ignores the comment of the
deleted column. The comment shows again when the column comes back.

## Admin API

The admin API reads and writes all comments of one table as one JSON document. Use it to copy the
comments to another table, or to keep them in a file under version control:

| Request | Effect |
|---|---|
| `GET /api/admin/table-comments/{table}` | Returns the comments. |
| `PUT /api/admin/table-comments/{table}` | Replaces all comments. Send `{}` to delete them. |
| `DELETE /api/admin/table-comments/{table}` | Deletes all comments. |

```json
{
  "table": "CTD casts in the North Sea, 2024. Source: RV Pelagia. License: CC-BY 4.0",
  "columns": {
    "depth": "Depth below the sea surface. Unit: m",
    "temp": "Sea water temperature (ITS-90). Unit: degC"
  }
}
```

`PUT` checks each column before it writes. An unknown column gives `400 Bad Request` and changes
nothing.

## Replace presets with views

Beacon no longer has table extensions or presets. Use a view with a comment for a named filter set.
See the example in [Write useful comments](#write-useful-comments).
