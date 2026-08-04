---
description: Read Arrow IPC stream files (.arrow, .feather). Beacon reads them directly with zero-copy column access.
---

# Arrow IPC

## Read the files

```text
read_arrow(glob_paths)
```

Beacon reads Arrow IPC stream files (`.arrow`, `.feather`).

```sql
SELECT * FROM read_arrow('streams/*.arrow')
```

## Inspect the schema

Check the columns and the types before you write a query:

```sql
SELECT * FROM read_arrow('cruises/*.arrow') LIMIT 0;
```

[Inspect a schema](/docs/2.0.0-rc2/formats/inspect-a-schema) compares the `_schema` functions,
`SUMMARIZE`, `DESCRIBE` and `LIMIT 0`, and says what each one costs.

## Format details

Beacon fully supports this format. It reads an Arrow IPC stream file (`.arrow`, `.feather`) directly
with zero-copy column access.

## As an external table

```sql
CREATE EXTERNAL TABLE cruise_data
STORED AS ARROW
LOCATION 'cruises/'
```

See [Create External Tables](/docs/2.0.0-rc2/data-sources/external-tables) for the full DDL. See [Data Sources](/docs/2.0.0-rc2/data-sources/) for the
full read model.
