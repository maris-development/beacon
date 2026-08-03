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

Check the columns of a file before you write a query. Also check their types.

[`read_schema()`](/docs/2.0.0-rc2/beacondb/sql/table-functions-utility#read-schema) returns the column names and types **without a read of any data**. It is
therefore the cheapest option on a large collection:

```sql
SELECT * FROM read_schema('cruises/*.arrow', 'arrow');
```

Pass a list to get the combined schema of several locations. This shows the files that disagree
about a column:

```sql
SELECT * FROM read_schema(['cruises/*.arrow', 'other/*.arrow'], 'arrow');
```

[`SUMMARIZE`](/docs/2.0.0-rc2/beacondb/sql/summarize) gives more than names and types. It profiles every column in
one pass. It adds the minimum, the maximum, the distinct count and the share of nulls:

```sql
SUMMARIZE (SELECT * FROM read_arrow('cruises/*.arrow'));
```

If the files have a table name, use `DESCRIBE`:

```sql
DESCRIBE cruise_data;
```

From Python, read the Arrow schema of a relation. Beacon collects no rows:

```python
con.sql("SELECT * FROM read_arrow('cruises/*.arrow') LIMIT 0").arrow().schema
```

## Format details

Beacon fully supports this format. It reads an Arrow IPC stream file (`.arrow`, `.feather`) directly
with zero-copy column access.

## As an external table

```sql
CREATE EXTERNAL TABLE cruise_data
STORED AS ARROW
LOCATION 'cruises/'
```

See [Create External Tables](/docs/2.0.0-rc2/beacondb/data-sources/external-tables) for the full DDL. See [Data Sources](/docs/2.0.0-rc2/beacondb/data-sources/) for the
full read model.
