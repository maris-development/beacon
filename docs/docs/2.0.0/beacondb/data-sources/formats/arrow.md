---
description: Read Arrow IPC stream files (.arrow, .feather) natively with zero-copy column access.
---

# Arrow IPC

## Reading

```text
read_arrow(glob_paths)
```

Reads Arrow IPC stream files (`.arrow`, `.feather`).

```sql
SELECT * FROM read_arrow('streams/*.arrow')
```

## Inspecting the schema

Before writing a query it is usually worth checking which columns a file actually has, and
what their types are.

[`read_schema()`](/docs/2.0.0/beacondb/sql/table-functions-utility#read_schema) returns the
inferred column names and types **without reading any data**, which makes it the cheapest
option on large collections:

```sql
SELECT * FROM read_schema('cruises/*.arrow', 'arrow');
```

Pass a list to see the combined schema across several locations, which is how you spot files
that disagree about a column:

```sql
SELECT * FROM read_schema(['cruises/*.arrow', 'other/*.arrow'], 'arrow');
```

To go further than names and types, [`SUMMARIZE`](/docs/2.0.0/beacondb/sql/summarize) profiles every column in one pass, adding
min/max, distinct counts, and the share of nulls:

```sql
SUMMARIZE (SELECT * FROM read_arrow('cruises/*.arrow'));
```

If the files are registered as a table, `DESCRIBE` works directly:

```sql
DESCRIBE cruise_data;
```

From Python, the Arrow schema of any relation is available without collecting rows:

```python
con.sql("SELECT * FROM read_arrow('cruises/*.arrow') LIMIT 0").arrow().schema
```

## Format details

Fully supported. Arrow IPC stream files (`.arrow`, `.feather`) are read natively with zero-copy column access.

## As an external table

```sql
CREATE EXTERNAL TABLE cruise_data
STORED AS ARROW
LOCATION 'cruises/'
```

See [Creating External Tables](/docs/2.0.0/beacondb/data-sources/external-tables) for the full DDL, and [Reading External Files](/docs/2.0.0/beacondb/data-sources/) for the general reading model.
