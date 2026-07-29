---
description: Read ODV ASCII files with read_odv_ascii(), including zstd-compressed files, from local disk or S3.
---

# ODV ASCII

## Reading

```text
read_odv_ascii(glob_paths)
```

```sql
SELECT * FROM read_odv_ascii('odv/**/*.txt') LIMIT 100;
```

ODV ASCII is also available as the `odv` source in the JSON query API.

## Inspecting the schema

Before writing a query it is usually worth checking which columns a file actually has, and
what their types are.

`read_schema()` does not cover this format, so inspect it through the reader itself.
A `LIMIT 0` query resolves the schema without returning any rows:

```sql
SELECT * FROM read_odv_ascii('odv/**/*.txt') LIMIT 0;
```

To go further than names and types, [`SUMMARIZE`](/docs/2.0.0-rc1/beacondb/sql/summarize) profiles every column in one pass, adding
min/max, distinct counts, and the share of nulls:

```sql
SUMMARIZE (SELECT * FROM read_odv_ascii('odv/**/*.txt'));
```

From Python, the Arrow schema of any relation is available without collecting rows:

```python
con.sql("SELECT * FROM read_odv_ascii('odv/**/*.txt') LIMIT 0").arrow().schema
```

## Format details

ODV is one of the two formats (with [Delta Lake](/docs/2.0.0-rc1/beacondb/data-sources/formats/delta-lake)) that is **not**
auto-discovered from the datasets store, and it has no `CREATE EXTERNAL TABLE ... STORED AS ODV`
form. Point `read_odv_ascii()` at the files directly, or wrap it in a
[view](/docs/2.0.0-rc1/data-lake/view) if you want a stable name:

```sql
CREATE VIEW odv_profiles AS
SELECT * FROM read_odv_ascii('odv/**/*.txt');
```

Storing ODV ASCII files with zstd compression is recommended to reduce storage and I/O:

```bash
zstd -9 < input.txt > output.txt.zst
```

Beacon detects compression automatically and decompresses on the fly. zstd-compressed ODV files work
with S3-backed storage.

See [Reading External Files](/docs/2.0.0-rc1/beacondb/data-sources/) for the general reading model.
