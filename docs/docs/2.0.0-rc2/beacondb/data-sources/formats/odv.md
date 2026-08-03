---
description: Read ODV ASCII files with read_odv_ascii(), on local disk or on S3. Beacon also reads zstd-compressed files.
---

# ODV ASCII

## Read the files

```text
read_odv_ascii(glob_paths)
```

```sql
SELECT * FROM read_odv_ascii('odv/**/*.txt') LIMIT 100;
```

The JSON query API also gives ODV ASCII as the `odv` source.

## Inspect the schema

Check the columns of a file before you write a query. Also check their types.

`read_schema()` does not cover this format. Inspect it through the reader. A `LIMIT 0` query
returns the schema and no rows:

```sql
SELECT * FROM read_odv_ascii('odv/**/*.txt') LIMIT 0;
```

[`SUMMARIZE`](/docs/2.0.0-rc2/beacondb/sql/summarize) gives more than names and types. It profiles every column in
one pass. It adds the minimum, the maximum, the distinct count and the share of nulls:

```sql
SUMMARIZE (SELECT * FROM read_odv_ascii('odv/**/*.txt'));
```

From Python, read the Arrow schema of a relation. Beacon collects no rows:

```python
con.sql("SELECT * FROM read_odv_ascii('odv/**/*.txt') LIMIT 0").arrow().schema
```

## Format details

Beacon does **not** find ODV in the dataset store automatically.
[Delta Lake](/docs/2.0.0-rc2/beacondb/data-sources/formats/delta-lake) is the other such format. ODV
also has no `CREATE EXTERNAL TABLE ... STORED AS ODV` form. Point `read_odv_ascii()` at the files.
Wrap the call in a [view](/docs/2.0.0-rc2/data-lake/view) to get a stable name:

```sql
CREATE VIEW odv_profiles AS
SELECT * FROM read_odv_ascii('odv/**/*.txt');
```

Compress your ODV ASCII files with zstd. This reduces the storage and the I/O:

```bash
zstd -9 < input.txt > output.txt.zst
```

Beacon detects the compression automatically. It decompresses the file during the read. A
zstd-compressed ODV file also works on object storage.

See [Data Sources](/docs/2.0.0-rc2/beacondb/data-sources/) for the full read model.
