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

Check the columns and the types before you write a query:

```sql
SELECT * FROM read_odv_ascii('odv/**/*.txt') LIMIT 0;
```

[Inspect a schema](/docs/2.0.0-rc5/formats/inspect-a-schema) compares the `_schema` functions,
`SUMMARIZE`, `DESCRIBE` and `LIMIT 0`, and says what each one costs.

## Format details

Beacon does **not** find ODV in the dataset store automatically.
[Delta Lake](/docs/2.0.0-rc5/formats/delta-lake) is the other such format. ODV
also has no `CREATE EXTERNAL TABLE ... STORED AS ODV` form. Point `read_odv_ascii()` at the files.
Wrap the call in a [view](/docs/2.0.0-rc5/server/view) to get a stable name:

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

See [Data Sources](/docs/2.0.0-rc5/data-sources/) for the full read model.
