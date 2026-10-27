---
description: Get Croissant metadata for Beacon tables. Load a table with mlcroissant, or let a search engine find it.
---

# Croissant

[Croissant](https://mlcommons.org/croissant/) is the MLCommons metadata format for machine learning
datasets. A Croissant document is schema.org JSON-LD. It gives the files of a dataset and the
columns of each record.

Beacon serves a Croissant 1.1 document for each table. Use it with these clients:

- **mlcroissant**: load the rows of a table in Python.
- **Search engines**: the home page contains the catalog as JSON-LD, so a crawler can find the
  tables.
- **MCP clients**: the `list_tables` and `describe_table` tools give the `croissant` link of each
  table.

## URLs

| URL | Response |
| --- | --- |
| `/api/croissant` | A schema.org `DataCatalog` of the tables that the caller can read |
| `/api/croissant/{table}` | The Croissant document of the table, as `application/ld+json` |
| `/api/croissant/{table}/data.parquet` | The columns of the document as one Parquet file |

The catalog has one `Dataset` entry for each table. The `url` of an entry is the Croissant document
of the table.

The Croissant document has one `cr:FileObject` with the name `data.parquet`. Its `contentUrl` is
the Parquet URL. It also has one `cr:RecordSet` with the name of the table. The record set has one
`cr:Field` for each column.

The table comment becomes the `description` of the dataset. A column comment becomes the
`description` of the field. Set them with `COMMENT ON`.

## Types

| Beacon type | Croissant `dataType` |
| --- | --- |
| `BOOLEAN` | `sc:Boolean` |
| `TINYINT`, `SMALLINT`, `INT`, `BIGINT` and the unsigned types | `sc:Integer` |
| `REAL`, `DOUBLE`, `DECIMAL` | `sc:Float` |
| `VARCHAR` | `sc:Text` |
| `DATE` | `sc:Date` |
| `TIMESTAMP` | `sc:DateTime` |
| `TIME` | `sc:Time` |

Beacon leaves out columns of other types, for example lists, structs, maps and binary columns.
These columns are not in the document and not in the Parquet file.

## Load a table with mlcroissant

```bash
pip install mlcroissant
```

```python
import mlcroissant as mlc

ds = mlc.Dataset("https://beacon.example.org/api/croissant/argo")
for record in ds.records(record_set="argo"):
    print(record["argo/temp"])
```

The key of each value is `{table}/{column}`. `mlcroissant` downloads `data.parquet` once, then
reads the rows from it.

To validate a document, use the `mlcroissant` command:

```bash
mlcroissant validate --jsonld https://beacon.example.org/api/croissant/argo
```

The validator gives warnings for `license`, `citeAs`, `datePublished` and `version`. Beacon does
not send these properties yet.

## Access control

The Croissant URLs use the same credentials as the HTTP API (Basic or Bearer). Without credentials,
Beacon uses the `anonymous` user when anonymous access is on. Bad credentials give HTTP 401.

- The catalog lists only the tables that the caller can read.
- A table that the caller cannot read gives HTTP 404, the same as an unknown table.
- `data.parquet` runs the query as the caller. The grants of the caller apply.

`mlcroissant` and search engines send no credentials. When `BEACON_AUTH_ENFORCE` is `false` (the
default), the `anonymous` user reads every table. When it is `true`, give the `anonymous` user read
access to each public table. See [Access control](/docs/2.1.0-rc1/security/access-control).

For example:

```sql
CREATE ROLE public_reader;
GRANT SELECT ON TABLE argo TO ROLE public_reader;
GRANT ROLE public_reader TO USER anonymous;
```

The home page contains the catalog of the `anonymous` user. Beacon keeps it for 60 seconds. When
anonymous access is off, the home page has no catalog.

## Links

The documents contain absolute links. Set `BEACON_PUBLIC_URL` to the public URL of the server, with
the base path:

```bash
BEACON_PUBLIC_URL=https://beacon.example.org
```

When `BEACON_PUBLIC_URL` is not set, Beacon makes the links from the `X-Forwarded-Proto`,
`X-Forwarded-Host` and `Host` request headers and `BEACON_BASE_PATH`. Behind a proxy, set
`BEACON_PUBLIC_URL` or make sure that the proxy sends these headers.

To turn off the Croissant URLs, set `BEACON_CROISSANT_ENABLED=false`. See
[Configuration](/docs/2.1.0-rc1/server/configuration).

## Limits

- The document has no `license`, `creator`, `citeAs` or `datePublished` yet.
- Beacon serves only flat columns. Lists, structs, maps, binary and union columns are left out.
- `data.parquet` is one file with all the rows. A large table gives a large file.
- The document has no `sha256` or `md5` checksum. The data is live, so the document sets
  `isLiveDataset` to `true`.
