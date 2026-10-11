---
description: Read Beacon tables with OPeNDAP DAP2 clients such as pydap and ERDDAP. Subset rows and columns with DAP2 constraints.
---

# OPeNDAP (DAP2)

Beacon serves each table as an OPeNDAP DAP2 dataset. A DAP2 client reads the table and sends a
constraint. Beacon returns only the rows and columns that match.

Use it with these clients:

- **pydap**: open the dataset URL and filter the Sequence.
- **ERDDAP**: add the table as an `EDDTableFromDapSequence` dataset.
- **xarray and netCDF tools**: these clients cannot read a DAP2 Sequence. Download the subset
  as `.nc` or `.csv` instead.

## URLs

| URL | Response |
| --- | --- |
| `/dap/{table}.dds?{constraint}` | The structure of the projected columns |
| `/dap/{table}.das` | The attributes |
| `/dap/{table}.dods?{constraint}` | The structure, then the binary rows |
| `/dap/{table}.csv?{constraint}` | The subset as a CSV file |
| `/dap/{table}.nc?{constraint}` | The subset as a flat NetCDF file |
| `/dap/{table}` | Links to the responses |

Each table is one Sequence with the name `s`. ERDDAP uses the same name.

## Constraints

A constraint has a projection and selections. The projection lists the columns. Each selection
starts with `&`:

```
/dap/argo.dods?time,temp,platform&temp>3&platform=~"69.*"
```

- An empty projection, or `s`, selects every column.
- A column name can have the prefix `s.`, for example `s.temp`.
- The operators are `=`, `!=`, `<`, `<=`, `>`, `>=` and `=~`.
- `=~` is a regular expression. It must match the full value.
- A text value has double quotes. A number has none.
- Compare a time column with seconds since 1970-01-01T00:00:00Z.
- `x=NaN` and `x!=NaN` work on float columns.
- Selections combine with AND.

Beacon does not support functions such as `orderBy` or `distinct()`, value lists or index ranges.
These give HTTP 400.

## Types

| Beacon type | DAP2 type |
| --- | --- |
| `TINYINT`, `SMALLINT`, `INT`, `BOOLEAN` | `Int32` |
| `INT UNSIGNED` | `UInt32` |
| `REAL` | `Float32` |
| `BIGINT`, `DOUBLE`, `DECIMAL` | `Float64` |
| `TIMESTAMP`, `DATE` | `Float64`, seconds since 1970-01-01T00:00:00Z |
| `VARCHAR` | `String` |

DAP2 has no 64-bit integers. A `BIGINT` value above 2^53 loses precision.

Beacon leaves out columns of other types, for example lists and structs. The DAS gives their
names in the global attribute `beacon_omitted_columns`.

DAP2 has no null. A null value becomes the fill value of its type: `2147483647` for `Int32`,
`4294967295` for `UInt32`, `NaN` for floats and `""` for text. A real value equal to the fill value
looks like a null.

## Column names

DAP2 uses `.` to separate levels. Beacon escapes every character outside `A-Z`, `a-z`, `0-9` and
`_` as `%XX`. For example, the column `temp.units` becomes `temp%2Eunits`.

## Read a table with pydap

```python
from pydap.client import open_url

dataset = open_url("http://localhost:5001/dap/argo", protocol="dap2")
seq = dataset["s"]
for row in seq[seq["temp"] > 3].iterdata():
    print(row)
```

pydap sends the filter to Beacon. Beacon runs it, so only the rows that match come back.

To send credentials, give pydap a `requests` session:

```python
import requests

session = requests.Session()
session.auth = ("alice", "secret")
dataset = open_url("http://localhost:5001/dap/argo", protocol="dap2", session=session)
```

## Read a subset with xarray

```python
import xarray as xr
import requests

url = "http://localhost:5001/dap/argo.nc?time,temp&temp>3"
with open("subset.nc", "wb") as f:
    f.write(requests.get(url).content)
ds = xr.open_dataset("subset.nc")
```

## Add a table to ERDDAP

Add this dataset to the ERDDAP `datasets.xml`. Change the URL and the table name:

```xml
<dataset type="EDDTableFromDapSequence" datasetID="beacon_argo" active="true">
    <sourceUrl>https://beacon.example.com/dap/argo</sourceUrl>
    <reloadEveryNMinutes>1440</reloadEveryNMinutes>
    <outerSequenceName>s</outerSequenceName>
    <sourceCanConstrainStringRegex>=~</sourceCanConstrainStringRegex>
    <skipDapperSpacerRows>false</skipDapperSpacerRows>
    <!-- addAttributes and one dataVariable for each column -->
</dataset>
```

ERDDAP's `GenerateDatasetsXml` tool can make the first draft from the `.dds` and `.das`.

ERDDAP cannot send credentials to the source. The table must allow anonymous read.

## Access control

The DAP2 endpoint uses the same credentials as the HTTP API (Basic or Bearer). Without credentials,
it uses the `anonymous` user when anonymous access is on. A table that the caller cannot read gives
HTTP 404, the same as an unknown table.

## Limits

- Beacon supports DAP2, not DAP4.
- pydap reads DAP2 text as ASCII. A value with non-ASCII characters can fail in pydap.
- An error after the first row closes the connection. The client sees a short response.
