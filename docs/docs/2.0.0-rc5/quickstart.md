---
description: Deploy a Beacon server in one command, then query it. Or query the public node first, with no setup at all.
---

# Quick Start

Beacon is a server. This page does both halves of that: **[deploy one](#deploy-a-server)**, then
**[query it](#query-a-server)**.

In a hurry? [Query the public node](#query-the-public-node) needs no setup at all. It is the fastest
way to see what a Beacon answers with.

## Deploy a server

One command starts a node and points it at a folder of files:

```bash
docker run -d --name beacon -p 5001:5001 \
  -e BEACON_ADMIN_USERNAME=admin \
  -e BEACON_ADMIN_PASSWORD=securepassword \
  -v ./datasets:/beacon/data/datasets \
  ghcr.io/maris-development/beacon:latest
```

Copy supported files into `./datasets`. Supported files include `.parquet`, `.nc`, `.zarr` and
`.csv`. Beacon finds them automatically. You register nothing first.

Then open the admin UI at [http://localhost:5001/admin](http://localhost:5001/admin) and log in with
the credentials above. It lists the datasets it found and gives you a query editor.

::: info Paths are relative to the datasets root
That mounted `./datasets` directory **is** the root, so a file at `./datasets/obs/a.parquet` is
`obs/a.parquet` in a query. Do not repeat `datasets/` in the path, and do not write an `s3://`
scheme: the server resolves everything against its own store. See
[Object Storage](/docs/2.0.0-rc5/data-sources/object-storage).
:::

That command starts one server. Put it on a workstation to try it. The same image runs in production
behind a real archive. Continue to **[Getting Started](/docs/2.0.0-rc5/getting-started)** for Docker
Compose, an S3 store, access control and performance.

## Query a server

Every node answers plain HTTP, so the lowest-common-denominator client is `curl`:

```bash
curl -X POST http://localhost:5001/api/query \
  -H "Content-Type: application/json" \
  -d '{
    "sql": "SELECT * FROM read_parquet([\"**/*.parquet\"]) LIMIT 10",
    "output": { "format": "csv" }
  }'
```

Set `output.format` to `parquet`, `netcdf` or `ipc` to get the result in that format instead.

From Python, use the client. You then write no HTTP request by hand:

```bash
pip install beacon-api
```

```python
from beacon_api import Client

client = Client("http://localhost:5001", basic_auth=("admin", "securepassword"))

df = client.sql_query(
    'SELECT * FROM read_parquet(["**/*.parquet"]) LIMIT 10'
).to_pandas_dataframe()
```

There are clients for [TypeScript](/docs/2.0.0-rc5/connect/typescript), the
[terminal](/docs/2.0.0-rc5/connect/cli), and any JDBC or ADBC tool over
[Arrow Flight SQL](/docs/2.0.0-rc5/connect/datagrip).

### Query the public node

You do not need your own node to try the SQL. `beacon-wod.maris.nl` is open to everyone, with no
account and no token:

<!-- PUBLIC NODE URL: also in available-nodes/available-nodes.md and below on this page. -->

```python
from beacon_api import Client

client = Client("https://beacon-wod.maris.nl")

df = client.sql_query("""
    SELECT time, latitude, longitude, depth, temperature, salinity
    FROM "easy-wod"
    WHERE temperature > 20
      AND depth < 10
      AND latitude BETWEEN 40 AND 65
      AND longitude BETWEEN -20 AND 20
    LIMIT 10
""").to_pandas_dataframe()

print(df)
```

```text
                     time  latitude  longitude  depth  temperature  salinity
0 1999-09-12 04:27:59.999   42.6919     5.1911    0.0        23.50       NaN
1 1999-09-12 04:27:59.999   42.6919     5.1911    3.0        23.84       NaN
2 1999-09-12 02:52:00.000   42.6961     5.1981    0.0        23.67       NaN
3 1999-09-12 02:52:00.000   42.6961     5.1981    3.0        23.92       NaN
4 1999-09-12 02:52:00.000   42.6961     5.1981    8.0        23.84       NaN
```

That node serves the **World Ocean Database**: 3.3 billion temperature measurements, as files in
object storage. Nothing is downloaded first. Nothing is converted first. The query returns in well
under a second, because the `WHERE` clause prunes whole files before any data is read.

::: info About this node
It accepts anonymous reads and applies a rate limit. Use it to try the SQL, not to run production
work.

The table is `easy-wod`. Quote the name: the hyphen makes it an identifier. Its columns are `time`,
`longitude`, `latitude`, `depth`, `temperature`, `salinity` and `oxygen`.

See [Available nodes](/available-nodes/available-nodes) for the other collections, such as Argo and
CORA.
:::

The same query over HTTP:

```bash
curl -X POST https://beacon-wod.maris.nl/api/query \
  -H "Content-Type: application/json" \
  -d '{"sql": "SELECT * FROM \"easy-wod\" LIMIT 5", "output": {"format": "csv"}}'
```

### Query more than one server

A Beacon server reads other Beacon servers. `ATTACH` a remote node, then join its tables against
your own files in one statement:

```sql
ATTACH 'beacon://wod.example.org:50051' AS wod;

SELECT local_files.station, local_files.temperature, wod."easy-wod".salinity
FROM read_netcdf('**/*.nc') AS local_files
JOIN wod."easy-wod" ON local_files.time = wod."easy-wod".time;
```

`ATTACH` uses Arrow Flight SQL. The remote server must open its Flight SQL port. It must also permit
the connection. See [ATTACH](/docs/2.0.0-rc5/data-sources/attach) for authentication. See
[Remote Tables](/docs/2.0.0-rc5/sql/remote-tables) to attach one table instead of a whole
catalog.

## Next

| | |
|---|---|
| **Run it properly** | [Getting Started](/docs/2.0.0-rc5/getting-started) · [Configuration](/docs/2.0.0-rc5/server/configuration) |
| **Name your data** | [Server Setup](/docs/2.0.0-rc5/server/) |
| **Write queries** | [SQL Reference](/docs/2.0.0-rc5/sql/) |
| **Coming from Python** | [Coming from xarray](/docs/2.0.0-rc5/coming-from-xarray) |
